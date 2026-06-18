"""Bronze: stream raw API responses from the landing Delta table.

The fetch job writes each batch directly to `landing_table` as a Delta append.
This bronze layer streams from that table — incrementally picking up new rows
per pipeline update — and applies minimal enrichment (ingest timestamp, date).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_TABLE = spark.conf.get("landing_table")


@dp.table(
    name="bronze_api_responses",
    comment="Raw API responses streamed from the landing table. Append-only, minimal transforms.",
    cluster_by=["endpoint", "ingest_date"],  # scalar columns only — VARIANT cannot cluster
    table_properties={
        "delta.enableChangeDataFeed": "true",
        # Row tracking lets serverless MVs reading this table refresh incrementally.
        "delta.enableRowTracking": "true",
        "quality": "bronze",
    },
)
@dp.expect("has_endpoint", "endpoint IS NOT NULL")
# response_body NULL on a failed/non-2xx fetch is expected (warn, don't drop); a present
# but empty body is suspect. Evaluates on the output select, which keeps response_body.
@dp.expect("nonempty_success_body", "response_body IS NULL OR length(response_body) > 0")
def bronze_api_responses():
    return (
        spark.readStream.option("skipChangeCommits", "true").table(LANDING_TABLE)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.col("_ingested_at")))
        # Parsed VARIANT alongside the raw STRING (response_body stays the loss-proof
        # audit copy). try_parse_json -> NULL on bad JSON; parse_json would raise
        # MALFORMED_RECORD_IN_PARSING and fail the whole streaming microbatch. The
        # document-shape silver gates on response_variant IS NOT NULL. VARIANT needs
        # DBR 15.3+ (serverless SDP satisfies). Not added to cluster_by — VARIANT
        # cannot be a clustering / partition / Z-order key.
        .withColumn("response_variant", F.expr("try_parse_json(response_body)"))
    )
