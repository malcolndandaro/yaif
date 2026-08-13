"""Bronze: stream raw API responses from the landing Delta table.

The fetch job writes each batch directly to `landing_table` as a Delta append.
This bronze layer streams from that table — incrementally picking up new rows
per pipeline update — and applies minimal enrichment (ingest timestamp, date).
"""

from pyspark import pipelines as dp

from shared.api_documents import with_response_variant
from shared.ingest_columns import with_ingest_stamps

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
        spark.readStream.option("skipChangeCommits", "true")
        .table(LANDING_TABLE)
        .transform(with_ingest_stamps)
        .transform(with_response_variant)
    )
