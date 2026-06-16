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
    cluster_by=["endpoint", "ingest_date"],
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "bronze",
    },
)
@dp.expect("has_endpoint", "endpoint IS NOT NULL")
def bronze_api_responses():
    return (
        spark.readStream.option("skipChangeCommits", "true").table(LANDING_TABLE)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.col("_ingested_at")))
    )
