"""Gold: per-endpoint freshness, volume, and error-rate metrics for monitoring.

`gold_api_endpoint_health` reads BRONZE, so it works for every silver shape unchanged.
`gold_api_records_per_day` counts the silver grain, so it follows the pipeline's
`silver_shape`: in the default "records" shape it counts `silver_api_records` (one row
per record); in the "document" shape it counts `silver_api_documents` (one row per
response). The MV name + grain (endpoint x ingest_date count) stay the same either way,
so dashboards bind to one table regardless of shape.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SHAPE = spark.conf.get("silver_shape", "records")
SILVER_TABLE = "silver_api_documents" if SILVER_SHAPE == "document" else "silver_api_records"


@dp.materialized_view(
    name="gold_api_endpoint_health",
    comment=(
        "Per-endpoint health: latest fetch time, success/error counts, and average response "
        "size over the last 7 days of ingestion. Powers an AI/BI dashboard widget. "
        "Reads BRONZE on purpose — this is fetch/response health (every attempt, incl. errors "
        "and retries), which the append-only bronze layer records; silver holds only "
        "current-state records and would not show failed or superseded fetches."
    ),
    cluster_by=["endpoint"],
    table_properties={"quality": "gold"},
)
def gold_api_endpoint_health():
    # current_date() is non-deterministic, so this MV fully recomputes each run rather
    # than refreshing incrementally. Accepted: it is a tiny per-endpoint monitoring table.
    # If incremental refresh is ever needed, drop the window here and apply the rolling
    # 7-day filter in the dashboard query instead.
    bronze = spark.read.table("bronze_api_responses").filter(
        F.col("ingest_date") >= F.date_sub(F.current_date(), 7)
    )

    return (
        bronze.groupBy("endpoint")
        .agg(
            F.max("fetched_at").alias("last_fetched_at"),
            F.count("*").alias("total_calls"),
            F.sum(F.when(F.col("status_code").between(200, 299), 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("status_code").between(400, 599), 1).otherwise(0)).alias("error_count"),
            F.sum(F.when(F.col("error_message").isNotNull(), 1).otherwise(0)).alias("network_error_count"),
            F.avg(F.length("response_body")).alias("avg_body_bytes"),
        )
        .withColumn(
            "success_rate",
            F.when(F.col("total_calls") > 0, F.col("success_count") / F.col("total_calls")).otherwise(F.lit(None)),
        )
    )


@dp.materialized_view(
    name="gold_api_records_per_day",
    comment=(
        "Current count by endpoint, bucketed by the date each row was last fetched "
        "(ingest_date). Silver is SCD Type 1 (deduped), so every row is counted exactly "
        "once — the live distribution, not cumulative re-fetch volume. Grain follows "
        "silver_shape: one row per record (records) or per response (document)."
    ),
    cluster_by=["ingest_date"],
    table_properties={"quality": "gold"},
)
def gold_api_records_per_day():
    # Reads the deduped silver for the pipeline's shape, so the deterministic aggregate
    # can refresh incrementally on serverless (silver sets delta.enableRowTracking).
    return (
        spark.read.table(SILVER_TABLE)
        .groupBy("ingest_date", "endpoint")
        .agg(F.count("*").alias("record_count"))
    )
