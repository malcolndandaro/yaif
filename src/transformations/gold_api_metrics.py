"""Gold: per-endpoint freshness, volume, and error-rate metrics for monitoring."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_api_endpoint_health",
    comment=(
        "Per-endpoint health: latest fetch time, success/error counts, and average response "
        "size over the last 7 days of ingestion. Powers an AI/BI dashboard widget."
    ),
    cluster_by=["endpoint"],
    table_properties={"quality": "gold"},
)
def gold_api_endpoint_health():
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
    comment="Daily record counts by endpoint — input to ingestion volume trend dashboards.",
    cluster_by=["ingest_date"],
    table_properties={"quality": "gold"},
)
def gold_api_records_per_day():
    return (
        spark.read.table("silver_api_records")
        .groupBy("ingest_date", "endpoint")
        .agg(F.count("*").alias("record_count"))
    )
