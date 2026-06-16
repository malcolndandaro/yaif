"""Gold: ingestion health + volume metrics for monitoring a cloud-storage file feed."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_files_ingestion_health",
    comment=(
        "Per-day file-ingestion health over the last 7 days: distinct files seen, rows "
        "ingested, bytes, and source-file freshness. Powers an AI/BI monitoring widget. "
        "Reads BRONZE on purpose — this tracks every file/row ingested (append-only), which "
        "silver would hide once it dedups to current state."
    ),
    cluster_by=["ingest_date"],
    table_properties={"quality": "gold"},
)
def gold_files_ingestion_health():
    # current_date() is non-deterministic, so this MV fully recomputes each run rather
    # than refreshing incrementally. Accepted: it is a tiny per-day monitoring table.
    # If incremental refresh is ever needed, drop the window here and apply the rolling
    # 7-day filter in the dashboard query instead.
    bronze = spark.read.table("bronze_cloud_files").filter(
        F.col("ingest_date") >= F.date_sub(F.current_date(), 7)
    )

    return (
        bronze.groupBy("ingest_date")
        .agg(
            F.countDistinct("source_file").alias("files_ingested"),
            F.count("*").alias("rows_ingested"),
            F.sum("source_file_size").alias("total_bytes"),
            F.max("source_file_modified_at").alias("latest_source_file_at"),
            F.max("_ingested_at").alias("last_ingest_run_at"),
        )
    )


@dp.materialized_view(
    name="gold_files_rows_per_day",
    comment=(
        "Daily clean-record counts from silver — input to ingestion volume trend dashboards. "
        "When dedup_keys is set, silver is SCD Type 1 (current-state), so each business row is "
        "counted once; otherwise every ingested row is counted."
    ),
    cluster_by=["ingest_date"],
    table_properties={"quality": "gold"},
)
def gold_files_rows_per_day():
    # Reads silver (which sets delta.enableRowTracking) so this deterministic aggregate
    # can refresh incrementally on serverless.
    return (
        spark.read.table("silver_cloud_files")
        .groupBy("ingest_date")
        .agg(F.count("*").alias("record_count"))
    )
