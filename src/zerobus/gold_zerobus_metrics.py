"""Gold: streaming-ingestion health and volume metrics for monitoring a Zerobus feed.

`gold_zerobus_ingestion_health` reads BRONZE, so it sees every record the wire
delivered — including the at-least-once re-deliveries silver dedups away. That makes
`duplicate_rate` the module's delivery-semantics health signal: ~0 means the producer
is idempotent or nothing was re-sent; a growing rate means retries/redeliveries are
happening upstream. `gold_zerobus_events_per_day` counts the silver grain (one row per
event id), so it is the live distribution, not cumulative wire volume.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from shared.ingest_columns import recent_ingest_days

BRONZE_TABLE = spark.conf.get("bronze_table")


@dp.materialized_view(
    name="gold_zerobus_ingestion_health",
    comment=(
        "Per-day streaming-ingestion health over the last 7 days: records arrived on the "
        "wire, distinct events, re-delivery (duplicate) rate, and last arrival time. "
        "Reads BRONZE on purpose — this is wire-level arrival health (at-least-once, incl. "
        "re-deliveries), which silver would hide once it dedups to current state."
    ),
    cluster_by=["ingest_date"],
    table_properties={"quality": "gold"},
)
def gold_zerobus_ingestion_health():
    # Bronze stores ingest stamps as BIGINT epoch micros (the Zerobus wire type for
    # TIMESTAMP); derive ingest_date from the record's own ingested_at, then reuse the
    # shared 7-day window. current_date() is non-deterministic, so this MV fully
    # recomputes each run rather than refreshing incrementally — accepted for a tiny
    # per-day monitoring table (same trade-off as the other modules' gold MVs).
    return (
        spark.read.table(BRONZE_TABLE)
        .withColumn("ingest_date", F.to_date(F.timestamp_micros("ingested_at")))
        .transform(recent_ingest_days)
        .groupBy("ingest_date")
        .agg(
            F.count("*").alias("records_arrived"),
            F.countDistinct("event_id").alias("distinct_events"),
            F.max(F.timestamp_micros("ingested_at")).alias("last_arrival_at"),
        )
        .withColumn(
            "duplicate_rate",
            F.when(
                F.col("records_arrived") > 0,
                F.lit(1.0) - (F.col("distinct_events") / F.col("records_arrived")),
            ).otherwise(F.lit(None)),
        )
    )


@dp.materialized_view(
    name="gold_zerobus_events_per_day",
    comment=(
        "Daily event counts from silver by device — input to ingestion volume trend "
        "dashboards. Silver is SCD Type 1 (one row per event_id), so each event is "
        "counted once regardless of wire re-deliveries."
    ),
    cluster_by=["ingest_date"],
    table_properties={"quality": "gold"},
)
def gold_zerobus_events_per_day():
    # Reads the deduped silver (which sets delta.enableRowTracking) so this
    # deterministic aggregate can refresh incrementally on serverless.
    return (
        spark.read.table("silver_zerobus_events")
        .groupBy("ingest_date", "device_name")
        .agg(
            F.count("*").alias("event_count"),
            F.max("event_time").alias("latest_event_at"),
        )
    )
