"""Silver: current-state Zerobus events via AUTO CDC (SCD Type 1).

Zerobus is **at-least-once**: records can arrive more than once (a producer retry
where the original had already been durably written). If silver simply streamed
bronze through, it would hold N copies of every re-delivered event. Instead the
bronze stream is parsed into a temporary view and fed to an AUTO CDC SCD Type 1
flow keyed on `event_id`, sequenced by `ingested_at` — silver converges to ONE
current row per event id, and re-deliveries upsert in place instead of
duplicating. The gap between bronze's row count and silver's IS the duplicate
traffic; `gold_zerobus_ingestion_health` surfaces it as `duplicate_rate`.

This mirrors the API module (`silver_api_records.py`) and the files module
(`silver_cloud_files.py`), which use the same AUTO CDC SCD1 mechanism for their
dedup paths — the modules are deliberately consistent.

Note: AUTO CDC is the default SCD Type 1 behaviour of `create_auto_cdc_flow` (so
`stored_as_scd_type` is left at its default rather than passed explicitly).
"""

from pyspark import pipelines as dp

from shared.zerobus_events import cast_micros_to_timestamp, project_event_columns

BRONZE_TABLE = spark.conf.get("bronze_table")


@dp.temporary_view()
def silver_zerobus_events_parsed():
    """Type + project the bronze stream into the silver CDC source.

    Bronze is a plain Delta table written by the Zerobus service (not an SDP table),
    read incrementally with skipChangeCommits like every YAIF bronze. The two shared
    transforms are unit-tested in `tests/test_zerobus_events.py`: wire BIGINT micros
    -> TIMESTAMP, then the exact silver column set with `ingest_date` derived from the
    record's own ingested_at.
    """
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(BRONZE_TABLE)
        .transform(lambda d: cast_micros_to_timestamp(d, ["event_time", "ingested_at"]))
        .transform(project_event_columns)
    )


# Empty target for the AUTO CDC flow. `has_event_key` guards the CDC key (SCD1 cannot
# key on NULL); it evaluates against rows being applied, which carry every column the
# parsed view selected.
dp.create_streaming_table(
    name="silver_zerobus_events",
    comment=(
        "Current-state Zerobus events (AUTO CDC, SCD Type 1): one row per event_id, "
        "latest delivery by ingested_at. At-least-once re-deliveries upsert in place."
    ),
    cluster_by=["ingest_date"],
    table_properties={
        "quality": "silver",
        # Row tracking lets the deterministic gold MV refresh incrementally on serverless.
        "delta.enableRowTracking": "true",
    },
    expect_all={"has_event_key": "event_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target="silver_zerobus_events",
    source="silver_zerobus_events_parsed",
    keys=["event_id"],
    sequence_by="ingested_at",  # latest delivery wins; SCD Type 1 (default)
)
