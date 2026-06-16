"""Silver: cleaned, optionally deduplicated records from the bronze file ingest.

Parquet arrives already typed, so silver here is about quality and idempotency
rather than parsing: surface schema drift (rescued data) and, when a connector
re-exports overlapping windows (full + incremental loads land the same business row
more than once), converge to one current row per business key.

Two shapes, chosen by config at pipeline-planning time:

  * `dedup_keys` set  -> AUTO CDC SCD Type 1 (`dp.create_auto_cdc_flow`) keyed on the
                         business key(s), sequenced by `dedup_order_by` (latest wins).
                         This is the recommended dedup path: state is bounded and
                         correct (a MERGE-style upsert), unlike streaming
                         `dropDuplicates`, whose keyed state grows without bound.
  * `dedup_keys` empty -> straight passthrough streaming table; every row from every
                         file is kept.

This mirrors the API module (`src/transformations/silver_api_records.py`), which uses
the same AUTO CDC SCD1 mechanism — the two modules are deliberately consistent.

Config (set in the pipeline `configuration` block of the domain resource):
  dedup_keys      — comma-separated business key(s), e.g. "order_id" or "id,region".
                    Empty (default) -> no dedup; every row from every file is kept.
  dedup_order_by  — column that sequences the upsert (latest wins per key);
                    defaults to the source file modification time.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

DEDUP_KEYS = [c.strip() for c in spark.conf.get("dedup_keys", "").split(",") if c.strip()]
DEDUP_ORDER_BY = spark.conf.get("dedup_order_by", "source_file_modified_at")

# `_rescued_data IS NULL` surfaces schema drift without dropping rows (warn-only).
_QUALITY = {"no_schema_drift": "_rescued_data IS NULL"}

if DEDUP_KEYS:
    # Bounded, correct dedup: SCD Type 1 keeps the latest row per business key. Source
    # is bronze directly (no pre-transform needed). SCD Type 1 is the default behaviour
    # of create_auto_cdc_flow, so stored_as_scd_type is left at its default.
    dp.create_streaming_table(
        name="silver_cloud_files",
        comment=(
            "Current-state rows from bronze_cloud_files, deduplicated to one row per "
            f"{DEDUP_KEYS} via AUTO CDC SCD Type 1 (latest by {DEDUP_ORDER_BY})."
        ),
        cluster_by=["ingest_date"],
        table_properties={
            "quality": "silver",
            # Row tracking lets the deterministic gold MV refresh incrementally on serverless.
            "delta.enableRowTracking": "true",
        },
        expect_all=_QUALITY,
    )

    dp.create_auto_cdc_flow(
        target="silver_cloud_files",
        source="bronze_cloud_files",
        keys=DEDUP_KEYS,
        sequence_by=DEDUP_ORDER_BY,  # latest wins per key; SCD Type 1 (default)
    )
else:
    @dp.table(
        name="silver_cloud_files",
        comment="Quality-checked passthrough of bronze_cloud_files (no dedup_keys configured).",
        cluster_by=["ingest_date"],
        table_properties={
            "quality": "silver",
            "delta.enableRowTracking": "true",
        },
    )
    # Expectation evaluates against the OUTPUT dataframe — `_rescued_data` survives the
    # passthrough, so it can be referenced here.
    @dp.expect("no_schema_drift", "_rescued_data IS NULL")
    def silver_cloud_files():
        return spark.readStream.table("bronze_cloud_files")
