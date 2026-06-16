"""Silver: cleaned, optionally deduplicated records from the bronze file ingest.

Parquet arrives already typed, so silver here is about quality and idempotency
rather than parsing: surface schema drift (rescued data) and optionally
deduplicate when the connector re-exports overlapping windows (full + incremental
loads land the same business row more than once).

Config (set in the pipeline `configuration` block of the domain resource):
  dedup_keys      — comma-separated business key(s), e.g. "order_id" or "id,region".
                    Empty (default) -> no dedup; every row from every file is kept.
  dedup_order_by  — column that decides which row survives per key (latest wins);
                    defaults to the source file modification time.

NOTE on scale: `dropDuplicates(keys)` on a stream keeps dedup state keyed on the
business key. For high-volume feeds with continuous re-exports, prefer an
APPLY CHANGES / `dp.create_auto_cdc_flow` upgrade keyed on `dedup_keys` with
`dedup_order_by` as the sequence column — that gives bounded, correct SCD-style
upserts instead of unbounded streaming dedup state.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

DEDUP_KEYS = [c.strip() for c in spark.conf.get("dedup_keys", "").split(",") if c.strip()]


@dp.table(
    name="silver_cloud_files",
    comment="Quality-checked, optionally deduplicated rows from bronze_cloud_files.",
    cluster_by=["ingest_date"],
    table_properties={"quality": "silver"},
)
# Expectations evaluate against the OUTPUT dataframe — these columns survive the
# passthrough. `_rescued_data IS NULL` surfaces schema drift without dropping rows.
@dp.expect("no_schema_drift", "_rescued_data IS NULL")
def silver_cloud_files():
    df = spark.readStream.table("bronze_cloud_files")

    if DEDUP_KEYS:
        # At-least-once dedup on the business key. See module docstring for the
        # APPLY CHANGES upgrade path on high-volume re-export feeds.
        df = df.dropDuplicates(DEDUP_KEYS)

    return df
