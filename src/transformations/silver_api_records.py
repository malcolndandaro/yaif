"""Silver: current-state API records via AUTO CDC (SCD Type 1).

Bronze is append-only: every scheduled fetch lands a *fresh full snapshot* of each
endpoint (a new `run_id`). If silver simply exploded every bronze row, it would hold
N copies of every record after N runs and the gold counts would inflate. Instead we
parse + explode bronze in a temporary view and feed an AUTO CDC SCD Type 1 flow keyed
on the record's business key — so silver converges to ONE current row per record
(latest snapshot wins) and scheduled re-runs upsert in place instead of duplicating.

Key model: REST record ids are unique only *within* an endpoint (`/posts/1` and
`/albums/1` both have id=1), so the CDC key is **(endpoint, record_id)**, sequenced by
`fetched_at` (latest fetch wins). Records with no `id` field can't be keyed for SCD1 —
the `has_record_key` expectation drops them; a feed whose records lack a stable id
should change the key (or stay snapshot-only in bronze).

This mirrors the files module (`src/files/silver_cloud_files.py`), which uses the same
AUTO CDC SCD1 mechanism for its dedup path — the two modules are deliberately consistent.

Note: AUTO CDC is the default SCD Type 1 behaviour of `create_auto_cdc_flow` (so
`stored_as_scd_type` is left at its default rather than passed explicitly).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType


@dp.temporary_view()
def silver_api_records_parsed():
    """Parse + explode bronze responses into one row per API record (CDC source).

    Bodies are JSON — a single object or an array. Try array first; `from_json`
    returns NULL when the cast doesn't fit, so the COALESCE picks the right shape
    per row without inspecting endpoint-by-endpoint. Nested objects come through as
    a single map; downstream consumers re-parse `record_json` with a per-endpoint
    schema. This is a streaming view (the CDC flow reads it incrementally).
    """
    bronze = spark.readStream.table("bronze_api_responses")

    parsed = bronze.withColumn(
        "_records_array",
        F.from_json(F.col("response_body"), ArrayType(MapType(StringType(), StringType()))),
    ).withColumn(
        "_records_single",
        F.from_json(F.col("response_body"), MapType(StringType(), StringType())),
    )

    return (
        parsed.withColumn(
            "_records",
            F.coalesce(
                F.col("_records_array"),
                F.array(F.col("_records_single")),
            ),
        )
        .withColumn("record", F.explode_outer("_records"))
        .filter(F.col("record").isNotNull())
        .select(
            F.col("endpoint"),
            F.col("url"),
            F.col("status_code"),
            F.col("fetched_at"),
            F.col("_ingested_at"),
            F.col("ingest_date"),
            F.col("run_id"),
            F.to_json(F.col("record")).alias("record_json"),
            F.col("record")["id"].alias("record_id"),
        )
    )


# Empty target for the AUTO CDC flow. Expectations evaluate against the rows being
# applied (which carry every column the parsed view selected), so they may reference
# status_code / record_json / record_id. `has_record_key` guards the CDC key (SCD1
# cannot key on NULL); `successful_response` drops non-2xx; `body_was_parseable` warns.
dp.create_streaming_table(
    name="silver_api_records",
    comment=(
        "Current-state API records (AUTO CDC, SCD Type 1): one row per "
        "(endpoint, record_id), holding the latest snapshot by fetched_at. "
        "Scheduled re-runs upsert in place — no duplicate accumulation."
    ),
    cluster_by=["endpoint", "record_id"],
    table_properties={
        "quality": "silver",
        # Row tracking lets the deterministic gold MV refresh incrementally on serverless.
        "delta.enableRowTracking": "true",
    },
    expect_all_or_drop={
        "successful_response": "status_code BETWEEN 200 AND 299",
        "has_record_key": "record_id IS NOT NULL",
    },
    expect_all={"body_was_parseable": "record_json IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target="silver_api_records",
    source="silver_api_records_parsed",
    keys=["endpoint", "record_id"],
    sequence_by="fetched_at",  # latest fetch wins; SCD Type 1 (default) keeps current state
)
