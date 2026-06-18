"""Silver (document shape): one row per response holding the parsed VARIANT.

The record-array silver (`silver_api_records.py`) assumes every response is an array
of flat records each with an `id`, keyed `(endpoint, record_id)`. Arbitrary nested /
semi-structured JSON — e.g. an Oracle EPM grid (`pov` / `columns` / `rows` / `data`,
no per-record `id`) — has no such key and would be dropped by that path's
`has_record_key`. This shape keeps the whole response as ONE row, holding the parsed
`response_variant` VARIANT, keyed on the scalar `(endpoint, run_id)` (a re-run of the
same run_id upserts; distinct runs accumulate as snapshot history). No explode, no
`record_id`, no `has_record_key` drop.

Selected per pipeline via the `silver_shape` configuration: this file materializes ONLY
when `silver_shape == "document"`; `silver_api_records.py` materializes only when
"records" (the default). Both files are globbed by every API pipeline, but each no-ops
when not selected, so exactly one silver table is created — no empty unused table. This
keeps the umbrella-repo principle intact: a shared `src/transformations/` file chosen by
config, NOT a new abstraction and NOT a per-domain copy.

VARIANT requires DBR 15.3+ (serverless SDP satisfies). VARIANT cannot be a clustering /
partition / Z-order key and cannot be compared / grouped / ordered / set-operated — so
clustering and the CDC key/sequence use only scalar columns, never `response_variant`.
Downstream consumers navigate the grid with the `:` path operator + `variant_explode`
(see README Playbook A2).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SHAPE = spark.conf.get("silver_shape", "records")

if SILVER_SHAPE == "document":

    @dp.temporary_view()
    def silver_api_documents_parsed():
        """One row per response holding the parsed VARIANT (CDC source).

        For non-record-array / arbitrary nested JSON APIs (e.g. EPM grids with no
        per-record id). Streaming view; the CDC flow reads it incrementally. Drops rows
        whose body did not parse (response_variant IS NULL) so the document table only
        holds navigable VARIANTs; the raw STRING is kept alongside for audit.
        """
        bronze = spark.readStream.table("bronze_api_responses")
        return bronze.select(
            F.col("endpoint"),
            F.col("url"),
            F.col("status_code"),
            F.col("fetched_at"),
            F.col("_ingested_at"),
            F.col("ingest_date"),
            F.col("run_id"),
            F.col("response_variant"),  # parsed VARIANT
            F.col("response_body"),  # raw STRING kept for audit / replay
        ).filter(F.col("response_variant").isNotNull())

    # Expectations evaluate against the rows being applied (which carry every column the
    # parsed view selected), so they may reference status_code / response_variant.
    # `successful_response` drops non-2xx; `body_was_parseable` warns (parsed view already
    # filtered NULLs, so this just surfaces the rare drop in the event log).
    dp.create_streaming_table(
        name="silver_api_documents",
        comment=(
            "One row per API response holding the parsed VARIANT; for non-record-array / "
            "arbitrary nested JSON APIs (e.g. EPM grids). Keyed (endpoint, run_id) — no "
            "per-record id required. Scheduled re-runs of a run_id upsert in place."
        ),
        cluster_by=["endpoint", "ingest_date"],  # scalar columns only — VARIANT cannot cluster
        table_properties={
            "quality": "silver",
            # Row tracking lets deterministic gold MVs refresh incrementally on serverless.
            "delta.enableRowTracking": "true",
        },
        expect_all_or_drop={"successful_response": "status_code BETWEEN 200 AND 299"},
        expect_all={"body_was_parseable": "response_variant IS NOT NULL"},
    )

    dp.create_auto_cdc_flow(
        target="silver_api_documents",
        source="silver_api_documents_parsed",
        keys=["endpoint", "run_id"],
        sequence_by="fetched_at",  # latest fetch wins; SCD Type 1 (default) keeps current state
    )
