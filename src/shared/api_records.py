"""Transforms that turn raw API response bodies into one row per API record.

The record-array silver shape (`silver_shape: records`) is three distinct steps, split
here so each is separately readable and unit-testable:

    bronze.transform(parse_json_records)      # body STRING -> _records array
          .transform(explode_records)         # _records   -> one row per record
          .transform(project_record_columns)  # -> the silver CDC source columns

Pure `DataFrame -> DataFrame` functions with no `spark` / `spark.conf` dependency, so
they import and run under a plain local SparkSession (see `tests/`).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType

# API records are read as a map of string -> string. Nested objects therefore arrive as
# a single map value; downstream consumers re-parse `record_json` with a per-endpoint
# schema when they need the nested structure typed.
_RECORD_MAP = MapType(StringType(), StringType())


def parse_json_records(df: DataFrame) -> DataFrame:
    """Parse `response_body` into a `_records` array, accepting an array OR one object.

    REST bodies are JSON — either an array of records (`/posts`) or a single object
    (`/posts/1`). `from_json` returns NULL when the target type doesn't fit, so trying
    both shapes and COALESCE-ing picks the right one **per row**, with no need to know
    each endpoint's shape in advance. A single object is wrapped in a 1-element array so
    the downstream explode is uniform.

    A body that is neither (malformed JSON) yields NULL, which `explode_records` drops.
    """
    return (
        df.withColumn("_records_array", F.from_json(F.col("response_body"), ArrayType(_RECORD_MAP)))
        .withColumn("_records_single", F.from_json(F.col("response_body"), _RECORD_MAP))
        .withColumn(
            "_records",
            F.coalesce(F.col("_records_array"), F.array(F.col("_records_single"))),
        )
    )


def explode_records(df: DataFrame) -> DataFrame:
    """Explode `_records` to one row per API record, dropping rows that yielded none.

    `explode_outer` keeps a row when the array is NULL/empty (so a malformed body is
    visible rather than silently vanishing mid-chain); the filter then removes those
    NULL records, because silver holds records and bronze already retains the raw body
    for audit.
    """
    return df.withColumn("record", F.explode_outer("_records")).filter(F.col("record").isNotNull())


def project_record_columns(df: DataFrame) -> DataFrame:
    """Select the silver CDC-source columns, plus `record_json` and the `record_id` key.

    `record_id` is the SCD1 key component. REST ids are unique only *within* an endpoint
    (`/posts/1` and `/albums/1` both have id=1), so the CDC key is
    `(endpoint, record_id)` — see `silver_api_records.py`.

    Every column the silver expectations reference must survive this projection:
    expectations evaluate against the rows being applied.
    """
    return df.select(
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
