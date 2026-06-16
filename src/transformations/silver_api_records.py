"""Silver: parse bronze response bodies into structured records, one row per API entity."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType


@dp.table(
    name="silver_api_records",
    comment=(
        "One row per record returned by the API. Parses the raw JSON body, explodes arrays, "
        "and keeps each record as a shallow string-keyed JSON string. "
        "Nested objects (e.g. {data:[...], meta:{...}}) come through as a single map; "
        "downstream consumers should re-parse `record_json` with the per-endpoint schema."
    ),
    cluster_by=["endpoint", "ingest_date"],
    table_properties={"quality": "silver"},
)
# Note: SDP expectations are evaluated against the OUTPUT dataframe, so they can
# only reference columns that survive the final select.
@dp.expect_or_drop("successful_response", "status_code BETWEEN 200 AND 299")
@dp.expect("body_was_parseable", "record_json IS NOT NULL")
def silver_api_records():
    bronze = spark.readStream.table("bronze_api_responses")

    # Bodies are JSON — they may be a single object or an array. Try array first;
    # `from_json` returns NULL when the cast doesn't fit, so the COALESCE picks the
    # right shape per row without us having to inspect endpoint-by-endpoint.
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
