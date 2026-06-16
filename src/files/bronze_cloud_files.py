"""Bronze: incrementally ingest files landed in cloud storage via Auto Loader.

A connector (e.g. Arcosoft exporting from SAP) drops files — Parquet by default —
into a UC-governed External Location surfaced as a Volume. Auto Loader
(`cloudFiles`) discovers new files incrementally and processes each exactly once,
so every pipeline update ingests only the files that arrived since the last run.

Inside a Lakeflow Declarative Pipeline the schema location and stream checkpoint
are managed by the pipeline automatically — do NOT set `cloudFiles.schemaLocation`
or `checkpointLocation` here (those are only needed for standalone Structured
Streaming jobs writing with their own `writeStream`).

Config (set in the pipeline `configuration` block of the domain resource):
  source_path  — volume path the connector writes to, e.g.
                 /Volumes/<catalog>/<schema>/<volume>/<feed>/
  file_format  — Auto Loader source format (default: parquet; csv/json/avro work too)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_PATH = spark.conf.get("source_path")
FILE_FORMAT = spark.conf.get("file_format", "parquet")


@dp.table(
    name="bronze_cloud_files",
    comment=(
        "Raw rows ingested from cloud-storage files via Auto Loader. Append-only; "
        "carries source-file lineage (path, size, modification time) and ingest stamps."
    ),
    cluster_by=["ingest_date"],
    table_properties={
        "delta.enableChangeDataFeed": "true",
        # Row tracking lets serverless MVs reading this table refresh incrementally.
        "delta.enableRowTracking": "true",
        "quality": "bronze",
    },
)
def bronze_cloud_files():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", FILE_FORMAT)
        # inferColumnTypes mainly helps text formats (csv/json); harmless for parquet,
        # which carries its own schema. New columns in later files are ADDED rather than
        # failing the stream; anything that can't be coerced lands in `_rescued_data`.
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(SOURCE_PATH)
        # File lineage from the hidden _metadata column — rename so it survives to the
        # table (a source column literally named `_metadata` would otherwise shadow it).
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("source_file_size", F.col("_metadata.file_size"))
        .withColumn("source_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
    )
