"""Source-file lineage transform for the files (Auto Loader) module."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_source_file_lineage(df: DataFrame) -> DataFrame:
    """Promote Auto Loader's hidden `_metadata` file lineage into real columns.

    The columns are renamed on the way out so they survive to the table: a source column
    literally named `_metadata` would otherwise shadow the hidden one. `source_file` also
    powers the `countDistinct` files-ingested metric in gold, and
    `source_file_modified_at` is the default `dedup_order_by` for the AUTO CDC silver.
    """
    return (
        df.withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("source_file_size", F.col("_metadata.file_size"))
        .withColumn("source_file_modified_at", F.col("_metadata.file_modification_time"))
    )
