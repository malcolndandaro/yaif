"""Transforms for the API bronze VARIANT column and the document silver shape.

`with_response_variant` is used by bronze regardless of silver shape;
`project_document_columns` builds the `silver_shape: document` CDC source.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_response_variant(df: DataFrame) -> DataFrame:
    """Add a parsed `response_variant VARIANT` beside the raw `response_body` STRING.

    `try_parse_json` yields NULL on bad JSON. `parse_json` would instead raise
    MALFORMED_RECORD_IN_PARSING and fail the whole streaming microbatch — never swap it
    in. The raw STRING stays as the loss-proof audit copy; the document silver shape
    gates on `response_variant IS NOT NULL`.

    VARIANT needs DBR 15.3+ (serverless SDP satisfies) and cannot be a clustering /
    partition / Z-order key, so it is never added to `cluster_by`.
    """
    return df.withColumn("response_variant", F.expr("try_parse_json(response_body)"))


def project_document_columns(df: DataFrame) -> DataFrame:
    """Select the document-shape silver columns and keep only navigable VARIANTs.

    One row per response: no explode, no `record_id`. Rows whose body did not parse are
    dropped so the document table holds only navigable VARIANTs — the raw STRING is
    carried alongside for audit / replay.
    """
    return df.select(
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
