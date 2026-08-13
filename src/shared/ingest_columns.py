"""Ingest-stamp and time-window transforms shared by every YAIF module.

These are the framework's cross-cutting column conventions. Both the API and files
medallions cluster and aggregate on `ingest_date`, so they must agree on how it is
derived — one definition here instead of one per module.

Applied with the transform pattern:

    df.transform(with_ingest_stamps)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_ingest_stamps(df: DataFrame) -> DataFrame:
    """Stamp the two standard YAIF ingest columns.

    `ingest_date` is derived from the `_ingested_at` column rather than from a second
    `current_timestamp()` call, so the date can never disagree with the timestamp it is
    named after. (Spark pins `current_timestamp()` per query, so two calls agree in
    practice — deriving it makes that guarantee structural instead of incidental.)

    Every bronze layer in the framework carries these; downstream medallion layers and
    the gold monitoring MVs cluster/aggregate on `ingest_date`.
    """
    return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
        "ingest_date", F.to_date(F.col("_ingested_at"))
    )


def recent_ingest_days(df: DataFrame, days: int = 7) -> DataFrame:
    """Keep only rows ingested within the last `days` days, by `ingest_date`.

    Used by the gold monitoring MVs. Note `current_date()` is non-deterministic, so an
    MV using this fully recomputes each run instead of refreshing incrementally. That is
    accepted for the tiny per-endpoint / per-day monitoring tables; if incremental
    refresh is ever needed, drop this transform from the MV and apply the rolling window
    in the dashboard query instead.

    Because `days` is a parameter, call it with `functools.partial` or a lambda when
    chaining a non-default window:

        df.transform(lambda d: recent_ingest_days(d, days=30))
    """
    return df.filter(F.col("ingest_date") >= F.date_sub(F.current_date(), days))
