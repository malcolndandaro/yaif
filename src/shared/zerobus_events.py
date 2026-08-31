"""Zerobus record transforms: wire types -> typed silver columns.

Zerobus pushes records whose fields must be JSON-friendly primitives (JSON mode) or
protobuf scalars. Delta `TIMESTAMP` maps to **int64 epoch microseconds** on the wire,
so the bronze table holds `event_time` / `ingested_at` as BIGINT micros and silver
casts them to TIMESTAMP here — one definition instead of inline casts scattered
through the medallion.

`ingest_date` is derived from the record's own `ingested_at` (when the producer pushed
it), NOT from a read-time `current_timestamp()` stamp like `with_ingest_stamps`. A
Zerobus bronze table is written by an external service and read incrementally across
many pipeline updates; a read-time stamp would drift between updates, while the
record's own push time is stable — which is what the AUTO CDC key and the gold
`ingest_date` aggregates need.

Applied with the transform pattern:

    df.transform(lambda d: cast_micros_to_timestamp(d, ["event_time", "ingested_at"]))
        .transform(project_event_columns)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def cast_micros_to_timestamp(df: DataFrame, cols: list[str]) -> DataFrame:
    """Cast BIGINT epoch-microsecond columns to TIMESTAMP, in place.

    Zerobus carries Delta TIMESTAMP as int64 epoch micros (see the module docstring);
    this is the reverse mapping for the silver layer. Applied per column name so the
    caller states which columns are wire timestamps.
    """
    for c in cols:
        df = df.withColumn(c, F.timestamp_micros(F.col(c)))
    return df


def project_event_columns(df: DataFrame) -> DataFrame:
    """Project the silver event grain: typed columns + the derived `ingest_date`.

    Drops nothing the bronze layer carries today; exists so the silver CDC source has
    exactly the columns the streaming table expects (and so a bronze schema that grows
    later does not silently leak into silver).
    """
    return df.select(
        "event_id",
        "device_name",
        "temp",
        "humidity",
        "event_time",
        "ingested_at",
        F.to_date("ingested_at").alias("ingest_date"),
    )
