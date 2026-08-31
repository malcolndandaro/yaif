"""Tests for the Zerobus record transforms in src/shared/zerobus_events.py."""

from datetime import datetime, timezone

import pytest
import pyspark.sql.functions as F

from shared.zerobus_events import cast_micros_to_timestamp, project_event_columns

# 2026-08-31T12:00:00Z and 12:01:00Z, as epoch microseconds — the wire type Zerobus
# uses for Delta TIMESTAMP.
BASE = int(datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc).timestamp() * 1_000_000)
BASE_EVENT = BASE + 60_000_000


@pytest.fixture(autouse=True)
def _utc_session(spark):
    # SQL-side functions (to_date) render through the session timezone; pin UTC so
    # they are machine-independent. NOTE collect-side TimestampType->datetime follows
    # the JVM default timezone and ignores this setting — so the assertions below
    # compare epoch micros, never wall-clock datetimes.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    yield
    spark.conf.unset("spark.sql.session.timeZone")


def _sample_df(spark):
    """Bronze-shaped rows: BIGINT micros in, exactly the columns the producer sends."""
    rows = [
        ("e1", "sensor-01", 22.5, 55.0, BASE_EVENT, BASE),
        ("e2", "sensor-02", 30.1, 60.0, BASE, BASE),
    ]
    return spark.createDataFrame(
        rows, "event_id string, device_name string, temp double, humidity double, "
        "event_time long, ingested_at long"
    )


def test_cast_micros_to_timestamp_converts_columns(spark):
    out = cast_micros_to_timestamp(_sample_df(spark), ["event_time", "ingested_at"])
    schema = {f.name: f.dataType.simpleString() for f in out.schema.fields}
    assert schema["event_time"] == "timestamp"
    assert schema["ingested_at"] == "timestamp"
    # Non-target columns pass through untouched.
    assert schema["event_id"] == "string"
    assert schema["temp"] == "double"


def test_cast_micros_round_trips_the_instant(spark):
    # The wire contract is BIGINT micros in -> TIMESTAMP -> same micros out. Compare in
    # micros (unix_micros), not wall-clock datetimes: pyspark's collect-side rendering
    # follows the JVM default timezone, which is a machine accident.
    out = cast_micros_to_timestamp(_sample_df(spark), ["event_time", "ingested_at"])
    micros = out.select(
        F.unix_micros("event_time").alias("event_time_micros"),
        F.unix_micros("ingested_at").alias("ingested_at_micros"),
    ).orderBy("event_id")
    first = micros.first()
    assert first.event_time_micros == BASE_EVENT
    assert first.ingested_at_micros == BASE


def test_project_event_columns_selects_the_silver_grain(spark):
    df = cast_micros_to_timestamp(_sample_df(spark), ["event_time", "ingested_at"])
    out = project_event_columns(df)
    assert out.columns == [
        "event_id",
        "device_name",
        "temp",
        "humidity",
        "event_time",
        "ingested_at",
        "ingest_date",
    ]


def test_project_derives_ingest_date_from_the_record(spark):
    # ingest_date must come from the record's own ingested_at (push time), not read
    # time. date_format on a DATE is timezone-stable, unlike collect-side datetimes.
    df = cast_micros_to_timestamp(_sample_df(spark), ["event_time", "ingested_at"])
    out = project_event_columns(df)
    got = out.select(F.date_format("ingest_date", "yyyy-MM-dd").alias("d")).first().d
    assert got == "2026-08-31"


def test_transform_chain_composes(spark):
    # The exact chain the zerobus silver view uses, end to end.
    out = project_event_columns(
        cast_micros_to_timestamp(_sample_df(spark), ["event_time", "ingested_at"])
    )
    row = out.orderBy("event_id").first()
    assert row.device_name == "sensor-01" and row.temp == 22.5
