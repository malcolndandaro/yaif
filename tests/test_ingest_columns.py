"""Tests for the shared ingest-stamp and time-window transforms."""

from __future__ import annotations

import datetime as dt

from shared.ingest_columns import recent_ingest_days, with_ingest_stamps


def test_adds_both_ingest_columns(spark):
    df = spark.createDataFrame([("a",)], "k string")

    out = df.transform(with_ingest_stamps)

    assert "_ingested_at" in out.columns
    assert "ingest_date" in out.columns


def test_ingest_date_is_derived_from_the_timestamp_not_a_second_clock_read(spark):
    """The whole point of the shared transform: the two columns cannot disagree."""
    df = spark.createDataFrame([("a",), ("b",)], "k string")

    out = df.transform(with_ingest_stamps)

    for row in out.collect():
        assert row["ingest_date"] == row["_ingested_at"].date()


def test_existing_columns_are_preserved(spark):
    df = spark.createDataFrame([("a", 1)], "k string, v int")

    out = df.transform(with_ingest_stamps)

    assert out.first()["k"] == "a"
    assert out.first()["v"] == 1


def test_recent_ingest_days_keeps_inside_and_drops_outside_the_window(spark):
    today = dt.date.today()
    df = spark.createDataFrame(
        [("fresh", today), ("edge", today - dt.timedelta(days=7)), ("stale", today - dt.timedelta(days=30))],
        "k string, ingest_date date",
    )

    kept = {r["k"] for r in df.transform(recent_ingest_days).collect()}

    assert kept == {"fresh", "edge"}  # boundary is inclusive (>=)


def test_recent_ingest_days_window_is_configurable(spark):
    today = dt.date.today()
    df = spark.createDataFrame(
        [("d1", today - dt.timedelta(days=1)), ("d10", today - dt.timedelta(days=10))],
        "k string, ingest_date date",
    )

    kept = {r["k"] for r in df.transform(lambda d: recent_ingest_days(d, days=30)).collect()}

    assert kept == {"d1", "d10"}
