"""Tests for the record-array (`silver_shape: records`) parsing chain.

The array-vs-single-object COALESCE is the subtlest logic in the repo; these pin it down.
"""

from __future__ import annotations

import json

import pytest

from shared.api_records import explode_records, parse_json_records, project_record_columns

BRONZE_SCHEMA = (
    "endpoint string, url string, status_code int, response_body string, "
    "fetched_at timestamp, run_id string, _ingested_at timestamp, ingest_date date"
)


def _bronze(spark, rows):
    """Build a bronze-shaped frame from (endpoint, response_body) pairs."""
    return spark.createDataFrame(
        [(ep, f"https://api.test{ep}", 200, body, None, "run-1", None, None) for ep, body in rows],
        BRONZE_SCHEMA,
    )


def test_parses_json_array_into_one_row_per_record(spark):
    df = _bronze(spark, [("/posts", '[{"id":"1","t":"a"},{"id":"2","t":"b"}]')])

    out = df.transform(parse_json_records).transform(explode_records)

    assert out.count() == 2


def test_parses_a_single_json_object_as_one_record(spark):
    """A single object must be wrapped, not dropped — `/posts/1` returns an object."""
    df = _bronze(spark, [("/posts/1", '{"id":"9","t":"solo"}')])

    out = df.transform(parse_json_records).transform(explode_records)

    assert out.count() == 1
    assert out.select("record").first()[0]["id"] == "9"


def test_array_and_object_bodies_coexist_in_one_batch(spark):
    """The COALESCE resolves shape per ROW, so mixed endpoints work in a single batch."""
    df = _bronze(
        spark,
        [("/posts", '[{"id":"1"},{"id":"2"}]'), ("/posts/9", '{"id":"9"}')],
    )

    out = df.transform(parse_json_records).transform(explode_records)

    assert out.count() == 3  # 2 from the array + 1 from the object
    by_endpoint = {r["endpoint"]: r["n"] for r in out.groupBy("endpoint").count().withColumnRenamed("count", "n").collect()}
    assert by_endpoint == {"/posts": 2, "/posts/9": 1}


@pytest.mark.parametrize("bad_body", ["not json at all", "", "{unclosed", "[{},"])
def test_unparseable_bodies_are_dropped_not_fatal(spark, bad_body):
    """Malformed JSON must not raise — bronze keeps the raw body for audit."""
    df = _bronze(spark, [("/broken", bad_body)])

    out = df.transform(parse_json_records).transform(explode_records)

    assert out.count() == 0


def test_null_body_is_dropped(spark):
    """A failed fetch lands response_body NULL; silver holds records, so it drops out."""
    df = _bronze(spark, [("/failed", None)])

    out = df.transform(parse_json_records).transform(explode_records)

    assert out.count() == 0


def test_projection_exposes_the_cdc_key_and_record_json(spark):
    df = _bronze(spark, [("/posts", '[{"id":"1","title":"hello"}]')])

    out = (
        df.transform(parse_json_records)
        .transform(explode_records)
        .transform(project_record_columns)
    )
    row = out.first()

    # record_id is the CDC key component; every expectation column must survive.
    assert row["record_id"] == "1"
    assert json.loads(row["record_json"]) == {"id": "1", "title": "hello"}
    for col in ("endpoint", "url", "status_code", "fetched_at", "run_id", "ingest_date"):
        assert col in out.columns
    # The intermediate scaffolding must NOT leak into silver.
    for col in ("_records", "_records_array", "_records_single", "record"):
        assert col not in out.columns


def test_record_without_id_yields_null_key_for_the_expectation_to_drop(spark):
    """No `id` -> record_id NULL. SCD1 can't key on NULL, so `has_record_key` drops it."""
    df = _bronze(spark, [("/noid", '[{"name":"x"}]')])

    out = (
        df.transform(parse_json_records)
        .transform(explode_records)
        .transform(project_record_columns)
    )

    assert out.first()["record_id"] is None


def test_nested_objects_survive_as_json_in_record_json(spark):
    """Values are read as strings, so a nested object arrives as its JSON text."""
    df = _bronze(spark, [("/nested", '[{"id":"1","meta":{"a":1}}]')])

    out = (
        df.transform(parse_json_records)
        .transform(explode_records)
        .transform(project_record_columns)
    )

    assert json.loads(out.first()["record_json"])["meta"] == '{"a":1}'
