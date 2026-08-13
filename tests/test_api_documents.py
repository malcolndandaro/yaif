"""Tests for the document-shape (`silver_shape: document`) transforms.

`with_response_variant` produces a VARIANT column, so it is asserted through SQL rather
than by collecting the value: VARIANT cannot be compared or grouped directly.
"""

from __future__ import annotations

from shared.api_documents import project_document_columns, with_response_variant

BRONZE_SCHEMA = (
    "endpoint string, url string, status_code int, response_body string, "
    "fetched_at timestamp, run_id string, _ingested_at timestamp, ingest_date date"
)


def _bronze(spark, rows):
    return spark.createDataFrame(
        [(ep, f"https://api.test{ep}", 200, body, None, "run-1", None, None) for ep, body in rows],
        BRONZE_SCHEMA,
    )


def test_valid_json_becomes_a_navigable_variant(spark):
    df = _bronze(spark, [("/grid", '{"pov": {"year": "FY24"}, "n": 1}')])

    out = df.transform(with_response_variant)
    out.createOrReplaceTempView("bronze_variant")

    # `variant_get(v, '$.path', type)` — the portable spelling. Databricks SQL also accepts
    # the shorthand `response_variant:pov.year::string` (which is what the docs show to
    # consumers), but that operator is a Databricks extension and does not parse in OSS
    # Spark, so tests use the function form. Both resolve the same path.
    row = spark.sql(
        "SELECT variant_get(response_variant, '$.pov.year', 'string') AS y FROM bronze_variant"
    ).first()
    assert row["y"] == "FY24"


def test_malformed_json_yields_null_variant_rather_than_failing(spark):
    """try_parse_json, not parse_json: a bad body must not kill the microbatch."""
    df = _bronze(spark, [("/broken", "{not valid json")])

    out = df.transform(with_response_variant)

    assert out.first()["response_variant"] is None


def test_raw_body_is_retained_alongside_the_variant(spark):
    body = '{"a": 1}'
    df = _bronze(spark, [("/grid", body)])

    out = df.transform(with_response_variant)

    assert out.first()["response_body"] == body  # loss-proof audit copy


def test_projection_drops_rows_whose_body_did_not_parse(spark):
    df = _bronze(spark, [("/ok", '{"a":1}'), ("/bad", "nope"), ("/null", None)])

    out = df.transform(with_response_variant).transform(project_document_columns)

    assert out.count() == 1
    assert out.first()["endpoint"] == "/ok"


def test_projection_keeps_no_per_record_id_and_no_explode(spark):
    """Document shape is ONE row per response — keyed (endpoint, run_id), no record_id."""
    df = _bronze(spark, [("/grid", '{"rows":[{"a":1},{"a":2},{"a":3}]}')])

    out = df.transform(with_response_variant).transform(project_document_columns)

    assert out.count() == 1  # not 3 — the nested array is NOT exploded
    assert "record_id" not in out.columns
    for col in ("endpoint", "run_id", "response_variant", "response_body"):
        assert col in out.columns
