# tests

Unit tests for the shared transform functions in `src/shared/**`.

## What is and isn't tested

**Tested:** every function in `src/shared/`. They are plain `DataFrame -> DataFrame`
functions with no `spark.conf`, `dbutils`, or SDP dependency, so they import and run under
a local SparkSession — no workspace, no deploy.

**Not tested here:** the SDP pipeline files (`src/transformations/**`, `src/files/**`).
They call `spark.conf.get(...)` and register datasets at *import* time, so they can only
run inside a real pipeline. Verify those with `databricks bundle run` (see the module docs).
That split is the reason the transform functions were extracted in the first place.

## Run them

Requires a JDK (PySpark needs a JVM) and OSS PySpark.

```bash
# One-time: an isolated venv with real OSS pyspark.
#   NOTE this must be a separate venv — the global env here has databricks-connect
#   installed, which PROVIDES the `pyspark` module but refuses local sessions
#   ("Only remote Spark sessions using Databricks Connect are supported").
python3 -m venv .venv-test
.venv-test/bin/pip install -r requirements-dev.txt

# Run
export JAVA_HOME=$(brew --prefix openjdk@21)/libexec/openjdk.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
.venv-test/bin/python -m pytest tests/ -q
```

Expected: `21 passed`.

## Notes

- `conftest.py` puts `src/` on `sys.path`, mirroring what `root_path: ../../src` does at
  pipeline runtime — so the tests import `shared.…` exactly as the pipeline files do.
- `conftest.py` also pins `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` to `sys.executable`.
  Spark otherwise launches workers with a bare `python3` from PATH, and if that is a
  different minor version than the venv (a system upgrade putting 3.14 ahead of the venv's
  3.12 is enough) every test that materializes rows fails with `PYTHON_VERSION_MISMATCH`.
  Run the suite via `.venv-test/bin/python -m pytest`, not a bare `pytest`.
- VARIANT assertions use `variant_get(v, '$.path', 'type')`, not the Databricks shorthand
  `v:path::type`. The `:` operator is a Databricks SQL extension and does not parse in OSS
  Spark; both resolve the same path.
