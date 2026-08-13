"""Shared, pipeline-agnostic transform functions for every YAIF module.

Deliberately OUTSIDE `src/transformations/**` and `src/files/**` (the two pipeline
globs) so SDP never loads these as pipeline source — they define no datasets. They are
plain `DataFrame -> DataFrame` functions, imported by pipeline files and applied with
`DataFrame.transform(...)`.

Importable because each pipeline sets `root_path: ../../src`, which Databricks appends
to `sys.path` when executing Python sources. See CLAUDE.md gotcha #10.

Nothing here touches `spark`, `spark.conf`, or `dbutils` at import time, which is what
makes it unit-testable with a plain local SparkSession (see `tests/`).
"""
