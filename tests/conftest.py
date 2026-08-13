"""Local pytest fixtures for the shared transform functions.

These tests exercise `src/shared/**` — plain `DataFrame -> DataFrame` functions with no
`spark.conf` / `dbutils` / SDP dependency, which is exactly why they are unit-testable
off-platform. The SDP pipeline files themselves are NOT tested here: they read
`spark.conf` and register datasets at import time, so they need a real pipeline run.

Requires a JDK (PySpark needs a JVM). See tests/README.md.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# src/ on sys.path mirrors what `root_path: ../../src` does at pipeline runtime, so the
# imports under test (`from shared.… import …`) are the same ones the pipeline uses.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    """A minimal local SparkSession, reused across the whole test session."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("yaif-tests")
        # Keep the local run small and quiet: no shuffle fan-out, no UI.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
