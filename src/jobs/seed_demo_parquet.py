# Databricks notebook source
# MAGIC %md
# MAGIC # Files Demo — Synthetic Parquet Seeder
# MAGIC
# MAGIC Writes two small Parquet "drops" into the demo Volume so the Auto Loader
# MAGIC pipeline (`src/files/`) has files to ingest. This stands in for a connector
# MAGIC like **Arcosoft landing SAP exports** — no external bucket or credential
# MAGIC required, so the whole files medallion is verifiable on any workspace.
# MAGIC
# MAGIC Lives in `src/jobs/` (NOT `src/files/`) on purpose: the files pipeline globs
# MAGIC `src/files/**` and would try to import anything there as pipeline source.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("target_path", "/Volumes/main/default/demo_landing")
TARGET = dbutils.widgets.get("target_path").rstrip("/")
print(f"seed: target={TARGET}")

# COMMAND ----------

REGIONS = ["north", "south", "east", "west"]


def make_batch(start_id: int, n: int):
    """A coalesce(1) batch -> exactly one Parquet file Auto Loader will discover."""
    return (
        spark.range(start_id, start_id + n)
        .withColumnRenamed("id", "order_id")
        .withColumn(
            "region",
            F.element_at(
                F.array(*[F.lit(r) for r in REGIONS]),
                (F.col("order_id") % F.lit(len(REGIONS)) + F.lit(1)).cast("int"),
            ),
        )
        .withColumn("amount", (F.col("order_id") * F.lit(10.5)).cast("double"))
        .withColumn("order_ts", F.current_timestamp())
        .coalesce(1)
    )


# Two separate drops => two Parquet files in two prefixes, mimicking a connector
# writing incremental exports. Auto Loader lists nested dirs recursively.
make_batch(1, 60).write.mode("overwrite").parquet(f"{TARGET}/batch_001")
make_batch(61, 40).write.mode("overwrite").parquet(f"{TARGET}/batch_002")
print("seed: wrote batch_001 (60 rows) + batch_002 (40 rows) = 100 rows")

# COMMAND ----------

# Sanity: read back exactly what we wrote (explicit paths — no partition inference).
written = spark.read.parquet(f"{TARGET}/batch_001", f"{TARGET}/batch_002")
total = written.count()
print(f"seed: parquet rows under {TARGET} = {total}")
if total != 100:
    raise RuntimeError(f"Expected 100 seeded rows, found {total} — failing loudly.")

import json

dbutils.notebook.exit(json.dumps({"seeded_rows": total, "files": 2}))
