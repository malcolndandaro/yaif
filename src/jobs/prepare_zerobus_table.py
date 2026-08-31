# Databricks notebook source
# MAGIC %md
# MAGIC # Zerobus Demo — Prepare the Bronze Table
# MAGIC
# MAGIC Zerobus does **not** create or alter tables: the target must already exist as a
# MAGIC managed Delta table in Unity Catalog, and the producer service principal needs
# MAGIC **explicit** `MODIFY` + `SELECT` on it (schema-level inherited grants are NOT
# MAGIC sufficient for Zerobus's `authorization_details` OAuth flow — it fails with
# MAGIC error 4024 without the table-level grant).
# MAGIC
# MAGIC This task runs before the push task, as the job owner: it creates the bronze
# MAGIC table if missing and grants the SP (whose `client_id` comes from the same secret
# MAGIC scope the push task reads its credentials from — no SP id hard-coded anywhere).

# COMMAND ----------

dbutils.widgets.text("table_name", "yaif.yaif_zerobus_demo.zerobus_events")
dbutils.widgets.text("secret_scope", "yaif_zerobus")

TABLE = dbutils.widgets.get("table_name")
SCOPE = dbutils.widgets.get("secret_scope")
CLIENT_ID = dbutils.secrets.get(SCOPE, "client_id")
print(f"prepare: table={TABLE} sp_client_id={CLIENT_ID}")

# COMMAND ----------

# Zerobus target table. Column types are wire-constrained: Delta TIMESTAMP maps to
# int64 epoch MICROSECONDS on the wire, so event_time / ingested_at are BIGINT micros
# and silver casts them (see src/shared/zerobus_events.py). Bronze stays exactly what
# the producer sends — no read-time stamps on an externally-written table.
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        event_id     STRING COMMENT 'Producer-assigned unique event id (the AUTO CDC / dedup key)',
        device_name  STRING COMMENT 'Synthetic device identifier',
        temp         DOUBLE COMMENT 'Temperature reading',
        humidity     DOUBLE COMMENT 'Humidity reading',
        event_time   BIGINT COMMENT 'Event instant as epoch MICROSECONDS (wire type for Delta TIMESTAMP)',
        ingested_at  BIGINT COMMENT 'Push instant as epoch MICROSECONDS (set by the producer; sequences re-deliveries)'
    )
    USING DELTA
    COMMENT 'YAIF zerobus demo bronze — written by the Zerobus SDK, read by the SDP pipeline'
    """
)
spark.sql(f"DESCRIBE TABLE {TABLE}").show(truncate=False)

# COMMAND ----------

# Explicit grants for the Zerobus producer SP: catalog/schema USAGE plus table-level
# MODIFY + SELECT. GRANT is idempotent, so re-running prepare is safe. The caller needs
# MANAGE on the catalog to grant — the job owner has it here.
APP = CLIENT_ID.replace("`", "")
for stmt in [
    f"GRANT USE CATALOG ON CATALOG {TABLE.split('.')[0]} TO `{APP}`",
    f"GRANT USE SCHEMA ON SCHEMA {'.'.join(TABLE.split('.')[:2])} TO `{APP}`",
    f"GRANT MODIFY, SELECT ON TABLE {TABLE} TO `{APP}`",
]:
    spark.sql(stmt)
    print(f"prepare: {stmt}")
spark.sql(f"SHOW GRANT ON TABLE {TABLE}").show(truncate=False)
print("prepare: done — bronze table ready for Zerobus pushes")
