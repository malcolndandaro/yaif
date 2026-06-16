# Databricks notebook source
# MAGIC %md
# MAGIC # API Fetch Job
# MAGIC
# MAGIC Pulls JSON from REST endpoints through a **Unity Catalog HTTP connection**
# MAGIC (governed credentials, grants, audit) using the SDK's `http_request`, fanned out
# MAGIC with a thread pool and retried with exponential backoff (tenacity). Raw responses
# MAGIC land in a Delta table that the SDP pipeline streams from.
# MAGIC
# MAGIC The API host, port, base path, and credential (Bearer/OAuth M2M) live in the
# MAGIC connection — this notebook only knows the connection name and endpoint paths.

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone

print("fetch: started")

# COMMAND ----------

dbutils.widgets.text("api_connection", "yaif_demo_api")
dbutils.widgets.text("api_endpoints", "/posts,/users,/comments")
dbutils.widgets.text("landing_table", "main.default.raw_api_responses")
dbutils.widgets.text("request_concurrency", "8")

API_CONNECTION = dbutils.widgets.get("api_connection")
ENDPOINTS = [e.strip() for e in dbutils.widgets.get("api_endpoints").split(",") if e.strip()]
LANDING_TABLE = dbutils.widgets.get("landing_table")
CONCURRENCY = int(dbutils.widgets.get("request_concurrency"))

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
print(f"fetch: run_id={RUN_ID} connection={API_CONNECTION} endpoints={len(ENDPOINTS)} concurrency={CONCURRENCY}")
print(f"fetch: landing_table={LANDING_TABLE}")

# COMMAND ----------

import threading

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Enum location varies across SDK versions (serverless preinstalls its own SDK,
# which can shadow the environment dependency). Fall back to the plain string.
try:
    from databricks.sdk.service.serving import ExternalFunctionRequestHttpMethod
    HTTP_GET = ExternalFunctionRequestHttpMethod.GET
except ImportError:
    HTTP_GET = "GET"

# One WorkspaceClient per thread — the SDK client is not guaranteed thread-safe.
_local = threading.local()


def _client() -> WorkspaceClient:
    if not hasattr(_local, "w"):
        _local.w = WorkspaceClient()
    return _local.w


class TransientHttpError(Exception):
    """HTTP 5xx from the target API — retryable."""


@retry(
    retry=retry_if_exception_type(
        (TransientHttpError, DatabricksError, requests.exceptions.RequestException)
    ),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
def get_with_retry(path: str):
    resp = _client().serving_endpoints.http_request(
        conn=API_CONNECTION,
        method=HTTP_GET,
        path=path,
    )
    if resp.status_code >= 500:
        raise TransientHttpError(f"HTTP {resp.status_code} on {path}")
    return resp


def fetch_one(endpoint: str) -> tuple:
    path = endpoint.lstrip("/")
    try:
        resp = get_with_retry(path)
        ok = 200 <= resp.status_code < 300
        body = resp.text if ok else None
        err = None if ok else f"HTTP {resp.status_code}: {resp.text[:500]}"
        return (endpoint, path, resp.status_code, body, err, datetime.now(timezone.utc), RUN_ID)
    except Exception as exc:
        return (endpoint, path, None, None, repr(exc), datetime.now(timezone.utc), RUN_ID)


rows = []
with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
    futures = [pool.submit(fetch_one, ep) for ep in ENDPOINTS]
    for fut in as_completed(futures):
        rows.append(fut.result())

print(f"fetch: got {len(rows)} responses")
for r in sorted(rows):
    print(f"  {r[0]:<12} status={r[2]} body_len={len(r[3]) if r[3] else 0} err={r[4]}")

success_count = sum(1 for r in rows if r[2] is not None and 200 <= r[2] < 300)
if success_count == 0:
    raise RuntimeError(
        f"All {len(rows)} requests failed — check connection '{API_CONNECTION}' "
        "(exists? granted? host reachable from serverless egress?). First error: "
        + str(rows[0][4])
    )

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

schema = StructType([
    StructField("endpoint", StringType(), False),
    StructField("url", StringType(), False),
    StructField("status_code", IntegerType(), True),
    StructField("response_body", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("fetched_at", TimestampType(), False),
    StructField("run_id", StringType(), False),
])

df = spark.createDataFrame(rows, schema=schema)
print(f"fetch: dataframe rows = {df.count()}")

df.write.mode("append").option("mergeSchema", "true").saveAsTable(LANDING_TABLE)
print(f"fetch: appended to {LANDING_TABLE}")

# COMMAND ----------

record_count = spark.read.table(LANDING_TABLE).filter(F.col("run_id") == RUN_ID).count()
print(f"fetch: verified {record_count} records for run_id={RUN_ID}")

if record_count == 0:
    raise RuntimeError(f"Fetch wrote 0 records to {LANDING_TABLE} — failing loudly.")

dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "records": record_count, "success": success_count}))
