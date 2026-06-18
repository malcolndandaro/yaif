# Databricks notebook source
# MAGIC %md
# MAGIC # API Fetch Job
# MAGIC
# MAGIC Pulls JSON from REST endpoints, fanned out with a thread pool and retried with
# MAGIC exponential backoff (tenacity). Raw responses land in a Delta table that the SDP
# MAGIC pipeline streams from. Each endpoint is a request spec — `{path, method, body,
# MAGIC headers}` — so the same job handles GET *and* POST-with-body APIs.
# MAGIC
# MAGIC Two auth modes (`auth_mode` widget), picked per domain:
# MAGIC - **`connection`** (default) — through a **Unity Catalog HTTP connection**
# MAGIC   (governed credentials, grants, audit) via the SDK's `http_request`. The host,
# MAGIC   port, base path, and credential (Bearer / OAuth M2M) live in the connection;
# MAGIC   this notebook only knows the connection name + endpoint specs. This is the
# MAGIC   path the `content`/`people` demos use, unchanged.
# MAGIC - **`basic_secret`** — direct Python `requests` with `Authorization: Basic
# MAGIC   base64(user:pass)` built from `dbutils.secrets`. A UC HTTP connection CANNOT
# MAGIC   carry clean Basic auth (it force-prefixes `Bearer ` / merges connection-auth
# MAGIC   into the `Authorization` header — see CLAUDE.md gotcha #9), so Basic-auth APIs
# MAGIC   (e.g. Oracle EPM) MUST bypass the proxy. Needs `api_base_url` + a secret scope.
# MAGIC
# MAGIC Backward compatible: a bare `api_endpoints` CSV with the default `connection`
# MAGIC auth behaves exactly as before (GET, no body, UC connection).

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone

print("fetch: started")

# COMMAND ----------

dbutils.widgets.text("api_connection", "yaif_demo_api")
dbutils.widgets.text("api_endpoints", "/posts,/users,/comments")
# JSON array of request specs [{"path","method","body","headers","name"}, ...].
# Overrides api_endpoints when non-empty. JSON-string survives the notebook
# base_parameters string-only contract.
dbutils.widgets.text("api_endpoints_json", "")
# "connection" (UC HTTP connection via http_request) | "basic_secret" (direct requests).
dbutils.widgets.text("auth_mode", "connection")
# Required for basic_secret mode: scheme + host, e.g. https://host (no trailing path).
dbutils.widgets.text("api_base_url", "")
# Secret scope + keys for basic_secret mode (password ALWAYS from a secret).
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("secret_key_username", "")
dbutils.widgets.text("secret_key_password", "")
dbutils.widgets.text("landing_table", "main.default.raw_api_responses")
dbutils.widgets.text("request_concurrency", "8")

API_CONNECTION = dbutils.widgets.get("api_connection")
AUTH_MODE = dbutils.widgets.get("auth_mode").strip() or "connection"
API_BASE_URL = dbutils.widgets.get("api_base_url").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SECRET_KEY_USERNAME = dbutils.widgets.get("secret_key_username").strip()
SECRET_KEY_PASSWORD = dbutils.widgets.get("secret_key_password").strip()
LANDING_TABLE = dbutils.widgets.get("landing_table")
CONCURRENCY = int(dbutils.widgets.get("request_concurrency"))

# Uniform request-spec list. api_endpoints_json (rich specs) wins when present;
# otherwise each CSV path becomes a bare GET spec — the legacy fast path.
_endpoints_json = dbutils.widgets.get("api_endpoints_json").strip()
if _endpoints_json:
    ENDPOINTS = json.loads(_endpoints_json)
else:
    ENDPOINTS = [
        {"path": e.strip(), "method": "GET"}
        for e in dbutils.widgets.get("api_endpoints").split(",")
        if e.strip()
    ]

if AUTH_MODE not in ("connection", "basic_secret"):
    raise ValueError(f"auth_mode must be 'connection' or 'basic_secret', got '{AUTH_MODE}'")
if AUTH_MODE == "basic_secret":
    missing = [
        name
        for name, val in (
            ("api_base_url", API_BASE_URL),
            ("secret_scope", SECRET_SCOPE),
            ("secret_key_password", SECRET_KEY_PASSWORD),
        )
        if not val
    ]
    if missing:
        raise ValueError(
            f"auth_mode='basic_secret' requires {missing} — set them as base_parameters."
        )

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
print(f"fetch: run_id={RUN_ID} auth_mode={AUTH_MODE} endpoints={len(ENDPOINTS)} concurrency={CONCURRENCY}")
if AUTH_MODE == "connection":
    print(f"fetch: connection={API_CONNECTION}")
else:
    print(f"fetch: api_base_url={API_BASE_URL} secret_scope={SECRET_SCOPE}")
print(f"fetch: landing_table={LANDING_TABLE}")

# COMMAND ----------

import base64
import threading

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

# Method resolver for connection mode. The enum location varies across SDK versions
# (serverless preinstalls its own SDK, which can shadow the environment dependency),
# so fall back to the plain string. Generalized from a GET-only constant so POST works
# on either SDK shape.
try:
    from databricks.sdk.service.serving import ExternalFunctionRequestHttpMethod as _HttpMethod

    def _method(name: str):
        return _HttpMethod[name.upper()]
except ImportError:
    def _method(name: str):
        return name.upper()

# One WorkspaceClient per thread — the SDK client is not guaranteed thread-safe.
_local = threading.local()


def _client() -> WorkspaceClient:
    if not hasattr(_local, "w"):
        _local.w = WorkspaceClient()
    return _local.w


def _session() -> "requests.Session":
    """One requests.Session per thread — mirrors the per-thread WorkspaceClient pattern."""
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
    return _local.s


# basic_secret mode: build the Authorization: Basic header once from secrets. A UC HTTP
# connection cannot carry clean Basic auth (it force-prefixes "Bearer " / merges
# connection-auth into Authorization), so Basic-auth APIs go direct via requests.
_BASIC_AUTH = None
if AUTH_MODE == "basic_secret":
    _user = (
        dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY_USERNAME)
        if SECRET_KEY_USERNAME
        else ""
    )
    _pwd = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY_PASSWORD)
    _BASIC_AUTH = "Basic " + base64.b64encode(f"{_user}:{_pwd}".encode()).decode()


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
def request_with_retry(spec: dict) -> tuple:
    """Issue one request per spec, returning (status_code, response_text).

    Only the transport differs by auth_mode; the retry policy, the 5xx->retry trigger,
    and the caller (fetch_one) are identical for both modes.
    """
    method = (spec.get("method") or "GET").upper()
    path = spec["path"].lstrip("/")
    body = spec.get("body")
    extra_headers = spec.get("headers") or {}

    if AUTH_MODE == "basic_secret":
        url = API_BASE_URL.rstrip("/") + "/" + path
        headers = {"Authorization": _BASIC_AUTH, **extra_headers}
        if body is not None:
            headers.setdefault("Content-Type", "application/json")
        resp = _session().request(
            method,
            url,
            headers=headers,
            json=body if body is not None else None,
            timeout=60,
        )
        status, text = resp.status_code, resp.text
    else:  # connection mode — UC HTTP connection via http_request, now method/body aware.
        # NOTE: the proxy strips/merges custom Authorization headers (gotcha #9); auth
        # rides the connection's own credential. Non-auth custom headers pass through.
        resp = _client().serving_endpoints.http_request(
            conn=API_CONNECTION,
            method=_method(method),
            path=path,
            json=json.dumps(body) if body is not None else None,
            headers=json.dumps(extra_headers) if extra_headers else None,
        )
        status, text = resp.status_code, resp.text

    if status >= 500:
        raise TransientHttpError(f"HTTP {status} on {path}")
    return status, text


def fetch_one(spec: dict) -> tuple:
    endpoint = spec.get("name") or spec["path"]
    path = spec["path"].lstrip("/")
    try:
        status, text = request_with_retry(spec)
        ok = 200 <= status < 300
        body = text if ok else None
        err = None if ok else f"HTTP {status}: {text[:500]}"
        return (endpoint, path, status, body, err, datetime.now(timezone.utc), RUN_ID)
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
    target = (
        f"connection '{API_CONNECTION}'"
        if AUTH_MODE == "connection"
        else f"base url '{API_BASE_URL}' + secrets in scope '{SECRET_SCOPE}'"
    )
    raise RuntimeError(
        f"All {len(rows)} requests failed — check {target} "
        "(exists? granted? host reachable from serverless egress? creds valid?). First error: "
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
