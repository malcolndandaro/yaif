# API ingestion (REST / HTTP)

The only module with real code: a thread-pooled fetch through a governed UC HTTP connection
lands raw JSON in a Delta table, and an SDP medallion parses it. It scales from one endpoint
to hundreds without ever writing new framework code.

## Data flow (per domain)

```
                              ┌────────────────────────────────┐
  REST APIs (n endpoints) ───►│  fetch_api_responses (Job)     │
                              │  thread pool + tenacity        │
                              └──────────────┬─────────────────┘
                                             ▼
                              ┌────────────────────────────────┐
                              │  raw_api_responses (Delta)     │
                              └──────────────┬─────────────────┘
                                             ▼
                              ┌────────────────────────────────┐
                              │  bronze_api_responses (STREAM) │  ← SDP pipeline
                              └──────────────┬─────────────────┘
                                             ▼
                              ┌────────────────────────────────┐
                              │  silver_api_records  (STREAM)  │   parse/explode/quality
                              │   — or silver_api_documents    │   (VARIANT, document shape)
                              └──────────────┬─────────────────┘
                                             ▼
                              ┌────────────────────────────────┐
                              │  gold_api_endpoint_health (MV) │   success_rate, errors
                              │  gold_api_records_per_day (MV) │   daily counts
                              └────────────────────────────────┘
```

## Try the demo first

A public test API, no real credential — but the UC HTTP connection must exist:

```sql
CREATE CONNECTION IF NOT EXISTS yaif_demo_api TYPE HTTP
OPTIONS (host 'https://jsonplaceholder.typicode.com', port '443',
         base_path '/', bearer_token 'unused');
```

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run content_fetch_and_pipeline   # posts, comments, albums, photos
databricks bundle run people_fetch_and_pipeline    # users, todos
```

Six endpoints across two demo domains, each with its own isolated medallion. Use it to
confirm the plumbing before pointing at real APIs.

## Onboard a domain (GET, Bearer/OAuth)

**1. Create a UC HTTP connection per API host / business domain.** The credential lives
here — encrypted in UC, granted per principal, audited:

```sql
-- Bearer token:
CREATE CONNECTION company_api TYPE HTTP
OPTIONS (host 'https://api.yourcompany.com', port '443', base_path '/v1',
         bearer_token '<your-token>');

-- Or OAuth M2M for APIs with rotating credentials:
-- OPTIONS (host ..., port ..., base_path ...,
--          client_id '...', client_secret '...',
--          oauth_scope '...', token_endpoint 'https://.../oauth/token');

GRANT USE CONNECTION ON CONNECTION company_api TO `data-engineers`;
```

**2. Point the bundle at your workspace, catalog, and connection** — edit `databricks.yml`:

```yaml
variables:
  catalog:        { default: "your_catalog" }   # must exist; schemas are created by the bundle
  api_connection: { default: "company_api" }    # the connection from step 1
targets:
  dev:
    workspace:
      profile: your-cli-profile                 # or host: https://your-workspace...
```

**3. Carve your endpoints into domain units.** Don't run everything in one job (blast
radius, mixed SLAs, one team's bad API blocks everyone) and don't create one job per
endpoint (operational sprawl). The unit is the **business domain**:

```
N endpoints ÷ business domain (finance, sales, logistics, ...) ≈ 10–30 units
each unit = resources/api/<domain>.yml = schema + pipeline + job, sharing src/
```

To onboard a domain: copy `resources/api/content_domain.yml`, rename the resource keys and
schema, set its endpoint list, deploy. ~60 lines of YAML, zero code. Split a domain further
only when freshness SLAs differ (e.g. `finance_hourly` vs `finance_daily` — a job has one
schedule).

Why this shape:
- **Failure isolation** — a broken API in one domain never blocks the others.
- **Independent schedules** — each domain job gets its own cron + concurrency.
- **Per-team governance** — grants on the domain schema and connection; each medallion
  lives in its domain schema. Pipeline/job `CAN_VIEW` grants are driven by the
  `viewers_group` variable (default `users` for the demo); override it globally, per
  target, or per-domain (set a domain's own team group in its `resources/*/*.yml`).
- **Rate-limit budgeting** — the sum of `request_concurrency` across jobs that overlap in
  schedule must stay under the gateway limit; stagger crons.

Start each domain with 10–20 endpoints, watch its `gold_api_endpoint_health`, then ramp
`request_concurrency` to what the gateway tolerates.

**4. Deploy, run, schedule:**

```bash
databricks bundle deploy -t dev
databricks bundle run content_fetch_and_pipeline -t dev   # one command per domain

# When ready, schedule each domain — add to its resources/api/<domain>.yml job:
#   schedule:
#     quartz_cron_expression: "0 0 */4 * * ?"   # every 4 hours
#     timezone_id: "America/Santiago"
# Stagger crons across domains sharing a gateway.
```

**5. Promote to prod:** `databricks bundle deploy -t prod` — `mode: production` marks every
pipeline `development: false` (full retries) and validates it, with isolated schemas.

## Scale to hundreds/thousands of endpoints (worked example: 900 APIs)

If you have 900 APIs you do **not** create 900 jobs by hand. Two levers keep "900 APIs"
from ever meaning 900 files or 900 redeploys — both are **real files in this repo**:

**Lever 1 — one control table is the source of truth.** Every endpoint is one row, tagged
with the business domain it belongs to. Two interchangeable forms, kept in sync (columns:
`domain, endpoint_name, path, method, params, schedule, enabled`):

| Form | File | Use for |
|---|---|---|
| CSV | [`examples/api/control_table.csv`](../examples/api/control_table.csv) | quick start / local — no workspace needed |
| SQL | [`examples/api/control_table.sql`](../examples/api/control_table.sql) | the governed Unity Catalog table (`<catalog>.config.api_endpoints`) |

Adding, pausing, or removing an endpoint is an edit to this one table — never a code change.

**Lever 2 — generate one domain YAML per domain from that table.** The script
[`scripts/generate_api_domains.py`](../scripts/generate_api_domains.py) reads the control
table, groups enabled endpoints by domain, and emits one `resources/api/<domain>.yml` per
domain — each byte-for-byte the same shape as the canonical `content_domain.yml`.

```bash
# From the CSV (no workspace needed):
python scripts/generate_api_domains.py --csv examples/api/control_table.csv

# ...or from the Unity Catalog control table:
python scripts/generate_api_domains.py \
  --table main.config.api_endpoints --warehouse-id <warehouse-id> [--profile <name>]
```

Output (run this, get that):

```
Reading control table from CSV: examples/api/control_table.csv
  wrote build/generated_api/accounts.yml  (1 endpoints)
  wrote build/generated_api/blog.yml  (2 endpoints)
  wrote build/generated_api/gallery.yml  (2 endpoints)

Done: read 5 enabled endpoints across 3 domains -> wrote 3 YAML files to build/generated_api/
  (1 disabled row(s) skipped)
```

The generator writes to a **preview** dir (`build/generated_api/`) — it never clobbers
`resources/`. See exactly what one emitted file looks like at
[`examples/api/generated_sample/blog.yml`](../examples/api/generated_sample/blog.yml) (the
disabled `todos` row is dropped; the `postId=1` params row becomes `/comments?postId=1`).
Review the YAML, move the domains you want into `resources/api/`, then deploy. Full
round-trip: [`scripts/README.md`](../scripts/README.md).

**The 900-endpoint run, end to end:**

1. Load all 900 endpoints into the control table, each tagged with its domain (≈45 domains
   × ≈20 endpoints — group by team / freshness SLA: `finance`, `sales`, …).
2. Run `python scripts/generate_api_domains.py --table <catalog>.config.api_endpoints
   --warehouse-id <id>` → ~45 YAML files in `build/generated_api/`.
3. Move them into `resources/api/`, then `databricks bundle deploy -t dev` → ~45 isolated
   pipelines + jobs. No `databricks.yml` edits — the target `mode` sets each pipeline's
   `development` flag automatically.
4. Stagger each job's schedule and ramp `request_concurrency` per `gold_api_endpoint_health`;
   promote with `-t prod`.

Framework code written: **zero**. Adding endpoints later = edit the control table + re-run
the generator.

> **Caveat / advanced — skip regenerating when you only change endpoints.** Instead of
> baking `api_endpoints` into each YAML, you can have the fetch job read its domain's slice
> from the control table at runtime (pass a `domain` parameter and
> `spark.read.table("<catalog>.config.api_endpoints").filter("enabled AND domain = …")`).
> Then adding an endpoint is a pure `INSERT`, no redeploy. The generator path is the
> simpler default; this is the trade-up when endpoint churn is high.

## POST / body / Basic-auth / semi-structured (VARIANT) APIs

The steps above handle GET endpoints through a UC HTTP connection. Some APIs need more: a
**POST** with a JSON **body**, **HTTP Basic auth**, and a response that is arbitrary
**nested JSON** (no flat array of records). Oracle EPM's `exportdataslice` is the motivating
case. The same shared `src/` code handles all of it — you change config, not code. The
data-safe, deployable demo is [`resources/api/echo_post_demo.yml`](../resources/api/echo_post_demo.yml)
(postman-echo, mock creds — no customer data); the real EPM template is in
[Oracle EPM](oracle-epm.md).

### 1. Per-endpoint request specs

Instead of the `api_endpoints` CSV (GET paths), a domain can pass `api_endpoints_json` — a
JSON array of specs, each `{"path", "method", "body"(obj), "headers"(map), "name"}` (all
but `path` optional). It **overrides** `api_endpoints` when present; a bare `api_endpoints`
CSV still means GET, no body (fully backward compatible). The generator emits the CSV when
every row is GET-with-no-body and `api_endpoints_json` otherwise — so existing GET domains
are byte-for-byte unchanged.

### 2. Auth mode

A `auth_mode` base_parameter picks the transport:

| `auth_mode` | Transport | Use for |
|---|---|---|
| `connection` (default) | `serving_endpoints.http_request(conn=…)` — UC HTTP connection, method/body aware | Bearer / OAuth M2M APIs (content/people demos). Keeps UC governance. |
| `basic_secret` | Direct Python `requests` with `Authorization: Basic base64(user:pass)` from `dbutils.secrets` | HTTP Basic-auth APIs (Oracle EPM). |

`basic_secret` mode needs `api_base_url` (scheme+host), `secret_scope`,
`secret_key_username`, and `secret_key_password`; the password **always** comes from a
secret, never inline. The fetch job is method/body aware in **both** modes — POST + JSON
body work through the connection too; only custom *auth* is stripped.

> **Why Basic auth can't ride the UC connection.** A UC HTTP connection force-prefixes
> `Bearer ` to its credential and merges connection-auth into the `Authorization` header,
> and you can't create a host-only (no-auth) connection — so a clean `Authorization: Basic …`
> is impossible through `http_request` (runtime-verified). `basic_secret` bypasses the
> proxy. Full detail: [Troubleshooting → gotcha #9](troubleshooting.md#basic-auth-cannot-ride-a-uc-http-connection-gotcha-9).
> Strategic alternative: if the API supports OAuth2 M2M (e.g. Oracle via OCI IAM/IDCS), use
> `auth_mode: connection` with OAuth on the connection to restore UC-connection governance.

### 3. VARIANT landing (`silver_shape`)

Bronze derives a `response_variant VARIANT` column with `try_parse_json` (NULL on bad JSON
— never fails the streaming batch), alongside the raw `response_body` STRING (loss-proof
audit). Silver forks on a per-pipeline `silver_shape` configuration:

| `silver_shape` | Silver table | Shape |
|---|---|---|
| `records` (default) | `silver_api_records` | one row per record, exploded from an array, keyed `(endpoint, record_id)` |
| `document` | `silver_api_documents` | one VARIANT row per response, keyed `(endpoint, run_id)` |

Both silver files are globbed by every API pipeline but guard on `silver_shape` and no-op
when not selected, so only the chosen table is created. `gold_api_records_per_day` follows
the shape; `gold_api_endpoint_health` reads bronze, so it is shape-agnostic.

Navigate a `document`-shape grid off the VARIANT with the `:` path operator and
`variant_explode`:

```sql
SELECT d.endpoint, d.run_id,
       d.response_variant:gridDefinition.suppressMissingBlocks::boolean AS suppress,
       r.value:members AS row_members
FROM   yaif_echo.silver_api_documents d,
LATERAL variant_explode(d.response_variant:gridDefinition.rows) AS r;
```

### 4. Try the demo (data-safe)

```bash
databricks secrets create-scope yaif_api
databricks secrets put-secret yaif_api mock_username --string-value postman
databricks secrets put-secret yaif_api mock_password --string-value password
databricks bundle deploy -t dev
databricks bundle run echo_post_demo_fetch_and_pipeline -t dev
```

It POSTs an EPM-shaped `gridDefinition` to `postman-echo.com/post`, lands the echoed nested
JSON as a VARIANT, and builds `silver_api_documents` + gold — proving POST + body + Basic +
VARIANT end-to-end with zero customer data. The real Oracle EPM template (customer-run-only)
is documented in [Oracle EPM](oracle-epm.md).

## Bonus: ad-hoc SQL through the same connection

The connection powering the API pipeline is queryable by analysts directly:

```sql
SELECT http_request(conn => 'company_api', method => 'GET', path => '/orders').text;
```

One governed credential serves bulk pipeline ingestion, ad-hoc SQL exploration, and
per-principal access control with audit — zero credential duplication. A Unity Catalog
governance differentiator worth demoing in platform comparisons.

## Caveats / advanced

> **Fallback — secret scopes.** If your workspace can't use HTTP connections yet (older
> DBR, missing privilege), swap the `get_with_retry` block for a plain `requests.Session`
> with `Authorization: Bearer {dbutils.secrets.get(scope, key)}` — the landing table
> contract and the SDP pipeline are identical either way.

> **Serverless SDK pin.** Serverless preinstalls `databricks-sdk` and pip silently skips an
> upgrade if the floor is already satisfied. The domain job's `environment` pins
> `databricks-sdk>=0.50.0` so `serving_endpoints.http_request()` exists — don't lower it.
> See [Troubleshooting](troubleshooting.md).
