# YAIF — Yet Another Ingestion Framework

Production-grade ingestion patterns for the Databricks Lakehouse, organized as an
**umbrella repo, not an abstraction layer**: one Asset Bundle, consistent
conventions (naming, medallion, monitoring, dev/prod targets), independent modules
per source type. No metadata engine, no wrapper around managed connectors — where
Databricks already provides a declarative primitive (Lakeflow Connect, SDP), YAIF
just configures it.

## Modules

| Module | Source type | Status | Mechanism |
|---|---|---|---|
| `resources/api/` | REST APIs | ✅ built & verified | UC HTTP Connection + thread-pool fetch + SDP medallion |
| `examples/sqlserver/` (move to `resources/sqlserver/` to activate) | SQL Server CDC | ✅ built & verified | Lakeflow Connect (gateway + ingestion pipeline) — pure config |
| `src/files/` + `examples/files/` | Files in cloud storage (Parquet/CSV/JSON) — incl. SAP via a connector like Arcosoft | ✅ built & verified (demo) | Auto Loader (`cloudFiles`) reading a UC-governed Volume → SDP bronze/silver/gold |

## Why would I use this?

Because the alternative is a bespoke notebook per source — and that is exactly the
thing that does not scale to dozens or hundreds of sources. YAIF gives every source
the same governed, observable, environment-aware plumbing for free. Here is the API
module versus a typical hand-rolled ingestion notebook:

| Concern | Typical legacy notebook | YAIF API module |
|---|---|---|
| Auth | notebook context token / hardcoded | UC HTTP Connection (governed, audited, OAuth M2M capable) |
| Distribution | serial loop or ad-hoc pandas_udf | `ThreadPoolExecutor` on the endpoint list |
| Retries | manual / missing | tenacity, exp backoff, retries 5xx + transient |
| Persistence | often discarded | UC managed Delta landing table |
| Schema evolution | manual | Delta `mergeSchema=true` on append |
| Orchestration | none | DABs job + SDP pipeline per domain |
| Environments | hardcoded host | `targets: dev / prod` with overrides |
| Observability | none | `gold_api_endpoint_health` MV (success rate, errors, body size) |
| Failure alerting | none | Email notifications on job + pipeline failure |

The other two modules (SQL Server, files) buy the same consistency without custom
code — they just configure a managed Databricks primitive (Lakeflow Connect, Auto
Loader) the same governed, dev/prod-aware way.

## Start here: you received YAIF — now what?

YAIF exists to **accelerate ingestion onboarding** — to turn "we have hundreds of
sources to land in the lakehouse" into a repeatable, *copy-one-file-per-source*
workflow instead of a bespoke notebook per source. The loop is always the same:

1. **Set up once** (~5 min) — CLI, a catalog, run a demo to confirm the plumbing.
2. **Pick your source type** and follow its playbook below.
3. **Repeat per source / domain** — each new source is a file copy + a few field
   edits (or one row in a control table). You never write new framework code.

> If you have **900 APIs**, you do *not* create 900 jobs by hand. You group them
> into ~45 domains, drive the endpoint lists from a control table, and **generate**
> the per-domain resources from a list. See **Playbook A → "Scale to hundreds/
> thousands"** for the exact, copy-pasteable steps.

### Step 0 — set up once

```bash
# a) Authenticate the CLI to your workspace
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# b) Get the code and point the bundle at your workspace + an existing catalog
git clone <this-repo> && cd yaif
#    edit databricks.yml → targets.dev.workspace.profile + var.catalog
#    (or just pass --profile <name> on every command)

# c) Confirm the plumbing with the self-contained files demo (no external setup)
databricks bundle deploy -t dev
databricks bundle run files_demo_seed_and_pipeline
```

That demo seeds synthetic Parquet into a managed Volume and runs the full Auto
Loader → bronze/silver/gold medallion — proving deploy + serverless + SDP work
before you point at anything real. (The API demo needs one extra line — the
`yaif_demo_api` connection — see Playbook A.)

### Pick your source type

| Your source is… | Module | Go to |
|---|---|---|
| A REST / HTTP API — one, or **hundreds** | API (real code: governed fetch + SDP) | **Playbook A** |
| A **SQL Server** database (ongoing change capture) | Lakeflow Connect (managed) | **Playbook B** |
| **Files a tool drops in a bucket** (Parquet/CSV/JSON — e.g. SAP exported by Arcosoft) | Auto Loader (`cloudFiles`) | **Playbook C** |

## Design principles

1. **Umbrella repo, not framework** — modules share conventions and deployment
   workflow, never code abstractions. The API module has real code; the SQL Server
   module is ~50 lines of Lakeflow Connect YAML. Wrapping a managed connector in a
   custom framework only adds indirection.
2. **One deployable unit per business domain** — each `resources/api/<domain>.yml`
   is a self-contained schema + pipeline + job. Failure isolation, independent
   schedules, per-team grants.
3. **Governed auth via Unity Catalog** — API credentials live in UC HTTP
   connections (granted, audited, OAuth M2M capable); database credentials live in
   UC connections used by Lakeflow Connect. No secrets in code.

## Layout

```
yaif/
├── databricks.yml                    # bundle + shared variables + targets
├── resources/
│   └── api/                          # API module — one file per business domain
│       ├── content_domain.yml        #   domain unit: schema + pipeline + job
│       └── people_domain.yml         #   second domain — same pattern
├── scripts/
│   ├── generate_api_domains.py       # control table → one resources/api/<domain>.yml per domain
│   └── README.md                     #   the control-table → generate → deploy round-trip
├── examples/
│   ├── api/                          # API scaling helpers (outside the deploy glob)
│   │   ├── control_table.csv         #   endpoints as a CSV (quick start / local)
│   │   ├── control_table.sql         #   endpoints as a UC table (governed runtime source of truth)
│   │   └── generated_sample/         #   committed example of one generated domain YAML
│   ├── sqlserver/orders_cdc.yml      # Lakeflow Connect scaffold (outside include glob)
│   └── files/erp_parquet.yml         # files-module domain unit (outside include glob; activate per feed)
└── src/                              # SHARED module source — never copied per-domain
    ├── jobs/
    │   └── fetch_api_responses.py    # API: threaded fetch → Delta landing table
    ├── transformations/              # API SDP pipeline source (raw .py files)
    │   ├── bronze_api_responses.py   #   streaming from landing table
    │   ├── silver_api_records.py     #   JSON parse + explode
    │   └── gold_api_metrics.py       #   MVs: endpoint health, daily counts
    └── files/                        # files SDP pipeline source (Auto Loader medallion)
        ├── bronze_cloud_files.py     #   cloudFiles stream from a UC Volume + file lineage
        ├── silver_cloud_files.py     #   quality + optional dedup
        └── gold_cloud_files.py       #   MVs: ingestion health, rows/day
```

## Data flow (API module, per domain)

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
                              └──────────────┬─────────────────┘
                                             ▼
                              ┌────────────────────────────────┐
                              │  gold_api_endpoint_health (MV) │   success_rate, errors
                              │  gold_api_records_per_day (MV) │   daily counts
                              └────────────────────────────────┘
```

## Prerequisites

| Requirement | Detail |
|---|---|
| Databricks CLI | `>= v1.0` (`databricks --version`), authenticated profile (`databricks auth login`) |
| Workspace | Unity Catalog enabled, serverless jobs **and** serverless pipelines available in the region |
| Permissions | `USE CATALOG` + `CREATE SCHEMA` on the target catalog; `CREATE CONNECTION` on the metastore (or ask an admin for the connection + `USE CONNECTION` grant) |
| Network | Workspace egress must reach your API gateway / database (private sources need NCC/private link) |
| Python deps | None to install — `tenacity` + `databricks-sdk` are declared in each domain job's serverless `environment` spec |
| `http_request` | DBR 15.4+ / SQL warehouse 2023.40+ (already true on serverless) |

## Demo quick start (public test API)

Create the demo connection once (the test API needs no real credential, but the
connection must exist):

```sql
CREATE CONNECTION IF NOT EXISTS yaif_demo_api TYPE HTTP
OPTIONS (host 'https://jsonplaceholder.typicode.com', port '443',
         base_path '/', bearer_token 'unused');
```

Then:

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run content_fetch_and_pipeline   # posts, comments, albums, photos
databricks bundle run people_fetch_and_pipeline    # users, todos
```

This ingests 6 endpoints across two demo domains, each with its own isolated
medallion. Use it to confirm the plumbing works before pointing at real APIs.

## Playbook A — REST / HTTP APIs (one source, or hundreds)

The only module with real code: a thread-pooled fetch through a governed UC HTTP
connection lands raw JSON in a Delta table, and an SDP medallion parses it. Steps
1–5 onboard a domain; **"Scale to hundreds/thousands"** at the end is the path to
900+ endpoints.

**1. Create a UC HTTP connection per API host / business domain.**
The credential lives here — encrypted in UC, granted per principal, audited:

```sql
-- Bearer token:
CREATE CONNECTION company_api TYPE HTTP
OPTIONS (
  host 'https://api.yourcompany.com',
  port '443',
  base_path '/v1',
  bearer_token '<your-token>'
);

-- Or OAuth M2M for APIs with rotating credentials:
-- OPTIONS (host ..., port ..., base_path ...,
--          client_id '...', client_secret '...',
--          oauth_scope '...', token_endpoint 'https://.../oauth/token');

GRANT USE CONNECTION ON CONNECTION company_api TO `data-engineers`;
```

**2. Point the bundle at your workspace, catalog, and connection** — edit `databricks.yml`:

```yaml
variables:
  catalog:
    default: "your_catalog"          # must exist; schemas are created by the bundle
  api_connection:
    default: "company_api"           # the connection from step 1
targets:
  dev:
    workspace:
      profile: your-cli-profile      # or host: https://your-workspace...
```

**3. Carve your endpoints into domain units.** Don't run everything in one job
(blast radius, mixed SLAs, one team's bad API blocks everyone) and don't create
one job per endpoint (operational sprawl). The unit is the **business domain**:

```
N endpoints ÷ business domain (finance, sales, logistics, ...) ≈ 10–30 units
each unit = resources/api/<domain>.yml = schema + pipeline + job, sharing src/
```

To onboard a domain: copy `resources/api/content_domain.yml`, rename the resource
keys and schema, set its endpoint list, deploy. ~60 lines of YAML, zero code.
Split a domain further only when freshness SLAs differ (e.g. `finance_hourly` vs
`finance_daily` — a job has one schedule).

Why this shape:
- **Failure isolation** — a broken API in one domain never blocks the others
- **Independent schedules** — each domain job gets its own cron + concurrency
- **Per-team governance** — grants on the domain schema and connection; each
  medallion (bronze/silver/gold) lives in its domain schema. Pipeline/job `CAN_VIEW`
  grants are driven by the `viewers_group` variable (default `users` for the demo);
  override it globally, per target, or per-domain (set a domain's own team group in
  its `resources/*/*.yml`) to realize true per-team visibility
- **Rate-limit budgeting** — the sum of `request_concurrency` across jobs that
  overlap in schedule must stay under the gateway limit; stagger crons

Start each domain with 10–20 endpoints, watch its `gold_api_endpoint_health`,
then ramp `request_concurrency` to what the gateway tolerates. To go past a
handful of domains, see **Scale to hundreds/thousands of endpoints** below.

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

**5. Promote to prod:** `databricks bundle deploy -t prod` — `mode: production` marks
every pipeline `development: false` (full retries) and validates it, with isolated schemas.

### Scale to hundreds/thousands of endpoints (worked example: 900 APIs)

Two levers keep "900 APIs" from ever meaning 900 files or 900 redeploys — and both
are **real files in this repo**, not pseudo-code:

**Lever 1 — one control table is the source of truth.** Every endpoint is one row,
tagged with the business domain it belongs to. Two interchangeable forms, kept in
sync (columns: `domain, endpoint_name, path, method, params, schedule, enabled`):

| Form | File | Use for |
|---|---|---|
| CSV | [`examples/api/control_table.csv`](examples/api/control_table.csv) | quick start / local — no workspace needed |
| SQL | [`examples/api/control_table.sql`](examples/api/control_table.sql) | the governed Unity Catalog table (`<catalog>.config.api_endpoints`) |

Adding, pausing, or removing an endpoint is an edit to this one table — never a code change.

**Lever 2 — generate one domain YAML per domain from that table.** The script
[`scripts/generate_api_domains.py`](scripts/generate_api_domains.py) reads the control
table, groups enabled endpoints by domain, and emits one `resources/api/<domain>.yml`
per domain — each byte-for-byte the same shape as the canonical `content_domain.yml`.
No hand-copying, no framework code.

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
[`examples/api/generated_sample/blog.yml`](examples/api/generated_sample/blog.yml)
(note the disabled `todos` row is dropped and the `postId=1` params row becomes
`/comments?postId=1`). Review the YAML, move the domains you want into `resources/api/`,
then deploy. (See [`scripts/README.md`](scripts/README.md) for the full round-trip.)

> **Optional — skip regenerating when you only change endpoints.** Instead of baking
> `api_endpoints` into each YAML, you can have the fetch job read its domain's slice
> from the control table at runtime (pass a `domain` parameter and
> `spark.read.table("<catalog>.config.api_endpoints").filter("enabled AND domain = …")`).
> Then adding an endpoint is a pure `INSERT`, no redeploy. The generator path above is
> the simpler default; this is the trade-up when endpoint churn is high.

**The 900-endpoint run, end to end:**

1. Load all 900 endpoints into the control table, each tagged with its domain
   (≈45 domains × ≈20 endpoints — group by team / freshness SLA: `finance`, `sales`, …).
2. Run `python scripts/generate_api_domains.py --table <catalog>.config.api_endpoints
   --warehouse-id <id>` → ~45 YAML files in `build/generated_api/`.
3. Move them into `resources/api/`, then `databricks bundle deploy -t dev` → ~45 isolated
   pipelines + jobs. No `databricks.yml` edits: the target `mode` (`development`/`production`)
   sets each pipeline's `development` flag automatically — onboarding is a pure file copy.
4. Stagger each job's schedule and ramp `request_concurrency` per
   `gold_api_endpoint_health`; promote with `-t prod`.

Framework code written: **zero**. Adding endpoints later = edit the control table +
re-run the generator (or just `INSERT` if you adopt the runtime-read option above).

## Playbook B — SQL Server (CDC via Lakeflow Connect)

A managed connector — YAIF supplies only conventions, no custom code. The template
is `examples/sqlserver/orders_cdc.yml` (gateway + ingestion + job), kept **outside**
the deploy glob because it needs a live connection.

1. **Enable CDC or Change Tracking** on the source tables (your DBA).
2. **Create the UC connection** (credentials live here, never in a file):
   ```sql
   CREATE CONNECTION sqlserver_conn TYPE SQLSERVER
   OPTIONS (host '<host>', port '1433', user '<user>', password '<password>',
            trustServerCertificate 'true');
   GRANT USE CONNECTION ON CONNECTION sqlserver_conn TO `data-engineers`;
   ```
   Use a **dedicated least-privilege login** for `<user>` — only the CT/CDC read grants the
   connector needs, never `SA`/`db_owner` (that broad role is for a DBA enabling CDC, not
   steady-state reads). `trustServerCertificate 'true'` disables server-cert validation, which
   is fine for a lab; **for prod, validate TLS** — drop the option (or pin the CA) so the
   gateway verifies the server certificate.
3. **Set the variables** in `databricks.yml`: `sqlserver_connection` and
   `sqlserver_source_database`. List your tables in the example's `objects:` block
   (one `table:` block each, or a single `schema:` block for a whole schema).
4. **Activate:** move `examples/sqlserver/orders_cdc.yml` → `resources/sqlserver/`
   (no `databricks.yml` edits — the target `mode` sets each pipeline's `development` flag).
5. **Deploy, start the gateway, run ingestion:** `databricks bundle deploy`, then start the
   **continuous gateway** once — `databricks bundle run sqlserver_gateway` — which runs
   continuously, capturing changes into staging. Trigger applies with
   `databricks bundle run sqlserver_ingestion_job` (or schedule that job). The job triggers
   **only** the ingestion pipeline — the gateway is continuous and is *not* a job task
   ([SQL Server ingestion docs](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline):
   *"You must run the gateway as a continuous pipeline"*; the scheduled job targets only the
   ingestion pipeline).
6. **Verify** with any SQL warehouse:
   `SELECT count(*) FROM <catalog>.yaif_sqlserver.<table>;`

Onboard more tables: add `table:` blocks (or one `schema:` block) to `objects:` and
redeploy — no new code.

**Cost & ops notes:**
- **The gateway is continuous** — once started it runs (and bills classic-compute DBUs)
  until stopped. Databricks recommends **not** stopping it in production: if it's down long
  enough for the source's change log to truncate, you must full-refresh the affected tables.
  For a demo you can stop it to halt cost (`databricks pipelines stop <gateway-id>`),
  accepting that trade-off. The scheduled job triggers **only** the ingestion pipeline.
- **Staging retention is 30 days** — the gateway → ingestion staging Volume keeps change
  data ~30 days by default; reprocessing further back requires a resnapshot.

**Table-count limit & sharding (verified against the docs).** Databricks recommends
**≤ 250 tables per ingestion pipeline** ([SQL Server connector limitations](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits)
— "Databricks recommends ingesting 250 or fewer tables per pipeline"; the
feature-availability table lists 250 as the maximum). A gateway and an ingestion
pipeline form a **pair** — the ingestion pipeline references exactly one gateway via
`ingestion_gateway_id` — so to go beyond ~250 tables you split the table list across
**multiple gateway-ingestion pairs**, each pair under the limit and all publishing into
the same `yaif_sqlserver` schema. The commented second pair in
`examples/sqlserver/orders_cdc.yml` shows the split. (Note: the docs describe scaling
via separate gateway-ingestion *pairs*, not one gateway feeding many ingestion
pipelines.)

## Playbook C — Files in cloud storage (Auto Loader)

Use this when a connector drops files into object storage and you own the
ingestion — e.g. **Arcosoft exporting SAP tables as `.parquet` into an
S3/ADLS/GCS bucket**. Unlike the API module (which lands payloads straight into a
Delta table), files genuinely arrive *as files in a bucket*, which is exactly what
Auto Loader (`cloudFiles`) is built for: incremental, exactly-once file discovery
with schema evolution. Inside SDP the schema location and checkpoint are managed
for you — there is no checkpoint to configure.

The shared medallion code is `src/files/` (bronze → silver → gold); a per-feed
domain unit is `examples/files/erp_parquet.yml`. Data flow:

```
  Connector (Arcosoft/SAP) ──► s3://bucket/arcosoft/*.parquet
                                        │  (UC external location + EXTERNAL volume)
                                        ▼
                              /Volumes/<cat>/yaif_erp/landing/
                                        │  Auto Loader (cloudFiles, format=parquet)
                                        ▼
                              bronze_cloud_files (STREAM)   + source-file lineage
                                        ▼
                              silver_cloud_files (STREAM)   quality + optional dedup
                                        ▼
                              gold_files_ingestion_health (MV)  files/rows/bytes/freshness
                              gold_files_rows_per_day     (MV)  daily volume trend
```

**To activate a feed:**

1. Register the bucket with Unity Catalog — a storage credential + external
   location over the prefix the connector writes to (full SQL, incl. ADLS/GCS
   variants, is in the header of `examples/files/erp_parquet.yml`).
2. Set `files_source_uri` in `databricks.yml` to that path (and `file_format` if
   not Parquet).
3. Move `examples/files/erp_parquet.yml` → `resources/files/erp_parquet.yml`
   (this is what brings it into the deploy glob) — no `databricks.yml` edits; the target
   `mode` (`development`/`production`) sets the pipeline's `development` flag automatically.
4. `databricks bundle deploy && databricks bundle run erp_ingestion_job`.
5. **Verify** with any SQL warehouse:
   `SELECT count(*), count(DISTINCT source_file) FROM <catalog>.yaif_erp.bronze_cloud_files;`
   and read `gold_files_ingestion_health` for files / rows / bytes / freshness.

> **Try it now without a bucket:** `databricks bundle run files_demo_seed_and_pipeline`
> runs this exact medallion against a MANAGED volume seeded with synthetic Parquet
> (`resources/files/demo.yml`). Same Auto Loader → SDP code as a real feed — only the
> volume type differs — so it's the fastest way to see the pattern work end to end.

Onboard another feed the same way you onboard an API domain: copy the domain
file, rename the schema/volume/pipeline/job keys, point `source_path` at the new
volume — the `src/files/` transformations are shared, zero code change. Set
`dedup_keys` in the pipeline `configuration` when a connector re-exports overlapping
windows (full + incremental): silver then dedups to current state via **AUTO CDC SCD
Type 1** (latest row per key, sequenced by `dedup_order_by` — defaults to the source
file modification time), the same bounded mechanism the API module uses. Leave
`dedup_keys` unset to keep every ingested row.

## Bonus: ad-hoc SQL access through the same connection

The connection powering the API pipeline is queryable by analysts directly:

```sql
SELECT http_request(conn => 'company_api', method => 'GET', path => '/orders').text;
```

One governed credential serves bulk pipeline ingestion, ad-hoc SQL exploration,
and per-principal access control with audit — zero credential duplication. This
is a Unity Catalog governance differentiator worth demoing in platform
comparisons.

> **Fallback — secret scopes:** if your workspace can't use HTTP connections yet
> (older DBR, missing privilege), swap the `get_with_retry` block for a plain
> `requests.Session` with `Authorization: Bearer {dbutils.secrets.get(scope, key)}` —
> the landing table contract and the SDP pipeline are identical either way.

> Agent/contributor notes — the dev/prod target conventions and the hard-won
> gotchas baked into these templates now live in `CLAUDE.md` (see "Gotchas that
> WILL bite you"). Read it before changing a pipeline, job, or path in this repo.
