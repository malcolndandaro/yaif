# CLAUDE.md — YAIF (Yet Another Ingestion Framework)

Context for AI agents working in this repo. Read this first.

## What this is

YAIF (Yet Another Ingestion Framework) is a **config-driven framework to accelerate
ingestion at scale** on the Databricks Lakehouse — a customer-agnostic, reusable
Asset Bundle (DAB). The goal: turn "we have hundreds of sources to land in the
lakehouse" into a repeatable, *copy-one-file-per-source* (or one-row-in-a-control-
table) workflow instead of a bespoke notebook per source. The motivating case is a
customer with **~900 REST APIs**: you never hand-build 900 jobs — you group endpoints
into ~45 domains, drive the endpoint lists from a control table, and **generate** the
per-domain resources. Onboarding a source writes **zero** new framework code.

It holds independent ingestion modules per source type that share conventions
(naming, medallion structure, dev/prod targets, monitoring) but **no code
abstraction**. The full user-facing guide is in `README.md` — read it for the
onboarding flow, the 900-API scaling playbook, and the per-module playbooks.
Verified end-to-end results live in **Current status** below.

**Origin & ownership:** built by Malcoln Dandaro (Databricks SSA). It is deliberately
scrubbed of all customer references so it can be reused with any customer. Do not
introduce a specific customer name into repo file contents — keep schemas, jobs, and
connections generically named (`yaif_*`, `company_api`, `sqlserver_conn`, etc.), and
keep workspace identifiers (hosts, IDs, warehouse IDs, IPs) out of committed files.

## Core design decision (do not violate)

**Umbrella repo, NOT a framework abstraction.** Modules sit side by side and share
conventions only. Where Databricks already provides a declarative primitive
(Lakeflow Connect, SDP), YAIF just configures it — it never wraps a managed
connector in custom Python. The API module has real code because raw REST fan-out
needs it; the SQL Server module is pure Lakeflow Connect YAML. If you're tempted to
build a "unified ingestion metadata layer," stop — that's the anti-pattern this repo
was explicitly designed against.

## Repo conventions (follow these when adding anything)

- **Shared code lives in `src/`, never copied per-feed.** `src/jobs/` (Python job
  tasks), `src/transformations/` (API SDP medallion source), `src/files/` (files SDP
  medallion source). A new domain/feed reuses this code unchanged — if you find
  yourself copying a `src/` file per domain, you're doing it wrong.
- **One deployable unit per feed = one domain YAML.** Each
  `resources/<module>/<feed>.yml` is a self-contained schema + pipeline + job.
  Onboarding a feed is a file copy + a few field edits, never a code change.
- **`resources/*/*.yml` is the deploy glob** (note the module-subdir level — files sit
  one dir deep under `resources/<module>/`). Anything matching it deploys on
  `bundle deploy`.
- **Modules that need external setup live in `examples/`, OUTSIDE the glob, and are
  "activated by moving."** A feed that depends on infra that may not exist yet (a UC
  `SQLSERVER` connection, a UC external location) would fail `bundle validate`/`deploy`
  if it were globbed. Its template lives in `examples/<module>/`; activate it by
  creating the prerequisite, setting the relevant vars, and moving the file into
  `resources/<module>/`. Never move one into the glob without its external prerequisite
  in place. (The files module ships a self-contained demo, `resources/files/demo.yml`,
  that IS in the glob because it needs no external setup — a managed volume + seeder.)

## Repo layout

```
yaif/
├── databricks.yml                    # bundle name "yaif"; vars (catalog, api_connection, …); targets dev/prod
│                                     # include glob is resources/*/*.yml (note the module subdir level)
├── resources/
│   ├── api/                          # API module — ONE file per business domain
│   │   ├── content_domain.yml        #   schema yaif_content + pipeline + job (posts, comments, albums, photos)
│   │   └── people_domain.yml         #   schema yaif_people  + pipeline + job (users, todos)
│   └── files/
│       └── demo.yml                  # files module self-contained demo (MANAGED volume + synthetic seeder) — in glob, deploys cleanly
├── examples/                         # activate-by-moving units — OUTSIDE the include glob (need external setup)
│   ├── sqlserver/orders_cdc.yml      #   Lakeflow Connect CDC: continuous gateway + ingestion + job (needs a UC SQLSERVER connection + CDC/CT on source)
│   ├── sqlserver/orders_query.yml    #   Lakeflow Connect QUERY-BASED: cursor-driven ingestion + scheduled job, NO gateway (use when source can't enable CDC/CT)
│   └── files/erp_parquet.yml         #   real file feed: schema + EXTERNAL volume + pipeline + job (needs a UC external location)
└── src/                              # SHARED module source — never copy per-domain
    ├── jobs/
    │   ├── fetch_api_responses.py    # API: threaded UC-connection fetch -> Delta landing table
    │   └── seed_demo_parquet.py      # files demo: writes synthetic Parquet into the demo volume (stands in for a connector)
    ├── transformations/              # API SDP pipeline source (raw .py, NOT notebooks); pipeline globs ../../src/transformations/**
    │   ├── bronze_api_responses.py   #   streaming read from landing table
    │   ├── silver_api_records.py     #   JSON parse + explode + quality expectations
    │   └── gold_api_metrics.py       #   2 MVs: endpoint health, daily counts
    └── files/                        # FILES SDP pipeline source; pipeline globs ../../src/files/** (sibling of transformations/, so API glob never picks it up)
        ├── bronze_cloud_files.py     #   Auto Loader cloudFiles stream from a UC Volume + file lineage
        ├── silver_cloud_files.py     #   quality (rescued-data) + optional dedup_keys
        └── gold_cloud_files.py       #   2 MVs: ingestion health, rows/day
```

## Current status

- ✅ **API module: built & verified end-to-end.** Both demo domains (`content`,
  `people`) deploy and run in parallel; counts exact (content silver=5,700,
  people silver=210); incremental streaming, success_rate 1.0, and 1MB payloads
  through the UC connection all verified.
- ✅ **Auth = UC HTTP Connection** (`yaif_demo_api` -> jsonplaceholder for the demo).
  Default path, not optional. Uses SDK
  `WorkspaceClient().serving_endpoints.http_request(conn=...)`.
- ✅ **SQL Server module: built & verified end-to-end** against SQL Server 2022 via
  Lakeflow Connect (gateway TLS-connects to the source; ingestion applies tables into
  UC; a source UPDATE was captured through Change Tracking). Template lives in
  `examples/sqlserver/orders_cdc.yml` (gateway + ingestion + job; gateway/ingestion
  pipelines need top-level `catalog`/`schema` fields). It is OUTSIDE the include glob
  because it needs a UC SQLSERVER connection; activate by setting the
  `sqlserver_connection` / `sqlserver_source_database` vars and moving the file into
  `resources/sqlserver/`. The gateway is CONTINUOUS when deployed (always runs/bills
  until stopped — `databricks pipelines stop <id>`).
  - **Two SQL Server patterns, pick by source constraint** (both Lakeflow Connect, both
    out-of-glob in `examples/sqlserver/`): **CDC** (`orders_cdc.yml`, continuous gateway +
    triggered ingestion) when the source can enable CDC/Change Tracking and you need full
    change/delete history; **query-based** (`orders_query.yml`, NO gateway, scheduled
    cursor-driven pulls) when the source CANNOT enable CDC/CT. Query-based **REQUIRES a
    monotonic _modified_ cursor column per table**
    (`table_configuration.query_based_connector_config.cursor_columns`) that advances on
    every INSERT **and UPDATE** — a `ModifiedDate`/`last_updated` timestamp or `rowversion`.
    An identity/auto-increment PK as the cursor is **insert-only** → it SILENTLY MISSES
    UPDATEs to existing rows; keep the PK as `primary_keys` (for SCD dedup), never as the
    cursor. Pair the cursor with `primary_keys` + `scd_type: SCD_TYPE_1` (current-state
    dedup keyed on the PK, the same semantics as the API/files AUTO CDC silver).
    Query-based deletes are API-only (soft `deletion_condition` GA; hard-delete Beta) and
    it captures latest-state-per-run, not every change. **The demo now models this best
    practice:** the `DemoDB` seeder (in the `demo-environments` repo —
    `environments/sqlserver/app/setup.sql` + `migrate_add_modifieddate.sql`) adds a
    `ModifiedDate DATETIME2` column (DEFAULT `SYSUTCDATETIME()` on insert + AFTER UPDATE
    trigger to bump it), and `orders_query.yml` uses `ModifiedDate` as the cursor. See
    README "Playbook B → CDC vs query-based".
- ✅ **Files module (Auto Loader): built & verified end-to-end.** Shared medallion in
  `src/files/` (cloudFiles bronze -> silver -> gold). Verified via the self-contained
  demo `resources/files/demo.yml` (MANAGED volume + synthetic Parquet seeder
  `src/jobs/seed_demo_parquet.py`): seeded 2 Parquet files / 100 rows -> bronze=100,
  silver=100, gold files_ingested=2 / rows=100, source-file lineage + freshness
  populated. Run it with `databricks bundle run files_demo_seed_and_pipeline`.
  - This is the SAP-via-connector Parquet-drop path (a connector lands .parquet to
    S3/ADLS/GCS).
  - **Real feed** = `examples/files/erp_parquet.yml`: OUTSIDE the include glob, uses an
    EXTERNAL volume (needs a UC external location, so it would block `bundle deploy`
    until set up — same "activate by moving" treatment as sqlserver). Activation steps
    in its header + README "Adding the files module".
  - `src/files/**` is globbed by the files pipeline; keep non-pipeline code (the seeder)
    in `src/jobs/`, never under `src/files/`.

## Environment / how to test

- **Default workspace = the SANDBOX.** Both `dev` and `prod` targets point at the
  sandbox via profile `sqlserver-ws` (https://dbc-f9cc83ac-844b.cloud.databricks.com),
  catalog `yaif` (the `var.catalog` default). The sandbox is a UC-enabled workspace with
  serverless jobs + pipelines and the `yaif` catalog already created; the live SQL Server
  Lakeflow gateway also lives there (deployed out-of-band; kept as `examples/`, not in the
  deploy glob — do not redeploy/touch it). Point at a different workspace by editing
  `databricks.yml` or overriding per command with `--profile <name> --var catalog=<cat>`.
  Dev mode prefixes schemas as `dev_<user>_<schema>`.
- **Demo connection** must exist in the target workspace before the first API run:
  `CREATE CONNECTION IF NOT EXISTS yaif_demo_api TYPE HTTP OPTIONS (host 'https://jsonplaceholder.typicode.com', port '443', base_path '/', bearer_token 'unused');`
- **Deploy/run loop** (targets the sandbox by default):
  ```bash
  databricks bundle validate -t dev
  databricks bundle deploy -t dev
  databricks bundle run content_fetch_and_pipeline -t dev --no-wait
  databricks bundle run people_fetch_and_pipeline  -t dev --no-wait
  databricks bundle run files_demo_seed_and_pipeline -t dev    # files module demo
  ```
- **Verify** counts with any SQL warehouse against the deployed schemas
  (`<catalog>.<schema>.silver_*` etc.).
- Private-network sources (no public egress) need serverless network connectivity
  (NCC / PrivateLink) configured on the workspace before the gateway can reach them.

## Gotchas that WILL bite you (all learned the hard way — CLAUDE.md is their canonical home)

1. Never hardcode `development: true` in a pipeline *resource* — the targets'
   `mode: development` / `mode: production` set the `development` flag per target
   automatically (dev → `true`, prod → validated `false`). Baking it into the resource
   is redundant in dev and **breaks `mode: production` validation** (prod validates
   `development: false`). This is also why onboarding a domain needs zero `databricks.yml`
   edits — no per-pipeline overrides to add.
2. Pipeline `schema:` must reference `${resources.schemas.<key>.name}` (resolves the
   dev-prefixed name), NOT a plain `${var.schema}`.
3. SDP `@dp.expect*` conditions evaluate against the **output** dataframe — they can
   only reference columns that survive the final `select`.
4. Land API payloads **straight into a Delta table**, not Volume + Auto Loader — the
   empty-dir schema-inference path failed repeatedly on serverless. (We tried Volumes
   first; don't go back.) **Scope: this rule is API-ONLY.** The files module
   (`src/files/`) legitimately uses Auto Loader because the source genuinely *is*
   files in a bucket — that is what cloudFiles is for. The failure above was about
   forcing tiny API JSON payloads through a Volume; it does not apply to real Parquet
   feeds. Don't "fix" the files module by removing Auto Loader. (One caveat that IS
   shared: Auto Loader on an *empty* directory can't infer a schema — point a feed at
   a path that already has at least one file, or expect the first update to no-op.)
5. **Serverless preinstalls `databricks-sdk`** and pip silently skips an upgrade if
   the floor is already satisfied. Pin a floor NEWER than preinstalled (`>=0.50.0`)
   or `serving_endpoints.http_request()` won't exist. This is the #1 thing that ate
   hours.
6. `ExternalFunctionRequestHttpMethod` import location moves between SDK versions —
   the fetch job imports it with a try/except string fallback to `"GET"`.
7. Paths in `resources/<module>/*.yml` are relative to the YAML file. The files live
   one level deep under `resources/<module>/`, so they use `../../src/...` (two
   levels), not `../src/...`. If you add a new module subdir, match this.
8. **Lakeflow Connect SQL Server: ≤250 tables per ingestion pipeline.** Databricks'
   verified recommendation / feature-availability maximum is 250 tables *per ingestion
   pipeline* (https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits
   — "Databricks recommends ingesting 250 or fewer tables per pipeline"). A gateway and
   an ingestion pipeline are a PAIR — the ingestion pipeline references exactly one
   gateway via `ingestion_gateway_id`. To exceed ~250 tables, shard the table list
   across **multiple gateway-ingestion pairs** publishing into the same schema; the docs
   do NOT document one gateway feeding many ingestion pipelines, so don't assume it.
   The commented second pair in `examples/sqlserver/orders_cdc.yml` shows the split.

## How to onboard a new API domain

Copy `resources/api/content_domain.yml` -> rename resource keys + schema name + job
name, set `api_endpoints`, deploy. ~60 lines of YAML, zero code change. Beyond a
handful of domains, don't hand-copy: keep all endpoints in a control table
(`examples/api/control_table.csv` for local, or the UC table in
`examples/api/control_table.sql`) and generate one domain YAML per domain with
`scripts/generate_api_domains.py` (it writes to a preview dir `build/generated_api/`,
NOT `resources/` — review, then move the files you want into `resources/api/` and
deploy). A committed sample of the generated output is at
`examples/api/generated_sample/blog.yml`.

## Related

- See `README.md` for the full onboarding guide, design rationale, the 900-API
  scaling playbook, and the per-module playbooks (A: API, B: SQL Server, C: files).
