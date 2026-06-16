# CLAUDE.md — YAIF (Yet Another Ingestion Framework)

Context for AI agents working in this repo. Read this first.

## What this is

YAIF is a **customer-agnostic, reusable Databricks ingestion umbrella repo**. It is
an Asset Bundle (DAB) holding independent ingestion modules per source type that
share conventions (naming, medallion structure, dev/prod targets, monitoring) but
**no code abstraction**. The full user-facing guide is in `README.md` — read it for
the onboarding flow, design rationale, and verified results.

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
│   ├── sqlserver/orders_cdc.yml      #   Lakeflow Connect gateway + ingestion + job (needs a UC SQLSERVER connection)
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

- **Any UC-enabled workspace** with serverless jobs **and** serverless pipelines in
  the region, and a Unity Catalog metastore assigned to the workspace. Set
  `var.catalog` to a catalog that exists (default `main`).
- **CLI profile:** the dev/prod targets use profile `DEFAULT`. Point them at your own
  workspace by editing `databricks.yml`, or override per command with
  `--profile <your-profile>`. Dev mode prefixes schemas as `dev_<user>_<schema>`.
- **Demo connection** must exist before the first API run:
  `CREATE CONNECTION IF NOT EXISTS yaif_demo_api TYPE HTTP OPTIONS (host 'https://jsonplaceholder.typicode.com', port '443', base_path '/', bearer_token 'unused');`
- **Deploy/run loop:**
  ```bash
  databricks bundle validate
  databricks bundle deploy   --auto-approve
  databricks bundle run content_fetch_and_pipeline --no-wait
  databricks bundle run people_fetch_and_pipeline  --no-wait
  databricks bundle run files_demo_seed_and_pipeline    # files module demo
  ```
- **Verify** counts with any SQL warehouse against the deployed schemas
  (`<catalog>.<schema>.silver_*` etc.).
- Private-network sources (no public egress) need serverless network connectivity
  (NCC / PrivateLink) configured on the workspace before the gateway can reach them.

## Gotchas that WILL bite you (all learned the hard way; also in README)

1. `development: true` belongs in **target overrides** in `databricks.yml`, never in
   the pipeline resource — else prod runs in dev mode with retries off.
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

## How to onboard a new API domain

Copy `resources/api/content_domain.yml` -> rename resource keys + schema name + job
name, set `api_endpoints`, deploy. ~60 lines of YAML, zero code change. Beyond ~20
domains, generate resources via Python for DABs instead of hand-copying.

## Related

- See `README.md` for the full onboarding guide, design rationale, and verified
  results across all three modules.
