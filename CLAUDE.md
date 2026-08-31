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
  medallion source), `src/shared/` (pipeline-agnostic transform functions). A new
  domain/feed reuses this code unchanged — if you find yourself copying a `src/` file
  per domain, you're doing it wrong.
- **The transform pattern is the DEFAULT idiom for medallion code.** Every non-trivial
  DataFrame chain is built from named, single-purpose `DataFrame -> DataFrame` functions
  in `src/shared/`, applied with `.transform(...)`:

  ```python
  return (spark.readStream.table("bronze_api_responses")
          .transform(parse_json_records)
          .transform(explode_records)
          .transform(project_record_columns))
  ```

  Rules: one job per function, typed `(df: DataFrame) -> DataFrame`, a docstring saying
  *why*, and **no** `spark`, `spark.conf`, or `dbutils` reference — that is what keeps
  them unit-testable in `tests/` without a workspace. Parameterized transforms take
  keyword args and are applied with a lambda (`.transform(lambda d: recent_ingest_days(d, days=30))`).
  Do NOT apply the pattern to cohesive one-off configuration (the Auto Loader `.option()`
  chain) or to `groupBy().agg()` blocks — wrapping those hurts readability. And do NOT
  grow `src/shared/` into a framework: plain functions only, no registry, no dispatch,
  no base classes.
- **Anything in `src/shared/` needs a test in `tests/`.** See `tests/README.md` (needs a
  dedicated venv — a global `databricks-connect` provides `pyspark` but refuses local
  sessions).
- **One deployable unit per feed = one domain YAML.** Each
  `resources/<module>/<feed>.yml` is a self-contained schema + pipeline + job.
  Onboarding a feed is a file copy + a few field edits, never a code change.
- **The deploy glob is one `include:` line per SHIPPING module** (`resources/api/*.yml`,
  `resources/files/*.yml`) — deliberately NOT a blanket `resources/*/*.yml`. The
  sqlserver gateway is `continuous: true` and bills until stopped, so a stray copy under
  `resources/sqlserver/` must never deploy just by existing. Activating that module is
  TWO steps: move the file AND uncomment its `include:` line. Files sit one dir deep
  under `resources/<module>/`.
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
│   │   ├── content_domain.yml        #   schema yaif_content + pipeline + job (posts, comments, albums, photos) — GET, connection auth
│   │   ├── people_domain.yml         #   schema yaif_people  + pipeline + job (users, todos) — GET, connection auth
│   │   └── echo_post_demo.yml        #   data-safe POST+body+Basic-auth+VARIANT demo (postman-echo, mock creds; silver_shape=document)
│   ├── files/
│   │   └── demo.yml                  # files module self-contained demo (MANAGED volume + synthetic seeder) — in glob, deploys cleanly
│   └── zerobus/
│       └── demo.yml                  # zerobus module self-contained demo (SDK push -> SDP medallion) — in glob; push needs the secret scope
├── examples/                         # activate-by-moving units — OUTSIDE the include glob (need external setup)
│   ├── api/epm_domain.yml            #   Oracle EPM exportdataslice template — CUSTOMER-RUN-ONLY (basic_secret + POST + silver_shape=document, placeholder host)
│   ├── api/control_table.{csv,sql}   #   API endpoint control table (now incl. optional body/auth_mode/silver_shape columns)
│   ├── sqlserver/orders_cdc.yml      #   Lakeflow Connect CDC: continuous gateway + ingestion + job (needs a UC SQLSERVER connection + CDC/CT on source)
│   ├── sqlserver/orders_query.yml    #   Lakeflow Connect QUERY-BASED: cursor-driven ingestion + scheduled job, NO gateway (use when source can't enable CDC/CT)
│   └── files/erp_parquet.yml         #   real file feed: schema + EXTERNAL volume + pipeline + job (needs a UC external location)
├── tests/                            # pytest over src/shared/** (local SparkSession, no workspace)
└── src/                              # SHARED module source — never copy per-domain
    ├── shared/                       # TRANSFORM-PATTERN helpers — OUTSIDE both pipeline globs (gotcha #10)
    │   ├── ingest_columns.py         #   with_ingest_stamps, recent_ingest_days (used by BOTH modules)
    │   ├── api_records.py            #   parse_json_records / explode_records / project_record_columns
    │   ├── api_documents.py          #   with_response_variant, project_document_columns
    │   └── file_lineage.py           #   with_source_file_lineage (_metadata -> real columns)
    ├── jobs/
    │   ├── fetch_api_responses.py    # API: threaded UC-connection fetch -> Delta landing table
    │   ├── seed_demo_parquet.py      # files demo: writes synthetic Parquet into the demo volume (stands in for a connector)
    │   ├── prepare_zerobus_table.py  # zerobus demo: creates the bronze table + grants the producer SP (Zerobus never creates tables)
    │   └── push_zerobus_events.py    # zerobus demo: the SDK producer — batch push with durability ACKs (classic cluster only)
    ├── transformations/              # API SDP pipeline source (raw .py, NOT notebooks); pipeline globs ../../src/transformations/**
    │   ├── bronze_api_responses.py   #   streaming read from landing table (+ response_variant VARIANT via try_parse_json)
    │   ├── silver_api_records.py     #   records shape (default): JSON parse + explode + quality; guards on silver_shape=="records"
    │   ├── silver_api_documents.py   #   document shape: one VARIANT row per response, keyed (endpoint, run_id); guards on silver_shape=="document"
    │   └── gold_api_metrics.py       #   2 MVs: endpoint health (bronze), daily counts (silver, follows silver_shape)
    ├── files/                        # FILES SDP pipeline source; pipeline globs ../../src/files/** (sibling of transformations/, so API glob never picks it up)
    │   ├── bronze_cloud_files.py     #   Auto Loader cloudFiles stream from a UC Volume + file lineage
    │   ├── silver_cloud_files.py     #   quality (rescued-data) + optional dedup_keys
    │   └── gold_cloud_files.py       #   2 MVs: ingestion health, rows/day
    └── zerobus/                       # ZEROBUS SDP pipeline source; pipeline globs ../../src/zerobus/**
        ├── silver_zerobus_events.py   #   wire BIGINT micros -> TIMESTAMP, AUTO CDC SCD1 on event_id (at-least-once dedup)
        └── gold_zerobus_metrics.py    #   2 MVs: ingestion health (duplicate_rate from bronze), events/day by device
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
- ✅ **Zerobus module (streaming push): built & verified end-to-end** (2026-08-31, workspace
  `dbc-ea097edd-802e`). Demo `resources/zerobus/demo.yml`: SDK producer pushes synthetic
  IoT events into a pre-created bronze table with durability ACKs; SDP medallion
  (`src/zerobus/`) dedups at-least-once re-deliveries via AUTO CDC SCD1 on `event_id`.
  Verified: bronze 1,020 wire arrivals (1,000 events + 2% injected re-deliveries) ->
  silver exactly 1,000; `gold_zerobus_ingestion_health.duplicate_rate` = 0.0196.
  One-time prereqs (SP `yaif-zerobus-producer` + OAuth secret + UC secret scope
  `yaif_zerobus`) are provisioned on this workspace; `prepare` creates the bronze table +
  grants the SP. Producer task = classic single-node cluster (SDK can't pip-install on
  serverless — gotcha #12); Protobuf is the recommended production serialization
  (see `docs/zerobus.md`). Run: `databricks bundle run zerobus_demo_push_and_pipeline -t dev`.

## Environment / how to test

- **Default workspace = profile `dbc-ea097edd-802e`** (https://dbc-ea097edd-802e.cloud.databricks.com,
  workspace id 7474648509963227, us-west-2), catalog `yaif` (the `var.catalog` default) —
  a UC-enabled workspace with serverless jobs + pipelines. This replaced the earlier
  sandbox (profile `sqlserver-ws` = dbc-f9cc83ac-844b), which has been RETIRED — the SQL
  Server gateway that lived there is gone with it (the `examples/sqlserver/` templates
  remain valid; re-verify them on a workspace that has the UC connection). The zerobus
  module's one-time prereqs (SP `yaif-zerobus-producer` + OAuth secret + UC secret scope
  `yaif_zerobus`) are set up in this workspace. Point at a different workspace by editing
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
9. **A UC HTTP connection CANNOT carry clean HTTP Basic auth** (runtime-verified on the
   sandbox). It force-prefixes `Bearer ` to its credential, and a custom `Authorization`
   header passed to `http_request(... headers=...)` is **merged/prepended** with the
   connection's own auth (→ a malformed `Bearer dummy,Basic …`), not sent clean. You
   also cannot create a host-only (no-auth) connection — an auth option is mandatory at
   create time. Non-auth custom headers (`Content-Type`, `X-*`) DO pass through. ⇒ For
   Basic-auth APIs (Oracle EPM `exportdataslice`), the fetch job uses
   **`auth_mode: basic_secret`** — direct Python `requests` building
   `Authorization: Basic base64(user:pass)` from `dbutils.secrets.get(scope, key)`,
   bypassing the proxy. `auth_mode: connection` (the default, content/people) keeps using
   `serving_endpoints.http_request` for Bearer/OAuth. The fetch job is now method/body
   aware in BOTH modes (POST + JSON body work through the connection too — only custom
   *auth* is stripped). VARIANT-shaped (semi-structured) responses land via
   `silver_shape: document` (`silver_api_documents`) instead of the default
   `silver_shape: records` (`silver_api_records`); both silver files guard on the
   pipeline `silver_shape` config and no-op when not selected. The data-safe demo of all
   of this is `resources/api/echo_post_demo.yml`; the real EPM template (out of glob,
   customer-run-only) is `examples/api/epm_domain.yml`. See README "Playbook A2".
10. **`src/shared/` MUST stay outside `src/transformations/**` and `src/files/**`.** Those
    two are the pipeline `libraries.glob` patterns — anything inside them is loaded as
    pipeline SOURCE. The shared transforms define no datasets, so they belong outside;
    a helper dropped into `src/transformations/` gets loaded as a (dataset-less) source
    file and muddies the convention. This is the same rule as "keep the seeder in
    `src/jobs/`, never `src/files/`" (see the layout tree).
    What makes the import work is **`root_path: ../../src` on every pipeline** — Databricks
    appends the pipeline root to `sys.path` when executing Python sources, so
    `from shared.ingest_columns import with_ingest_stamps` resolves. If you add a new
    pipeline resource, set `root_path` or every `shared.*` import fails at runtime.
    (`root_path` is Public Preview; it is also what makes a pipeline editable in the
    Lakeflow multi-file editor.)
11. **`.transform()` needs no SDP plumbing** — it is plain PySpark `DataFrame` API and
    works inside `@dp.table` / `@dp.temporary_view` today. Only *cross-directory imports*
    need `root_path` (#10). Don't conflate the two: an in-file helper needs nothing.
12. **Zerobus: the SDK cannot pip-install on serverless** — the producer task must run on
    a classic job cluster (`num_workers: 0` + `data_security_mode: SINGLE_USER` is the
    modern single-node recipe). Do NOT add `spark.databricks.cluster.profile: singleNode`
    to the spark_conf: the Jobs API REJECTS it with 400 `INVALID_PARAMETER_VALUE` when an
    access mode is set ("Spark Conf: ... is not allowed when choosing an access mode").
    `databricks bundle validate` still WARNS that single-node needs that conf — cosmetic,
    deploy works; the cluster is genuinely single-node via `num_workers: 0`.
13. **Zerobus auth + wire types**: producers authenticate with a service principal that
    needs EXPLICIT table-level `MODIFY` + `SELECT` (schema-inherited grants fail with
    error 4024 — the demo's `prepare` task grants them from the secret scope). Delta
    `TIMESTAMP` travels as int64 epoch MICROSECONDS, so bronze stores
    `event_time`/`ingested_at` as BIGINT and silver casts them
    (`src/shared/zerobus_events.py`). Delivery is at-least-once: silver is AUTO CDC SCD1
    keyed on `event_id`, and `gold_zerobus_ingestion_health.duplicate_rate` is the
    duplicate-traffic signal.

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

- Human-facing guides now live in `docs/` (quickstart, concepts, api-ingestion, files,
  sqlserver, oracle-epm, troubleshooting); `README.md` is the slim landing page that links into them.
- See `README.md` for the full onboarding guide, design rationale, the 900-API
  scaling playbook, and the per-module playbooks (A: API, B: SQL Server, C: files).
