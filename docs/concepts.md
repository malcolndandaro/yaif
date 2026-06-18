# Concepts

The ideas shared by every YAIF module. Read this once; the per-connector guides assume it.

## Umbrella repo, not an abstraction layer

YAIF is **one Asset Bundle** with independent modules per source type that share
conventions (naming, medallion structure, dev/prod targets, monitoring) but **no code
abstraction**. There is no metadata engine and no wrapper around managed connectors: where
Databricks already provides a declarative primitive (Lakeflow Connect, SDP), YAIF just
configures it.

- The **API** module has real code, because raw REST fan-out needs it.
- The **SQL Server** module is ~50 lines of Lakeflow Connect YAML.
- The **Files** module configures Auto Loader.

If you're tempted to build a "unified ingestion metadata layer," don't — that's the
anti-pattern this repo was designed against. Wrapping a managed connector in a custom
framework only adds indirection.

### Why not a notebook per source?

Because a bespoke notebook per source is exactly the thing that does not scale to dozens or
hundreds of sources. YAIF gives every source the same governed, observable,
environment-aware plumbing for free. The API module versus a typical hand-rolled notebook:

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

The SQL Server and files modules buy the same consistency without custom code — they
configure a managed Databricks primitive the same governed, dev/prod-aware way.

## Design principles

1. **Umbrella repo, not framework** — modules share conventions and deployment workflow,
   never code abstractions.
2. **One deployable unit per business domain** — each `resources/<module>/<feed>.yml` is a
   self-contained schema + pipeline + job. Failure isolation, independent schedules,
   per-team grants.
3. **Governed auth via Unity Catalog** — API credentials live in UC HTTP connections
   (granted, audited, OAuth M2M capable); database credentials live in UC connections used
   by Lakeflow Connect; secret-based credentials live in UC secret scopes. No secrets in code.

## The medallion

Every module lands data through the same bronze → silver → gold shape, each domain in its
own schema:

- **bronze** — streaming ingest of the raw payload, append-only, audit-safe. (API: the raw
  response body as STRING plus a `response_variant VARIANT`; files: the raw rows plus
  source-file lineage.)
- **silver** — parsed / cleaned / quality-checked current state, deduplicated to one row
  per key (see AUTO CDC below).
- **gold** — materialized views for monitoring: success/ingestion health and daily volume.

## Silver shapes (API module)

The API silver layer forks on a per-pipeline `silver_shape` configuration, because some
APIs return a flat array of records and others return one arbitrary nested document:

| `silver_shape` | Silver table | Shape |
|---|---|---|
| `records` (default) | `silver_api_records` | one row per record, exploded from an array, keyed `(endpoint, record_id)` |
| `document` | `silver_api_documents` | one VARIANT row per response, keyed `(endpoint, run_id)` — no per-record id needed |

Both `silver_api_records.py` and `silver_api_documents.py` are globbed by every API
pipeline, but each guards on `silver_shape` and no-ops when not selected, so only the
chosen table is created. `gold_api_records_per_day` follows the shape automatically;
`gold_api_endpoint_health` reads bronze, so it is shape-agnostic. See
[API ingestion](api-ingestion.md) for VARIANT navigation.

## AUTO CDC SCD Type 1 dedup

Where a source can re-deliver the same key (an API re-fetch, a connector re-exporting an
overlapping window, a SQL Server change feed), silver deduplicates to **current state**
using **AUTO CDC, SCD Type 1, keyed on the primary key** — the latest row per key wins,
sequenced by an ordering column:

- **API / files** — sequenced by an order-by column (files default: source file
  modification time, via `dedup_order_by`; set `dedup_keys` to enable, leave unset to keep
  every row).
- **SQL Server** — query-based sequences by the cursor column; CDC by the change sequence.

The mechanism and semantics are identical across modules — only the sequencing column differs.

## Activate-by-moving (`examples/` → `resources/`)

`resources/*/*.yml` is the **deploy glob** (note the module-subdir level — files sit one
dir deep under `resources/<module>/`). Anything matching it deploys on `bundle deploy`.

A feed that depends on infra that may not exist yet (a UC `SQLSERVER` connection, a UC
external location, real EPM creds) would fail `bundle validate`/`deploy` if it were
globbed. So its template lives in `examples/<module>/`, **outside** the glob. You
**activate it by moving** it: create the prerequisite, set the relevant vars, then move the
file into `resources/<module>/`. Never move one into the glob without its external
prerequisite in place.

The files module ships a self-contained demo (`resources/files/demo.yml`) that **is** in
the glob because it needs no external setup — a managed volume + a seeder.

## Catalogs, targets, profiles

- **Catalog** — `var.catalog` (default `yaif`) must already exist; the bundle creates the
  schemas inside it. Each domain gets its own schema (`yaif_content`, `yaif_sqlserver`, …).
- **Targets** — `dev` (`mode: development`) and `prod` (`mode: production`). The mode sets
  each pipeline's `development` flag and pauses/enables schedules automatically, so there
  are no per-pipeline `development:` overrides and onboarding needs no `databricks.yml` churn.
- **Profiles** — both targets default to the sandbox (profile `sqlserver-ws`). Override
  with `--profile <name>` or by editing `databricks.yml`.
- **Dev prefixing** — `mode: development` prefixes schema names `dev_<user>_<schema>`.

## Repo layout

```
yaif/
├── databricks.yml                    # bundle "yaif"; vars; targets dev/prod; include glob resources/*/*.yml
├── resources/                        # DEPLOY GLOB — what ships
│   ├── api/                          #   API module — ONE file per business domain
│   │   ├── content_domain.yml        #     schema yaif_content + pipeline + job (posts, comments, albums, photos) — GET, connection auth
│   │   ├── people_domain.yml         #     schema yaif_people  + pipeline + job (users, todos) — GET, connection auth
│   │   └── echo_post_demo.yml        #     data-safe POST+body+Basic+VARIANT demo (postman-echo; silver_shape=document)
│   └── files/
│       └── demo.yml                  #   files module self-contained demo (MANAGED volume + synthetic seeder) — in glob
├── examples/                         # ACTIVATE-BY-MOVING units — OUTSIDE the glob (need external setup)
│   ├── api/epm_domain.yml            #   Oracle EPM exportdataslice template — CUSTOMER-RUN-ONLY (basic_secret + POST + silver_shape=document)
│   ├── api/control_table.{csv,sql}   #   API endpoint control table (incl. optional body/auth_mode/silver_shape columns)
│   ├── api/generated_sample/blog.yml #   committed example of one generated domain YAML
│   ├── sqlserver/orders_cdc.yml      #   Lakeflow Connect CDC: continuous gateway + ingestion + job
│   ├── sqlserver/orders_query.yml    #   Lakeflow Connect QUERY-BASED: cursor-driven ingestion + scheduled job, NO gateway
│   └── files/erp_parquet.yml         #   real file feed: schema + EXTERNAL volume + pipeline + job
├── scripts/
│   ├── generate_api_domains.py       #   control table → one resources/api/<domain>.yml per domain
│   └── README.md                     #   the control-table → generate → deploy round-trip
└── src/                              # SHARED module source — never copied per-domain
    ├── jobs/
    │   ├── fetch_api_responses.py    #   API: threaded UC-connection fetch → Delta landing table (method/body/auth_mode aware)
    │   └── seed_demo_parquet.py      #   files demo: writes synthetic Parquet into the demo volume
    ├── transformations/              #   API SDP pipeline source (raw .py); pipeline globs ../../src/transformations/**
    │   ├── bronze_api_responses.py   #     streaming from landing table (+ response_variant VARIANT via try_parse_json)
    │   ├── silver_api_records.py     #     records shape: JSON parse + explode + quality
    │   ├── silver_api_documents.py   #     document shape: one VARIANT row per response
    │   └── gold_api_metrics.py       #     MVs: endpoint health (bronze), daily counts (silver, follows silver_shape)
    └── files/                        #   FILES SDP pipeline source; pipeline globs ../../src/files/**
        ├── bronze_cloud_files.py     #     Auto Loader cloudFiles stream from a UC Volume + file lineage
        ├── silver_cloud_files.py     #     quality (rescued-data) + optional dedup_keys
        └── gold_cloud_files.py       #     MVs: ingestion health, rows/day
```

> Paths inside `resources/<module>/*.yml` are relative to the YAML file — one level deep,
> so they use `../../src/...` (two levels up). See [Troubleshooting](troubleshooting.md) if
> you add a new module subdir.
