# Quickstart

This expands the [README](../README.md) Quickstart: prerequisites, how the bundle is
wired (profiles / catalog / variables), the two data-safe demos you can run immediately,
and a pointer to the full real-source walkthrough.

The loop is always the same:

1. **Set up once** (~5 min) — CLI, a catalog, run a demo to confirm the plumbing.
2. **Pick your source type** and follow its guide.
3. **Repeat per source / domain** — each new source is a file copy + a few field edits
   (or one row in a control table). You never write new framework code.

## Prerequisites

| Requirement | Detail |
|---|---|
| Databricks CLI | `>= v1.0` (`databricks --version`), authenticated profile (`databricks auth login`) |
| Workspace | Unity Catalog enabled; serverless jobs **and** serverless pipelines available in the region |
| Permissions | `USE CATALOG` + `CREATE SCHEMA` on the target catalog; `CREATE CONNECTION` on the metastore (or ask an admin for the connection + `USE CONNECTION` grant) |
| Network | Workspace egress must reach your API gateway / database. Private sources (no public egress) need serverless network connectivity — NCC / PrivateLink — configured before a pipeline or gateway can reach them |
| Python deps | None to install — `tenacity` + `databricks-sdk` (+ `requests` for `basic_secret` APIs) are declared in each domain job's serverless `environment` spec |
| `http_request` | DBR 15.4+ / SQL warehouse 2023.40+ (already true on serverless) |

## How the bundle is wired

- **Targets.** `dev` (`mode: development`) and `prod` (`mode: production`). The target
  `mode` sets each pipeline's `development` flag automatically — dev → `true`, prod →
  validated `false` — so onboarding a source needs **zero** `databricks.yml` edits.
- **Profiles + catalog.** Both targets default to the **sandbox** workspace (profile
  `sqlserver-ws`, catalog `yaif`, the `var.catalog` default). Point at your own workspace
  by editing `databricks.yml`, or per command with `--profile <name> --var catalog=<cat>`.
- **Dev schema prefixing.** In `mode: development`, schema names are prefixed
  `dev_<user>_<schema>` (e.g. `dev_jane_yaif_content`). Account for this in verify queries.
- **Variables** (set in `databricks.yml` or via `--var`): `catalog`, `api_connection`
  (Bearer/OAuth HTTP connection), `api_base_url` / `api_secret_scope` (basic_secret APIs),
  `epm_host` / `epm_secret_scope` (Oracle EPM template), `request_concurrency`,
  `viewers_group`, `files_source_uri` / `file_format`, `sqlserver_connection` /
  `sqlserver_source_database`. See `databricks.yml` for descriptions and defaults.

See [Concepts](concepts.md) for the medallion layers, silver shapes, and the
"activate-by-moving" model.

## A — Run the data-safe demos (no external setup)

### Files demo (zero setup)

Seeds synthetic Parquet into a **managed** Volume and runs the full Auto Loader →
bronze/silver/gold medallion — proving deploy + serverless + SDP work before you point at
anything real.

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run files_demo_seed_and_pipeline -t dev
```

Verify (any SQL warehouse — remember the dev prefix):

```sql
SELECT count(*) FROM <catalog>.yaif_files_demo.bronze_cloud_files;   -- 100
SELECT count(*) FROM <catalog>.yaif_files_demo.silver_cloud_files;   -- 100
SELECT * FROM <catalog>.yaif_files_demo.gold_files_ingestion_health; -- files=2, rows=100
```

### API demo (one connection)

The public test API needs no real credential, but the UC HTTP connection must exist:

```sql
CREATE CONNECTION IF NOT EXISTS yaif_demo_api TYPE HTTP
OPTIONS (host 'https://jsonplaceholder.typicode.com', port '443',
         base_path '/', bearer_token 'unused');
```

Then deploy and run the two demo domains (6 endpoints, each with its own isolated medallion):

```bash
databricks bundle deploy -t dev
databricks bundle run content_fetch_and_pipeline -t dev   # posts, comments, albums, photos
databricks bundle run people_fetch_and_pipeline  -t dev   # users, todos
```

Verify: `SELECT count(*) FROM <catalog>.yaif_content.silver_api_records;` (5,700) and
`yaif_people.silver_api_records` (210).

### POST / body / Basic-auth / VARIANT demo (postman-echo)

The data-safe proof of the path Oracle EPM uses — POST a JSON body with HTTP Basic auth
and land arbitrary nested JSON as a VARIANT. Uses a **public mock** (postman-echo) and
mock creds; no customer data.

```bash
databricks secrets create-scope yaif_api
databricks secrets put-secret yaif_api mock_username --string-value postman
databricks secrets put-secret yaif_api mock_password --string-value password
databricks bundle deploy -t dev
databricks bundle run echo_post_demo_fetch_and_pipeline -t dev
```

It POSTs an EPM-shaped `gridDefinition` to `postman-echo.com/post`, lands the echoed nested
JSON as a VARIANT, and builds `silver_api_documents` + gold — proving POST + body + Basic +
VARIANT end-to-end with zero customer data. Details: [API ingestion](api-ingestion.md).

## B — Activate a real source

Once a demo proves the plumbing, onboard a real source via its guide:

- [API ingestion](api-ingestion.md) — REST/HTTP, one source or hundreds (Bearer/OAuth, GET & POST).
- [Files](files.md) — Parquet/CSV/JSON dropped in a bucket (incl. SAP via a connector).
- [SQL Server](sqlserver.md) — CDC or query-based via Lakeflow Connect.
- [Oracle EPM](oracle-epm.md) — the **customer-run-only** POST + Basic-auth template, with a
  full from-zero workspace walkthrough.

Hit a snag? See [Troubleshooting](troubleshooting.md).
