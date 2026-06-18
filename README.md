# YAIF — Yet Another Ingestion Framework

A Databricks **Lakeflow** reference for landing **REST APIs, files, and SQL Server**
into Unity Catalog. One Asset Bundle, consistent conventions, a bronze → silver → gold
medallion per source with **AUTO CDC SCD-1** dedup. Onboarding a new source is a file
copy — **zero new framework code**.

It's an *umbrella repo, not an abstraction layer*: where Databricks already provides a
declarative primitive (Lakeflow Connect, SDP), YAIF just configures it. All modules are
built and verified end-to-end on the sandbox.

## What's in here

| Module | Source | What it does | Docs |
|---|---|---|---|
| **API** | REST / HTTP APIs (one, or hundreds) | Governed UC HTTP fetch → SDP medallion; control-table driven; GET & POST/body | [docs/api-ingestion.md](docs/api-ingestion.md) |
| **Files** | Parquet / CSV / JSON in a bucket (incl. SAP via a connector) | Auto Loader (`cloudFiles`) over a UC Volume → SDP medallion | [docs/files.md](docs/files.md) |
| **SQL Server — CDC** | A SQL Server DB *with* CDC / Change Tracking | Lakeflow Connect: continuous gateway + ingestion; full change & delete history | [docs/sqlserver.md](docs/sqlserver.md) |
| **SQL Server — query-based** | A SQL Server DB *without* CDC/CT | Lakeflow Connect: scheduled cursor-driven pulls, no gateway | [docs/sqlserver.md](docs/sqlserver.md) |
| **Oracle EPM** *(example)* | Oracle EPM `exportdataslice` (POST + Basic auth) | Customer-run template for POST/body/Basic-auth APIs | [docs/oracle-epm.md](docs/oracle-epm.md) |

## Quickstart

Two paths. **A** proves the framework end-to-end with **zero external setup** — run it
now. **B** is the worked example of activating a *real* source (Oracle EPM): the exact
steps a customer follows in their own workspace. Full detail in
[docs/quickstart.md](docs/quickstart.md).

### A — Prove the plumbing (data-safe, runnable now)

The self-contained **files demo** runs the full Auto Loader → bronze/silver/gold medallion
over synthetic Parquet — no bucket, no credentials, no connection.

```bash
# 1. Authenticate the CLI to your workspace
databricks auth login --host https://<your-workspace>.cloud.databricks.com

# 2. Get the code and point the bundle at your workspace + an existing catalog
git clone <this-repo> && cd yaif
#    edit databricks.yml → targets.dev.workspace.profile + var.catalog
#    (defaults target the sandbox profile `sqlserver-ws`, catalog `yaif`)

# 3. Deploy
databricks bundle deploy -t dev

# 4. Run the files demo (zero external setup)
databricks bundle run files_demo_seed_and_pipeline -t dev

# 5. See rows — in any SQL warehouse
#      SELECT * FROM <catalog>.yaif_files_demo.silver_cloud_files;
```

Want a data-safe **POST + body + Basic-auth + VARIANT** proof (the exact shape Oracle EPM
uses, against a public mock)? Run the postman-echo demo:

```bash
databricks secrets create-scope yaif_api
databricks secrets put-secret yaif_api mock_username --string-value postman
databricks secrets put-secret yaif_api mock_password --string-value password
databricks bundle run echo_post_demo_fetch_and_pipeline -t dev   # POSTs to postman-echo.com
```

### B — Activate a real source: Oracle EPM, step by step

> **Oracle EPM is CUSTOMER-RUN-ONLY.** This is how a **customer** activates EPM in **their
> own** workspace, against **their** EPM, with **their** credentials. The repo ships only
> placeholders (`var.epm_host` → `REPLACE-ME.example.com`; a customer-managed secret scope)
> and **never** calls `exportdataslice` from the SA sandbox — that returns live planning
> data. For a runnable sandbox proof of the same POST/Basic/VARIANT path, use the
> postman-echo demo in **A** above.

Done entirely from the Databricks workspace. Full walkthrough: [docs/oracle-epm.md](docs/oracle-epm.md).

1. **Import the repo into your workspace.** Sidebar → **Workspace → Repos → Add Repo**,
   paste the Git URL, **Create Repo**. (Or clone locally and drive it with the CLI — either
   way you have the `yaif` bundle.)

2. **Store the EPM credentials as UC secrets** (never in a file). In a terminal with the
   CLI authenticated to *your* workspace:
   ```bash
   databricks secrets create-scope epm_secrets
   databricks secrets put-secret epm_secrets epm_username --string-value '<your-epm-user>'
   databricks secrets put-secret epm_secrets epm_password --string-value '<your-epm-password>'
   ```

3. **Activate the example** — move it into the deploy glob:
   ```bash
   mv examples/api/epm_domain.yml resources/api/epm_domain.yml
   ```

4. **Set the variables** in `databricks.yml`, and edit the request in the moved file:
   ```yaml
   variables:
     catalog:          { default: "your_catalog" }                 # must already exist
     epm_host:         { default: "https://your-epm-host.example.com" }
     epm_secret_scope: { default: "epm_secrets" }                  # the scope from step 2
   targets:
     dev:
       workspace:
         profile: your-cli-profile
   ```
   Then open `resources/api/epm_domain.yml` and edit the `path` + `gridDefinition` body to
   the application / plan type / slice you want to export (the file header explains each field).

5. **Deploy the bundle:**
   ```bash
   databricks bundle deploy -t dev
   ```

6. **Run the ingestion job** (it fetches, then runs the pipeline):
   ```bash
   databricks bundle run epm_customer_fetch_and_pipeline -t dev
   ```

7. **Verify the rows landed** — in any SQL warehouse (in dev mode schemas are prefixed
   `dev_<you>_…`):
   ```sql
   SELECT count(*) FROM your_catalog.yaif_epm.bronze_api_responses;     -- raw landed
   SELECT * FROM your_catalog.yaif_epm.silver_api_documents LIMIT 20;   -- parsed VARIANT
   ```

For Bearer/OAuth APIs (the simpler default path) and scaling to hundreds of endpoints, see
[docs/api-ingestion.md](docs/api-ingestion.md).

## Project layout

```
yaif/
├── databricks.yml          # bundle + shared vars + dev/prod targets
├── resources/              # deploy glob (resources/*/*.yml) — what ships
│   ├── api/                #   one file per API business domain (+ echo demo)
│   └── files/              #   self-contained files demo
├── examples/               # activate-by-moving templates (need external setup)
│   ├── api/                #   control table, generated sample, Oracle EPM template
│   ├── sqlserver/          #   CDC + query-based Lakeflow Connect templates
│   └── files/              #   real bucket feed template
├── scripts/                # control table → generate one domain YAML per domain
└── src/                    # SHARED medallion + job code — never copied per source
```

## Choose your connector

| Your source is… | Go to |
|---|---|
| A REST / HTTP API — one, or hundreds | [docs/api-ingestion.md](docs/api-ingestion.md) |
| Files a tool drops in a bucket (Parquet/CSV/JSON) | [docs/files.md](docs/files.md) |
| A SQL Server database | [docs/sqlserver.md](docs/sqlserver.md) |
| Oracle EPM (or any POST + Basic-auth API) | [docs/oracle-epm.md](docs/oracle-epm.md) |

**New here?** Read the [Quickstart](docs/quickstart.md), then [Concepts](docs/concepts.md)
(medallion, silver shapes, the activate-by-moving model, catalogs/targets/profiles).
Hit a snag? [Troubleshooting](docs/troubleshooting.md).

> Contributing or working with an AI agent in this repo? `CLAUDE.md` is the agent-facing
> playbook (conventions + the hard-won gotchas). Read it before changing a pipeline, job,
> or path.
