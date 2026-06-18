# Oracle EPM (customer-run-only)

[`examples/api/epm_domain.yml`](../examples/api/epm_domain.yml) is the real Oracle EPM
(Hyperion Planning) `exportdataslice` template. It demonstrates the **POST + JSON body +
HTTP Basic auth + VARIANT** path of the [API module](api-ingestion.md) against a *real* EPM
host.

> **CUSTOMER-RUN-ONLY — never run `exportdataslice` from the SA sandbox.** It returns live
> planning data. This file lives **outside** the deploy glob and never deploys or runs from
> this repo. The capability is proven data-safe against a public mock (postman-echo) by
> [`resources/api/echo_post_demo.yml`](../resources/api/echo_post_demo.yml) — **that** is
> what the SA sandbox runs. This template is the **customer's** to run in **their** workspace,
> against **their** EPM, with **their** credentials.

No real EPM host or credentials are committed: `api_base_url` is `${var.epm_host}` (a
placeholder defaulting to `https://REPLACE-ME.example.com`) and the username/password come
from a customer-managed UC secret scope (`${var.epm_secret_scope}`).

## Why `basic_secret` (not a UC HTTP connection)

EPM uses HTTP Basic auth, and a UC HTTP connection force-prefixes `Bearer ` / merges its own
credential into the `Authorization` header — so a clean `Authorization: Basic …` is
impossible through `http_request` (runtime-verified). So the EPM template sets
`auth_mode: basic_secret`: the fetch job builds `Authorization: Basic base64(user:pass)`
from `dbutils.secrets` with direct Python `requests`, bypassing the proxy. Full rationale:
[Troubleshooting → gotcha #9](troubleshooting.md#basic-auth-cannot-ride-a-uc-http-connection-gotcha-9).

> **Strategic alternative:** if the EPM tenancy supports OAuth2 M2M (Oracle via OCI
> IAM/IDCS), use `auth_mode: connection` with OAuth on the connection to restore
> UC-connection governance.

EPM grids are arbitrary nested JSON (`pov` / `columns` / `rows` / `data`, no per-record id),
so the pipeline uses `silver_shape: document` — one VARIANT row per response in
`silver_api_documents`. See [API ingestion → VARIANT landing](api-ingestion.md#3-variant-landing-silver_shape).

## Prove it first (data-safe, in the SA sandbox)

Before touching a real EPM, run the postman-echo demo — same code path, public mock, no
customer data:

```bash
databricks secrets create-scope yaif_api
databricks secrets put-secret yaif_api mock_username --string-value postman
databricks secrets put-secret yaif_api mock_password --string-value password
databricks bundle deploy -t dev
databricks bundle run echo_post_demo_fetch_and_pipeline -t dev
```

It POSTs an EPM-shaped `gridDefinition` to `postman-echo.com/post`, lands the echoed nested
JSON as a VARIANT, and builds `silver_api_documents` + gold — proving POST + body + Basic +
VARIANT end-to-end.

## Activate against a real EPM — full walkthrough

For a reader who has never used this framework. Done entirely from the customer's Databricks
workspace, against the customer's EPM, with the customer's credentials.

### 1. Get the repo into your workspace

In the Databricks UI: sidebar → **Workspace → Repos → Add Repo**, paste the Git URL, click
**Create Repo**. (Or clone locally and drive everything with the Databricks CLI — either way
you end up with the `yaif` bundle.) Make sure the CLI is authenticated to *your* workspace:

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

### 2. Store the EPM credentials as UC secrets

Credentials never live in a file. Create a secret scope and put your EPM username/password
in it (use the customer's EPM login, never an SA's):

```bash
databricks secrets create-scope epm_secrets
databricks secrets put-secret epm_secrets epm_username --string-value '<your-epm-user>'
databricks secrets put-secret epm_secrets epm_password --string-value '<your-epm-password>'
```

The template reads `epm_username` / `epm_password` from the scope named by
`var.epm_secret_scope` — keep these key names, or change them in the file's
`secret_key_username` / `secret_key_password` fields.

### 3. Activate the example (move it into the deploy glob)

The template ships outside the deploy glob so a placeholder host can't accidentally deploy.
Bring it in by moving it:

```bash
mv examples/api/epm_domain.yml resources/api/epm_domain.yml
```

### 4. Set the variables and the request

Edit `databricks.yml` (or pass `--var` on deploy):

```yaml
variables:
  catalog:          { default: "your_catalog" }                  # must already exist
  epm_host:         { default: "https://your-epm-host.example.com" }
                    #          e.g. https://planning-<tenant>.epm.<region>.oraclecloud.com
  epm_secret_scope: { default: "epm_secrets" }                   # the scope from step 2
targets:
  dev:
    workspace:
      profile: your-cli-profile
```

Then open `resources/api/epm_domain.yml` and edit the request to your application:

- the `path` — your app's `exportdataslice` endpoint, e.g.
  `/HyperionPlanning/rest/v3/applications/<APP>/plantypes/<PLANTYPE>/exportdataslice`;
- the `gridDefinition` body — your `pov` / `columns` / `rows` (the slice to export).

The file header explains each field.

### 5. Deploy the bundle

```bash
databricks bundle deploy -t dev
```

### 6. Run the ingestion job

The job runs two tasks: `fetch` (POST + Basic auth → Delta landing table) then
`run_pipeline` (the SDP medallion).

```bash
databricks bundle run epm_customer_fetch_and_pipeline -t dev
```

There is no schedule on the template on purpose — add a cron only after a first manual,
reviewed run.

### 7. Verify the rows landed

In any SQL warehouse (in `mode: development` the schema is prefixed `dev_<you>_`):

```sql
-- raw response landed (audit-safe, append-only):
SELECT count(*) FROM your_catalog.yaif_epm.bronze_api_responses;

-- parsed VARIANT, one row per response:
SELECT endpoint, run_id,
       response_variant:gridDefinition.suppressMissingBlocks::boolean AS suppress
FROM   your_catalog.yaif_epm.silver_api_documents
LIMIT 20;
```

Navigate the grid off the VARIANT with `variant_explode` — see
[API ingestion](api-ingestion.md#3-variant-landing-silver_shape).

## Once it works

Add a `schedule:` to the job, ramp `request_concurrency`, and promote with
`databricks bundle deploy -t prod`. Onboarding another EPM slice or app is another copy of
the template with a different `path` + `gridDefinition` — no framework code.
