# Troubleshooting

The gotchas, in one place — each next to the topic it bites. For the full engineering
gotcha list (canonical, agent-facing), see **`CLAUDE.md` → "Gotchas that WILL bite you."**

## Basic auth cannot ride a UC HTTP connection (gotcha #9)

**Symptom:** an HTTP Basic-auth API returns 401, or the wire shows a malformed
`Authorization: Bearer dummy,Basic …` header.

**Why:** a UC HTTP connection force-prefixes `Bearer ` to its credential and *merges* a
custom `Authorization` header you pass to `http_request(... headers=...)` with the
connection's own auth — so a clean `Authorization: Basic …` is impossible through
`http_request` (runtime-verified on the sandbox). You also cannot create a host-only
(no-auth) connection — an auth option is mandatory at create time. Non-auth custom headers
(`Content-Type`, `X-*`) *do* pass through.

**Fix:** use `auth_mode: basic_secret` — the fetch job builds
`Authorization: Basic base64(user:pass)` from `dbutils.secrets` with direct Python
`requests`, bypassing the proxy. `auth_mode: connection` (the default) keeps using
`serving_endpoints.http_request` for Bearer/OAuth. The fetch job is method/body aware in
both modes — POST + JSON body work through the connection too; only custom *auth* is
stripped. The data-safe demo is [`resources/api/echo_post_demo.yml`](../resources/api/echo_post_demo.yml);
the real EPM template is [Oracle EPM](oracle-epm.md). See also
[API ingestion → Auth mode](api-ingestion.md#2-auth-mode).

**Strategic alternative:** if the API supports OAuth2 M2M (e.g. Oracle via OCI IAM/IDCS),
use `auth_mode: connection` with OAuth on the connection to restore UC-connection governance.

## `serving_endpoints.http_request()` doesn't exist (gotcha #5)

**Symptom:** the API fetch job fails with an AttributeError on `http_request`.

**Why:** serverless preinstalls `databricks-sdk`, and pip silently skips an upgrade if the
floor is already satisfied — so an older preinstalled SDK (without `http_request`) wins.

**Fix:** pin a floor **newer** than preinstalled — the domain job's `environment` declares
`databricks-sdk>=0.50.0`. Don't lower it. (This is the #1 thing that ate hours.) Relatedly,
`ExternalFunctionRequestHttpMethod` moves between SDK versions; the fetch job imports it with
a try/except string fallback to `"GET"`.

## `bundle deploy -t prod` fails validating `development` (gotcha #1)

**Symptom:** prod deploy/validate complains about a pipeline's `development` flag.

**Why:** something hardcoded `development: true` in a pipeline *resource*. The targets'
`mode: development` / `mode: production` set the flag per target automatically (dev → `true`,
prod → validated `false`). Baking it into the resource is redundant in dev and **breaks
`mode: production` validation**.

**Fix:** never set `development:` in a resource. This is also why onboarding a domain needs
zero `databricks.yml` edits.

## Auto Loader can't infer a schema on an empty directory (gotcha #4)

**Symptom:** the files pipeline's first run no-ops or errors on schema inference.

**Why:** Auto Loader (`cloudFiles`) needs at least one file present to infer a schema.

**Fix:** point a feed at a path that already has at least one file, or expect the first
update to no-op until a file arrives. (Note: the **API** module deliberately lands payloads
straight into a Delta table, *not* Volume + Auto Loader — the empty-dir schema-inference path
failed repeatedly on serverless for tiny API JSON. That rule is API-only; don't "fix" the
files module by removing Auto Loader — real Parquet feeds are exactly what `cloudFiles` is for.)

## Wrong workspace or catalog (profile/catalog defaults)

**Symptom:** resources deploy to an unexpected workspace, or schemas land in the wrong catalog.

**Why:** both `dev` and `prod` targets default to the **sandbox** (profile `sqlserver-ws`,
catalog `yaif`, the `var.catalog` default).

**Fix:** point at your own workspace by editing `databricks.yml`, or per command with
`--profile <name> --var catalog=<cat>`. Remember `mode: development` prefixes schema names
`dev_<user>_<schema>` — account for that in verify queries. If a `prod --strict` validate
warns about `/Workspace/Shared`, that is a known pre-existing warning, not your change.

## Don't redeploy or touch the live sandbox SQL Server gateway

The live SQL Server Lakeflow gateway in the sandbox workspace was deployed out-of-band and
is kept as `examples/` (out of the deploy glob) precisely so a `bundle deploy` won't touch
it. Leave it alone. The continuous gateway also bills classic-compute DBUs until stopped —
see [SQL Server → CDC cost & ops](sqlserver.md#cdc-cost--ops-notes).

## Relative paths in `resources/<module>/*.yml` (gotcha #7)

Paths in a resource YAML are relative to the YAML file. The files sit one level deep under
`resources/<module>/`, so they use `../../src/...` (two levels), not `../src/...`. If you add
a new module subdir, match this.

## CLI auth errors

If commands fail with authentication errors, your profile token may have expired — re-run
`databricks auth login --host https://<your-workspace>.cloud.databricks.com` (or
`--profile <name>`).

---

Still stuck? The full set of hard-won engineering gotchas lives in `CLAUDE.md`; the
per-connector guides ([API](api-ingestion.md), [Files](files.md), [SQL Server](sqlserver.md),
[Oracle EPM](oracle-epm.md)) carry topic-specific caveats inline.
