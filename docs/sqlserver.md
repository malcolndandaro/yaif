# SQL Server (Lakeflow Connect)

A managed connector — YAIF supplies only conventions, no custom code. Two sibling
templates, both kept **outside** the deploy glob because they need a live UC `SQLSERVER`
connection (activate by moving):

| Template | Pattern | Use when |
|---|---|---|
| [`examples/sqlserver/orders_cdc.yml`](../examples/sqlserver/orders_cdc.yml) | **CDC** — continuous gateway + triggered ingestion | The source can enable CDC / Change Tracking; you need every intermediate change and full delete capture |
| [`examples/sqlserver/orders_query.yml`](../examples/sqlserver/orders_query.yml) | **Query-based** — scheduled, no gateway | The source **cannot** enable CDC/CT; each table has a monotonic cursor column |

Both land governed UC tables and use the same **SCD Type 1, keyed on the primary key**,
current-state dedup the API/files silver layers use (query-based sequences by the cursor
column; CDC by the change sequence).

## Decision table: which connector?

|   | **CDC (gateway)** | **Query-based** |
|---|---|---|
| Source prerequisite | CDC or Change Tracking enabled on the source | **None** — just a `SELECT`-able cursor column per table |
| Infra | Continuous gateway (classic compute) + staging Volume | **No gateway, no staging Volume** — connector queries the source directly |
| Cost / ops | Gateway runs and bills continuously until stopped | Cheaper/simpler: serverless, runs only on its schedule |
| Change fidelity | Every intermediate change between runs | **Latest state at each scheduled run** only |
| Deletes | Full delete capture from the change feed | Soft delete via `deletion_condition` (GA, API-only); **hard-delete capture is Beta** (API-only) |
| Source load | Low (reads the change feed) | Higher — queries the source table each run |
| Cursor column | — | Requires **one monotonic *modified* cursor** per table (advances on every INSERT **and** UPDATE) |

Pick query-based when the source owner won't (or can't) turn on CDC/CT; pick CDC when you
need full change history or delete capture.

> **⚠️ Query-based REQUIRES a monotonic *modified* cursor to capture updates.** The cursor
> must advance on every INSERT **and UPDATE** — a `ModifiedDate`/`last_updated` timestamp
> (or `rowversion`). An identity / auto-increment primary key only advances on INSERT, so
> using it as the cursor yields an **insert-only feed that silently misses every UPDATE** to
> existing rows. Keep the PK as `primary_keys` (for SCD dedup); choose a modified-timestamp
> for `cursor_columns`. The bundled demo models this: the `DemoDB` seeder (in the
> `demo-environments` repo) adds a `ModifiedDate DATETIME2` column — DEFAULT
> `SYSUTCDATETIME()` on insert, bumped by an AFTER UPDATE trigger — and `orders_query.yml`
> uses it as the cursor.

## CDC activation (gateway)

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
   steady-state reads). `trustServerCertificate 'true'` disables server-cert validation,
   fine for a lab; **for prod, validate TLS** — drop the option (or pin the CA) so the
   gateway verifies the server certificate.
3. **Set the variables** in `databricks.yml`: `sqlserver_connection` and
   `sqlserver_source_database`. List your tables in the example's `objects:` block (one
   `table:` block each, or a single `schema:` block for a whole schema).
4. **Activate:** move `examples/sqlserver/orders_cdc.yml` → `resources/sqlserver/` (no
   `databricks.yml` edits — the target `mode` sets each pipeline's `development` flag).
5. **Deploy, start the gateway, run ingestion:** `databricks bundle deploy`, then start the
   **continuous gateway** once — `databricks bundle run sqlserver_gateway` — which runs
   continuously, capturing changes into staging. Apply with
   `databricks bundle run sqlserver_ingestion_job` (or schedule that job). The job triggers
   **only** the ingestion pipeline — the gateway is continuous and is *not* a job task
   ([SQL Server ingestion docs](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-pipeline):
   *"You must run the gateway as a continuous pipeline"*).
6. **Verify** with any SQL warehouse:
   `SELECT count(*) FROM <catalog>.yaif_sqlserver.<table>;`

Onboard more tables: add `table:` blocks (or one `schema:` block) to `objects:` and
redeploy — no new code.

### CDC cost & ops notes

- **The gateway is continuous** — once started it runs (and bills classic-compute DBUs)
  until stopped. Databricks recommends **not** stopping it in production: if it's down long
  enough for the source's change log to truncate, you must full-refresh the affected tables.
  For a demo you can stop it to halt cost (`databricks pipelines stop <gateway-id>`),
  accepting that trade-off.
- **Staging retention is 30 days** — the gateway → ingestion staging Volume keeps change
  data ~30 days by default; reprocessing further back requires a resnapshot.

### Table-count limit & sharding

Databricks recommends **≤ 250 tables per ingestion pipeline**
([SQL Server connector limitations](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-limits)
— "Databricks recommends ingesting 250 or fewer tables per pipeline"; 250 is the
feature-availability maximum). A gateway and an ingestion pipeline form a **pair** — the
ingestion pipeline references exactly one gateway via `ingestion_gateway_id` — so to go
beyond ~250 tables you split the table list across **multiple gateway-ingestion pairs**,
each pair under the limit and all publishing into the same `yaif_sqlserver` schema. The
commented second pair in `examples/sqlserver/orders_cdc.yml` shows the split. (The docs
describe scaling via separate gateway-ingestion *pairs*, not one gateway feeding many
ingestion pipelines — don't assume the latter.)

## Query-based activation (no CDC/CT on the source)

Template: [`examples/sqlserver/orders_query.yml`](../examples/sqlserver/orders_query.yml)
(ingestion pipeline + scheduled job — **no gateway, no staging Volume**).

1. **Create the UC connection** — same `SQLSERVER` connection as CDC (a least-privilege
   login needs only `SELECT` on the tables you ingest; no CDC/CT setup on the source).
2. **Pick a modified-timestamp cursor per table** — exactly one monotonically-increasing
   column that advances on every INSERT **and UPDATE**: a `ModifiedDate`/`last_updated`
   timestamp (or `rowversion`). **Do not use an identity/auto-increment PK as the cursor** —
   it only advances on INSERT, so the feed becomes insert-only and silently misses every
   UPDATE. Set the cursor under each table's
   `table_configuration.query_based_connector_config.cursor_columns`, and keep
   `primary_keys` + `scd_type: SCD_TYPE_1` for current-state dedup keyed on the PK (the PK
   is for dedup, not for the cursor). Without any monotonic cursor the connector
   **full-reloads every run**. (The demo's `DemoDB` tables ship a `ModifiedDate DATETIME2`
   column — DEFAULT on insert + AFTER UPDATE trigger — so `orders_query.yml` uses it.)
3. **Set the variables** in `databricks.yml` (`sqlserver_connection`,
   `sqlserver_source_database`) and replace the example table/cursor/key names.
4. **Activate:** move `examples/sqlserver/orders_query.yml` → `resources/sqlserver/` (no
   `databricks.yml` edits — the target `mode` sets the pipeline's `development` flag).
5. **Deploy & schedule:** `databricks bundle deploy`, then
   `databricks bundle run sqlserver_query_ingestion_job` (or let its schedule drive it). No
   gateway to start.
6. **Verify:** `SELECT count(*) FROM <catalog>.yaif_sqlserver_query.<table>;`

### Query-based trade-offs

No CDC/CT and no continuous gateway → simpler, cheaper, fully serverless. But:

- (a) it **requires a monotonic cursor per table**;
- (b) **deletes** are limited — soft deletes via `deletion_condition` (GA, **API-only** —
  not settable in the bundle YAML), and **hard-delete capture is Beta**
  (`hard_deletion_sync_min_interval_in_seconds`, also API-only) — vs full delete capture in
  CDC;
- (c) it captures the **latest state at each scheduled run, not every intermediate change**
  between runs;
- (d) it **queries the source directly on each run**, adding source load the CDC change-feed
  path avoids.

> **Operational note (sandbox):** the live SQL Server Lakeflow gateway in the sandbox
> workspace was deployed out-of-band and is kept as `examples/` (out of glob) — do not
> redeploy or touch it. See [Troubleshooting](troubleshooting.md).
