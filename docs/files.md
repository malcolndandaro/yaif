# Files in cloud storage (Auto Loader)

Use this when a connector drops files into object storage and you own the ingestion — e.g.
**Arcosoft exporting SAP tables as `.parquet` into an S3/ADLS/GCS bucket**. Unlike the API
module (which lands payloads straight into a Delta table), files genuinely arrive *as files
in a bucket*, which is exactly what Auto Loader (`cloudFiles`) is built for: incremental,
exactly-once file discovery with schema evolution. Inside SDP the schema location and
checkpoint are managed for you — there is no checkpoint to configure.

The shared medallion code is `src/files/` (bronze → silver → gold); a per-feed domain unit
is [`examples/files/erp_parquet.yml`](../examples/files/erp_parquet.yml).

## Data flow

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

## Try it now without a bucket

```bash
databricks bundle run files_demo_seed_and_pipeline -t dev
```

This runs the exact medallion against a **managed** volume seeded with synthetic Parquet
(`resources/files/demo.yml`). Same Auto Loader → SDP code as a real feed — only the volume
type differs — so it's the fastest way to see the pattern work end to end. It's in the
deploy glob because it needs no external setup.

Verify:

```sql
SELECT count(*), count(DISTINCT source_file)
FROM <catalog>.yaif_files_demo.bronze_cloud_files;       -- 100 rows, 2 files
SELECT * FROM <catalog>.yaif_files_demo.gold_files_ingestion_health;
```

## Activate a real feed

1. **Register the bucket with Unity Catalog** — a storage credential + external location
   over the prefix the connector writes to (full SQL, incl. ADLS/GCS variants, is in the
   header of [`examples/files/erp_parquet.yml`](../examples/files/erp_parquet.yml)).
2. **Set `files_source_uri`** in `databricks.yml` to that path (and `file_format` if not
   Parquet).
3. **Move** `examples/files/erp_parquet.yml` → `resources/files/erp_parquet.yml` (this is
   what brings it into the deploy glob) — no `databricks.yml` edits beyond the vars; the
   target `mode` sets the pipeline's `development` flag automatically.
4. `databricks bundle deploy && databricks bundle run erp_ingestion_job`.
5. **Verify** with any SQL warehouse:
   ```sql
   SELECT count(*), count(DISTINCT source_file)
   FROM <catalog>.yaif_erp.bronze_cloud_files;
   ```
   and read `gold_files_ingestion_health` for files / rows / bytes / freshness.

## Onboard another feed

Same as onboarding an API domain: copy the domain file, rename the
schema/volume/pipeline/job keys, point `source_path` at the new volume — the `src/files/`
transformations are shared, zero code change.

## Caveats / advanced

- **Dedup overlapping re-exports.** Set `dedup_keys` in the pipeline `configuration` when a
  connector re-exports overlapping windows (full + incremental). Silver then dedups to
  current state via **AUTO CDC SCD Type 1** (latest row per key, sequenced by
  `dedup_order_by` — defaults to the source file modification time), the same bounded
  mechanism the API and SQL Server modules use. Leave `dedup_keys` unset to keep every
  ingested row. See [Concepts → AUTO CDC](concepts.md#auto-cdc-scd-type-1-dedup).
- **Empty-directory schema inference.** Auto Loader on an *empty* directory can't infer a
  schema — point a feed at a path that already has at least one file, or expect the first
  update to no-op. See [Troubleshooting](troubleshooting.md).
