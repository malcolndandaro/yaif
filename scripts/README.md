# scripts/

Helpers for operating the YAIF modules. Nothing here is required to deploy a
single domain by hand — these exist to make **scaling to many domains** a
copy-free, repeatable step.

## `generate_api_domains.py` — control table → per-domain YAML

Turns one control table (every endpoint, tagged by domain) into one
`resources/api/<domain>.yml` per domain, each identical in shape to the canonical
`resources/api/content_domain.yml`. This is "Lever 2" of the 900-API playbook in
the root `README.md` (Playbook A): you never hand-copy YAML or write framework code.

### Control table

One row per endpoint. Two interchangeable forms, kept in sync:

| Form | File | Use it for |
|---|---|---|
| CSV | [`examples/api/control_table.csv`](../examples/api/control_table.csv) | quick start / local — no workspace needed |
| SQL | [`examples/api/control_table.sql`](../examples/api/control_table.sql) | the governed Unity Catalog table the pipelines read at runtime |

Columns: `domain, endpoint_name, path, method, params, schedule, enabled`.

### Run this

```bash
# From a CSV (no workspace needed):
python scripts/generate_api_domains.py --csv examples/api/control_table.csv

# ...or from the Unity Catalog control table:
python scripts/generate_api_domains.py \
  --table main.config.api_endpoints --warehouse-id <warehouse-id> [--profile <name>]
```

### Get that

```
Reading control table from CSV: examples/api/control_table.csv
  wrote build/generated_api/accounts.yml  (1 endpoints)
  wrote build/generated_api/blog.yml  (2 endpoints)
  wrote build/generated_api/gallery.yml  (2 endpoints)

Done: read 5 enabled endpoints across 3 domains -> wrote 3 YAML files to build/generated_api/
  (1 disabled row(s) skipped)
Next: review the files, move the ones you want into resources/api/, then `databricks bundle deploy`.
```

The generator writes to a **preview** dir (`build/generated_api/`, default) — it
never clobbers `resources/`. A committed example of exactly what one emitted file
looks like is checked in at
[`examples/api/generated_sample/blog.yml`](../examples/api/generated_sample/blog.yml)
(note how the disabled `todos` row is dropped and the `postId=1` params row becomes
`/comments?postId=1` in `api_endpoints`).

### The whole loop

1. Edit the control table (CSV or the UC table) — add/disable endpoints.
2. Run the generator → review the YAML in `build/generated_api/`.
3. Move the domains you want into `resources/api/`, add each pipeline's
   `development:` override to the dev/prod targets in `databricks.yml`, and
   `databricks bundle deploy`.

Notes:
- The shared fetch job issues **GET** by path; `params` is appended as a query
  string. Non-GET rows are reported and skipped.
- A job has one schedule, so keep the `schedule` value identical for every row in
  a domain (split the domain if SLAs differ).
