# Streaming push with Zerobus

Use this when **your application produces the data** — IoT devices, apps, microservices
emitting events — and you want it in a Delta table *without* a message bus (Kafka,
Kinesis, Event Hubs). [Zerobus Ingest](https://docs.databricks.com/ingestion/zerobus-overview)
is a serverless connector apps push records straight into a **pre-created managed Delta
table** over gRPC, with per-batch durability ACKs. YAIF's zerobus module provides the
receiving side: the bronze table, the SP the producers authenticate as, and the medallion
that turns the append-only firehose into clean, deduplicated tables.

The shared medallion code is `src/zerobus/` (bronze is the Zerobus target table itself;
silver → gold in `src/zerobus/`). The demo unit is
[`resources/zerobus/demo.yml`](../resources/zerobus/demo.yml).

## Data flow

```
  Producer app / devices (the Zerobus SDK, classic compute or your own infra)
        │  gRPC + JSON (demo) or Protobuf (production), at-least-once + ACKs
        ▼
  <catalog>.yaif_zerobus_demo.zerobus_events      ← BRONZE = the Zerobus target table
        │  SDP streams it (skipChangeCommits)
        ▼
  silver_zerobus_events (STREAM)                  AUTO CDC SCD1 keyed on event_id →
        │                                         re-deliveries upsert, never duplicate
        ▼
  gold_zerobus_ingestion_health (MV)              wire-level arrival + duplicate_rate
  gold_zerobus_events_per_day     (MV)            daily volume by device (deduped)
```

## One-time workspace setup

Zerobus producers authenticate with a **service principal** (OAuth M2M) that needs
explicit grants on the target table. The demo automates the table + grants (its
`prepare` task creates the bronze table and GRANTs the SP from the secret scope); you
only provision the identity once per workspace:

```bash
# 1. Service principal for producers
databricks service-principals create --profile <ws-profile> --display-name "yaif-zerobus-producer"
#    → note id and applicationId from the response

# 2. OAuth secret for it (account-level API; account admin)
databricks api post \
  /api/2.0/accounts/<account-id>/servicePrincipals/<sp-id>/credentials/secrets \
  --profile <account-profile> --json '{}'
#    → response contains {"id": "...", "secret": "..."}

# 3. UC secret scope the demo job reads (keys: client_id, client_secret)
databricks secrets create-scope yaif_zerobus --profile <ws-profile>
databricks secrets put-secret yaif_zerobus client_id --string-value <applicationId>
databricks secrets put-secret yaif_zerobus client_secret --string-value <secret>
```

Explicit **table-level** `MODIFY` + `SELECT` are mandatory — schema-level inherited
grants are not sufficient for Zerobus's `authorization_details` OAuth flow (fails with
error 4024). The `prepare` task grants them; it runs as the job owner and needs `MANAGE`
on the catalog.

## Try it now

```bash
databricks bundle run zerobus_demo_push_and_pipeline -t dev
```

`prepare` → creates the bronze table + grants · `push` → the producer (a **5-minute
stream** by default: ~1,000 synthetic IoT events at a ~10s cadence, ~2% re-delivered
mid-flight to exercise at-least-once dedup, then flush and close) · `run_pipeline` → the
SDP medallion. The push task runs on a **classic single-node job cluster** because the
Zerobus SDK cannot pip-install on serverless; everything downstream is serverless as
usual. For a fast single-batch run (e.g. re-verification), pass
`--var zerobus_stream_minutes=0`.

Verify:

```sql
SELECT count(*)                                              -- wire arrivals (incl. re-deliveries)
FROM <catalog>.yaif_zerobus_demo.zerobus_events;
SELECT count(*)                                              -- distinct events (deduped)
FROM <catalog>.yaif_zerobus_demo.silver_zerobus_events;
SELECT * FROM <catalog>.yaif_zerobus_demo.gold_zerobus_ingestion_health;  -- duplicate_rate ≈ 2%
```

## Wire a real producer

1. **Give the producer the identity**: the SP's `client_id`/`client_secret` from the
   scope (or its own SP, granted the same way), plus the server endpoint
   `<workspace-id>.zerobus.<region>.cloud.databricks.com` and the target table name.
2. **Copy the `ZerobusProducer` class** out of `src/jobs/push_zerobus_events.py` — it is
   plain Python (no dbutils, no Spark): open the stream, `push(records)` blocks on the
   durability ACK, the context manager flushes and closes. Swap the demo-events cell for
   your real records and the notebook is your producer. For a producer outside
   Databricks, the same class works verbatim (or use the Rust/Go/Java/TypeScript SDKs).
3. **Prefer Protobuf in production** — generate the `.proto` from the UC table
   (`python -m zerobus.tools.generate_proto --uc-endpoint … --table …`) and pass
   `RecordType.PROTO` + the compiled descriptor to `TableProperties`. JSON (the demo)
   is fine for prototypes and simple schemas; Protobuf gives compile-time type safety
   and forward-compatible schema evolution.
4. **Scale by streams**: one stream ≈ 15k rows/s and 100 MB/s; open multiple streams
   to the same table for more. Handle drops with the SDK's recovery options (on by
   default) — delivery is at-least-once either way, which silver's AUTO CDC dedup
   absorbs.

## Caveats / gotchas

- **Zerobus never creates or alters tables.** The target must pre-exist as a *managed*
  Delta table with a schema your records match exactly (JSON keys = column names). The
  demo's `prepare` task owns creation; schema evolution is manual (regenerate the
  `.proto`, redeploy).
- **Serverless cannot pip-install the SDK** — the producer task needs classic compute
  (or the Zerobus REST API, Beta, for notebook-only environments). Everything
  downstream of bronze is serverless SDP as usual.
- **Timestamps travel as int64 epoch microseconds** (the wire type for Delta
  `TIMESTAMP`), so bronze stores `event_time`/`ingested_at` as BIGINT and silver casts
  them — see `src/shared/zerobus_events.py`.
- **At-least-once by design**: duplicates are normal, not a bug. Key silver on a
  producer-assigned event id (AUTO CDC SCD1) and watch `duplicate_rate` in gold.
- The endpoint format and supported regions are in the
  [Zerobus docs](https://docs.databricks.com/ingestion/zerobus-ingest); the workspace
  and target table must be in one.
