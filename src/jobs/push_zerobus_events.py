# Databricks notebook source
# MAGIC %md
# MAGIC # Zerobus Demo — Push Events (the producer)
# MAGIC
# MAGIC The producer side of the zerobus module: pushes synthetic IoT events straight
# MAGIC into the bronze Delta table over the Zerobus gRPC API, standing in for the
# MAGIC real thing — an application/service that emits events without a message bus.
# MAGIC Swap this cell's record generation for your own and the rest is the template.
# MAGIC
# MAGIC Shape of a push (the production-relevant parts):
# MAGIC   * **Classic compute only** — the Zerobus SDK cannot pip-install on serverless,
# MAGIC     so this task runs on the job's classic cluster with the SDK as a pypi library.
# MAGIC   * **At-least-once delivery** — Zerobus may redeliver (e.g. after a retry where
# MAGIC     the original did land). `duplicate_pct` re-sends a slice of event ids so the
# MAGIC     demo shows silver's AUTO CDC dedup handling it; check gold's duplicate_rate.
# MAGIC   * **Durability ACKs** — batch ingest with `ingest_records_offset` +
# MAGIC     `wait_for_offset`, so the task only succeeds once every record is durably
# MAGIC     written. SDK stream recovery is on (v1.x re-establishes dropped streams).
# MAGIC   * **JSON records** — dicts whose keys match the bronze columns. Fine for the
# MAGIC     demo and simple schemas; production producers should prefer Protobuf
# MAGIC     (generate the .proto from the UC table — see docs/zerobus.md).
# MAGIC
# MAGIC Server endpoint format: `<workspace-id>.zerobus.<region>.cloud.databricks.com`.

# COMMAND ----------

import random
import time
import uuid
from datetime import datetime, timedelta, timezone

dbutils.widgets.text("server_endpoint", "7474648509963227.zerobus.us-west-2.cloud.databricks.com")
dbutils.widgets.text("table_name", "yaif.yaif_zerobus_demo.zerobus_events")
dbutils.widgets.text("secret_scope", "yaif_zerobus")
dbutils.widgets.text("event_count", "1000")
dbutils.widgets.text("duplicate_pct", "2")

ENDPOINT = dbutils.widgets.get("server_endpoint")
TABLE = dbutils.widgets.get("table_name")
SCOPE = dbutils.widgets.get("secret_scope")
EVENT_COUNT = int(dbutils.widgets.get("event_count"))
DUPLICATE_PCT = float(dbutils.widgets.get("duplicate_pct"))

WORKSPACE_URL = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
CLIENT_ID = dbutils.secrets.get(SCOPE, "client_id")
CLIENT_SECRET = dbutils.secrets.get(SCOPE, "client_secret")
print(f"push: endpoint={ENDPOINT} table={TABLE} workspace={WORKSPACE_URL}")
print(f"push: events={EVENT_COUNT} duplicate_pct={DUPLICATE_PCT}")

# COMMAND ----------

from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk

SDK = ZerobusSdk(ENDPOINT, WORKSPACE_URL)
# recovery defaults are on (4 retries, 2s backoff) — v1.x re-establishes dropped
# streams itself; keep them explicit so the production posture is visible.
OPTIONS = StreamConfigurationOptions(
    record_type=RecordType.JSON,
    recovery=True,
    recovery_retries=4,
    recovery_backoff_ms=2000,
)


def open_stream(max_attempts: int = 3):
    """Create the Zerobus stream, retrying stream creation (not records) on failure.

    Record-level durability is handled by ACKs + SDK recovery; only stream creation
    gets a hand-rolled retry because nothing can be ingested until it succeeds.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            stream = SDK.create_stream(
                CLIENT_ID, CLIENT_SECRET, TableProperties(TABLE), OPTIONS
            )
            print(f"push: stream open (stream_id={stream.stream_id}, attempt {attempt})")
            return stream
        except Exception as e:  # noqa: BLE001 — log and back off, last attempt re-raises
            print(f"push: create_stream attempt {attempt}/{max_attempts} failed: {e}")
            if attempt == max_attempts:
                raise
            time.sleep(2**attempt)


def generate_events(n: int, duplicate_pct: float):
    """Synthetic IoT events + an at-least-once slice, as JSON-able dicts.

    Keys MUST match the bronze columns exactly (JSON mode — schema mismatch is a
    runtime error). event_time spreads over the last two hours; a duplicate_pct
    slice is re-appended with a later ingested_at, mimicking a producer retry that
    re-delivers an event Zerobus had already durably written.
    """
    now = datetime.now(timezone.utc)
    devices = [f"sensor-{i:02d}" for i in range(1, 21)]
    rng = random.Random(42)  # deterministic demo data

    events = []
    for _ in range(n):
        pushed = now - timedelta(minutes=rng.randint(0, 120), seconds=rng.randint(0, 59))
        events.append(
            {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
                "device_name": rng.choice(devices),
                "temp": round(rng.uniform(18.0, 42.0), 1),
                "humidity": round(rng.uniform(30.0, 90.0), 1),
                "event_time": int(pushed.timestamp() * 1_000_000),
                "ingested_at": int(now.timestamp() * 1_000_000),
            }
        )

    n_dupes = max(1, int(n * duplicate_pct / 100.0))
    later = int((now + timedelta(seconds=5)).timestamp() * 1_000_000)
    for e in rng.sample(events, n_dupes):
        redelivered = dict(e)
        redelivered["ingested_at"] = later  # a retry re-delivers the same event id later
        events.append(redelivered)
    return events, n_dupes


RECORDS, N_DUPES = generate_events(EVENT_COUNT, DUPLICATE_PCT)
print(f"push: generated {len(RECORDS)} records ({N_DUPES} re-deliveries of existing event ids)")

# COMMAND ----------

# Batch ingest with durability tracking: one offset for the whole batch, wait for the
# ACK that says every record up to it is durably in Delta, then flush/close.
STREAM = open_stream()
try:
    offset = STREAM.ingest_records_offset(RECORDS)
    STREAM.wait_for_offset(offset)
    print(f"push: ACK received — {len(RECORDS)} records durable up to offset {offset}")
finally:
    STREAM.flush()
    STREAM.close()
    print("push: stream flushed and closed")

# COMMAND ----------

# Soft check: rows may lag the ACK by a moment as Zerobus materializes; warn, don't fail.
count = spark.sql(f"SELECT COUNT(*) AS c FROM {TABLE}").first().c
print(f"push: bronze row count now = {count}")
if count < len(RECORDS):
    print(f"push: WARNING — bronze ({count}) below pushed ({len(RECORDS)}); "
          "materialization may lag the durability ACK slightly. The pipeline/gold tables "
          "are the source of truth.")
else:
    print("push: bronze count matches the pushed record count (expected: includes re-deliveries)")

print("push: done")
