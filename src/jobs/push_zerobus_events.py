# Databricks notebook source
# MAGIC %md
# MAGIC # Zerobus producer — push events straight into Delta
# MAGIC
# MAGIC An application emits records; Zerobus delivers them over gRPC into a managed
# MAGIC Delta table. No message bus, durability ACKs per batch. Full playbook:
# MAGIC `docs/zerobus.md`. Five small cells:
# MAGIC
# MAGIC 1. **Configure** — endpoint, target table, credentials
# MAGIC 2. **`ZerobusProducer`** — the reusable client class (the part to copy into your app)
# MAGIC 3. **Demo events** — synthetic data; swap for your real records
# MAGIC 4. **Push** — one batch (fast) or a timed stream, with durability ACKs
# MAGIC 5. **Verify arrival**
# MAGIC
# MAGIC Runs on classic compute — the SDK cannot pip-install on serverless.

# COMMAND ----------

# 1 · Configure ------------------------------------------------------------------

dbutils.widgets.text("server_endpoint", "7474648509963227.zerobus.us-west-2.cloud.databricks.com")
dbutils.widgets.text("table_name", "yaif.yaif_zerobus_demo.zerobus_events")
dbutils.widgets.text("secret_scope", "yaif_zerobus")
dbutils.widgets.text("event_count", "1000")
dbutils.widgets.text("duplicate_pct", "2")
dbutils.widgets.text("stream_minutes", "0")

server_endpoint = dbutils.widgets.get("server_endpoint")
table_name = dbutils.widgets.get("table_name")
secret_scope = dbutils.widgets.get("secret_scope")
event_count = int(dbutils.widgets.get("event_count"))
duplicate_pct = float(dbutils.widgets.get("duplicate_pct"))
stream_minutes = float(dbutils.widgets.get("stream_minutes"))

workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
client_id = dbutils.secrets.get(secret_scope, "client_id")
client_secret = dbutils.secrets.get(secret_scope, "client_secret")

mode = f"stream for {stream_minutes:g} min" if stream_minutes > 0 else "single batch"
print(f"producer: {table_name}")
print(f"producer: endpoint {server_endpoint}")
print(f"producer: mode = {mode}, ~{event_count} records, duplicate_pct = {duplicate_pct:g}%")

# COMMAND ----------

# 2 · ZerobusProducer — the reusable client ---------------------------------------
# Plain Python: no dbutils, no Spark — copy this class into your own service as-is.
# It opens a stream, pushes batches, and blocks on the ACK that says every record is
# durably written to Delta; use it as a context manager so the stream is always
# flushed and closed. Call push() as many times as you like — one batch or many.
#
#   Delivery is AT-LEAST-ONCE: a retried batch may land some records twice. Give every
#   record a stable business/event id and deduplicate downstream (this demo's silver
#   uses AUTO CDC SCD1 keyed on event_id) rather than assuming exactly-once.
#
#   Stream drops mid-push are re-established by the SDK's built-in recovery. If stream
#   creation fails, this task fails and the job re-runs it (max_retries) — safe, because
#   re-delivered records dedupe downstream. Records are dicts whose keys match the
#   table columns (JSON mode); production producers should prefer Protobuf
#   (docs/zerobus.md).

from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
from zerobus.sdk.sync import ZerobusSdk


class ZerobusProducer:
    """Push record dicts to a Delta table over Zerobus, with durability ACKs."""

    def __init__(self, server_endpoint, workspace_url, table_name, client_id, client_secret):
        self.sdk = ZerobusSdk(server_endpoint, workspace_url)
        self.client_id = client_id
        self.client_secret = client_secret
        self.table_properties = TableProperties(table_name)
        # recovery=True (default, kept visible): the SDK re-establishes dropped streams.
        self.options = StreamConfigurationOptions(record_type=RecordType.JSON, recovery=True)
        self.stream = None

    def __enter__(self):
        self.stream = self.sdk.create_stream(
            self.client_id, self.client_secret, self.table_properties, self.options
        )
        return self

    def push(self, records):
        """Ingest a batch and block until Zerobus ACKs it as durably written."""
        offset = self.stream.ingest_records_offset(records)
        self.stream.wait_for_offset(offset)
        return offset

    def __exit__(self, exc_type, exc, tb):
        try:
            self.stream.flush()
        finally:
            self.stream.close()
        return False

# COMMAND ----------

# 3 · Demo events — swap this cell for your real records --------------------------
# Synthetic IoT telemetry standing in for a real producer. The two things that matter
# for ANY producer here: every record carries a stable event id (the downstream dedup
# key), and a re-delivered record keeps the SAME id with a later ingested_at.

import random
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

rng = random.Random()
TICK_SECONDS = 10  # streaming cadence: one batch every 10s


def make_event(event_time):
    """One IoT reading. Delta TIMESTAMP travels as int64 epoch MICROseconds on the wire."""
    now = datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "device_name": f"sensor-{rng.randint(1, 20):02d}",
        "temp": round(rng.uniform(18.0, 42.0), 1),
        "humidity": round(rng.uniform(30.0, 90.0), 1),
        "event_time": int(event_time.timestamp() * 1_000_000),
        "ingested_at": int(now.timestamp() * 1_000_000),
    }


def re_deliver(record):
    """A producer retry: same event id and payload, pushed again slightly later."""
    later = datetime.now(timezone.utc) + timedelta(seconds=5)
    return {**record, "ingested_at": int(later.timestamp() * 1_000_000)}


def generate_one_batch(n, duplicate_pct):
    """Single-batch mode: n readings (event times spread over the last 2h) + re-deliveries."""
    two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
    events = [
        make_event(two_hours_ago + timedelta(minutes=rng.randint(0, 120)))
        for _ in range(n)
    ]
    return events + [re_deliver(e) for e in rng.sample(events, max(1, int(n * duplicate_pct / 100)))]


def stream_batches(minutes, total, duplicate_pct):
    """Streaming mode: yield a batch every ~10s until `minutes` elapse.

    Spreads ~`total` readings evenly across the window and re-delivers ~duplicate_pct%
    of them mid-flight — sampled from recent batches, which is what a producer retry
    actually looks like in a live stream.
    """
    batch_size = max(1, total // max(1, round(minutes * 60 / TICK_SECONDS)))
    recent = deque(maxlen=200)  # events a retry might re-deliver
    deadline = time.monotonic() + minutes * 60
    while time.monotonic() < deadline:
        batch = [make_event(datetime.now(timezone.utc)) for _ in range(batch_size)]
        # ~duplicate_pct% of FRESH records get a mid-flight re-delivery (2% of 33 ≈ 0.7
        # per batch), sampled from events already sent earlier in the stream.
        batch += [
            re_deliver(rng.choice(tuple(recent)))
            for _ in range(batch_size)
            if recent and rng.random() < duplicate_pct / 100
        ]
        recent.extend(batch)
        yield batch
        time.sleep(TICK_SECONDS)

# COMMAND ----------

# 4 · Push — durability ACKs -------------------------------------------------------
# stream_minutes = 0 -> one batch (fast verify). Otherwise stream, then flush and end
# like a real producer shutting down. A re-run re-streams safely: re-delivered records
# dedupe downstream.

with ZerobusProducer(server_endpoint, workspace_url, table_name, client_id, client_secret) as producer:
    if stream_minutes > 0:
        pushed = 0
        started = time.monotonic()
        for i, batch in enumerate(stream_batches(stream_minutes, event_count, duplicate_pct), 1):
            producer.push(batch)
            pushed += len(batch)
            elapsed = time.monotonic() - started
            print(f"push: batch {i} — {len(batch)} records "
                  f"(ACK durable; {pushed} pushed, {elapsed:.0f}s elapsed)")
    else:
        records = generate_one_batch(event_count, duplicate_pct)
        producer.push(records)
        pushed = len(records)

print(f"push: done — {pushed} records durably acknowledged")

# COMMAND ----------

# 5 · Verify arrival (soft check) --------------------------------------------------
# The medallion tables are the source of truth; this only confirms materialization.

counts = spark.sql(
    f"SELECT COUNT(*) AS arrivals, COUNT(DISTINCT event_id) AS events FROM {table_name}"
).first()
print(f"push: bronze now holds {counts.arrivals} records "
      f"({counts.events} distinct events — the difference is at-least-once re-delivery)")
