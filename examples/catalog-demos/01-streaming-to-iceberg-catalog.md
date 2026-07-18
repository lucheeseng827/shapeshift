# Demo 1 — Streaming source → Iceberg → data catalog

**Goal:** land a live **Kafka/Redpanda topic** into a governed **Iceberg** table on object
storage, register it in a **catalog** (AWS Glue *or* a REST catalog like Nessie/Polaris),
and query it from Trino / Athena / DuckDB.

```text
Kafka topic ──(micro-batch drain)──▶ JSONL ──▶ shapeshift shape --to iceberg --append ──▶ s3://lake/db/events
                                                                                                │
                                                          register_table(metadata.json) ────────┤
                                                                                                ▼
                                                                       Glue / Nessie / Polaris catalog
                                                                                                │
                                                                    Trino · Athena · Spark · DuckDB
```

**Why micro-batch.** shapeshift shapes a finite input and exits; a topic is infinite. So we
drain the topic in short windows: each window's messages become one JSONL file, and one
`shape --append` turns that file into **one Iceberg snapshot**. The table grows
append-only, one snapshot per window — which is also exactly how you get time-travel and
incremental reads for free.

## 0. Setup

```sh
cargo build --release -p shapeshift-cli --features object_store   # s3:// output
docker compose -f scripts/docker-compose.yaml up -d               # local Kafka+MinIO+Nessie
cp scripts/env.example scripts/.env && source scripts/.env
```

Produce a few events onto the topic (stand-in for your real producer):

```sh
printf '%s\n' \
 '{"id":1,"user":{"name":"Ada","plan":"pro"},"amount":12.50,"event_at":"2026-07-13T09:15:00Z","region":"us"}' \
 '{"id":2,"user":{"name":"Grace","plan":"free"},"amount":3,"event_at":"2026-07-13T10:00:00Z","region":"eu"}' \
 | kcat -b "$BROKER" -t events -P
```

## 1. Drain the stream into an Iceberg table (micro-batches → snapshots)

[`scripts/stream_drain.sh`](./scripts/stream_drain.sh) loops: consumer-group drain for a
time window → `shape --append` → repeat. First non-empty window **creates** the table;
every later one **appends** a snapshot.

```sh
TOPIC=events \
OUT=s3://lake/db/events \
PARTITION_BY='day(event_at),region' \
DRAIN_SECONDS=30 \
MAX_WINDOWS=1 \
  ./scripts/stream_drain.sh
```

```text
draining localhost:9092/events (group=shapeshift-events) → s3://lake/db/events  [30s windows]
window 1: shaping 2 messages → s3://lake/db/events
shaped `batch` → s3://lake/db/events (Iceberg)
rows_in=2 rows_out=2 rejected=0 parse_errors=0 row_groups=1 bytes=…
```

Drop `MAX_WINDOWS` to run continuously. Each window appends a snapshot; the shape below is
what three windows produce (**verified locally** with `--to iceberg --append`):

```text
events/
  data/event_at_day=2026-07-13/region=us/<uuid>.parquet   # partitioned, one file per value
  data/event_at_day=2026-07-13/region=eu/<uuid>.parquet
  data/event_at_day=2026-07-14/region=us/<uuid>.parquet
  metadata/v1.metadata.json   # window 1 (create)
  metadata/v2.metadata.json   # window 2 (append snapshot)
  metadata/v3.metadata.json   # window 3 (append snapshot)
  metadata/version-hint.text  # → 3  (current)
```

> **Tuning the batch.** Longer `DRAIN_SECONDS` = fewer, larger snapshots and Parquet files
> (better scan performance, less metadata churn); shorter = fresher data, more small files.
> Partition on a **coarse** column (`day(event_at)`, `region`) — never a high-cardinality
> id, or you get a file per row. Run a periodic compaction (re-shape a day's data into one
> snapshot) if small files pile up; the OSS engine is append-only, so compaction is just
> another shape.

## 2. Register the table in a catalog

The table is already valid and readable by path. A **catalog** adds discovery-by-name and
lets remote engines find it. [`scripts/register_catalog.py`](./scripts/register_catalog.py)
reads `version-hint.text` → the current `metadata.json` and calls PyIceberg
`register_table` — **no data is copied**.

**Option A — REST catalog (Nessie / Polaris / any Iceberg REST):**

```sh
pip install "pyiceberg[s3fs]"
CATALOG_TYPE=rest CATALOG_URI="$CATALOG_URI" CATALOG_WAREHOUSE="$CATALOG_WAREHOUSE" \
  python scripts/register_catalog.py --table s3://lake/db/events --identifier analytics.events
# → registered analytics.events → s3://lake/db/events/metadata/v3.metadata.json
```

**Option B — AWS Glue:**

```sh
pip install "pyiceberg[glue,s3fs]"
CATALOG_TYPE=glue AWS_REGION=us-east-1 \
  python scripts/register_catalog.py --table s3://lake/db/events --identifier analytics.events
```

After each subsequent batch of windows, move the catalog pointer to the newest metadata:

```sh
python scripts/register_catalog.py --table s3://lake/db/events --identifier analytics.events --replace
```

> To keep the pointer live automatically, register once at table creation, then re-register
> `--replace` on a schedule (cron / the loop that runs `stream_drain.sh`). Auto-advancing
> commits on every append is what the commercial control plane's managed catalog does.

## 3. Query it from any engine

**Trino** (Iceberg connector pointed at Nessie/Glue). shapeshift wrote a real Iceberg
**hidden** partition spec (`day(event_at)` → `source-id` = `event_at`), so you filter the
**source column** and the engine prunes partitions automatically — you never reference the
physical `event_at_day` directory:

```sql
SELECT region, count(*), round(sum(amount),2)
FROM analytics.events
WHERE event_at >= TIMESTAMP '2026-07-13 00:00:00'   -- Iceberg prunes via day(event_at)
  AND event_at <  TIMESTAMP '2026-07-14 00:00:00'   -- + per-file min/max bounds
GROUP BY region;
```

**Athena** (after Glue registration) — same SQL, `FROM analytics.events`.

**DuckDB** (catalog-less, straight at the bucket — great for a quick check):

```sh
duckdb < scripts/read_back.sql   # edit the s3:// block + MinIO creds first
```

## 4. Handling updates & deletes (append-only)

shapeshift snapshots are append-only, so a changed row arrives as a **new event**, not an
in-place update. Resolve the current row at read time (latest event per key).

`event_at` alone is **not** a safe ordering key — two events for the same `id` can share a
millisecond, and the tie would resolve arbitrarily. Order by `event_at` **plus a monotonic
tiebreaker** that your producer stamps per event (a sequence number, a Kafka
`partition`+`offset`, or a ULID). Assuming the producer includes a monotonic `seq`:

```sql
SELECT * EXCLUDE (rn, seq) FROM (
  SELECT *, row_number() OVER (PARTITION BY id ORDER BY event_at DESC, seq DESC) AS rn
  FROM analytics.events
) WHERE rn = 1;
```

If your events truly have no per-key ordering column, add one upstream — there is no
correct latest-per-key without it.

Or materialise it downstream with a periodic `MERGE`/CTAS in your engine. True in-table
`MERGE`/upsert and CDC state are commercial-edition features — see the module
[README](../../README.md).

---

Next: [Demo 2 — a database as the source](./02-database-to-iceberg-catalog.md) ·
[Demo 4 — the full CDC round-trip](./04-cdc-roundtrip-stream-and-db.md).
