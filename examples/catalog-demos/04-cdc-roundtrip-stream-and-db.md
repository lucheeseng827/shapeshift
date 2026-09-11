# Demo 4 — CDC round-trip: database → stream → lake → catalog → database

**Goal:** the full loop. A **database** is the source *via its change log* (Debezium CDC),
a **stream** (Kafka) carries the changes, shapeshift lands them as an Iceberg **change-log
table** in the **catalog**, and a **database sink** materialises current state. This is the
demo that uses streaming data **and** a database as both source and sink at once.

```text
Postgres ──WAL──▶ Debezium ──▶ Kafka topic  ──(micro-batch)──▶ shapeshift --append ──▶ Iceberg changelog
 (source DB)      (CDC)         (stream)                                                     │
                                                                        register_table ──────┤
                                                                                             ▼
                                                                                    REST catalog
                                                                                             │
                                                    latest-per-key MERGE / view ─────────────┤
                                                                                             ▼
                                                                              Snowflake / Postgres (sink DB)
```

**Why CDC (vs. Demo 2's watermark).** A watermark query catches inserts and updates but
**never sees a hard `DELETE`**. The database's change log does — every insert, update, *and*
delete, in commit order. Debezium turns that log into a Kafka stream of change events;
shapeshift shapes those events append-only, so the Iceberg table becomes a faithful,
replayable **change-log** you collapse to current state on read.

## 0. Setup

```sh
cargo build --release -p shapeshift-cli --features object_store
docker compose -f scripts/docker-compose.yaml up -d      # Postgres has wal_level=logical
cp scripts/env.example scripts/.env && source scripts/.env
```

The compose file starts Postgres with `wal_level=logical` (required for CDC). Seed the
source table (same `orders` as Demo 2 is fine).

## 1. Point Debezium at Postgres, flatten to the stream

Run Debezium (Kafka Connect) — or Redpanda Connect / Conduit; any CDC tool that writes
JSON to Kafka works. The one important setting is the **ExtractNewRecordState** SMT
(`unwrap`) with delete rewriting, so each Kafka message is a **flat row** plus CDC
metadata (`__op`, `__deleted`, `__source_ts_ms`) instead of the nested `before/after`
envelope — which is exactly the flat JSONL shapeshift wants:

```json
// connector config (essentials)
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "postgres", "database.port": "5432",
  "database.user": "postgres", "database.password": "postgres",
  "database.dbname": "warehouse", "topic.prefix": "wh",
  "table.include.list": "public.orders", "plugin.name": "pgoutput",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "op,source.ts_ms,lsn"
}
```

`lsn` is added because `source.ts_ms` (a millisecond timestamp) is **not** unique — several
changes can share a millisecond. The Postgres LSN (log sequence number, an `int64`) is
strictly monotonic in commit order, so it is the correct tiebreaker for "latest change per
key" (see step 4).

A change then arrives on topic `wh.public.orders` looking like:

```json
{"id":1,"customer":"Ada","amount":12.50,"status":"shipped","created_at":"2026-07-13T09:15:00Z","__op":"u","__deleted":"false","__source_ts_ms":1752400000000,"__lsn":24567123}
{"id":2,"customer":"Grace","amount":3.00,"status":"pending","created_at":"2026-07-13T10:00:00Z","__op":"d","__deleted":"true","__source_ts_ms":1752400500000,"__lsn":24567891}
```

## 2. A strict spec for the change-log table

Map the flat CDC record to a governed change-log schema — the business columns **plus** the
CDC bookkeeping that lets you collapse to current state. Save as `orders_cdc.spec.yaml`:

```yaml
dataset: orders_changelog
source: { format: jsonl }
output:
  format: iceberg
  path: s3://lake/db/orders_changelog
  compression: snappy
  partition_by: [ 'day(cdc_ts)' ]          # partition by change time
schema: strict
columns:
  - { name: id,       from: id,             type: int64,  required: true }
  - { name: customer, from: customer,       type: string }
  - { name: amount,   from: amount,         type: float64 }
  - { name: status,   from: status,         type: string }
  - { name: op,       from: __op,           type: string }   # c=create u=update d=delete r=snapshot
  - { name: deleted,  from: __deleted,      type: string }   # "true" on a delete event
  - { name: cdc_ts,   from: __source_ts_ms, type: timestamp } # integer ms → TIMESTAMP
  - { name: cdc_lsn,  from: __lsn,          type: int64 }     # monotonic tiebreaker for cdc_ts
```

> Integer epoch-ms coerces straight to `timestamp` (shapeshift reads integer timestamps as
> epoch-milliseconds — see the module [README](../../README.md) "v0.1 limits").

## 3. Stream the change log into Iceberg

Same drainer as Demo 1, aimed at the CDC topic and the strict spec:

```sh
TOPIC=wh.public.orders \
SPEC=orders_cdc.spec.yaml \
OUT=s3://lake/db/orders_changelog \
PARTITION_BY='day(cdc_ts)' \
DRAIN_SECONDS=30 \
  ./scripts/stream_drain.sh
```

Every window appends the latest change events as a new snapshot. Now make some changes
upstream and watch them flow:

```sh
psql -c "UPDATE orders SET status='shipped', updated_at=now() WHERE id=1;"
psql -c "DELETE FROM orders WHERE id=2;"
# → the update and the delete both land as rows in orders_changelog
```

## 4. Register and materialise current state

```sh
python scripts/register_catalog.py --table s3://lake/db/orders_changelog \
  --identifier analytics.orders_changelog
```

Collapse the change log to a **current-state** view — latest event per key, deletes removed:

```sql
CREATE VIEW analytics.orders_current AS
SELECT * EXCLUDE (rn, op, deleted, cdc_ts, cdc_lsn) FROM (
  SELECT *, row_number() OVER (PARTITION BY id ORDER BY cdc_ts DESC, cdc_lsn DESC) AS rn
  FROM analytics.orders_changelog
) WHERE rn = 1 AND deleted = 'false';
```

Ordering by `(cdc_ts DESC, cdc_lsn DESC)` breaks millisecond ties deterministically on the
monotonic LSN, so the true latest change always wins.

`id=1` shows `shipped` (latest update wins); `id=2` is gone (its last event was a delete).
The **change-log table keeps full history** — audit, time-travel, and replay all still work.

## 5. Land current state in the sink database

Push change into the sink DB on a schedule (this is the "database as sink" edge from
[Demo 3](./03-iceberg-to-database-sink.md)). Source the `MERGE` from the **changelog**, not
`orders_current` — the current-state view has already dropped tombstones, so it could never
tell the sink to *delete* a row. Take the latest change per key (including deletes) and give
`MERGE` a delete arm. Snowflake has no `UPDATE SET *` / `INSERT *`, so map columns explicitly:

```sql
MERGE INTO prod.orders t
USING (
  SELECT id, customer, amount, status, deleted
  FROM analytics.orders_changelog
  QUALIFY row_number() OVER (PARTITION BY id ORDER BY cdc_ts DESC, cdc_lsn DESC) = 1
) s
ON t.id = s.id
WHEN MATCHED AND s.deleted = 'true' THEN DELETE
WHEN MATCHED THEN UPDATE SET
  t.customer = s.customer, t.amount = s.amount, t.status = s.status
WHEN NOT MATCHED AND s.deleted <> 'true' THEN
  INSERT (id, customer, amount, status) VALUES (s.id, s.customer, s.amount, s.status);
```

Or, locally, DuckDB → Postgres — a full refresh from the current-state view (which already
excludes deletes, so dropped rows disappear from the sink too):

```sql
CREATE OR REPLACE TABLE pg.public.orders AS SELECT * FROM analytics.orders_current;
```

## The full picture

| Stage | System | Role |
|-------|--------|------|
| Capture | Postgres + Debezium | **database as source** (via change log) |
| Transport | Kafka / Redpanda | **streaming** carrier |
| Shape | shapeshift `--append` | JSON change events → Iceberg change-log |
| Govern | Nessie / Glue / Polaris | **catalog** — discovery + snapshot pointer |
| Serve | Snowflake / Postgres | **database as sink** (current state via MERGE) |

shapeshift owns exactly one box — *shape JSON → Iceberg, append-only, bounded RAM* — and the
open Iceberg format makes every neighbouring box swappable. CDC connectors, in-table
`MERGE`/upsert, exactly-once delivery, and multi-writer catalog commits are out of scope;
this demo shows how far shapeshift alone gets you.

---

Back to the [index](./README.md).
