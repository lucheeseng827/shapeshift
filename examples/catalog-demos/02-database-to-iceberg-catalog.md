# Demo 2 — Database source → Iceberg → data catalog

**Goal:** replicate a **Postgres** table into a governed **Iceberg** table in the catalog —
a **full load** first, then **incremental** watermark loads — without a managed connector
and without paying per active row.

```text
Postgres ──(row_to_json + COPY)──▶ JSONL ──▶ shapeshift (strict spec) ──▶ Iceberg ──▶ REST catalog ──▶ Spark/Trino
   full load ─────────────────────────────────────▶ create table (v1)
   incremental (updated_at > watermark) ───────────▶ shape --append (v2, v3, …)
```

**The seam again.** A relational result set is JSON the moment you ask for `row_to_json`.
Postgres streams it with `COPY … TO STDOUT` (bounded memory), shapeshift shapes it (bounded
memory), and a **strict spec** pins the exact lakehouse schema — column names, types, and
transforms — instead of inferring per batch (so an incremental batch can never drift the
schema).

## 0. Setup

```sh
cargo build --release -p shapeshift-cli --features object_store
docker compose -f scripts/docker-compose.yaml up -d
cp scripts/env.example scripts/.env && source scripts/.env
```

Seed a table (stand-in for your production Postgres):

```sh
psql -c "CREATE TABLE orders(
           id bigint PRIMARY KEY, customer text, amount numeric,
           status text, created_at timestamptz, updated_at timestamptz DEFAULT now());"
psql -c "INSERT INTO orders(id,customer,amount,status,created_at) VALUES
           (1,'Ada',   12.50,'paid',   '2026-07-13T09:15:00Z'),
           (2,'Grace',  3.00,'pending','2026-07-13T10:00:00Z'),
           (3,'Linus', 99.99,'paid',   '2026-07-14T00:01:00Z');"
```

## 1. Pin the lakehouse schema with a strict spec

Inference is great for exploration, but for a replicated table you want a **contract**.
Save this as `orders.spec.yaml` — it selects DB columns, coerces types, and normalises
money to integer cents:

```yaml
dataset: orders
source: { format: jsonl }
output:
  format: iceberg
  path: s3://lake/db/orders
  compression: snappy
  partition_by: [ 'month(created_at)' ]    # coarse partition for scan pruning
schema: strict                             # write ONLY these columns; drift is refused
columns:
  - { name: id,           from: id,         type: int64,     required: true }
  - { name: customer,     from: customer,   type: string }
  - { name: amount_cents, from: amount,     type: int64,     transform: dollars_to_cents }
  - { name: status,       from: status,     type: string,    transform: uppercase }
  - { name: created_at,   from: created_at, type: timestamp }
  - { name: updated_at,   from: updated_at, type: timestamp }
```

> `output.partition_by` in the spec is equivalent to `--partition-by` on the CLI. Keep
> partitions coarse (`month`/`day`, `region`, `status`) — not the primary key.

## 2. Full load (creates the table)

Export the whole table to JSONL and shape it. [`scripts/pg_to_jsonl.sh`](./scripts/pg_to_jsonl.sh)
wraps `row_to_json` + `COPY`:

```sh
TABLE=public.orders ./scripts/pg_to_jsonl.sh full > orders.jsonl
shapeshift shape -s orders.spec.yaml -i orders.jsonl --to iceberg
# shaped `orders` → s3://lake/db/orders (Iceberg)   rows_in=3 rows_out=3 …
```

For very large tables, pipe straight through (no intermediate file) on Linux:

```sh
TABLE=public.orders ./scripts/pg_to_jsonl.sh full \
  | shapeshift shape -s orders.spec.yaml -i /dev/stdin --to iceberg
```

## 3. Register in the catalog

```sh
pip install "pyiceberg[s3fs]"
python scripts/register_catalog.py --table s3://lake/db/orders --identifier analytics.orders
# → registered analytics.orders → s3://lake/db/orders/metadata/v1.metadata.json
```

## 4. Incremental loads (watermark → append snapshots)

Pull only rows changed since the last run, keyed on a monotonic column (`updated_at` here),
and append each delta as a new snapshot. The watermark is tracked in a local file:

```sh
# some rows change upstream…
psql -c "UPDATE orders SET status='shipped', updated_at=now() WHERE id=1;"
psql -c "INSERT INTO orders(id,customer,amount,status,created_at)
           VALUES (4,'Edsger',7.25,'paid','2026-07-15T08:00:00Z');"

# …pull just the delta and append it:
TABLE=public.orders WATERMARK_COL=updated_at WATERMARK_FILE=.wm.orders \
  ./scripts/pg_to_jsonl.sh incremental > delta.jsonl
shapeshift shape -s orders.spec.yaml -i delta.jsonl --to iceberg --append
# watermark(updated_at) → 2026-07-15 08:00:00+00

# move the catalog pointer to the new current metadata:
python scripts/register_catalog.py --table s3://lake/db/orders --identifier analytics.orders --replace
```

Run steps 4 on a schedule (cron / dagron / your orchestrator) for continuous replication.
Because the delta contains the *new* version of a changed row (id=1 now `shipped`), the
table holds both versions — resolve latest-per-key at read time:

```sql
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
  FROM analytics.orders
) WHERE rn = 1;
```

## 5. What this is (and isn't)

- ✅ Full + incremental (append) replication of a table into governed Iceberg, self-hosted,
  no per-row bill, schema pinned by a strict spec.
- ✅ Works for **MySQL** (`JSON_OBJECT(...)` / `SELECT ... INTO OUTFILE` as JSON),
  **SQL Server**, **Snowflake** (`SELECT OBJECT_CONSTRUCT(*)`), or any DB that can emit one
  JSON per row — swap the exporter, keep everything downstream.
- ⚠️ **Watermark incremental catches inserts and updates, not hard deletes.** For deletes
  you need the DB's change log — that's [Demo 4 (CDC)](./04-cdc-roundtrip-stream-and-db.md).
  In-table `MERGE`/upsert is a commercial-edition feature.

---

Next: [Demo 3 — read it back into a database sink](./03-iceberg-to-database-sink.md).
