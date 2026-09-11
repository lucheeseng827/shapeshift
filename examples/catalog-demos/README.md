# shapeshift + data catalogs — streaming & databases as source/sink

These demos show how to wire the **OSS shapeshift** engine into a real lakehouse:
a **data catalog** (Iceberg REST catalog, AWS Glue, Nessie, Polaris, Unity) in the
middle, with **streaming systems** (Kafka / Redpanda / Kinesis) and **databases**
(Postgres / MySQL / Snowflake …) on either end as source *and* sink.

Everything here is the Apache-2.0 core — no row cap, no telemetry, no JVM. The demos
add only *thin, swappable* glue at the edges (a stream drainer, a DB exporter, a
`register_table` call). shapeshift stays exactly what it is: the shaping data-plane.

## The one idea that makes all of this work

shapeshift does **not** connect to Kafka or your database. It shapes **JSON/JSONL** into
Parquet or a self-contained **Apache Iceberg v2** table, and it exits. That is a feature,
not a gap — it means the seam between "your systems" and "the lakehouse" is a boring,
universal one: **a stream of JSON objects.**

```text
┌─────────────┐   JSONL    ┌───────────────────────┐  Iceberg v2 table   ┌──────────────┐
│  SOURCES    │ ─────────▶ │      shapeshift       │ ──────────────────▶ │   CATALOG    │
│ Kafka topic │  (one obj  │  infer · shape · cost │  data/*.parquet +   │ Glue/Nessie/ │
│ Postgres    │  per line) │  → Parquet / Iceberg  │  metadata.json      │ Polaris/REST │
│ Kinesis     │            │  --append (snapshots) │  (absolute paths,   │ register_    │
│ files/APIs  │            │  --partition-by       │   catalog-less)     │  table()     │
└─────────────┘            └───────────────────────┘                     └──────┬───────┘
       ▲                                                                          │
       │                                                                          ▼
┌──────┴──────┐                                                          ┌──────────────┐
│   SINKS     │  ◀─────────────  query / attach / COPY  ◀──────────────  │ QUERY ENGINE │
│ Snowflake   │                                                          │ Trino/Athena/│
│ Postgres    │                                                          │ Spark/DuckDB │
│ ClickHouse  │                                                          └──────────────┘
│ DuckDB      │
└─────────────┘
```

- **Streaming as a source** → drain the topic into JSONL in micro-batches; each batch is
  one `shape --append` = one Iceberg snapshot. (Demo 1, Demo 4.)
- **A database as a source** → `SELECT … → JSON` (one row per line) piped into `shape`.
  Full loads and watermark-based incrementals both fall out of this. (Demo 2.)
- **A catalog in the middle** → shapeshift writes the table files + `metadata.json`; a
  one-line `register_table(identifier, metadata_location)` publishes it to Glue / a REST
  catalog (Nessie, Polaris) so *any* engine discovers it by name. (Demo 1, Demo 2.)
- **A database as a sink** → point the warehouse at the shaped table: DuckDB
  `iceberg_scan`, Snowflake Iceberg/external tables, ClickHouse, or `COPY` into Postgres.
  (Demo 3.)
- **Many writers at once** → shapeshift is share-nothing, so N instances shape in parallel
  (~linear scaling); land them as shard tables, or in one table via a single atomic catalog
  commit. Concurrent `--append` to *one* dir is the one thing not to do. (Demo 5.)

## The demos

| # | Demo | Source | Sink | Catalog |
|---|------|--------|------|---------|
| [1](./01-streaming-to-iceberg-catalog.md) | **Streaming → Iceberg → catalog** | Kafka / Redpanda topic | Iceberg on S3 | Glue **or** Nessie/Polaris (REST) |
| [2](./02-database-to-iceberg-catalog.md) | **Database → Iceberg → catalog** | Postgres (full + incremental) | Iceberg on S3 | Nessie/Polaris (REST) |
| [3](./03-iceberg-to-database-sink.md) | **Iceberg → database sink** | shaped Iceberg/Parquet | Snowflake / Postgres / ClickHouse / DuckDB | any |
| [4](./04-cdc-roundtrip-stream-and-db.md) | **CDC round-trip** (DB → stream → lake → DB) | Postgres via Debezium → Kafka | Iceberg → Snowflake | Nessie/Polaris |
| [5](./05-parallel-multi-write.md) | **Parallel multi-writer** (scale writes across cores/hosts) | any JSONL, sharded | shard tables / one table via atomic commit | Nessie/Polaris (REST) |

Each demo is self-contained and copy-pasteable. The reusable glue lives in
[`scripts/`](./scripts/), and [`scripts/docker-compose.yaml`](./scripts/docker-compose.yaml)
brings up a **fully local** stack (Redpanda + Postgres + Nessie + MinIO) so you can run
Demos 1, 2, and 4 end-to-end on your laptop with no cloud account.

Demos 1, 2, 3, and 5 were run end-to-end against this stack on a clean EC2 box — see
[VALIDATION.md](./VALIDATION.md) for results, throughput/headroom numbers, and
arm64/Amazon-Linux notes.

## Prereqs

```sh
# 1. Build shapeshift. For object-store output (s3://, gs://, az://) add the feature:
cargo build --release -p shapeshift-cli --features object_store
alias shapeshift=./target/release/shapeshift

# 2. Edge tooling used by the scripts (install what your demo needs):
#    kcat (aka kafkacat) — Kafka consumer that emits one JSON message per line
#    psql               — Postgres client for the DB-source demo
#    pip install "pyiceberg[glue,s3fs]"   # register_table into Glue / a REST catalog
#    duckdb             — read the tables back (verification)

# 3. (Optional) the local all-in-one stack for Demos 1/2/4:
docker compose -f scripts/docker-compose.yaml up -d
cp scripts/env.example scripts/.env    # then `source scripts/.env`
```

> **Where shapeshift stops.** It writes **append-only** snapshots — no merge/upsert, no
> CDC state machine, no scheduler. The demos handle deletes/updates the lakehouse way
> (append the change events, resolve latest-per-key at read time or in a downstream
> `MERGE`). Connectors, incremental capture, multi-writer catalog commits, and
> copy-anywhere relocation are out of scope — see the
> [README](../../README.md) "What shapeshift is NOT" and the [ROADMAP](../../ROADMAP.md).

© 2026 Nicholas Lu Chee Seng and the shapeshift contributors — Apache-2.0.
