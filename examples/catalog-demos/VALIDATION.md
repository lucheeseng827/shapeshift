# catalog-demos — end-to-end validation

These demos were validated end-to-end on a clean AWS EC2 box against the local
lakehouse stack ([`scripts/docker-compose.yaml`](./scripts/docker-compose.yaml):
Redpanda + Postgres + Nessie + MinIO). All four runnable demos pass; Demo 4 (CDC)
was out of scope (the compose stack ships no Debezium connector).

## Environment

| | |
|---|---|
| Instance | `c7g.xlarge` — arm64 Graviton, 4 vCPU / 7.6 GiB, spot |
| OS | Amazon Linux 2023 |
| shapeshift | `0.1.1`, built `--release --features object_store` |
| Stack | Redpanda `v24.2.7`, Postgres 16, Nessie `0.99.0`, MinIO `2024-09-22` |
| Verifiers | DuckDB `1.1.3` (`iceberg_scan`), PyIceberg `0.10.0` (`register_table`) |

## Results

| Demo | Path | Result |
|------|------|--------|
| **5** Parallel multi-write | 500k JSONL → 4 local Iceberg shard tables | **PASS** — 0 rows lost; the `--race` probe correctly loses ~half (250k/500k) |
| **1** Streaming → Iceberg → catalog | Redpanda topic (100k) → `shape --append` → `s3://lake/db/events` → Nessie `analytics.events` | **PASS** — DuckDB `iceberg_scan` reads back 100000 rows |
| **2** Database → Iceberg → catalog | Postgres `public.orders` (200k) → `pg_to_jsonl` → shape → `s3://lake/db/orders` → Nessie `analytics.orders` | **PASS** — DuckDB reads back 200000 rows |
| **3** Iceberg → database sink | DuckDB `iceberg_scan('s3://…')` over the shaped tables | **PASS** — both tables read back with correct counts + sums |

## Performance & headroom

Single 4-vCPU / 7.6 GiB box; the docker stack idles at ~1.8 GiB, leaving ~5.5 GiB
free throughout — memory is never the constraint.

| Operation | Rows | Wall | Throughput | Peak RSS |
|-----------|------|------|------------|----------|
| `shape` → local Iceberg (1 core) | 500k | 1.53 s | ~327k rows/s | 16 MB |
| `shape` → local Iceberg (4 parallel) | 500k | 0.43 s | ~1.16M rows/s (~3.6×) | flat (+18 MB total) |
| `pg_to_jsonl` full export | 200k | 0.29 s | — | 10 MB |
| `shape` → `s3://` Iceberg (MinIO) | 200k | 0.71 s | ~282k rows/s | 20 MB |
| drain + `shape --append` → `s3://` | 100k | 1.13 s | ~88k rows/s | 26 MB* |

\* includes the kcat container. Per-`shape` process RSS stays flat in the 15–20 MB
range regardless of input size — the streaming, one-row-group-at-a-time write keeps
memory bounded. Parallel writers scale near-linearly to the core count.

Container footprint during the runs: Redpanda ~956 MiB, Nessie ~232 MiB, MinIO
~83 MiB, Postgres ~30 MiB.

## Notes for running on arm64 / Amazon Linux

Findings from the validation that make the scripts run on a fresh arm64 box:

- **`kcat` on arm64** — `edenhill/kcat:1.7.1` is amd64-only (`exec format error` on
  Graviton). Use the multi-arch `confluentinc/cp-kcat:7.7.0`, or the native `rpk`
  in the Redpanda container.
- **`stream_drain.sh` and a pre-existing backlog** — `kcat -G` on a *new* consumer
  group starts at `latest`, so draining a topic that already has messages reads 0.
  Either start the drainer *before* producing (it catches live messages, the real
  streaming case), or seed the group to the start first, e.g.
  `rpk group seek <group> --to start --topics <topic> --allow-new-topics`.
- **DuckDB as root** — set `HOME` (or `SET home_directory='/root';`) before `INSTALL`.
- **`shape -o s3://…` above a few MB** — requires the multipart part-size fix in
  `shapeshift-objstore` (parts must be a full 8 MiB, not whatever a single `read`
  returns, or S3/MinIO rejects non-final parts under 5 MiB with `EntityTooSmall`).
  Fixed upstream; needed for Demo 2's 200k-row table (3.95 MB) to land.
