# A real Debezium feed, checked against its database

`run.sh` puts a real change feed through shapeshift and checks the result against the database
it came from:

1. It starts Postgres or MySQL, Kafka, and Kafka Connect with Debezium, in containers.
2. It runs a workload of 400 inserts, updates and deletes over 60 keys, with keys deleted and
   inserted again among them.
3. It drains the connector's topic to JSON Lines.
4. It applies the feed with `shapeshift shape --spec postgres.yaml` (or `mysql.yaml`), which use
   `changes.preset: debezium-postgres` or `debezium-mysql`. The feed is **shuffled and split
   into four writes**, so a key's later change often lands before its earlier one. The preset
   orders by the log position, so the order must not matter.
5. It has DuckDB compare the Iceberg table with the database's final rows.
6. It replays the whole feed once more, and the table must not change.

```sh
cargo build -p shapeshift-cli
DOCKER=podman examples/changes/debezium/run.sh postgres
DOCKER=podman examples/changes/debezium/run.sh mysql
```

It needs `python3` with `duckdb`, and `curl`. On a machine where the shapeshift binary does not
run (a Linux binary on another OS), set `FEED_OUT=<dir>`. The script then stops after draining
and leaves `feed.jsonl` and `expected.csv` there, for `verify.py` to run where the binary runs.
Set `PYTHON` if `python3` is not on the path; it can be a container command, such as
`podman run --rm -i python:3.13-slim python3`.

The connector runs the JSON converter with schemas off and `tombstones.on.delete: false`. These
are what the presets read: a delete's key comes from `before`, and Kafka's own null tombstones
are not needed.

## A transaction open across the snapshot (`handoff/`)

A connector's snapshot and the stream after it meet at a point, and a transaction open across
that point belongs cleanly to neither. The presets handle it by making every snapshot row
(`op: r`) older than every change (docs/SPEC.md §5b). Two feeds taken with such a transaction
open are kept in `handoff/`, each with its database's final rows. `shapeshift-cli/tests/handoff.rs`
applies them in shuffled writes and compares.

- `postgres-feed.jsonl`: taken with Debezium Server 2.7.4 and Postgres 16.15. The transaction
  commits after the snapshot with LSNs below the snapshot's, so ordered by LSN alone its update
  and delete would lose to the snapshot rows. Taking it needs a replication slot made
  consistent around the open transaction, which `run.sh` does not arrange.
- `mysql-feed.jsonl`: taken with `HANDOFF=1 run.sh mysql`, which holds the transaction open while
  the connector takes its snapshot. MySQL's binlog is in commit order, so the transaction's
  changes come after the snapshot's position: the presets' rule is not needed there, and must
  change nothing.

```sh
HANDOFF=1 FEED_OUT=<dir> DOCKER=podman examples/changes/debezium/run.sh mysql
```

In Git Bash on Windows, also set `MSYS_NO_PATHCONV=1`, or the paths inside the containers are
rewritten.

## Recorded

Recorded on 2026-11-27 with Debezium 2.7, Postgres 16 and MySQL 8. For both databases:

- 400 changes in four shuffled writes produced 39 rows, exactly the database's. On the way, 12
  live rows were deleted and 83 changes were stale.
- The replay changed nothing.
