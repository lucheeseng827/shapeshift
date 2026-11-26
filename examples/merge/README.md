# Upsert into an Iceberg table — `--merge-on`

A source with updates (a nightly export of an `orders` table, or a change feed reshaped
to JSONL) carries the same key again and again. `--append` keeps one row per *version*;
`--merge-on` keeps one per *key*.

## Try it

```sh
cat > day1.jsonl <<'EOF'
{"id": 1, "name": "alice", "status": "new", "amount": 10.0}
{"id": 2, "name": "bob",   "status": "new", "amount": 20.0}
{"id": 3, "name": "carol", "status": "new", "amount": 30.0}
EOF
cat > day2.jsonl <<'EOF'
{"id": 2, "name": "bob",  "status": "paid", "amount": 25.0}
{"id": 4, "name": "dave", "status": "new",  "amount": 40.0}
{"id": 4, "name": "dave", "status": "paid", "amount": 45.0}
{"name": "ghost", "status": "new", "amount": 1.0}
EOF

shapeshift shape -i day1.jsonl -o orders --to iceberg --merge-on id
shapeshift shape -i day2.jsonl -o orders --to iceberg --merge-on id
```

The second run reports what it did:

```text
merged on (id): 2 key(s) — 1 updated, 1 inserted; 1 prior row(s) replaced
  1 earlier occurrence(s) of a repeated key in the input were dropped (the last one wins)
  warning: 1 row(s) have a null key — appended, but they match nothing and can never be updated by a later merge
  scanned 1 data file(s), pruned 0 by key bounds; 2 position delete(s) in 1 delete file(s)
```

And any Iceberg v2 reader sees one row per key:

```sql
-- DuckDB 2.0
INSTALL iceberg; LOAD iceberg;
SELECT * FROM iceberg_scan('orders') ORDER BY id NULLS LAST;
-- 1 alice new 10.0 | 2 bob paid 25.0 | 3 carol new 30.0 | 4 dave paid 45.0 | NULL ghost new 1.0
```

The rules:

- A row replaces any row already in the table with the same key.
- A key repeated in one input keeps its **last** occurrence (dave → `paid`).
- A row with a null key is appended and matches nothing, as in SQL `MERGE` (ghost).
- `--merge-on region,id` matches on every column of a composite key.
- `output.merge_on: [id]` is the spec equivalent.
- The first merge creates the table.

## How it lands on disk

```text
orders/
  data/<uuid>.parquet            day 1's rows (never rewritten)
  data/<uuid>.parquet            day 2's rows
  data/delete-<uuid>.parquet     (file_path, pos) of the 2 superseded rows
  metadata/<uuid>-m0.avro        data manifests
  metadata/<uuid>-m1.avro        the delete manifest
  metadata/v2.metadata.json      snapshot 2: operation "overwrite"
```

`shapeshift inspect orders` shows `live-records: 5 (7 written, 2 removed by position
deletes)`. Time-travel to snapshot 1 still shows day 1 exactly as it was.

## Verify it yourself: `verify_duckdb.py`

[`verify_duckdb.py`](./verify_duckdb.py) model-checks merge against DuckDB's Iceberg
reader. Six randomized scenarios each run a sequence of merges:

- unpartitioned;
- identity-partitioned, with keys moving between partitions;
- hidden partitions with a composite key;
- files of many row groups;
- climbing keys, so file pruning kicks in;
- large files: past the 64 KiB footer prefetch, with a composite key whose columns sit
  in adjacent chunks and delete files of tens of thousands of rows that the next merge
  reads back.

After every merge, the script compares `iceberg_scan` with a plain-Python model of the
rules above. It compacts the table along the way (see [Compact](#compact)). At the end it
time-travels to every snapshot and checks each one.

Every write runs with `--summary-json`, and the summary is checked against the table too:
the snapshot a merge, compaction or expiry says it committed must be DuckDB's newest, a
merge's key counts (updated, inserted, replaced, repeated, null) must be the model's, and an
expiry must name exactly the snapshots that are gone.

```sh
cargo build -p shapeshift-cli
python3 -m venv .venv && .venv/bin/pip install -r examples/merge/requirements.txt
.venv/bin/python examples/merge/verify_duckdb.py --bin target/debug/shapeshift
```

To see that it can fail, add `--negative-control`. Each delta is then written with
`--append` instead of `--merge-on`, and the run succeeds only if the check catches it
(the first round reports the duplicate rows).

### On an object store

`--merge-on` works the same with an `s3://` / `gs://` / `az://` output in a build with
`--features object_store`. Point `--tables` at a URL prefix to run the same six
scenarios there. Any S3-compatible server works for a local check; here is moto:

```sh
cargo build -p shapeshift-cli --features object_store
.venv/bin/moto_server -H 127.0.0.1 -p 5555 &   # moto and boto3 come with requirements.txt
.venv/bin/python examples/merge/s3_proxy.py --listen 5556 --upstream 5555 --delay-ms 1 &
export AWS_ENDPOINT=http://127.0.0.1:5556 AWS_ALLOW_HTTP=true \
       AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
.venv/bin/python -c "import boto3; boto3.client('s3', endpoint_url='http://127.0.0.1:5555').create_bucket(Bucket='lake')"
.venv/bin/python examples/merge/verify_duckdb.py --bin target/debug/shapeshift --tables s3://lake/verify
```

DuckDB reads the tables back through `httpfs` with the same `AWS_*` settings.
`--tables file:///abs/dir` runs the object-store code path on local disk, with no
server.

[`s3_proxy.py`](./s3_proxy.py) makes moto behave like S3 on the wire. moto closes each
connection after its response; S3 keeps it open, and a client that reuses a pooled
connection the wrong way only hangs against S3. Through the proxy it hangs here too, and any
request the binary had to retry fails the check. `--delay-ms` adds latency to every request,
so a run's request count costs what it would against a real store, and `GET /__stats` counts
the requests by kind (HEAD, GET, ranged GET, PUT, POST, DELETE) and the most in flight.

## Compact

Each merge leaves one more data file and one more delete file for every reader to apply.
`compact` rewrites the files carrying deletes without their deleted rows, packs small files
per partition, and commits the result as one `replace` snapshot:

```sh
shapeshift compact orders
```

```text
compacted orders → snapshot 4021474057372123141 (replace)
  rewrote 2 data file(s) (3.4 KiB) into 1 (1.7 KiB); kept 0 as they were
  applied 2 position delete(s) from 1 delete file(s) — the table has none left
  5 live row(s) rewritten across 1 partition(s); earlier snapshots still read their own files
```

`inspect` then shows `total-records: 5` with nothing removed by deletes, DuckDB reads the
same five rows, and time travel to snapshots 1 and 2 still returns what it did. Run it again
and it reports `nothing to compact`. `--target-file-size 0` only applies deletes and packs
nothing.

Compaction keeps the old files for the snapshots before it. `expire` lets those go:

```sh
shapeshift expire orders --older-than 0s
```

```text
expired 2 snapshot(s) of orders; kept 1
  deleted 2 data file(s), 1 delete file(s), 3 manifest(s), 2 manifest list(s) — 12.8 KiB
```

Without `--older-than` it expires snapshots older than five days. `--retain-last N` keeps the
newest N however old, and `--dry-run` reports without changing anything.

`verify_duckdb.py` compacts too: after every second merge and after the last
(`--compact-every N`, `0` to skip), at a randomly picked target, and checks the rows, that
no delete file is left live, the merges that follow, and time travel to every snapshot.
After each compaction it expires all but the newest 1-3 snapshots and checks that DuckDB
lists exactly those, reads the model, and time-travels to each.

## Concurrent writers

[`race_writers.py`](./race_writers.py) starts several writers merging into one table at once
(each over its own keys, a round at a time) while a janitor runs `compact` and `expire` in a
loop. A writer that exits 75 — the commit guard refused it, nothing was published — runs the
same round again; its `--summary-json` must say `conflict`. At the end DuckDB must read exactly
what each writer's last round says, and the table versions the successful runs reported must be
1..N, each claimed by exactly one run:

```sh
.venv/bin/python examples/merge/race_writers.py --bin target/debug/shapeshift
# ok  582 rows exactly as the writers left them: 40 writer commits, 67 retried after a conflict, ...
```

`--tables` takes a URL prefix as `verify_duckdb.py` does. `--negative-control` runs a binary
built before the guard (and before `--summary-json`, so it reads the text report) and succeeds
only if writes were lost.

## Tables another engine made: `verify_foreign.py`

[`verify_foreign.py`](./verify_foreign.py) makes a table with PyIceberg (deflate manifests,
partition summaries in the manifest list, zstd data files, as Iceberg's Java writers write
them), then has shapeshift append, merge, compact and expire it while PyIceberg writes to it in
between. DuckDB and PyIceberg must both read exactly the model after every step, and PyIceberg's
partition summaries must survive shapeshift's commits. It also checks that a format version 1
table is refused. It runs on a local path and through a `file://` URL:

```sh
cargo build -p shapeshift-cli --features object_store,zstd
.venv/bin/python examples/merge/verify_foreign.py --bin target/debug/shapeshift
# ok  interleaved writers (local path): 10 rows, v9; DuckDB and PyIceberg read the model after every step; ...
```

The `zstd` feature is needed to merge into and compact PyIceberg's zstd data files; appends
work in any build.

## Limits

- Concurrent writers are refused, not merged: the second of two that built on the same version
  exits 75 and must run again (`race_writers.py` checks exactly that).
- Delete files accumulate until the table is compacted. `inspect` shows how many rows they
  hide.
- `expire` keeps the old `metadata.json` files, one per commit.
- Memory grows with the delta, not with the table: an entry per distinct key, plus every position
  the merge deletes (each earlier repeat of a key in the input is one) and the positions earlier
  merges deleted.
