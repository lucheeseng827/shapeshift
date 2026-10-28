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
rules above. At the end it time-travels to every snapshot and checks each one.

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
export AWS_ENDPOINT=http://127.0.0.1:5555 AWS_ALLOW_HTTP=true \
       AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
.venv/bin/python -c "import boto3; boto3.client('s3', endpoint_url='http://127.0.0.1:5555').create_bucket(Bucket='lake')"
.venv/bin/python examples/merge/verify_duckdb.py --bin target/debug/shapeshift --tables s3://lake/verify
```

DuckDB reads the tables back through `httpfs` with the same `AWS_*` settings.
`--tables file:///abs/dir` runs the object-store code path on local disk, with no
server.

## Limits

- One writer at a time, like `--append`.
- Delete files accumulate until a compaction command exists. `inspect` shows how many
  rows they hide.
- Memory grows with the delta, not with the table: an entry per distinct key, plus every position
  the merge deletes (each earlier repeat of a key in the input is one) and the positions earlier
  merges deleted.
