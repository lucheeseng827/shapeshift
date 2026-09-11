# Demo 5 — parallel multi-writer: scaling shapeshift across cores & hosts

**Goal:** run **many shapeshift instances at once** to write faster, and do it *correctly*.
This demo shows the safe patterns (which scale ~linearly), the one atomic-commit pattern
that lands parallel output in a **single** table, and — proven with real numbers — the
anti-pattern that silently loses data, plus why.

## shapeshift's write model: share-nothing

Each `shape` run is a **self-contained writer**: it produces its own data Parquet, its own
Avro manifests, and its own snapshot commit, sharing *no* mutable state with any other run.
That is what makes horizontal scale-out trivial — *as long as two writers never commit to
the same table at the same time.* A shapeshift append commits like a server-less Hadoop
catalog:

```text
read metadata/version-hint.text  →  N
write metadata/v{N+1}.metadata.json          ← fixed filename
write metadata/version-hint.text = N+1        ← fixed filename, published last
```

There is **no lock and no compare-and-swap** on that pointer (by design — the OSS core has
no catalog server). So the rule is simple:

> **Parallelise across disjoint commit targets. Never point two concurrent writers at one
> table dir.** Coordinating concurrent commits *into one table* is a catalog's job (atomic
> CAS on the pointer) — that's Pattern B/C below, or a REST catalog.

## Pattern A — parallel writers → shard tables → union at read *(recommended)*

Give each instance its own table; present them as one dataset at read time. Nothing is
shared, so there is nothing to race. [`scripts/parallel_write.sh`](./scripts/parallel_write.sh)
splits an input round-robin and launches one instance per shard:

```sh
INPUT=events.jsonl SHARDS=4 OUT=warehouse/events PARTITION_BY=region \
  ./scripts/parallel_write.sh
```

```text
writing events.jsonl across 4 parallel shapeshift instances → warehouse/events_shard0..3
parallel wall-clock: 0.37s   shard totals sum=120000   input rows=120000   OK — no rows lost
```

**Measured scaling** (120k–200k rows, 4-core box, local Iceberg output):

| writers | wall-clock | vs sequential |
|--------:|-----------:|--------------:|
| 1 (sequential, 4 shards) | 2.24s | 1.0× |
| 4 (parallel) | 0.61s | **~3.7×** |

Near-linear to core count — because the writers never touch each other's files. On a
cluster the same idea scales across **hosts**: each host writes shards to a shared bucket
prefix (`--features object_store`, `-o s3://lake/db/events_shardK`).

**Read the shards as one dataset** — register each in a catalog namespace and let the engine
union them, or a view:

```sql
-- DuckDB: one logical view over all shards
CREATE VIEW events AS
  SELECT * FROM iceberg_scan('s3://lake/db/events_shard0') UNION ALL
  SELECT * FROM iceberg_scan('s3://lake/db/events_shard1') UNION ALL
  SELECT * FROM iceberg_scan('s3://lake/db/events_shard2') UNION ALL
  SELECT * FROM iceberg_scan('s3://lake/db/events_shard3');
-- Trino/Spark: register each shard in the catalog and UNION ALL by name, or use a
-- table with per-writer partitions so the engine sees one table.
```

## Pattern B — parallel data production → one atomic catalog commit

When you want the output in a **single** governed table (not N shards), keep the *shaping*
parallel but do the *commit* once, through a catalog. This is how Spark/Flink write a lake:
many tasks emit data files concurrently; the catalog registers them all in one commit with
optimistic concurrency. [`scripts/commit_parallel_add_files.py`](./scripts/commit_parallel_add_files.py)
runs N shapeshift instances → N Parquet files in parallel, then one PyIceberg `add_files`:

```sh
CATALOG_TYPE=rest CATALOG_URI="$CATALOG_URI" CATALOG_WAREHOUSE="$CATALOG_WAREHOUSE" \
SHAPESHIFT=./target/release/shapeshift \
  python scripts/commit_parallel_add_files.py \
    --input events.jsonl --shards 4 --identifier analytics.events
# shaped 4 Parquet files across 4 parallel instances
# committed 4 files into analytics.events in one snapshot; total-records=120000
```

The expensive step (shaping bytes) is parallel; the commit is a fast metadata op. `add_files`
copies **no data** — it registers the existing Parquet into the table (applying the table's
name-mapping), so it's the cheap, safe way to land parallel output in one table.

## Pattern C — concurrent commits into one table (catalog optimistic concurrency)

If writers must commit **independently and concurrently** to the same table (e.g. long-lived
per-source jobs), route every commit through a catalog and **retry on conflict**. The catalog
CAS-es the table pointer, so exactly one commit wins each round and the losers retry against
the new base — no lost data, unlike the file-system append:

```python
from pyiceberg.exceptions import CommitFailedException
for attempt in range(10):
    try:
        tbl.refresh()
        tbl.add_files(file_paths=my_parquet_files)   # atomic CAS commit
        break
    except CommitFailedException:
        continue      # another writer committed first; refresh + retry
```

This is precisely the guarantee shapeshift's OSS file-system append cannot give on its own,
and what an Iceberg **REST catalog** provides out of the box
(multi-writer commits + copy-anywhere relocation — see the [README](../../README.md)
and [ROADMAP](../../ROADMAP.md)).

## Anti-pattern — concurrent `--append` to one table dir *(silent data loss)*

For contrast, point several concurrent `--append` runs at the **same** table dir. The
script has a `--race` mode that does exactly this on purpose:

```sh
./scripts/parallel_write.sh --race INPUT=events.jsonl SHARDS=4
```

```text
RACE MODE: seed + 3 concurrent --append into a single table (expect data loss)
input rows=120000   table total-records=60000   (mismatch ⇒ lost writes from the race)
```

**What happened** (verified end-to-end): the script writes shard 0 synchronously as the
**seed** (a fresh table → `version-hint = 1`), then races the remaining `SHARDS-1` writers as
concurrent `--append`s. All three appenders read `version-hint = 1`, each built a
`v2.metadata.json` carrying only *its own* new manifest, and each wrote the two fixed-name
files (`v2.metadata.json`, `version-hint.text`). Last writer wins → the final snapshot
references the **seed plus one** winning appender; the other **two** appenders' Parquet is
**on disk but orphaned** (not in any live snapshot). Here 60,000 of 120,000 rows survived
(seed 30k + one 30k append). The loss amount is **nondeterministic** (it depends on
scheduling) and, worst of all, **no error is raised** — the run reports success. Never do this.

```text
✗ same dir:  seed  ──▶ v1              ✓ shard / commit-coordinated:
             appender1 ─┐ all read v1,   each writer → disjoint target,
             appender2 ─┤ all write "v2"  or one atomic catalog commit →
             appender3 ─┘ → last wins;    every row lands exactly once
                          2 appends orphaned
```

## Which pattern to use

| You want… | Use | Safe? | Scaling |
|---|---|---|---|
| Max write throughput, union at read | **A** — shard tables | ✅ share-nothing | ~linear to cores/hosts |
| One governed table, batch load | **B** — parallel shape + one `add_files` | ✅ single commit | parallel shaping |
| Independent concurrent committers, one table | **C** — catalog CAS + retry | ✅ optimistic concurrency | serialised commits |
| (anything) concurrent `--append`, same dir | — | ❌ **silent data loss** | — |

**Rule of thumb:** parallelise the *shaping* freely; serialise (or CAS) the *commit*.
shapeshift makes shaping embarrassingly parallel; let a catalog own the commit when the
target is a single shared table.

---

Back to the [index](./README.md) · why a multi-writer catalog is out of scope:
[ROADMAP](../../ROADMAP.md).
