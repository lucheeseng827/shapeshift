# shapeshift — Architecture

The internal design of record for **shapeshift**, a streaming shaper from JSON, CSV, TSV and
XML (files, or pulled from an endpoint or a database) to Parquet and Iceberg. This document describes how the engine is split into crates, how a record flows from
bytes to a columnar file, and the correctness-critical choices (the transactional per-row
append, the field-id chain, and the hand-rolled Avro encoder) that are easy to get wrong and
expensive to get wrong.

Scope: this is the **OSS engine** (Apache-2.0) — fully self-hostable, no row cap, no telemetry.
Scheduling, run history, and per-vendor connectors sit outside it, behind the boundary described
in the last section; nothing in this document depends on them. The generic pulled sources of 0.6
(§10b) are in it, each behind an off-by-default feature.

---

## 1. Design goals (the invariants everything else serves)

1. **Write the transform once, shape unbounded rows for free.** The product boundary is a
   declarative *dataset transform Spec* (§6). Everything downstream is deterministic execution
   of that spec. This is the direct answer to MAR (Monthly Active Rows) billing: a managed ELT
   vendor charges again every time the same rows are touched; shapeshift charges the CPU once.
2. **Bounded RAM regardless of input size.** A 100M-row JSONL file must shape in the memory
   footprint of a single row group. This constrains the whole pipeline to be pull-based and
   flush-per-batch (§5, §7).
3. **Single static binary.** The dependency set is chosen so the whole thing links musl-static:
   Snappy-only Parquet (no zstd/brotli C codecs), a hand-rolled Avro encoder instead of a
   general Avro crate, and `chrono` without the `clock` feature. One file, no runtime.
4. **The engine has no I/O backend.** `shapeshift-core` knows nothing about Parquet, Iceberg,
   or files. It speaks Arrow `RecordBatch` in and `Sink` out. Physical formats are drivers.
5. **Honesty over flattery.** Bad input lines are counted and sidecarred, never silently
   dropped and never fatal by default. The `cost` command never guesses a vendor price and will
   happily print a *negative* saving for a job too small to amortize.

---

## 2. The crate split

shapeshift is a standalone Cargo workspace with its own `Cargo.lock` (the heavy Arrow / parquet
/ simd-json stack resolves independently of the shared monorepo workspace, the same pattern as
sibling modules forge/recall/evald). Six crates make the engine; five more (0.5–0.6) are the
console, the agent server and the pulled sources, each a leaf the CLI links only behind a feature
(`mcp`'s is the exception: it is in the default binary, and depends on `serde_json` alone):

| Crate | Responsibility | Notable deps | I/O backend? |
|---|---|---|---|
| `shapeshift-core` | The engine. Spec model, JSON schema inference, the named Transform library, the Shaper (JSON value → Arrow `RecordBatch`), the `Sink` trait, MAR cost arithmetic. | arrow-array/arrow-schema 54, serde/serde_json/serde_yaml, chrono (no `clock`) | **none — by design** |
| `shapeshift-json` | The file readers. JSON over the simd-json SIMD lexer: `JsonlReader` (one object/line, bounded RAM) and `JsonArrayReader` (streamed element-at-a-time, bounded RAM); and, from 0.6, `CsvReader` (CSV and TSV) and `XmlReader`, each yielding one record a row or element (§10a). | simd-json 0.14, csv, quick-xml | read |
| `shapeshift-parquet` | `ParquetSink` (RecordBatch → Parquet, one row group per batch, Snappy default) + `inspect()` read-back. | parquet 54 `["arrow","snap"]` | write |
| `shapeshift-iceberg` | `IcebergSink`: a self-contained Apache **Iceberg v2** table (no catalog server), **append-to-existing / multi-snapshot** (`--append`) + `inspect()`. Owns the hand-rolled Avro encoder **and reader** (the reader carries a prior snapshot's manifests forward on append), and a **backend-agnostic table builder** (`IcebergDataWriter` + `build_metadata_artifacts`, paths anchored at a base location) the object-store sink reuses. | parquet 54, uuid v4 | write |
| `shapeshift-objstore` | `ObjectStoreParquetSink` **and** `ObjectStoreIcebergSink`: land a Parquet file / a whole Iceberg v2 table (data + manifests + metadata, put 8 at a time) in S3/GCS/Azure — data via a bounded-RAM multipart upload, metadata via `put`, paths anchored at the destination. The driver that reuses other drivers (`parquet`, `iceberg`); opt-in behind the CLI's `object_store` feature. | object_store 0.14, tokio (current-thread) | write (remote) |
| `shapeshift-cli` | The single `shapeshift` binary. Orchestrates source → shaper → sink and owns the CLI surface. | clap 4 (derive), anyhow, tracing | driver |
| `shapeshift-source-http` | A REST or SOAP endpoint as a `Source` (§10b): paging, the incremental cursor, credentials from the environment, redirects followed by hand. Feature `http`. | ureq 3 (rustls) | read (remote) |
| `shapeshift-source-mysql` | A MySQL query or table as a `Source`, in one consistent snapshot. Feature `mysql`. | mysql 28 (rustls, ring) | read (remote) |
| `shapeshift-source-postgres` | The same for Postgres, through a server-side portal. Feature `postgres`. | postgres 0.19, tokio-postgres-rustls (ring) | read (remote) |
| `shapeshift-serve` | `shapeshift serve`, the local console: a hand-rolled HTTP server on `std::net` (no async runtime), datasets, runs, alerts, push connectors, a read-only catalog browser. Feature `serve`. | ureq 3 (alert webhooks) | driver |
| `shapeshift-mcp` | `shapeshift mcp`'s core: JSON-RPC on stdio, the read-only tool tier, a path sandbox, capped results. | serde_json | none |

**Why this shape.** `core` is the I/O-free brain: it can be inference-tested, coercion-tested,
and cost-tested with no filesystem and no format crate on the dependency graph. `json`,
`parquet`, and `iceberg` are *drivers* — each depends on `core` (for the `Sink` trait, the error
type, and the compression enum) but never on each other. `objstore` is the one exception to
"never on each other": it reuses `parquet`'s `ParquetSink` and `iceberg`'s backend-agnostic table
builder and adds a remote-upload tail, so it depends on `core`, `parquet`, and `iceberg` — the
composition pattern for a new backend that reuses existing writers instead of reimplementing them.
`cli` is the only crate that knows they all exist; it wires a concrete source iterator into the
`Shaper` into a concrete `Sink`. The record model that crosses every seam is `serde_json::Value`
on the way in and Arrow `RecordBatch` on the way out — two well-understood, backend-neutral types.
`shapeshift-objstore` is the proof of the claim: adding object-store output for **both** formats
was a new crate implementing `Sink` (by wrapping the existing writers) with **nothing in `core`
changed**, and it is opt-in behind the CLI's `object_store` feature so the default binary links
no cloud SDKs.

The pulled sources of 0.6 follow the same pattern from the other end. `core` owns the `Source`
trait and every rule about positions (§10b); each source crate depends on `core` alone and
implements the trait, and the CLI links it only behind its feature, so the lean binary links no
network client and no database driver. `serve` is the second crate that composes others: behind
the same features it opens a dataset's source and browses a catalog, so a feature turned on in
the CLI turns on its console half too.

---

## 3. Component / flow diagram

```text
                          shapeshift-cli  (the `shapeshift` binary, clap)
                                   │  orchestrates; logs → stderr, output → stdout
        ┌──────────────────────────┼───────────────────────────────────────────┐
        │                          │                                            │
        ▼                          ▼                                            ▼
 shapeshift-json           shapeshift-core  (I/O-free engine)          shapeshift-parquet
 ┌───────────────┐        ┌────────────────────────────────┐         ┌──────────────────┐
 │ JsonlReader   │        │ infer   : sample → DatasetSpec  │         │ ParquetSink      │
 │  (stream,     │        │ spec    : DatasetSpec model     │         │  one row group   │
 │   bounded RAM)│        │ transform: value→value library  │         │  per batch       │
 │ JsonArray-    │  Value │ shaper  : Value → RecordBatch   │ Record  │  Snappy default  │
 │  Reader       ├───────►│   (2-phase transactional push)  ├──Batch─►│  + inspect()     │
 │               │  iter  │ types/coerce: logical→Arrow     │  per    └──────────────────┘
 │ simd-json     │        │ sink    : the Sink trait ◄──────┼── row_group_rows           │
 │  SIMD lexer   │        │ mar     : MAR cost arithmetic   │         shapeshift-iceberg
 └───────────────┘        └────────────────────────────────┘         ┌──────────────────┐
        ▲                                                             │ IcebergSink      │
        │ raw bad line preserved                                      │  data + manifest │
        │ → <output>.rejects.jsonl                                    │  + metadata json │
        │   {"line","error","raw"}                                    │  hand-rolled Avro │
        └────────────────────────────────────────────────────────────│  + inspect()     │
                                                                      └──────────────────┘

   Sink trait (in core):  write_batch(&RecordBatch)  ·  finish() -> SinkSummary
   Both sinks are driven ONLY through this trait; the engine never names a format.
```

---

## 4. End-to-end data flow

A `shapeshift shape` run is a single pass:

1. **Source.** The CLI opens a source iterator: `JsonlReader` for `jsonl` (one `serde_json::Value`
   per non-blank line, bounded RAM) or `JsonArrayReader` for `json-array` (streamed: a depth- and
   string-aware scanner yields one element at a time, bounded RAM). Each item is
   `Result<Value, JsonError>` carrying its 1-based source line (element index in array mode) so a
   downstream reject can be attributed back to the input.
2. **Spec.** Either a spec is supplied with `--spec`, or one is inferred on the fly (in which case
   `--input` and `--output` are required). Inference samples the first `infer_sample` records
   (default 1000; `--sample 0` = all), flattens nested objects into dotted columns, makes arrays
   `json` leaves, detects `YYYY-MM-DD` → date and RFC3339 → timestamp, widens int+float → float64
   and mixed scalar shapes → string. The result is a `DatasetSpec`.
3. **Compile.** The `Shaper` compiles the effective column list (declared columns win over
   inferred ones of the same name; strict mode uses declared columns only) into one `Compiled`
   struct per output column — name, source path, logical type, optional transform, required flag —
   and constructs one typed Arrow builder per column. The Arrow schema is fixed here and every
   emitted batch carries it.
4. **Shape loop.** For each source `Value`, `Shaper::push` produces `Appended` or `Rejected` (§5).
   Parse errors from the source and shaping rejects are both counted; both are written, one JSON
   object per line, to `<output>.rejects.jsonl` — `{"line","error","raw"}` — and neither aborts the
   run (lenient policy).
5. **Batch → Sink.** Whenever `pending` reaches `row_group_rows` (default 50000), `Shaper::flush`
   finalizes the builders into a `RecordBatch` and resets them; the CLI hands that batch to the
   `Sink` via `write_batch`. A final `flush` drains the tail.
6. **Finish.** `Sink::finish` writes footers / table metadata and returns a `SinkSummary`
   (files, rows, bytes). The CLI prints:
   `rows_in=.. rows_out=.. rejected=.. parse_errors=.. row_groups=.. bytes=..`.

---

## 5. The transactional per-row append (correctness-critical)

The Shaper holds one Arrow builder per column and appends **transactionally per row**. Because
column builders are independent, a naive "append each cell as you compute it" loop would, on a
mid-row failure, leave some builders with N+1 values and others with N — permanently misaligning
every subsequent column. `Shaper::push` avoids this with two strict phases:

- **Phase 1 — compute & validate, append nothing.** For each column: select the value at the
  column's path, apply the transform (if any), then coerce to the target type. Results go into a
  local `Vec<Cell>`. If a **required** column resolves to null/missing/uncoercible, the row is
  rejected *before any builder is touched*: in lenient mode `push` returns `Rejected(reason)`; in
  strict mode a required miss or *any* coercion failure returns `Err` and aborts the run. An
  optional column that is null or holds an uncoercible value becomes `Cell::Null`.
- **Phase 2 — commit.** Only once every cell for the row exists does the loop zip the cells into
  the builders and append them, then bump `pending`. Commit cannot half-happen.

The invariant: **every builder always has exactly `pending` values.** A rejected row leaves the
column state untouched, so `flush` can call `finish()` on all builders and `RecordBatch::try_new`
can never fail on a length mismatch. This is the mechanism that makes "the run never aborts on a
bad line" safe rather than merely permissive.

### Coercion policy

| Policy | Optional column | Required column |
|---|---|---|
| **lenient** (default) | missing / null / uncoercible → `null` | missing / null / uncoercible → **soft row reject** (row dropped, counted) |
| **strict** | — | a required miss **or any coercion failure anywhere** → **abort run** |

Coercion rules (value → logical type): int from an integral float or numeric string; float from
a number or numeric string; string stringifies scalars; date parses `YYYY-MM-DD`/RFC3339 (or an
integer epoch-day); timestamp parses RFC3339 / zone-less ISO as UTC (or an integer
epoch-millisecond). Transforms run **before** coercion.

---

## 6. The dataset transform Spec (the product boundary)

The spec is the durable artifact — write it once per source shape, then shape unbounded rows.
It is YAML or JSON:

```yaml
dataset: <name>
where:   <predicate>               # optional row filter (0.5): which source records the run takes
source:  { format: jsonl|json-array, path: <optional> }
output:  { format: parquet|iceberg, path: <path>, compression: snappy|zstd|uncompressed,
           partition_by: [<cols>] }
schema:  infer | strict            # default infer
columns:
  - { name, from: <dotted/$ path, default = name>, type, transform: <optional>, required: <bool> }
options: { flatten: <bool, default true>, row_group_rows: <default 50000>, infer_sample: <default 1000> }
```

### Path selection

The `from` path selects a value out of the record: dotted (`user.name`), an optional leading `$`
or `$.`, a numeric segment as an array index (`tags.0`), and a bare `$` for the whole record. An
**unresolved path is null** (which then follows the coercion policy above).

### Row filter (`where`, 0.5)

`where` is a subset of SQL's `WHERE` over the same paths (`shapeshift-core::predicate`). It is
parsed once when the spec loads, into a tree whose paths are compiled like a column's, and
evaluated first thing in `Shaper::push`, before drift detection and the per-row append: a record it
does not keep returns `PushOutcome::Filtered` and touches no builder, no drift counter and no
rejects sidecar. Evaluation runs on the `Record` trait, so it reads the simd-json tape as cheaply as
an owned value, and allocates nothing unless it has to describe a type mismatch.

- **SQL's three-valued logic.** Null, missing, and a comparison across kinds are unknown; a record
  is kept only on true. Cross-kind comparisons are counted, and the first is described, because the
  likeliest mistake (`status = '500'` against a number) otherwise keeps nothing in silence.
- **Its paths count as read by the spec**, so a field the filter tests but no column keeps
  (`record_type` in a table of one record type) is not drift.
- **Inference samples through it**, so an inferred schema is the kept records' alone. On a stream,
  dropped records are buffered for replay under the same bound as malformed ones.
- **Checked against DuckDB**: the grammar is SQL's, so CI runs generated expressions in both and
  requires the same rows (`examples/where/verify_duckdb.py`). It is also what 0.6's change-feed
  deletes need (`delete_when:`), so it is a module of its own.

### Type mapping (logical → Arrow → Parquet → Iceberg)

| Spec type | Arrow | Parquet | Iceberg |
|---|---|---|---|
| `bool` | `Boolean` | BOOLEAN | `boolean` |
| `int64` | `Int64` | INT64 | `long` |
| `float64` | `Float64` | DOUBLE | `double` |
| `string` | `Utf8` | BYTE_ARRAY (UTF8) | `string` |
| `date` | `Date32` | INT32 (DATE) | `date` |
| `timestamp` | `Timestamp(µs, no zone)` | INT64 (TIMESTAMP) | `timestamp` (not `timestamptz`) |
| `json` | `Utf8` | BYTE_ARRAY (UTF8) | `string` (nested value kept as a JSON-encoded string) |

The logical type set is deliberately small — the scalar spine plus two temporal types plus a JSON
escape hatch — because that is exactly what a JSON→columnar shaper needs and every member maps
cleanly to both Parquet and Iceberg. `date` is days-since-epoch; `timestamp` is
microseconds-since-epoch, zone-less.

### Transforms (value → value, applied before coercion)

| Transform | Effect |
|---|---|
| `lowercase` / `uppercase` | ASCII/Unicode case fold on strings |
| `trim` | strip surrounding whitespace |
| `json_encode` | serialize the value to a JSON string |
| `to_string` | stringify a scalar |
| `dollars_to_cents` | number × 100 → integer |
| `abs` | absolute value |
| `empty_to_null` | empty string → null |

---

## 7. Bounded RAM

Two independent mechanisms keep memory flat:

- **Source side.** `JsonlReader` reads one line at a time into a reused buffer (with SIMD slack
  past the line length so simd-json's chunked reads never touch memory outside the allocation)
  and yields one `Value`. Nothing accumulates. `JsonArrayReader` does the same per array
  *element* — a depth- and string-aware scanner finds each element's boundary, so only one
  element (under the same per-record byte cap) is buffered at a time. Both formats are
  bounded-RAM streaming paths.
- **Sink side.** The Shaper flushes a `RecordBatch` every `row_group_rows` and resets its
  builders. `ParquetSink::write_batch` writes the batch and immediately `flush()`es the writer,
  closing that batch's row group so the Parquet writer never buffers more than one row group. So
  peak memory is bounded by one row group's worth of column builders plus one in-flight batch,
  regardless of whether the input is 4 rows or 100M.

**Row group == batch.** The one-row-group-per-batch identity is the load-bearing decision: it is
simultaneously the RAM bound and the unit of streaming progress.

---

## 8. The Iceberg v2 sink

`IcebergSink` writes a **self-contained** Iceberg v2 table — no catalog server. One shape
produces exactly one atomic append snapshot (unpartitioned by default; `--partition-by` fans
rows out to one data file per partition under `data/<col>=<value>/`) with this on-disk layout:

```text
<table>/
  data/<uuid>.parquet                 data file (Parquet, carrying PARQUET:field_id)
  metadata/<uuid>-m0.avro             manifest       (lists data files)
  metadata/snap-<id>-1-<uuid>.avro    manifest list  (lists manifests)
  metadata/v1.metadata.json           table metadata (format-version 2)
  metadata/version-hint.text          → 1
```

`inspect` reads this back and reports format-version, table-uuid, current-snapshot-id,
total-records, and columns. The only wall-clock read in the entire engine is the snapshot
timestamp written here (which is why `chrono` is compiled without the `clock` feature everywhere
else — all other time is derived from the data).

### The field-id chain (why the table is genuinely readable)

Iceberg readers do **not** match manifest and data columns by name — they match by **field-id**.
shapeshift threads one consistent id space, `1..=N`, through four layers:

```text
Arrow field metadata            Parquet                Iceberg schema        Manifest Avro schema
PARQUET:field_id = 1..N   ──►   field_id on each  ──►  "id": 1..N in    ◄──   "field-id": … on the
(stamped by the sink)           Parquet column         v1.metadata.json       manifest_entry fields
```

Concretely: `schema_with_field_ids` clones the Arrow schema and stamps
`PARQUET:field_id = i+1` into each field's metadata before the data file is written, so
`parquet-rs` emits `field_id` on every Parquet column. `v1.metadata.json` assigns the same ids
`1..=N` in the Iceberg schema. The two Avro manifest schemas carry Iceberg's fixed `field-id`
attributes on their own fields. A reader (DuckDB `iceberg_scan`) joins the whole thing by id.

> Note: the plain `ParquetSink` (the `--to parquet` path) writes whatever Arrow schema it is
> given and does **not** stamp `PARQUET:field_id` — field-ids are an Iceberg concern and are
> stamped only by `IcebergSink`.

### Why hand-rolled Avro (correctness-critical)

The manifest and manifest-list are Avro Object Container Files. We **do not** use the general Rust
Avro crate (`apache-avro`) for them, and this is not a stylistic choice — it is a correctness
requirement:

- Iceberg readers map manifest columns by the `field-id` attributes embedded in the manifest's
  Avro **schema JSON** in the file header.
- `apache-avro` **drops those custom attributes** when it serializes the schema into the header.
- The resulting table is then unreadable: DuckDB fails with the internal error
  **"No default expression in FieldId Map"** — the reader can't map the manifest columns without
  the ids.

So `shapeshift-iceberg` emits the Object Container File itself (~150 LOC in `avro.rs`): magic
`Obj\x01`, a `map<string,bytes>` metadata block carrying the exact `field-id`-carrying
`avro.schema` JSON verbatim plus `avro.codec = null`, a sync marker, then one data block of
pre-encoded records. The binary encoding needed is small — zig-zag varint longs, length-prefixed
strings, unions, and (empty) records — and the two manifest schemas
(`manifest_entry`, `manifest_file`) are fixed constants with their Iceberg field-ids baked in.
Emitting it ourselves preserves the schema JSON byte-for-byte **and** drops a dependency, which
keeps the single-static-binary ethos intact.

**Reading is schema-driven, and takes other engines' files (0.5).** The reader parses the schema
each file carries and decodes against it, so it reads manifests and manifest lists whatever
fields and order the writer chose: other engines add `partitions` summaries and `key_metadata`
to the list, and v1 tables name the counts differently. Blocks are decompressed per the header's
`avro.codec`: `null` (ours), `deflate` (Iceberg's `gzip`, the Java and PyIceberg default; raw
RFC 1951 through `miniz_oxide`, pure Rust) and `snappy` (with its CRC-32 checked); a block must
decode to exactly its objects, and a decompressed block over 256 MiB is refused. `zstandard`
would need the C library and is refused by name. The `partitions` summaries are carried on
every manifest written back into a new list (`ManifestFileEntry::partitions`), so readers keep
pruning other engines' manifests. Those engines also write zstd **data** files, which only the
`zstd` build decodes; a merge or compaction in the lean build names the file and the build
(`batch_err`) instead of parquet's "Disabled feature at compile time". Writes onto format
version 1 or 3 tables are refused where every write checks the table's partition spec.

---

## 8a. Merge — row-level upsert (`--merge-on`)

`IcebergSink::create_merge` turns a write into an upsert on key columns. It is **merge-on-read**
with Iceberg v2 **position deletes**, chosen over copy-on-write so a merge's cost follows the size of
the *delta*, not the table: no existing data file is rewritten.

```text
write phase (streaming; memory follows the delta's keys and the rows it supersedes)
  batch ──► PartitionedWriter ──► data/<partition>/<uuid>.parquet     (exactly as an append)
              │ placement of every row: (file slot, position)
              ▼
            MergeTracker: key bytes ──► latest (slot, pos)    a repeat of a key → the earlier
                          per-key-column min/max              (slot, pos) is "superseded"

finish phase
  prior manifests ──► live data files + their already-deleted positions
     for each live data file:
       manifest key bounds disjoint from the write's key range? ──► skip (pruned)
       else read ONLY the key columns; a live row whose key the write carries ──► delete (file, pos)
  superseded positions in this write's own files ──► delete (file, pos)
  group by partition ──► data/<partition>/delete-<uuid>.parquet   (file_path, pos), sorted
  delete manifest (content = deletes) + data manifest ──► one snapshot, operation `overwrite`
```

The decisions that make it correct:

- **Placement is recorded at write time.** The partitioned writer reports where each row landed —
  its file's slot and its 0-based position in that file. That is exactly what a position delete
  names, so a key repeated within one write is resolved without re-reading anything: the earlier
  occurrence's `(slot, pos)` becomes a delete, and the last occurrence wins.
- **Deletes carry the deleted file's partition.** A reader applies a position delete only to data
  files in the delete file's own partition, so a delete written into the wrong partition is
  silently never applied. That is why the manifest reader decodes against the Avro schema each
  manifest carries (the partition record's layout depends on the spec) instead of assuming one.
- **Sequence numbers do the scoping.** The delete files and the new data files share the snapshot's
  sequence number, and a position delete applies to data files with a sequence number less than or
  equal to its own — so one snapshot's deletes reach both the old files and the ones it just wrote.
- **Never delete a dead row twice.** Before scanning a file, the merge reads the table's existing
  position-delete files; a row an earlier merge already removed is neither matched nor deleted
  again, so `total-position-deletes` stays exact and `inspect`'s live count is
  `total-records − total-position-deletes`.
- **Pruning only ever errs toward reading.** A file is skipped only when, for some key column, its
  manifest `[lower, upper]` bounds cannot overlap the write's `[min, max]`. Missing or malformed
  bounds never prune.
- **Null keys match nothing** (SQL `MERGE` semantics) and are appended, counted, and warned about.
- **Keys are compared exactly.** `int64`, `string`, `date`, `timestamp`, and `bool` keys are encoded
  to bytes (strings length-prefixed, so composite keys are unambiguous); `float64` is refused.

Memory is the trade: an append holds one row group, but a merge also holds **one entry per distinct
key in the delta** (the key's bytes plus a few dozen bytes) and the positions it deletes: one per
earlier repeat of a key in the input, one per prior row it replaces. It also loads the positions
earlier merges deleted, to skip them; those pile up until the table is compacted (§8b). It never
holds anything per live table row. A merge that loses a race to another writer is refused by the
commit guard (§8d) and run again.

**Where the table lives does not change the merge.** Every read of the existing table goes through
one trait, `TableFiles` — a whole file, a file's size, byte ranges of a file — implemented for local
disk (`LocalTableFiles`) and for an object store (`StoreFiles`, in `shapeshift-objstore`), and the
write half (`MergeWriter`) is shared by both sinks. Parquet is never read straight from the source:
the merge fetches a data file's footer, then — one row group at a time — exactly the byte ranges of
the key columns' chunks, and parquet decodes from that in-memory `RangeCache`. The file's size comes
from its manifest entry (`file_size_in_bytes`), so against an object store a data file costs one
footer `GET` and one coalesced ranged `GET` per row group — no `HEAD`, unless the recorded size
proves wrong and the footer cannot be found at it — where parquet's own sync reader would issue a
request per page header. The footer's read takes the file's last 64 KiB, so a smaller file arrives
whole with it, and a column chunk that read already holds is not fetched again: a small file is
one `GET`.

**Reads overlap.** A store answers each request in tens of milliseconds however many are in flight,
so reading one file after another leaves the merge waiting. The manifests, the delete files, and the
data files the merge must scan are each read by a small pool of threads — `TableFiles::readers`: 8
for an object store, up to 4 for local disk, `SHAPESHIFT_READERS` to override — and every result is
used in table order, so the delete files a merge writes do not depend on which read finished first.
Memory is bounded by one row group's key chunks *per reader*. The threads share `StoreFiles`' one
current-thread tokio runtime (whichever blocks on it first drives the I/O for all), and `StoreFiles`
has an HTTP client of its own that nothing else uses: a pooled keep-alive connection belongs to the
runtime it was opened on, and one reused from another runtime while its own is idle never sends its
request.

**Writes overlap too.** The object-store sink stages its delete files beside its data files, and
puts them, the manifests and the manifest list as one batch — 8 requests in flight, at most 16 MiB
of file bytes in memory (two 8 MiB parts, what one multipart upload held when uploads went one at a
time) — since nothing names those objects until the metadata file does. A file that fits in a part
is one `PUT`; a larger one is a multipart upload. Then the guarded metadata file, then
`version-hint.text`. If any put fails or the commit is refused, every object of the batch is
deleted again, so a failed write leaves nothing behind. Expiry deletes the same way, 8 at a time.

## 8b. Compaction — `shapeshift compact`

Merge-on-read makes every merge cheap and every read a little dearer: one more data file, one more
delete file every reader loads and applies, one more manifest (two, with the delete manifest).
`compact` (the `compact` module of `shapeshift-iceberg`) pays that back in one snapshot.

```text
current snapshot ──► live data files + the positions their delete files remove
  group by partition, in manifest order
    a file carrying a delete ─────────────► always rewritten
    a clean file below the target size ───► packed, if its partition has another to pack it with
    any other file ───────────────────────► kept
  bins of up to --target-file-size (by each input's live share of its bytes)
    each bin ──► read every column, one row group at a time; drop the deleted positions;
                 lay the rows out as the current schema (a column a file predates → null)
             ──► data/<partition>/<uuid>.parquet   (a bin with no live row writes nothing)
  (readers fetch the bins' files ahead of the one writer, in order — see below)
one data manifest:   ADDED (new files) · EXISTING (kept, original sequence numbers) · DELETED (inputs)
one delete manifest: DELETED (every delete file)       ──► one snapshot, operation `replace`
```

The decisions that make it correct:

- **Every file carrying a delete is rewritten, so every delete file can go.** A delete file names
  the data files it applies to by path; once each of them is rewritten, the delete file applies to
  nothing live. The table ends with **no delete files**, `total-position-deletes` is 0, and
  `current_data_files` lists the table again.
- **Kept files keep their sequence numbers.** An entry a snapshot carries rather than adds must
  state its data sequence number explicitly (only an ADDED entry may inherit the manifest's). A
  delete written by a later merge applies to data files at or below its own number, and the kept
  files' numbers are below any later delete's, so later merges still reach them. The new files take
  the compaction's own number.
- **The snapshot is a `replace`.** The rows are unchanged; only the files holding them change. Its
  summary records what it removed (`deleted-data-files`, `removed-delete-files`,
  `removed-position-deletes`) beside the usual totals.
- **One data manifest, whatever came before.** Every live entry is rewritten into it, so a reader
  of the new snapshot opens one data manifest however many merges built the table. The delete
  manifest records the delete files the snapshot removed and holds nothing live, so the next write
  does not carry it forward.
- **Earlier snapshots are left alone.** Their manifests and files are not touched, so time travel
  reads what it read before. Deleting the files only they read is snapshot expiry (§8c), a
  separate operation.
- **A file is checked as it is rewritten.** The rows read from a file must equal its manifest's
  `record_count`, and a column it stores must have the type the table says. A mismatch stops the
  compaction before anything is committed, and the files it already wrote are removed.

**Reads ahead, writes in order.** One writer copies the bins' files in table order, so the files it
writes do not depend on how the reads were scheduled. Ahead of it, `TableFiles::readers` readers (8
against an object store) each take the next file, across bins: its footer, then its row groups in
order. What they fetch waits for the writer in table order, about 16 MiB of it (`read_ahead` in
the `files` module), and the row group the writer needs next is always fetched, however large, so
the budget can slow reading but never stop it. A failed read is returned when the writer reaches
it; a reader that panics stops the others rather than leaving the writer waiting.

Memory is one output row group, the row group being copied, about 16 MiB of fetched bytes read
ahead, and the deleted positions of the table (the same list a merge loads). Reads go through
`TableFiles`, as a merge's do, so a local table and an object-store table compact with the same
code; the object-store variant stages the new files and uploads them before the metadata that names
them, `version-hint.text` last. A compaction runs longest of any commit, so it is the likeliest to
lose a race; the commit guard (§8d) refuses it and its files are removed.

## 8c. Snapshot expiry — `shapeshift expire`

Every commit keeps the whole history: each snapshot's manifest list, its manifests, and every file
they name stay on disk, so any snapshot can be time-travelled to. A compaction makes that
expensive: it rewrites the table's live rows and keeps its inputs for the snapshots before it.
`expire` (the `expire` module of `shapeshift-iceberg`) lets the old history go.

```text
keep: the current snapshot · every snapshot a ref names · the newest N of the current
      snapshot's ancestry (--retain-last) · every snapshot at or after the cutoff (--older-than)
read every snapshot's manifest list, then every manifest they name (once each; no data file)
needed  = retained snapshots' manifest lists + their manifests + the files those list as live
delete  = everything expired snapshots reference − needed − anything outside the table
publish v{N+1}.metadata.json without the expired snapshots, then version-hint.text
then    delete, one file at a time (a file already gone counts as deleted)
```

The decisions that make it safe:

- **Needed means live in a retained snapshot.** A file a retained snapshot lists only as removed
  (a DELETED entry, as a compaction writes) is not read by it, so it can go. A file listed as live
  by any retained snapshot stays, whichever snapshot wrote it. Paths are compared exactly as
  recorded: shapeshift writes each file's path once and carries it verbatim.
- **Never outside the table.** A file is deleted only if its recorded path lies under one of
  the table's anchors (below) or under where the table is now, and it is deleted at its key
  under where the table is now. A copy's paths name the original's directory: they are the
  copy's own files, deleted in the copy, never in the original. A path under no anchor (an
  engine's `write.data.path` elsewhere) is counted and left.
- **Anchors: where a moved table's paths point.** Paths are absolute, and a table that was
  moved, written to and moved again holds paths written at several directories. Every reader
  (append, merge, compact, expire, scan) resolves a path the same way: the table's anchors are
  its `location` plus the directory above `metadata/` of every snapshot's manifest list and
  every `metadata-log` entry (except files under the table's own `write.metadata.path`, which
  an engine keeps there wherever the table is); a path under one, cut at the longest on a path-component
  boundary, is read at that key where the table is now; a key with a `.` or `..` segment is
  refused; any other path is read where it says. A catalog's table has one anchor, its
  location, since its recorded paths are where its files are.
- **Commit first, delete after.** The metadata without the expired snapshots is published before
  any file is deleted. A crash in between leaves files nothing references, never a table that
  names a missing file.
- **The metadata keeps its shape.** The current snapshot and sequence numbers are unchanged;
  `refs` keep the references whose snapshot survives; `snapshot-log` keeps its newest entries up
  to the first expired one, so it stays the current snapshot's unbroken history; the superseded
  metadata file goes into `metadata-log`. Retained snapshots can still name an expired parent, as
  Iceberg allows.
- **Old `metadata.json` files are kept.** Each commit leaves one; they are small, and deleting
  them is a separate retention setting in Iceberg as well.

Memory is the manifest entries of the table's history, read once; no data file is opened. The
reads go through the merge's reader pool (§8a: 8 at a time against an object store, where they are
what planning costs), and the plan is built from them in table order, so it does not depend on which
read finished first. Against an object store the metadata is put, then the files go to the store's many-delete call (on S3 one
`DeleteObjects` request per thousand keys; any key it does not confirm is retried alone). If another
writer commits after the plan is made, the commit guard (§8d) refuses the expiry before anything is
deleted. A reader still on an expired snapshot loses its files.

## 8d. Concurrent writers — the commit guard

Every commit — an append, a merge, a compaction, an expiry — builds on the version it loaded and
publishes the next one. Without a guard, two writers that loaded version N both write
`v{N+1}.metadata.json`, and the second silently replaces the first: its snapshot, and every row in
it, is gone, while its writer reported success. Measured, not assumed: four writers merging into one
table while a janitor compacted and expired it lost 400-odd of ~580 rows their writers had been
told were committed, and other runs crashed on a half-written `version-hint.text`.

```text
commit(N+1):
  data files, delete files, manifests, manifest list      fresh names; nothing can collide
  create metadata/v{N+1}.metadata.json — only if absent   THE commit point
      local:  write .v{N+1}.<uuid>.tmp, fsync, hard_link → the name (fails if it exists)
      store:  put with PutMode::Create (S3: If-None-Match: *; GCS, Azure: their preconditions)
      taken by a successor of N  → ShapeError::Conflict, everything above is removed, CLI exit 75
      taken by anything else     → a leftover from an earlier table: an error that says how to clear it
  version-hint.text := N+1 — atomically (temp + rename), and only if no N+2 exists yet
read(version):
  hint (0 if missing), then v+1, v+2, … while each file exists and its metadata-log names the one before
```

The decisions that make it hold:

- **The metadata file is the commit, the hint is a hint.** Creating `v{N+1}` is atomic and
  exclusive, so exactly one writer wins each version. The hint is written after, so a crash or a
  slower writer can leave it behind; readers and writers walk forward past it, so a committed
  snapshot is never lost behind a stale hint and a writer never wedges on one.
- **Only successors count.** A later file is followed only if its `metadata-log` names the version
  before it. shapeshift 0.3 and earlier rewrote `v1` in place on a write without `--append`,
  leaving older `v2`… beside it; those never continue from the rewritten `v1`, so they are not
  mistaken for commits. A write without `--append` now replaces the table *as the next version*,
  recording its predecessor, so it never needs to overwrite anything.
- **The loser leaves nothing.** A refused writer deletes the data, delete and manifest files it
  staged (locally, or in one many-delete call in a store). Its run reported nothing as committed,
  and running it again rebuilds it on the winner's version.
- **An error is not a refusal.** On a store a commit's error can hide a put that landed: a 5xx
  after the store applied it is retried and the retry fails as "already exists", and a timeout
  can come back for a put that went through. So the metadata key is read back; holding this
  write's exact bytes, the commit stands. Holding another writer's, it is refused as above. If the
  outcome stays unknown, the error is returned and the staged files are left in place: an orphan
  costs storage, a deleted file of a committed snapshot breaks every reader.
- **A failure after the table moved on is a conflict.** A merge reads the files of the version it
  loaded; a compaction plus an expiry committed meanwhile can delete them. The resulting
  missing-file error is reported as `Conflict` — the retry, not the I/O, is the remedy.
- **Retrying is the caller's job.** The engine refuses rather than serialises: exit status 75
  (`EX_TEMPFAIL`) tells an orchestrator to run the step again. Serialising writers belongs to a
  catalog.
- **The store must honour the precondition.** AWS S3 (since 2024), GCS, Azure, MinIO and R2 do;
  a store that ignores `If-None-Match` accepts both puts and cannot be guarded.

`examples/merge/race_writers.py` checks it end to end with real processes, on local disk and over
the S3 protocol, and its `--negative-control` mode demonstrates the loss on a build without it.

## 8e. Catalog commits — `--catalog` (0.5)

A table an Iceberg REST catalog names has another commit point: not the next metadata file, but
the catalog's pointer to the table's current metadata. shapeshift is a client here. The catalog
checks each commit against the table as it stands and applies it or refuses it.

Credentials come from the environment (SPEC §5a): a bearer token sent as is, or OAuth2 client
credentials exchanged at the token endpoint (`{uri}/v1/oauth/tokens`, or the one named) for a
token. One `RestCatalog` serves a whole run across several short-lived runtimes, so it holds the
token behind a plain mutex and fetches a fresh one before any request once nine tenths of its life
have passed; a commit that cannot get a token is refused, since it never left. Headers carrying a
token are marked sensitive, and `CatalogAuth`'s `Debug` prints none of the secrets.

```text
auth:   POST {token endpoint}  grant_type=client_credentials  (with a credential; again near expiry)
open:   GET  /v1/config[?warehouse=]                   the catalog's prefix, if it has one
        GET  …/namespaces/{ns}/tables/{t}             metadata, location, main's snapshot
             404 → POST …/namespaces/{ns}/tables       create it empty (this run's schema + spec)
                   409 → load the one another writer created
        evolve fields against its schema; partitioning must match; data without --append → refuse
write:  data files, manifests, manifest list          under the table's location, any store
commit: POST …/namespaces/{ns}/tables/{t}
          requirements  assert-table-uuid, assert-ref-snapshot-id (main as loaded)
                        + assert-current-schema-id, assert-last-assigned-field-id (new schema)
          updates       [add-schema, set-current-schema -1,] add-snapshot, set-snapshot-ref main
        200                              committed
        409                              refused by another writer → rebase (below)
        other 4xx, connection never made refused (never applied) → error, files removed
        5xx, timeout, dropped connection → load the table again:
            this snapshot in it                  committed
            main moved, this snapshot absent     refused by another writer → rebase
            main unmoved, this snapshot absent   send the same commit again (≤ 3 times);
                                                 a refused copy → load: this snapshot in it = committed
            no table to load, load failed        unknown → error, files stay
rebase (an append, ≤ 16 times, after a random pause of up to 20 ms × 2ⁿ):
        load the table; table-uuid, location, schema and partition spec as loaded? else Conflict
        remove the refused attempt's manifest + manifest list   (data files stay)
        build a new snapshot on the new head over the same data files, put its manifest + list
        commit again                                             past the bound → Conflict, exit 75

merge (--merge-on): open as above; at finish read the head's key columns and delete files
        write:  data files + delete files, manifests (+ delete manifest), manifest list
        commit: as above; 409 / main moved → Conflict, exit 75, every file removed (no rebase)
        a read that fails after main moved → Conflict (another writer's compaction or expiry)
compact --catalog: load (must exist); read the head; write the rewritten files at its location
        commit: a `replace` snapshot, as above, no rebase
expire --catalog:  load (must exist); plan against the head as loaded
        commit: POST …/tables/{t}
          requirements  assert-table-uuid, assert-ref-snapshot-id for main AND every other ref
          updates       remove-snapshots [the expired ids]
        lost answer → load: none of the ids left = committed; some left and a ref moved =
                      refused; some left, nothing moved = send it again (≤ 3), else unknown
        committed → delete the files only the expired snapshots read; else delete nothing
```

- **The snapshot is built as for any append.** `build_metadata_artifacts` runs against the
  metadata the catalog returned; `catalog_commit` reads the new snapshot (and, when columns were
  added, the new schema) out of the result and sends them as updates. The metadata file it built
  is never written: the catalog writes its own, so a catalog commit has no `vN`.
- **The requirement is the guard.** `assert-ref-snapshot-id` is the catalog's form of "create
  `v{N+1}` only if absent": of two writers that loaded the same `main`, the catalog applies one.
  The table's uuid is asserted too, so a table dropped and recreated under the same name refuses
  a commit built for the old one.
- **An unanswered commit is judged by the table** (`judge_reload`). A timeout does not cancel a
  request the catalog is still applying, so a table whose `main` has not moved may yet take it.
  The same commit can then be sent again: every copy asserts the same head and adds the same
  snapshot id, so at most one copy ever applies, and the table shows which outcome happened. A
  copy that is refused is checked against the table before anything is deleted or rebuilt, since
  the refusal may be because the first copy landed. Once `main` has moved to another snapshot,
  no copy's requirement can hold again, so the attempt's files can go.
- **An append rebases; a planned write would not.** An append's data files do not depend on what
  the table holds, so after another writer's commit only its manifest list (and the snapshot
  over it) is built again, on the new head, with a fresh snapshot id; the refused attempt's
  manifest and list are removed, the data files reused. The rebuild is refused
  (`rebase_blocked`) if the table's identity, location, schema or partition spec moved, since
  the files were written for those. A merge, compaction or expiry was planned against the head
  it read, so through the catalog none of them rebases: a merge found the rows it replaces in
  that head (another writer may have added rows with the same keys), a compaction rewrote files
  another writer may have deleted rows from, and an expiry chose which files only the old
  snapshots read. They are sent with `max_rebases` 0 and exit 75 on a refusal.
- **An expiry is a commit with no snapshot** (`expiry_commit`, `CommitEffect::RemovesSnapshots`).
  It asserts `main` and every other ref as loaded, since its plan kept every snapshot a ref
  pointed at and a ref moved onto an expired one would be left naming deleted files. A lost
  answer is judged by `judge_removal`: snapshot ids are never reused, so an expired id still in
  the table means the commit has not applied, and it never can once an asserted ref has moved.
  Files are deleted only after the commit is known to have landed; an expiry that stays unknown
  deletes nothing, and running it again is safe (the files it would have deleted are left
  unreferenced). A ref created while it runs cannot be asserted: the protocol has no "no other
  ref" requirement, and a catalog may drop a ref left pointing at a removed snapshot.
- **A table holding data needs `--append`.** Replacing a catalog table's contents is an overwrite
  snapshot, a separate feature; a write without `--append` onto data is refused before anything
  is staged. An empty table, just created or made by DDL in another engine, is written to.
- **No new dependency.** The client uses the `reqwest` that `object_store` already links, without
  a connection pool, so each call gets a connection opened for it.

Verified three ways: a stand-in catalog in `shapeshift-objstore`'s tests applies commits like a
real one and can fail each in every way a client can see (409, 5xx before or after applying, a
dropped connection, an answer after the client gave up); Apache Iceberg's reference REST server
(SQLite-backed), with DuckDB attaching the same catalog and reading every table back; and real
concurrent writers against that server, where every run commits (12 writers starting together
needed up to 5 rebases each), every snapshot is claimed by one run, no refused attempt's files
remain, and DuckDB reads exactly the committed runs' rows. Merge, compaction and expiry through
the catalog are covered by the stand-in: every refusal they meet (another writer's commit, a ref
moved under an expiry, an earlier attempt of the run) leaves nothing behind and is never
rebuilt, a lost answer settles by the table, and an expiry deletes exactly the files its dry run
named once its commit lands.

## 8f. Run ids — retries that never append twice (0.5)

An orchestrator retries a failed run, and hands a run to another worker when the first stops
answering. Losing contact is not proof the first attempt stopped: it may still be committing. Two
attempts of one append would then both land, and the table would hold the rows twice. A run id
closes that. The caller gives every attempt of a run the same id (`shape --run-id`), and the
table's own history records which run each snapshot came from.

```text
snapshot summary  shapeshift.run-id   the caller's id (when given)
                  shapeshift.version  the engine that wrote it (always)
                  shapeshift.rebases  rebuilds before it landed (a catalog append, when > 0)

before reading    the table holds a snapshot with this run id → report it, write nothing
at finish         the table the sink loaded holds it           → remove this attempt's files
on a refusal      reload; the head's history holds it         → remove this attempt's files,
                  (guard or catalog, before any rebase)           report that snapshot (exit 0)
                  otherwise                                    → as before: rebase, or exit 75
```

- **The table is the record.** No ledger beside the table, no lock: an attempt reads the same
  metadata a reader does. `snapshot_of_run` scans the snapshots still in the history, so the
  record lasts as long as the snapshot does (an expiry that removes it removes the record).
- **Every path checks it.** A path table's guard refusal (`moved_on` for a merge that found the
  table moved), an object-store guard refusal, and a catalog 409 all reload and look before
  failing or rebuilding. The catalog loop does so even when no rebuild is left, so the bound on
  rebases never turns an earlier attempt's success into an exit 75.
- **Ids are opaque and caller-owned.** shapeshift checks their shape (`validate_run_id`) and
  compares them byte for byte. One id per run, shared by its attempts; two runs given one id
  would be taken for one.

Verified by deterministic races in each crate (two attempts built on one version, local, object
store and the stand-in catalog, plus an attempt that begins after the run committed), mutants of
every check, and the reference REST server: 20 runs started as 4 concurrent attempts each landed
exactly once, every late retry read nothing, no attempt left a file, and DuckDB read each run's
rows once.

## 8g. Relocation — `shapeshift relocate` (0.5)

A table records absolute paths, and a write into a moved table records its files under the new
place while `location` keeps the old one, so a table can hold paths written at several
directories. Every reader finds a recorded path the same way, through the table's **anchors**:
its `location` plus the directory above `metadata/` of every snapshot's manifest list and every
`metadata-log` entry, except those under the table's own `write.metadata.path`. A path under one
(cut at the longest, on a path-component boundary) is read
at that key where the table is now; a key with `.` or `..` is refused; any other path is read
where it says. `relocate` uses the same keys to write the new paths.

```text
read   metadata (current version N)          anchors = location + every place it was written at
check  manifest lists, then manifests, then every live file a snapshot reads, against ONE listing
       of the table: anything missing → refused, naming it, nothing written
ask    each anchor that is not here: a table still answers there (or cannot be checked)
       → refused unless --moved (same table-uuid) or --copy (a new one); none answers → a move
write  (new names, beside the old)
       position delete files whose rows name a moved data file: paths replaced, rows re-sorted
         by (file_path, pos); other columns carried; size, file_path bounds, column sizes and
         split offsets read back from the new file
       manifests naming a moved or rewritten file: transcoded (decoded against their own schema,
         every other value written back as it was), one uncompressed block
       manifest lists naming a moved or rewritten manifest: transcoded, its new path and length
commit v{N+1}.metadata.json through the guard: location, every manifest-list, metadata-log,
       statistics paths, the table's own path properties; table-uuid if a copy
then   version-hint.text; then the superseded files (the rewritten ones' old copies) removed
with --catalog: POST …/namespaces/{ns}/register {name, metadata-location} (never overwrite)
       409, 5xx or a lost answer → load the name: at this file = already registered;
       at another = refused (the relocation stands; nothing in the catalog changed)
```

- **Transcoded, not re-modelled.** A manifest another engine wrote carries fields shapeshift
  never writes (column sizes, split offsets, sort order ids, `referenced_data_file`, partition
  summaries in a list). Decoding into shapeshift's own entry model would drop them, so the file is
  decoded into values that keep everything (floats and fixed values as their bytes, an enum's
  index, a union's branch) and encoded back against the same schema. The header keeps every key
  but the codec.
- **Delete files are rewritten, not just re-pointed.** A reader applies a position delete only to
  the data file whose path it names exactly. A relocation that rewrote the manifests but not the
  delete files would bring every deleted row back; the tests catch a build that skips them.
- **Data files are never touched**, and nothing is overwritten: a refused commit (another writer
  first: exit 75) removes what the relocation wrote, and the table is as it was.
- **The reader contract is `expire`'s.** A reader that loaded the previous version can find a
  superseded file gone and fails rather than reading wrong rows; relocate before pointing readers
  at the new place. A writer that loaded it loses the commit guard to the relocation (exit 75):
  a table at a path has no rebase, so nothing republishes a superseded file.
- **Registering is last, and never replaces.** The catalog records where the metadata file is,
  so it is registered only once every path names where the table is. A local table registers
  by its plain path, and the catalog write path opens a plain absolute location as `file://`.
  An unclear answer is settled by loading the name. It is done when the catalog holds this
  file, or a later version whose `metadata-log` names it (written through the catalog since,
  until the catalog trims the log). The table's uuid is not enough: a move keeps it, so the
  same table still held at its old place would pass.
- **`s3a://` is S3.** A table Spark recorded as `s3a://` and reached as `s3://` is not "moved",
  and a relocation keeps its spelling, so its other readers keep their client.

## 8h. Change feeds — `changes:` (0.6)

A change feed is a merge whose rows say what happened. The spec's `changes:` block names the
field that says it (`op`, with its values per kind: insert, update, delete, skip), where the row
is in each record (`row`), where a delete's key is (`key_from`), and which output columns version
a change (`order_by`). A preset (`debezium-postgres`, `debezium-mysql`, `flink-cdc`) fills these in
and adds the version columns. The shaper classifies each record before shaping it, and the merge
writer of §8a takes the kind with the row:

- **An upsert** is §8a's merge: the row replaces the key's live row, with a position delete.
- **A delete** writes only the position delete: the key's live row goes, and nothing is
  appended.
- **With `order_by`**, a change wins only if it is newer than the key's live row (compared on
  the order columns, as the table records them), so a feed delivered out of order, or replayed,
  ends in the same state, for a deleted key while its tombstone is kept (below). A change that
  is not newer is counted *stale* and changes nothing. A
  table records its order (`shapeshift.changes.order-by`) and refuses a merge ordered another way.
- **`apply: changelog`** skips all of this and appends every change with an `_op` column.

**Tombstones.** With `order_by`, a deleted key leaves no row to compare a late change against,
so the delete itself is remembered: its key and version, for `changes.tombstone_retention`
(default 7 days). They are kept in one Puffin file under `metadata/`, named twice: in every
snapshot's summary (`shapeshift.tombstones`, with its count, size and the snapshot that wrote
it) and in the table's `statistics` list (Iceberg's place for a snapshot's statistics files).
The current
tombstones are those the nearest snapshot in the current snapshot's ancestry names, since a write
by another engine names none. A merge with `order_by` reads them, drops the ones a newer change
superseded (the key was re-created) and the ones past retention, adds its own deletes, and
writes a new file whole: its size is the deletes of the retention window, not the table. Every
other shapeshift write (an append, `compact`) names the same file again, unchanged. If the file
a snapshot names cannot be read, a merge with `order_by` is refused with the path and the
reason: nothing is merged on a guess. `expire` deletes a tombstone file only when no kept
snapshot names it, and a REST catalog commit sends `set-statistics` / `remove-statistics`. The format of this state is the table property
`shapeshift.changes.format`; a build that does not know the value refuses to write the table.

**Debezium's snapshot rows.** A row the connector read from the table as it stood (`op: r`) is
given the lowest version of its preset (`cdc_lsn` 0; an empty `cdc_file` at position 0), so
every change outranks it: a transaction open across the initial snapshot commits after it with
lower LSNs, and its updates and deletes must still win.

## 9. The Parquet sink

`ParquetSink` streams RecordBatches to one file via `parquet::arrow::ArrowWriter`, one row group
per batch (§7). Compression maps the spec's choice onto the codecs actually compiled in:
`snappy` → SNAPPY (default), `uncompressed` → UNCOMPRESSED, and `zstd` → ZSTD when built with the
opt-in `zstd` feature, otherwise a **clear error**, not a silent surprise — zstd is not linked by
default because it would pull a C codec and break the musl-static build (the crate is built
`default-features = false, features = ["arrow","snap"]`; `--features zstd` is the sanctioned
fat build).
`inspect()` reads the file back and reports rows / row-groups / columns; it doubles as a
self-contained round-trip check.

---

## 10. The JSON source & the simd-json serde bridge

`shapeshift-json` drives the **simd-json** SIMD lexer, but exposes plain `serde_json::Value` to
the rest of the system rather than simd-json's borrowed/tape types. This is a deliberate seam:

- The fast SIMD parse happens on the hot path (per line), where it matters.
- The record that crosses into `core` is an owned `serde_json::Value` — the same type inference,
  path selection, transforms, and coercion are written against. `core` never depends on simd-json;
  the whole engine could be fed from any `Iterator<Item = Result<Value, _>>`.
- `JsonlReader` preserves the **raw bad line** on a parse error so the CLI can write it verbatim
  into `<output>.rejects.jsonl`. Blank / whitespace-only lines are skipped, not errored.

The cost is one owned `Value` allocation per record; the benefit is a clean, backend-neutral
record model and a `core` crate that is trivially testable without any parser at all.

## 10a. CSV, TSV and XML readers (0.6)

Both readers keep that seam: each turns its format into the same stream of `serde_json::Value`
records, so inference, paths, transforms, drift and `where` apply unchanged, and `convert` prints
exactly what a spec will see. Both are pure Rust (`csv`, `quick-xml`), so they are in the lean
binary.

- **`CsvReader`** (CSV and TSV): a row becomes an object keyed by the column names (from the
  header, or `source.csv.columns`, or `column_N`). Quoting is RFC 4180's; a byte-order mark is
  skipped. With `types: auto` a field that is plainly a boolean, an integer or a decimal arrives
  as one, so inference sees types; `strings` leaves every field text for the spec to coerce. A
  row with the wrong number of fields is a reject, kept with its line and its text re-quoted, as
  a bad JSON line is.
- **`XmlReader`**: each element at `source.xml.record` becomes a record: attributes as `@name`,
  children by name (a repeated one an array), text alone as the value or beside them as
  `#text`, every value a string. Namespace prefixes are dropped. A document with a
  `<!DOCTYPE>` is refused, so no entity is ever fetched or expanded, and one that is not well
  formed fails the run there: nothing after the break can be read, so it is not one bad record.

Both stream one record at a time, and one record may be no larger than a JSON one (256 MiB): a
quote that never closes, or a record path that matches the whole document, fails the run there
instead of filling memory.

## 10b. Pulled sources (0.6)

A pulled source is read by the engine itself, a little at a time, and the **table is its
checkpoint**. `core` defines the `Source` trait and every rule about positions; a source crate
(`shapeshift-source-http`, `-mysql`, `-postgres`) implements it:

- `kind()` names it (`http`, `mysql`, `postgres`); its position is recorded in the snapshot
  summary as `shapeshift.source.<kind>`.
- `identity()` says what the position is a position *in* (the endpoint without its query and the
  cursor field; a database's URL, table or query digest, and cursor). It is recorded beside the
  position, and `resume_from` refuses a run whose source says otherwise: a table never resumes
  another source's position.
- `open(start, bounds)` starts from the table's recorded position (`None` on the first run) and
  returns a streaming iterator of records: a source holds one page, or one batch of rows, at a
  time. `outcome()`, once the records are drained, gives the position to commit and what the
  run read (new, re-read, already read).

The CLI reads the position from the table, opens the source, shapes its records as any input,
and hands the new position to the sink (`Sink::set_source_position`), which writes it **in the
same commit as the rows**. A run that fails commits neither, so the next one starts from the last
commit. A sink that cannot record a position refuses an incremental source. The rules a position
follows (an `at v` or `inside v, after k` position; records at `v` re-read on purpose, so a source
needs `merge_on` or a strictly growing cursor; bounds counting new records only; `lookback`;
`max_tie`) are SPEC §2.6's, implemented once in `core` and shared by every source.

What each source owns is its protocol and its safety:

- **HTTP** holds each page whole (at most 64 MiB), follows the paging style the spec names,
  and checks the declared order as records arrive (a record out of order fails the run, since a
  position past it could skip one). Credentials come from environment variables the spec names,
  go only to the URL's own origin, and over plain http only to this machine unless
  `--insecure-source`; redirects are followed by the source (at most 5), never by the client, so
  a credential is never sent on. A SOAP service is the same source with an envelope built around
  a template and a WS-Security token; a `Fault` fails the run.
- **MySQL and Postgres** read in one `REPEATABLE READ`, `READ ONLY` transaction in UTC, so a run
  sees one state of the database, streaming (a queue of a few hundred rows; a Postgres portal,
  500 a fetch). The password comes from the environment; TLS is required off this machine,
  checked against the bundled public roots and any `ca_file`.
- **`--source-hosts`** holds a run to a list of hosts whatever the spec says, checked on the
  first URL, every redirect and page, and the database hosts as the driver parses them.

---

## 11. MAR cost arithmetic

`shapeshift cost` turns a row count into the honest "why self-host" comparison. Given billable
rows (from `--rows N` or counted from `--input`), the vendor's effective `$/million MAR`
(`--vendor-per-million`, which **you** supply from your own plan — it is never guessed), and an
optional `--self-host-cost`, it computes `vendor_cost = rows/1e6 × price`,
`saved = vendor_cost − self_host_cost`, and `saved_fraction = saved / vendor_cost`. It is honest
by construction: a tiny job whose self-host cost exceeds the vendor cost prints a **negative**
saving. This lives in `core` (`mar.rs`) with no I/O, so it is unit-tested as pure arithmetic.

---

## 12. Error handling & the rejects sidecar

The pipeline distinguishes two failure classes and treats both non-fatally under the default
lenient policy:

- **Source parse errors** — a malformed JSON line. Counted as `parse_errors`; the raw line is
  written to the sidecar.
- **Shaping rejects** — a required column resolved to null/missing/uncoercible. Counted as
  `rejected`; the offending record is written to the sidecar.

Both are appended to `<output>.rejects.jsonl` as one JSON object per line —
`{"line","error","raw"}` — keyed by source line. The run completes and the summary line reports
the counts. Strict mode (`schema: strict`) converts these into aborts. Logs go to stderr at the
`-v`/`-vv` level; the summary and command output go to stdout — as text, or under
`--summary-json` (`shape`, `compact`, `expire`) as one versioned JSON document built from the
`SinkSummary` / `CompactSummary` / `ExpireSummary` and the shaper's drift report, which also
carries a failed run's error and its kind.

### Schema drift — the third loss channel

Parse errors and rejects are *loud*: both are counted and both keep the row. There is a third way a
run loses data, and until the `drift` block existed it was **silent**:

- A **new field** the spec has no column for. `schema: infer` builds columns from the first
  `infer_sample` records, so a field a producer adds at record 100,000 has no column; `schema:
  strict` writes only declared columns by construction. Either way the values are simply never
  selected — `rows_in == rows_out`, nothing counted, nothing sidecarred.
- A **type mismatch** in an *optional* column under the lenient policy: `"12.50"` arriving in a
  `float64` column is not coercible, so the cell is written as null. A required column would have
  rejected the row and counted it; an optional one just… nulls.

Both are now detected by `drift.policy` (anything but `ignore`) and reported: per path, the count,
the record it first appeared in, example values, and the type inference would give it — i.e. the
column declaration that would keep it. The CLI prints a summary and writes the full report to
`<output>.drift.json`; the engine returns it as `RunReport.drift`.

**How detection works.** The effective columns' source paths are compiled once into a small trie
(sorted children, binary search — a schema has a handful of paths per level and this runs per
record, so comparing a few short strings beats hashing one, and it adds no dependency). Each record
is walked against it: a key whose trie node is *terminal* is consumed (including a `json` column
over a whole subtree); a key with a deeper node is descended into; a key with **no** node at all is
uncovered, and its leaves are reported the way `flatten` would have named the columns. A column that
reaches *into* a value (`tags.0` into an array) counts as covering it — the trie has a node there,
so only paths nothing reads at all are reported. The path string is built lazily, on a miss, so a
run whose source has not drifted does one lookup per key and no string work.

Deliberately **not** tracked: a declared column simply *absent* from a record. That loss is already
visible as a null in the output, and counting it per row would spend throughput to report what the
data already says.

**What a policy does** — `ignore` (no detection at all, the v0.1 hot loop), `warn` (default: count
and report; the rows written are byte-identical to `ignore`), `rescue` (a `json` catch-all column,
appended *after* every real column so enabling it is an additive Iceberg schema change, holding
whatever the row would have lost), `quarantine` (a drifted row is rejected — counted and sidecarred
— so the table holds only exactly-on-schema rows), and `error` (fail the run at the first event).
`error` fails *before* the row reaches the builders, and a run that aborts never calls `finish()` on
its sink: a Parquet file without its footer is unreadable and an Iceberg table without its new
`metadata.json` is unchanged, so a failed contract run cannot be half-consumed. This holds however
many batches were already flushed — verified with `row_group_rows: 1`, where twenty clean rows reach
the writer before the drifted one: the 3 KB Parquet file left behind is rejected outright
(*"Invalid Parquet file. Corrupt footer"*), and an aborted `--append` leaves the table on its
previous snapshot with the new data files referenced by no manifest. Those bytes *do* stay on disk —
a footer-less file, or Iceberg orphan files — but they are leftovers to delete, not data any reader
can see.

**Bounded, like everything else.** At most `MAX_TRACKED_PATHS` (1,000) distinct paths are tracked
per run, and at most `drift.max_examples` values per path; past that the counts stop and the report
says `truncated`. A source that uses UUIDs as keys must not turn a bounded-RAM run unbounded.

Measured cost of the default (`warn` vs `ignore`, 1M-row 7-column nested JSONL, release build):
**~6% wall time, peak RSS unchanged**. A pathological run where *every* row drifts on six uncovered
fields is 1.47×, and `rescue` on that same run 2.44× (it builds and serializes a JSON object per
row) — see [BENCHMARKS.md §7](./BENCHMARKS.md#7-performance-measured).

This is drift detection *within one run*. A schema registry, drift history across runs, or alerting
is orchestration — §14's line, not the engine's.

---

## 13. v0.1 limits & roadmap (architectural)

Stated honestly; the layout is chosen so each is additive rather than a rewrite.

- **Iceberg**: **append-to-existing / multi-snapshot** works — `--append` chains a new snapshot
  onto an existing table, with **additive schema evolution**: new optional columns get fresh field-ids and a new schema object in the metadata (old snapshots stay readable; old rows read the new column as null), while drops/renames/type-changes are refused. Each data file
  records **per-column statistics** (value / null counts + min/max bounds), so readers prune files
  by predicate. **Partitioning** is applied — identity and **hidden transforms**
  (`bucket(N, col)` with spec-exact Murmur3, `truncate(W, col)`, temporal
  `year|month|day|hour(col)`): `--partition-by` writes the partition spec with the real transform
  strings and fans rows out to one data file per **transformed** value under Hive-style
  directories, each file's partition tuple recorded in the manifest with the transform's result
  type. Paths are
  **location-anchored** (absolute): a table is valid at the location it was written to (a bucket
  prefix or a local dir), so it reads back in place. A moved or copied table is still
  **readable** — DuckDB resolves it with `iceberg_scan('<new path>', allow_moved_paths=true)`
  (verified for partitioned and multi-snapshot tables), and `shapeshift relocate` re-anchors it
  so any engine reads it with no flag (§8g). A catalog does not do that: it records where a
  table's metadata is, and the paths inside stay as written.
- **zstd** is off by default (musl-static); in the lean binary `--compression zstd` is a clear
  error (the opt-in `--features zstd` fat build enables it) — otherwise use
  `snappy`/`uncompressed`.
- Both input formats stream with bounded RAM — **JSONL** a line at a time, **json-array** an
  element at a time (per-element rejects recover; structural errors report once).
- **Object-store output** — **both Parquet and a whole Iceberg v2 table** now write straight to
  S3 / GCS / Azure via a URL `--output`, through `shapeshift-objstore` (a new `Sink` crate reusing
  `ParquetSink` and the Iceberg table builder; data via a bounded-RAM multipart upload, metadata
  via `put`), opt-in behind the CLI's off-by-default `object_store` feature so the default binary
  links no cloud SDKs. `inspect <url>` reads an object-store Iceberg table back. Verified with
  DuckDB `iceberg_scan('file://…')`, identical to the local path.
- **Temporal edge cases**: integer timestamps read as epoch-milliseconds, integer dates as
  epoch-days; `timestamp` is zone-less (Iceberg `timestamp`, not `timestamptz`).
- **Schema drift** is detected and mitigated **within a run**: undeclared fields and
  silently-nulled coercions are counted and reported (`<output>.drift.json`, carrying the column
  declarations that would keep them), and `drift.policy` chooses what happens — `warn` (the
  default), `rescue`, `quarantine`, `error`, or `ignore`. What is *not* here is the stateful half:
  remembering a dataset's shape between runs, propagating a new column into the destination on the
  next run, drift history, alerting. Those need memory and a schedule — an orchestrator's job.
- **Upsert** (`--merge-on`, §8a) works on local and object-store tables, refuses (rather than
  serialises) a concurrent writer (§8d), and leaves position-delete files in place, so read cost grows with every merge until the table
  is compacted (`compact`, §8b). Compaction keeps every earlier snapshot's files until
  `expire` (§8c) lets the old snapshots go; old `metadata.json` files are kept.
- **No CDC log capture, no scheduling, no per-vendor connectors** — deliberately kept out of the
  engine (next section). Applying a change feed it is handed (§8h), and reading what is new from
  an endpoint or a table by its cursor (§10b), are the engine's job; reading a database's log
  (WAL, binlog) to find what changed is Debezium's or Flink CDC's. A query sees what a query
  sees: a hard `DELETE` needs the change feed.

---

## 14. The deliberate boundary: engine vs. orchestrator

shapeshift is a **shaper**, not an orchestrator. It has no scheduler, no state store, no metering,
and no connector registry — and that omission is intentional. Scheduling, retries, run history,
and metering are the job of an orchestrator (such as **dagron**),
which composes tools like shapeshift as steps. Keeping that boundary sharp is what lets the engine
stay a pure, I/O-free-core, single static binary that anyone can self-host with no row cap and no
telemetry.

The engine's contract to an orchestrator is exactly the CLI surface — its flags, its exit
statuses (75: refused by the commit guard, run it again) and the `--summary-json` document it
prints; nothing more couples them, so capability added at the orchestration layer never needs to
change — or cap — the engine.

A pulled source (§10b) does not change this. Its position is not engine state: it is written in
the table, in the commit that lands the rows, so an orchestrator that runs the same spec on a
cadence needs to remember nothing between runs, and two runs of it can never both advance it.

---

*shapeshift v0.1 — © 2026 Nicholas Lu Chee Seng and the shapeshift contributors. Apache-2.0.
Repository: https://github.com/lucheeseng827/shapeshift*
