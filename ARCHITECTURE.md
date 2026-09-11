# shapeshift — Architecture

The internal design of record for **shapeshift**, a streaming JSON/JSONL → Parquet/Iceberg
shaper. This document describes how the engine is split into crates, how a record flows from
bytes to a columnar file, and the correctness-critical choices (the transactional per-row
append, the field-id chain, and the hand-rolled Avro encoder) that are easy to get wrong and
expensive to get wrong.

Scope: this is the **OSS engine** (Apache-2.0) — fully self-hostable, no row cap, no telemetry.
Scheduling, run history, and connectors sit outside it, behind the boundary described in the last
section; nothing in this document depends on them.

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

## 2. The six-crate split

shapeshift is a standalone Cargo workspace with its own `Cargo.lock` (the heavy Arrow / parquet
/ simd-json stack resolves independently of the shared monorepo workspace, the same pattern as
sibling modules forge/recall/evald). The member list is a closed set of six crates:

| Crate | Responsibility | Notable deps | I/O backend? |
|---|---|---|---|
| `shapeshift-core` | The engine. Spec model, JSON schema inference, the named Transform library, the Shaper (JSON value → Arrow `RecordBatch`), the `Sink` trait, MAR cost arithmetic. | arrow-array/arrow-schema 54, serde/serde_json/serde_yaml, chrono (no `clock`) | **none — by design** |
| `shapeshift-json` | Streaming JSON source over the simd-json SIMD lexer: `JsonlReader` (one object/line, bounded RAM) and `JsonArrayReader` (streamed element-at-a-time, bounded RAM). | simd-json 0.14 | read |
| `shapeshift-parquet` | `ParquetSink` (RecordBatch → Parquet, one row group per batch, Snappy default) + `inspect()` read-back. | parquet 54 `["arrow","snap"]` | write |
| `shapeshift-iceberg` | `IcebergSink`: a self-contained Apache **Iceberg v2** table (no catalog server), **append-to-existing / multi-snapshot** (`--append`) + `inspect()`. Owns the hand-rolled Avro encoder **and reader** (the reader carries a prior snapshot's manifests forward on append), and a **backend-agnostic table builder** (`IcebergDataWriter` + `build_metadata_artifacts`, paths anchored at a base location) the object-store sink reuses. | parquet 54, uuid v4 | write |
| `shapeshift-objstore` | `ObjectStoreParquetSink` **and** `ObjectStoreIcebergSink`: land a Parquet file / a whole Iceberg v2 table (data + manifests + metadata) in S3/GCS/Azure — data via a bounded-RAM multipart upload, metadata via `put`, paths anchored at the destination. The driver that reuses other drivers (`parquet`, `iceberg`); opt-in behind the CLI's `object_store` feature. | object_store 0.14, tokio (current-thread) | write (remote) |
| `shapeshift-cli` | The single `shapeshift` binary. Orchestrates source → shaper → sink and owns the CLI surface. | clap 4 (derive), anyhow, tracing | driver |

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

---

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
`-v`/`-vv` level; the summary and command output go to stdout.

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
  (verified for partitioned and multi-snapshot tables). **Catalog-managed** relocation
  (re-anchoring the embedded paths so any engine reads a moved table with no flag, plus
  multi-writer commits) is left to an external Iceberg REST catalog.
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
- **No incremental/CDC, no scheduling, no connectors** — deliberately kept out of the engine
  (next section).

---

## 14. The deliberate boundary: engine vs. orchestrator

shapeshift is a **shaper**, not an orchestrator. It has no scheduler, no state store, no metering,
and no connector registry — and that omission is intentional. Scheduling, retries, run history,
and metering are the job of an orchestrator (such as **dagron**),
which composes tools like shapeshift as steps. Keeping that boundary sharp is what lets the engine
stay a pure, I/O-free-core, single static binary that anyone can self-host with no row cap and no
telemetry.

The engine's contract to an orchestrator is exactly the CLI surface and the `SinkSummary` it
prints; nothing more couples them, so capability added at the orchestration layer never needs to
change — or cap — the engine.

---

*shapeshift v0.1 — © 2026 Nicholas Lu Chee Seng and the shapeshift contributors. Apache-2.0.
Repository: https://github.com/lucheeseng827/shapeshift*
