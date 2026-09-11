# shapeshift — Design

**shapeshift** turns a stream of JSON or JSONL into a columnar table — a Parquet file or a
self-contained Apache **Iceberg v2** table — driven by a declarative *dataset transform Spec*.
This document explains the design: why the product boundary is a spec rather than a row, how a
record travels from bytes to a columnar file, the two contracts (`Shaper` and `Sink`) that the
whole engine hangs off, the inference and coercion rules with worked examples, the Iceberg writer
and why it hand-rolls its Avro, and the consistency and bounded-RAM properties the design buys.

It sits between the [README](../README.md) (start here — install and first run) and
[ARCHITECTURE.md](../ARCHITECTURE.md) (the internal design of record: crate-by-crate, the
two-phase append, the field-id chain). The full spec grammar — every field, every default, the
complete type and transform tables — is in [SPEC.md](./SPEC.md); this doc quotes only what a
design discussion needs.

Scope: everything here is the **OSS engine** (Apache-2.0) — the single `shapeshift` binary and its
six crates, self-hostable with no row cap and no telemetry. Orchestration sits outside it, behind
a boundary described last; nothing in the engine depends on it.

---

## 1. Thesis: price the transform, not the row

Managed ELT vendors bill on **MAR — Monthly Active Rows**. A row that changes twelve times in a
month is twelve billable events, whether or not any of those changes matters to you. The price
axis is decoupled from the cost of the work: it scales with your data's *churn*, not with compute.
A row costs a fraction of a cent of CPU to reshape, and it is *your* CPU.

shapeshift breaks that coupling by moving the unit of billing off the row and onto the **shape of
the source**. You write one declarative spec per source shape — "these events look like this, map
them to these columns" — and then shape **unbounded rows for free**. The spec is the authored
artifact; rows are just deterministic execution of it. There is no per-row rent because there is no
per-row decision left to make once the spec exists.

This single idea sets almost every downstream design choice:

- The spec is **durable and portable** (YAML or JSON, checked into your repo), because it is the
  thing worth writing carefully once.
- Execution is **deterministic** given a spec and an input: the same records, the same schema, and
  the same Parquet column data every run. (An Iceberg *table*'s generated metadata is deliberately
  *not* byte-identical across runs — data-file names carry random UUIDs and each snapshot carries a
  wall-clock timestamp and snapshot id — but the rows and schema it exposes are.) A re-run is cheap
  to reason about and trust.
- The engine is a **single static binary** with no runtime, no license server, no phone-home,
  because a tool you run unbounded rows through must not meter you.

Everything the rest of this document describes is in service of "write the transform once, shape
rows for free, and be able to prove the output is correct."

---

## 2. The pipeline in one picture

A `shapeshift shape` run is a single streaming pass. Three things move through it: **bytes**
become a neutral **`serde_json::Value`** per record, the `Shaper` turns batches of values into
Arrow **`RecordBatch`**es, and a `Sink` writes those batches to a physical format.

```text
   bytes            Value (neutral record)          RecordBatch            file(s)
 ─────────►  source ───────────────►  Shaper  ──────────────────►  Sink  ─────────►
  JSONL /    (simd-json lexer,        (spec-driven:               (Parquet or
  json-array  one Value/line,          select → transform →        Iceberg v2)
              bounded RAM)             coerce → build columns)

              bad line ┐                required miss ┐
                       └──── counted, raw record ─────┴──► <output>.rejects.jsonl
                                                            {"line","error","raw"}
```

The seams are chosen so each stage is independently testable and independently replaceable. The
source is any `Iterator<Item = Result<Value, _>>`; the engine (`shapeshift-core`) has **no I/O
backend at all** and never names a file format; each `Sink` is a driver behind one trait. The full
crate map lives in [ARCHITECTURE §2](../ARCHITECTURE.md); the design point here is that the two
types crossing the seams — `serde_json::Value` in, Arrow `RecordBatch` out — are both
well-understood and backend-neutral.

---

## 3. The neutral record model

The record that crosses from the source into the engine is an **owned `serde_json::Value`**.

shapeshift parses with the **simd-json** SIMD lexer, which is the fast path where it matters (once
per line). But it deliberately does *not* let simd-json's borrowed / tape types leak past the
source crate. Instead it bridges through simd-json's serde support into a plain, owned
`serde_json::Value`. This costs one owned allocation per record; it buys three things that the
design leans on heavily:

1. **The engine depends on no parser.** `shapeshift-core` — inference, path selection, transforms,
   coercion, the Shaper — is written entirely against `serde_json::Value`. It can be unit-tested
   with hand-built `json!(...)` values and no filesystem, no bytes, no SIMD.
2. **The source is swappable.** Anything that yields `Result<Value, _>` feeds the same engine.
   `JsonlReader` (streaming, one object per non-blank line, bounded RAM) and `JsonArrayReader`
   (streaming, one array element at a time, bounded RAM) are two such sources today; a future
   object-store or NDJSON-over-HTTP source is just another one.
3. **A JSON value is the right neutral shape for the job.** shapeshift maps JSON to columns. Its
   value model — scalars, objects, arrays, null — *is* JSON's, so path selection and the `json`
   escape-hatch type fall out naturally rather than being bolted on.

On a parse error, the source preserves the **raw bad line verbatim** so it can be written into the
rejects sidecar unchanged (§9). Blank and whitespace-only lines are skipped, not errored.

---

## 4. The two contracts: `Shaper` and `Sink`

The whole engine is two small contracts. Understand these and you understand the system.

### 4.1 The `Shaper` — Value → RecordBatch

The `Shaper` is compiled once from the effective column list of a spec. For each output column it
holds a `Compiled` descriptor — name, source path, logical type, optional transform, required flag
— and one typed Arrow builder. The Arrow schema is fixed at compile time and stamped on every
batch it emits.

Its core method, `push(&Value)`, appends **one row transactionally** and returns `Appended` or
`Rejected`. "Transactionally" is the load-bearing word. Column builders are independent, so a naive
"append each cell as I compute it" loop would, on a mid-row failure, leave some builders one value
ahead of others — permanently misaligning every later column. `push` prevents this with two strict
phases:

- **Phase 1 — compute and validate, touch no builder.** For every column: select the value at its
  path, apply the transform if any, coerce to the target type. Cells accumulate in a local vector.
  If a **required** column resolves to null / missing / uncoercible, the row is rejected *before
  any builder is written*.
- **Phase 2 — commit.** Only once every cell for the row exists does it append them all and bump
  the row count. Commit cannot half-happen.

The invariant this maintains — **every builder always holds exactly the same number of values** —
is what makes "the run never aborts on a bad row" *safe* rather than merely permissive: a rejected
row leaves column state untouched, so `flush()` can always finalize the builders into a valid
`RecordBatch`. (The mechanism is detailed in [ARCHITECTURE §5](../ARCHITECTURE.md).)

`flush()` finalizes the builders into a `RecordBatch` and resets them. The driver calls it every
`row_group_rows` (default 50 000) and once more to drain the tail.

### 4.2 The `Sink` — RecordBatch → format

A `Sink` is two methods:

```text
write_batch(&RecordBatch)  →  append one batch
finish()                   →  write footer/metadata, return SinkSummary { files, rows, bytes }
```

That is the entire coupling between the engine and any physical format. The engine flushes a batch
and hands it to `write_batch`; at the end it calls `finish` and prints the summary. Four sinks
implement it today — `ParquetSink`, `IcebergSink`, and their object-store counterparts
`ObjectStoreParquetSink` / `ObjectStoreIcebergSink` — and none reference the engine's internals.
The object-store sinks were added as a new crate (`shapeshift-objstore`) implementing these two
methods by reusing the local writers; **nothing in the engine changed**, because the engine already
speaks only Arrow and `Sink`.

The `Sink` contract also carries the RAM discipline (§8) and the delivery semantics (§7): a sink is
free to write one row group per batch, and a sink's `finish()` is the atomic commit point.

---

## 5. Inference: from a sample to a spec

`shapeshift infer` (and `shape` with no `--spec`) reads a **sample** — the first `infer_sample`
records, default 1000, `--sample 0` means all — and emits a ready-to-edit `DatasetSpec`. Inference
is a proposal, not a contract: the output is YAML you are expected to read and adjust before it
becomes the durable artifact.

The rules, briefly:

- **Flatten (default on).** Nested objects expand into dotted columns (`user.name`, `user.plan`).
  Arrays become `json` leaves (kept as a JSON-encoded string). `--no-flatten` keeps the top level
  only and makes any nested value a `json` column.
- **Temporal detection.** A string matching `YYYY-MM-DD` infers `date`; an RFC3339 string infers
  `timestamp`.
- **Widening.** A field seen as both int and float widens to `float64`. A field with mixed scalar
  shapes (say string in one record, number in another) widens to `string`. Anything nested or
  array-shaped becomes `json`.

### Worked example

Given [`examples/events.jsonl`](../examples/events.jsonl):

```json
{"id": 1, "user": {"name": "Ada",   "plan": "pro"},  "amount": 12.50, "event_at": "2026-07-13T09:15:00Z", "day": "2026-07-13", "tags": ["a","b"], "active": true}
{"id": 2, "user": {"name": "Grace", "plan": "free"}, "amount": 3,     "event_at": "2026-07-13T10:00:00Z", "day": "2026-07-13", "tags": [],        "active": false}
{"id": 3, "user": {"name": "Linus"},                 "amount": 99.99, "event_at": "2026-07-12T23:59:59Z", "day": "2026-07-12", "tags": ["x"],     "active": true}
```

`shapeshift infer -i examples/events.jsonl` reasons:

| Column | Observed | Inferred type | Why |
|---|---|---|---|
| `id` | `1`, `2`, `3` | `int64` | integral throughout |
| `user.name` | `"Ada"`, … | `string` | nested object, flattened |
| `user.plan` | `"pro"`, `"free"`, *(absent for Linus)* | `string` | present-when-present; absence ≠ a type |
| `amount` | `12.50`, `3`, `99.99` | `float64` | int + float in one field → widen |
| `event_at` | `"2026-07-13T09:15:00Z"` | `timestamp` | RFC3339 |
| `day` | `"2026-07-13"` | `date` | `YYYY-MM-DD` |
| `tags` | `["a","b"]`, `[]`, `["x"]` | `json` | array leaf |
| `active` | `true`, `false` | `bool` | |

The emitted spec is inference's best guess with everything optional. Two edits promote it to a
production contract: mark `id` `required: true`, and switch `schema: infer` to `schema: strict` so
the output carries exactly your declared columns. That is precisely what
[`examples/billing.spec.yaml`](../examples/billing.spec.yaml) does — and it adds transforms
(`user.plan` → `uppercase`, `amount` → `dollars_to_cents` producing an `int64` `amount_cents`) that
inference will never propose, because they are semantic choices only you can make.

---

## 6. Coercion: turning a selected value into a typed cell

Once the spec exists, every row runs the same three steps per column: **select** the value at the
column's `from` path, **transform** it (before coercion), then **coerce** it to the logical type.
Coercion is where lenient, honest behavior is defined.

### Selection and the coercion policy

The `from` path is dotted (`user.plan`), tolerates a leading `$`/`$.`, treats a numeric segment as
an array index (`tags.0`), and reads the whole record as bare `$`. An **unresolved path is null**,
which then follows the policy below:

| Policy | Optional column | Required column |
|---|---|---|
| **lenient** (default) | missing / null / uncoercible → `null` | missing / null / uncoercible → **soft row reject** (row dropped, counted) |
| **strict** | — | a required miss **or any coercion failure anywhere** → **abort the run** |

Lenient is the default because a real feed has ragged records, and one malformed row should not
lose you the other 99,999. Strict exists for the case where a coercion failure means your
understanding of the source is wrong and you would rather stop than write a subtly wrong table.

### Coercion rules

- `int64` ← an integral float or a numeric string (a non-integral float is a failure)
- `float64` ← any number or a numeric string
- `string` ← stringify any scalar
- `date` ← `YYYY-MM-DD` or RFC3339, or an integer read as **epoch-day**
- `timestamp` ← RFC3339 or a zone-less ISO string read as **UTC**, or an integer read as
  **epoch-millisecond** (microsecond precision, zone-less)
- `bool` ← a JSON boolean
- `json` ← the value, kept as a JSON-encoded string

### Worked examples (on the sample above, with the billing spec)

- **Transform then coerce.** `amount = 12.50` → `dollars_to_cents` (× 100) → `1250` → coerced to
  `int64`. The transform runs first, on the value; coercion is the last step.
- **Case fold.** `user.plan = "pro"` → `uppercase` → `"PRO"` → `string`.
- **Optional miss becomes null.** Record 3 (Linus) has no `user.plan`. `plan` is optional, so the
  cell is `null`. The row is kept.
- **Required miss becomes a reject.** If a record omitted `id` (required), lenient mode drops that
  one row and records it; strict mode aborts.
- **Bad source line becomes a parse error.** The literal line `not-json-a-bad-line` in the sample
  never reaches coercion — the source fails to parse it, counts a `parse_error`, and sidecars the
  raw line. The run continues.

Shaping the sample yields **4 rows** (the three above plus record 4), one parse error, zero row
rejects. Verified end to end: `sum(amount) = 115.49` read back identically from both the Parquet
and the Iceberg output by DuckDB 1.5.4 (§7, §10).

---

## 7. Delivery and consistency semantics

shapeshift's sinks are **write-once**. A run produces a fresh output; there is no
append-to-existing table in v0.1 (Iceberg gets exactly one snapshot per run — roadmap: multi-
snapshot append). The commit story differs slightly by format, but both give the same guarantee: a
reader sees **either nothing or the complete result**, never a torn half-table.

**Parquet.** A Parquet file is only readable once its **footer** is written, and the footer is
written last, by `finish()`. A reader that opens a still-being-written file gets a "no footer"
error, not partial rows. The footer is the commit point.

**Iceberg v2.** The table's current state is resolved through `metadata/version-hint.text`, which
names the live metadata JSON. shapeshift writes the table in dependency order and flips that
pointer **last**:

```text
1. data/<uuid>.parquet              write the data file
2. metadata/<uuid>-m0.avro          write the manifest (lists the data file)
3. metadata/snap-…-<uuid>.avro      write the manifest list (lists the manifest)
4. metadata/v1.metadata.json        write the table metadata (the snapshot)
5. metadata/version-hint.text → 1   flip the pointer  ── the snapshot becomes visible here
```

Until step 5, a reader following `version-hint.text` sees no current table (or the prior one, in a
future append world); after step 5 it sees a fully-materialized snapshot whose every referenced
file already exists. That ordering *is* the atomicity — the same mechanism Iceberg catalogs use,
reduced to a single file write because shapeshift writes one snapshot with no catalog server.

Because execution is deterministic (§1), the safe way to "retry" a run is to re-run it to a fresh
output path — same input, same spec, same table. There is no partial-commit state to reconcile.

> v0.1 honesty: file paths inside the metadata are **absolute**, so moving the directory breaks the
> references for a *default* reader — but DuckDB reads a moved/copied table in place with
> `iceberg_scan('<new path>', allow_moved_paths=true)` (verified for partitioned and multi-snapshot
> tables). Appends now ship; catalog-managed relocation (re-anchoring paths so any engine reads with
> no reader flag) is left to an external Iceberg REST catalog. All additive; none of it changes the engine.

---

## 8. Bounded RAM

A 100M-row JSONL file must shape in the footprint of a single row group. Two independent mechanisms
enforce that, and one identity ties them together.

- **Source side.** `JsonlReader` reads one line into a reused buffer (with SIMD slack past the line
  so simd-json's chunked reads never touch memory outside the allocation) and yields one `Value`.
  Nothing accumulates across lines. `JsonArrayReader` does the same per array *element*: a small
  depth- and string-aware scanner finds each element's boundary, so only one element (under the
  same per-record byte cap) is buffered at a time — **both input formats are unbounded, streaming
  paths**.
- **Sink side.** The Shaper flushes a `RecordBatch` every `row_group_rows` and resets its builders.
  `ParquetSink::write_batch` writes the batch and immediately flushes the writer, closing that
  batch's row group, so the Parquet writer never buffers more than one row group at a time.

**Row group == batch == unit of streaming progress.** That identity is the load-bearing decision:
the same boundary that bounds memory is the boundary at which work becomes durable. Peak memory is
one row group of column builders plus one in-flight batch — flat whether the input is the 4-row
sample or millions of rows. **Measured:** on the shipped musl-static binary, peak RSS is
9.8 → 13.3 MiB across a 1.5 MB → 1.26 GB (~820×) input spread
([BENCHMARKS.md §7](../BENCHMARKS.md#7-performance-measured); one caveat there worth knowing —
glibc builds *retain* the freed batch buffers in the allocator, so their RSS appears to grow;
musl, the shipped libc, returns them).

Throughput claims must come from the reproducible harness in [`benchmarks/`](../benchmarks/)
with hardware stated; the design guarantee here is the memory bound, the harness is what turns
a rate into a fact.

---

## 9. Errors are counted, sidecarred, and non-fatal

Under the default lenient policy the pipeline distinguishes two failure classes and lets neither
abort the run:

- **Source parse errors** — a malformed JSON line. Counted as `parse_errors`; the raw line is
  preserved verbatim.
- **Shaping rejects** — a required column resolved to null / missing / uncoercible. Counted as
  `rejected`; the offending record is preserved.

Both append to `<output>.rejects.jsonl`, one JSON object per line — `{"line","error","raw"}` —
keyed by the 1-based source line, so a rejected row is always traceable back to its input. The run
finishes and prints a single honest summary line:

```text
rows_in=.. rows_out=.. rejected=.. parse_errors=.. row_groups=.. bytes=..
```

`schema: strict` converts both classes into aborts, for pipelines that would rather fail loudly
than write a partially-correct table. Logs go to **stderr** (at `-v` / `-vv`); the summary and all
command output go to **stdout**, so shapeshift composes cleanly in a shell pipeline.

The same honesty runs through `shapeshift cost`: it computes `vendor_cost = rows/1e6 × price` from
a `$/million MAR` figure **you** supply from your own plan (it never guesses a price) and will
print a **negative** saving for a job too small to amortize its self-host cost. See
[cost-comparison context in the README](../README.md) for how the number is meant to be read.

---

## 10. The Iceberg v2 writer, and why it hand-rolls its Avro

`IcebergSink` writes a **self-contained** Iceberg v2 table — no catalog server — with this layout
(the write order and atomicity are §7):

```text
<table>/
  data/<uuid>.parquet                 data file (Parquet, carrying PARQUET:field_id)
  metadata/<uuid>-m0.avro             manifest        (lists data files)
  metadata/snap-<id>-1-<uuid>.avro    manifest list   (lists manifests)
  metadata/v1.metadata.json           table metadata  (format-version 2)
  metadata/version-hint.text          → 1
```

The single wall-clock read in the entire engine is the snapshot timestamp written here — which is
exactly why `chrono` is compiled **without** the `clock` feature everywhere else: all other time is
derived from the data, so the output of a shape run depends only on its input and its spec.

### The field-id chain

Iceberg readers match manifest columns to data columns by **field-id**, not by name. shapeshift
threads one consistent id space, `1..=N`, through four layers so a reader can join the whole table
by id:

```text
Arrow field metadata          Parquet column         Iceberg schema          Manifest Avro schema
PARQUET:field_id = 1..N  ──►   field_id = 1..N   ──►  "id": 1..N in     ◄──   "field-id": … on the
(stamped by the sink)          on each column         v1.metadata.json        manifest_entry fields
```

(The plain `ParquetSink` does **not** stamp field-ids — they are an Iceberg concern, stamped only
by `IcebergSink`. Details in [ARCHITECTURE §8](../ARCHITECTURE.md).)

### Why the Avro is hand-rolled (correctness-critical)

The manifest and manifest-list are Avro Object Container Files, and a reader recovers the field-ids
from the `field-id` attributes embedded in the manifest's Avro **schema JSON** in the file header.
This is where a general Avro library breaks Iceberg:

- The general Rust Avro crate (`apache-avro`) **drops those custom attributes** when it serializes
  the schema into the file header.
- The resulting table is then unreadable. DuckDB fails with the internal error
  **"No default expression in FieldId Map"** — without the ids, the reader cannot map manifest
  columns at all.

So `shapeshift-iceberg` emits the Object Container File itself (~150 LOC): the `Obj\x01` magic, a
metadata block carrying the exact `field-id`-bearing `avro.schema` JSON **verbatim** plus
`avro.codec = null`, a sync marker, and one data block of pre-encoded records. The binary encoding
required is small — zig-zag varint longs, length-prefixed strings, unions, and (empty) records —
and the two manifest schemas are fixed constants with their Iceberg field-ids baked in. Emitting it
ourselves preserves the schema JSON byte-for-byte **and** drops a dependency, keeping the
single-static-binary ethos intact.

The payoff is verified with DuckDB 1.5.4: `iceberg_scan('<table dir>')` reads the sample end to
end — the same 4 rows and the same `sum(amount) = 115.49` as the Parquet path — and the data
Parquet carries `PARQUET:field_id` `1..N` while the manifest's embedded Avro schema carries the
matching `field-id`s.

---

## 11. Type and transform reference (brief)

The full grammar, defaults, and edge cases are in [SPEC.md](./SPEC.md). The design-relevant summary:

**Logical types** map cleanly all the way down — a deliberately small set (the scalar spine, two
temporal types, and a `json` escape hatch) that is exactly what a JSON→columnar shaper needs:

| Spec type | Arrow | Iceberg |
|---|---|---|
| `bool` | `Boolean` | `boolean` |
| `int64` | `Int64` | `long` |
| `float64` | `Float64` | `double` |
| `string` | `Utf8` | `string` |
| `date` | `Date32` (epoch-day) | `date` |
| `timestamp` | `Timestamp(µs, no zone)` | `timestamp` (not `timestamptz`) |
| `json` | `Utf8` | `string` (nested value as a JSON-encoded string) |

**Transforms** run **before** coercion, value → value:

| Transform | Effect |
|---|---|
| `lowercase` / `uppercase` | case fold a string |
| `trim` | strip surrounding whitespace |
| `json_encode` | serialize the value to a JSON string |
| `to_string` | stringify a scalar |
| `dollars_to_cents` | number × 100 → integer |
| `abs` | absolute value |
| `empty_to_null` | empty string → null |

---

## 12. What the design refuses (and why)

Each omission keeps the engine small enough to trust and to run unbounded rows through for free.
The forward-looking ones are roadmap; the rest are permanent boundaries.

| Not in the OSS engine | Why | Where it belongs |
|---|---|---|
| Append-to-existing / multi-snapshot Iceberg | shipped: `--append` chains a new snapshot onto an existing table (reads prior metadata + manifest list, carries manifests forward, bumps version) | ✅ done — row-level **merge / upsert** (CDC) is out of scope |
| Partitioning (identity + hidden transforms) | shipped: `--partition-by` accepts columns and transform expressions (`bucket(N, col)` with spec-exact Murmur3, `truncate(W, col)`, `year\|month\|day\|hour(col)`); the spec records the real transform strings, rows fan out per **transformed** value, and the manifest partition tuple stores the transform's result type | ✅ done |
| Object-store output (S3/GCS/Azure) | shipped for **both Parquet and Iceberg** via `shapeshift-objstore` (a new `Sink` crate, opt-in behind the CLI's `object_store` feature) | ✅ done — a moved table reads with `allow_moved_paths=true`; only **catalog-managed** relocation + multi-writer is left to an external **REST catalog** |
| `zstd` compression | not linked **by default** — it would pull a C codec and break the musl-static single binary; the lean binary gives a **clear error** | ✅ opt-in: `cargo build --features zstd` (the fat build) enables it for Parquet + Iceberg; otherwise `snappy` (default) / `uncompressed` |
| Streaming json-array input | shipped: `JsonArrayReader` streams one element at a time (a depth- and string-aware boundary scanner, same per-record byte cap as JSONL) | ✅ done — both input formats are bounded-RAM streaming paths |
| Incremental / CDC, scheduling, connectors, metering | the moment runs relate or must be operated, you need an orchestrator — a different product with different failure modes | an orchestrator (such as **dagron**) |

The last row is the load-bearing boundary. shapeshift is a **shaper**, not an orchestrator: no
scheduler, no state store, no metering, no connector registry. Keeping that out is exactly what
lets the engine stay a pure, I/O-free-core, single static binary that anyone self-hosts with no row
cap and no telemetry.

The engine's contract to an orchestrator is exactly the CLI surface and the `SinkSummary` it
prints; nothing more couples them, so capability added at the orchestration layer never needs to
change — or cap — the engine.

---

## Further reading

- [README](../README.md) — install, quick start, the `cost` command
- [SPEC.md](./SPEC.md) — the complete dataset transform spec: every field, default, type, transform
- [ARCHITECTURE.md](../ARCHITECTURE.md) — the internal design of record: crates, the two-phase
  transactional append, the field-id chain, the hand-rolled Avro encoder

---

*shapeshift v0.1 — © 2026 Nicholas Lu Chee Seng and the shapeshift contributors. Apache-2.0.
Repository: https://github.com/lucheeseng827/shapeshift*
