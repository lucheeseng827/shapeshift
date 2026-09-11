# Changelog

All notable changes to `shapeshift` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

`shapeshift` is a streaming JSON/JSONL → Parquet/Iceberg shaper. Copyright © 2026
Nicholas Lu Chee Seng and the shapeshift contributors. Licensed under Apache-2.0.

## [Unreleased]

## [0.2.0] - 2026-09-12

### Upgrading from 0.1.x

Three changes can break an existing setup; everything else is additive.

- **Drift detection is on by default.** `drift.policy` defaults to `warn`: every `shape` run now
  prints a drift summary and writes `<output>.drift.json` beside its output. The rows written are
  identical to 0.1.x. To keep the old behaviour exactly, set `drift.policy: ignore` in the spec or
  pass `--on-drift ignore`.
- **Unknown spec keys are rejected.** A spec with a misspelled or extra key (`colums:`) used to
  run on pure inference and report success; it now fails, naming the key, its line, and the valid
  names. Fix the key.
- **Library: `RunReport` changed.** It gained a public `drift` field and no longer derives `Eq`.
  Code that builds a `RunReport` with a struct literal, or compares reports with `Eq`, needs
  updating. `Shaper::new` and `Shaper::push(&value)` are unchanged.

### Added

- **`json-array` input parses onto a tape too, and doubles on the shipped binary.** The array
  reader already framed each element into a reusable buffer, exactly as the line reader framed
  lines — so the framing was factored out of the `Value` iterator (one implementation of the
  array's state machine, shared by both readers, the same split JSONL uses) and a
  `TapeJsonArrayReader` built on it. A new `TapeSource` trait keeps the shape loop to one tape
  path instead of one per input format. Streaming, the element scanner, and the error semantics
  are untouched: a bad element is still recoverable and still reports its 1-based element index,
  a malformed array is still reported once and ends iteration. Measured on one machine, 1M rows
  → Parquet, three interleaved reps, median: **151,268 → 308,298 rows/s (+103.8%) on musl**,
  318,094 → 436,462 (+37.2%) on glibc, peak RSS unmoved. That leaves `json-array` at **93.5% of
  JSONL's rate on musl**, against 46% before. Output is byte-identical — the same Parquet md5 as
  the JSONL framing of the same records, on both targets.
- **`shapeshift-core` reads records through a `Record` trait.** The engine only ever *reads* a
  record, so it no longer names a concrete type to do it. **The `serde_json::Value` contract is
  unchanged** — `&Value` implements the trait, `Shaper::push(&value)` behaves exactly as before,
  and `core` still names no parser. This is what let a second, cheaper record model exist beside
  the first rather than replace it. Both run the same coercion code, and a conformance test
  asserts the two answer identically on every accessor.
- **`shapeshift-json/examples/parsebench.rs`** — the parse-cost measurement behind the change,
  runnable on your own data and against either allocator.

- **`--pipeline` — overlap encoding a row group with shaping the next one.** Finished row
  groups are handed to a writer thread, so Parquet encoding and Snappy compression stop
  taking turns with shaping on one core. The output is byte-identical with it on or off.
  Measured on one machine, 1M rows → Parquet: **+13.5% on a glibc build** (226,947 →
  257,628 rows/s; **+16.4%** over the 8M-row sweep, with peak RSS *falling* 138.5 → 61.5
  MiB), and **−5.3% on musl**, whose malloc takes a single global lock so a second
  allocating thread contends rather than scales. *(That made the default `auto` — on,
  except on musl. The tape reader below removed the contention, and the default is now on
  everywhere; see the Changed entry.)* `--pipeline on|off` overrides it either way. Memory stays bounded
  by the row group (at most two batches at once), and the thread is spawned lazily on the
  *second* row group so small runs and the ~3 ms cold start are untouched.
  See [`BENCHMARKS.md` §7](./BENCHMARKS.md#7-performance-measured), which also records the
  discarded first design — putting the *source* on the far thread and passing
  `serde_json::Value`s measured **~2× slower** on both targets — and the standing
  finding that parsing alone is **6.2× slower on musl than glibc**, which is what bounds
  further threading.

- **Standard input (`-i -`).** A shape composes in a pipeline now —
  `producer | shapeshift shape -i - -o out.parquet` — the usual shell convention, on `shape`,
  `infer`, and `cost`. Previously `-i -` failed with `opening -: No such file or directory`,
  and the only documented pipe recipe (`-i /dev/stdin`) was the data-loss bug fixed below.

- **Schema drift detection and mitigation (`drift` in the spec, `--on-drift` on the CLI).** A spec is
  written once per source *shape*, but sources move — and until now two kinds of movement passed
  **silently**: a field the spec has no column for (its values were never selected; nothing counted,
  nothing sidecarred) and a value that stopped coercing in an *optional* column under the lenient
  policy (written as null, uncounted). Both showed `rows_in == rows_out` and looked like a clean run.
  They are now detected per record and reported: for each drifted path, how many rows carried it, the
  record it first appeared in, example values, and the type inference would give it — i.e. the
  `columns:` block to paste into the spec to keep it. `shape` prints a summary and writes the full
  report to `<output>.drift.json`; the console returns it on `/api/shape` and renders it; embedders
  read `RunReport.drift` / `Shaper::drift_report()`.
  Five policies decide what a drift *does*: `ignore` (v0.1 behaviour, no detection),
  **`warn` (the new default** — count and report; the rows written are identical to `ignore`),
  `rescue` (a `json` catch-all column, appended after every real column so enabling it is an additive
  Iceberg schema change, holding whatever the row would have lost), `quarantine` (a drifted row is
  rejected into the existing reject sidecar, so the table holds only exactly-on-schema rows), and
  `error` (fail the run at the first event — and since a failed run never finalizes its sink, there
  is no footer-less Parquet file or new Iceberg metadata to half-consume).
  Detection is bounded like the rest of the engine (at most 1,000 distinct paths and
  `drift.max_examples` values per path, then the report says `truncated`) and measured: **~6% of wall
  time on a clean 1M-row run, peak RSS unchanged** — see [BENCHMARKS.md §7](./BENCHMARKS.md). A
  column merely *absent* from a record is deliberately not tracked; that loss is already visible as a
  null in the output.

### Fixed

- **The `json-array` reader built an owned `String` of every element before parsing**, so the
  raw text would be available if the parse failed — while the JSONL reader had been switched to
  a scratch copy precisely so the raw line is materialized only on the error path. The array
  reader now does the same. On glibc this measured as nothing (one small alloc/free against a
  3.2 µs row budget); it is corrected for consistency with the path it was modelled on.

- **A piped source shaped only its inference sample, and reported success.** `shape` read the
  input twice — once to sample for inference, once to shape it. That is harmless for a regular
  file and destroys any non-seekable input, because the sampling pass consumes bytes the shaping
  pass never sees. Piping 2,000 records through `-i /dev/stdin` (the recipe
  `examples/catalog-demos/02` gave) shaped **542** of them and exited **0** — `rows_in` simply
  reported the truncated count, so nothing looked wrong. The source is now read **once**: the
  head is sampled for inference and chained back in front of the remainder, so a pipe shapes
  exactly what a file would. Parse errors met while sampling ride along in the prefix, so they
  are still counted and sidecarred with their original line numbers. This also drops a redundant
  re-parse of the first `infer_sample` records from every inferred run.
- **An unknown key in a spec was silently ignored.** `colums:` (the classic typo) parsed cleanly
  and the run proceeded on pure inference — every type pin, `required` flag, and transform
  silently not applied, reported as a clean run. The spec model now rejects unknown fields with
  the offending key, its line, and the list of valid names.

### Changed

- **JSONL parses onto a reusable tape instead of building a `serde_json::Value` per record
  — ~1.7× faster shaping on the shipped binary.** The record model was a tree of small heap
  allocations, built and discarded for every row; that was the largest remaining cost, and it
  fell hardest on the musl-static build, whose allocator takes one global lock. The JSONL
  reader now parses onto a flat simd-json tape, reused between records, whose strings point
  back into the read buffer — nothing is allocated per record in the steady state. Measured on
  one machine, 1M rows → Parquet, three interleaved reps, median: **143,557 → 241,211 rows/s
  (+68.0%) on musl**, 238,817 → 351,486 (+47.2%) on glibc. Over the 8M-row sweep the shipped
  binary now does **273,495 rows/s**, against 144,745 before and 121,185 before any of this
  release's performance work. Peak RSS is unchanged and the flat-RSS check still passes;
  `json-array` input followed shortly after (below).
- **The `--pipeline` default is now on everywhere.** It was off on musl because a second
  allocating thread contended on that allocator's global lock. The tape reader removed the
  allocation, and with it the contention: the same A/B now reads **+10.0% on musl** and +22.3%
  on glibc. The earlier reasoning was not wrong; its premise is gone.

- **~26% faster shaping, same output, same bounded RAM.** Three per-row allocations that did no
  work are gone: the column's dotted path is compiled to segments once instead of being re-split
  every row; coercion reads the record by reference (`Cell::Str` holds a `Cow`) instead of cloning
  every selected value, so a string, date, timestamp, or json column no longer copies bytes it
  only reads; and `JsonlReader` no longer materializes each line's raw text for the error path on
  rows that parse fine. Measured on one machine (Xeon 2.80GHz, 4 cores), 1M rows, three
  interleaved A/B reps, median: **119,276 → 150,815 rows/s** on the shipped musl-static binary
  (glibc: 172,706 → 227,508). Peak RSS is unchanged (marginally lower) and the flat-RSS check
  still passes; the shaped Parquet is byte-identical to the previous binary's on both the Parquet
  and Iceberg paths. See [`BENCHMARKS.md` §7](./BENCHMARKS.md#7-performance-measured).

- `RunReport` gained a `drift` field (and dropped its `Eq` derive, which a report carrying JSON
  example values cannot have). `Shaper::new` is unchanged and does no drift work; `Shaper::from_spec`
  honours the spec's `drift` block, and `Shaper::with_drift` is the explicit constructor.

## [0.1.2] - 2026-07-18

### Fixed

- **Object-store multipart uploads no longer fail with `EntityTooSmall`.** The S3
  writer sized each multipart part by a single `read()` — which returns short for a
  regular file — instead of filling the 8 MiB buffer, so any `-o s3://…` (or
  `gs://` / `az://`) table larger than a few MB emitted non-final parts below S3's
  5 MiB minimum and the upload was rejected (400). Parts are now filled to
  `PART_SIZE` before upload, so every part but the last is a full 8 MiB.

### Added

- **`examples/catalog-demos/` — wiring shapeshift into a lakehouse.** Five
  copy-pasteable demos (streaming and databases → Iceberg → REST catalog; Iceberg →
  database sink; parallel multi-writer) with a fully local Redpanda + Postgres +
  Nessie + MinIO stack and reusable glue scripts (`stream_drain`, `pg_to_jsonl`,
  `register_catalog`, `parallel_write`, `read_back`). Validated end-to-end on EC2 —
  see [`examples/catalog-demos/VALIDATION.md`](./examples/catalog-demos/VALIDATION.md).

## [0.1.1] - 2026-07-18

### Added

- **Prebuilt static binary via a tag→release workflow.** Pushing a `shapeshift-v*`
  tag now builds the single `shapeshift` CLI for `x86_64-unknown-linux-musl` from the
  default (Snappy-only, C-free) build, asserts it is statically linked and boots, and
  attaches it — with a `.sha256` — to the GitHub Release. The install artifact behind
  the README's "single relocatable binary, no runtime deps" line is now real, not
  build-from-source-only. The tag namespace is distinct from the `shapeshift-oss-v*`
  source-mirror tag.
- **`shapeshift serve` — a local web console (new `shapeshift-serve` crate).** A
  browser UI over the shaper's four verbs: infer a `DatasetSpec` from pasted sample
  JSON, edit it, shape to Parquet or an Iceberg v2 table (compression, hidden/identity
  partitioning, `--append`), read the run report + a sample of rejected rows, inspect
  the output, and price a run against MAR. True to the single-binary ethos: the HTTP
  server is **hand-rolled on `std::net`** (a fixed thread pool over a shared listener —
  **no async runtime, no web framework, no C code**) and the UI is three static files
  **embedded** into the binary (`include_str!`), so `serve` stays one relocatable
  static binary with no Node/JVM/Python at runtime. Compiled out of the default build:
  it lives behind the CLI's off-by-default **`serve`** feature, so the lean
  musl-static `shapeshift` binary is byte-for-byte unchanged. Local & single-user by
  design (no scheduler, run queue, catalog server, connectors, metering, or auth —
  those belong to an orchestrator); binds to `127.0.0.1` with a
  DNS-rebinding `Host` allow-list and a JSON-only/no-CORS gate on POSTs. Verified
  end-to-end (unit + an over-TCP integration test, plus a headless-browser run of the
  full infer→shape→inspect→cost flow).
- **Additive schema evolution on append.** `--append` no longer requires an identical
  schema: a write may **add optional columns**. Existing columns keep their field-ids
  (matched by *name*, never position — type and required-ness must be unchanged); new
  columns get fresh ids continuing from the table's `last-column-id`; the metadata gains
  a new schema object with the next `schema-id` while `schemas` keeps the old ones, so
  prior snapshots stay readable and readers fill the new column with null for old rows.
  An unchanged schema is carried forward, never re-added (schemas are compared as sets of
  `(id, name, type, required)`). Non-additive changes — dropping, renaming, or re-typing
  a column, or adding a *required* column — are refused fail-fast at `create`, before any
  data is written. The evolved field-ids flow end-to-end: the data Parquet's
  `PARQUET:field_id` stamps, the manifest's per-column statistics keys, and the partition
  spec's source-ids all use them (`build_metadata_artifacts` re-runs the idempotent
  evolution as a safety net). Works for local and object-store tables and composes with
  identity + hidden partitioning. DuckDB-verified: old rows read the new column as null,
  new rows carry values, and partition pruning stays correct across evolved snapshots.
- **Hidden (transform) partitioning — `--partition-by 'day(event_at), bucket(16, id)'`.**
  `partition_by` entries may now be Iceberg transform expressions alongside identity
  columns: `bucket(N, col)` (long/string/date/timestamp; the spec-mandated Murmur3
  x86-32 bucketing, verified against the Iceberg spec's Appendix B test vectors),
  `truncate(W, col)` (long floors to a multiple of W, string keeps the first W code
  points), and temporal `year|month|day|hour(col)` (date/timestamp; exact for pre-1970
  values via floor division and proleptic-Gregorian civil-date math). Queries prune
  without the consumer knowing the physical layout. The table's partition spec records
  the real transform strings (`day`, `bucket[16]`, …); the manifest's partition tuple
  stores the transform's **result** type (int for bucket/temporal, source type for
  identity/truncate); partition field names follow the Java convention
  (`event_at_day`, `id_bucket`, `name_trunc`); temporal data directories render
  human-readable (`data/event_at_day=2026-01-01/`). Composes with `--append` — the
  existing spec must match exactly, transform included, checked fail-fast at create.
  Type applicability is validated at resolve time with clear errors (e.g. bucket over
  a double is refused). The CLI flag splits entries on top-level commas only, so the
  comma in `bucket(16, id)` does not split; strict-mode spec validation checks the
  transform's *source* column against the declared columns. DuckDB-verified
  end-to-end: full scans, predicates, and appends across snapshots for day-, bucket-,
  and multi-transform tables.

- **Measured benchmark suite — `benchmarks/bench.py`.** A reproducible harness (python3
  stdlib + cargo; datasets generated by a deterministic formula in the shape of
  `examples/events.jsonl`, every child measured with per-child `wait4` `ru_maxrss`) that
  measures bounded RAM across input sizes, rows/s + MB/s for JSONL→Parquet and
  JSONL→Iceberg, the streamed json-array path, cold-start wall time, and Snappy-vs-zstd
  on a fat build. Committed numbers (Intel Xeon 2.80 GHz, shipped musl-static binary):
  peak RSS **flat at 9.8 → 13.3 MiB across a 1.5 MB → 1.26 GB (~820×) input spread** — verifying
  the row-group==batch bounded-RAM design — at ~140k rows/s on both output paths
  (within ~1% of each other), 3 ms cold start, zstd 1.85× smaller than Snappy. The
  harness also surfaced and documents the **allocator trade**: glibc builds run ~1.6×
  faster but glibc malloc retains the freed batch buffers (RSS appears to grow; not a
  leak — `MALLOC_MMAP_THRESHOLD_=65536` or the musl binary shows the flat curve).
  `BENCHMARKS.md` §7 is now *measured* instead of "intended methodology", and the
  "illustrative / to be measured" labels across the docs point at the harness.
- **True streaming json-array input.** `JsonArrayReader` no longer reads the whole
  document: a small depth- and string-aware scanner finds element boundaries (`{`/`[`
  nest; strings may contain `,`/`]`/escapes), buffering **one element at a time** under
  the same per-record byte cap as JSONL — a multi-GB array streams with bounded RAM.
  Error semantics now mirror JSONL: an invalid or oversized *element* is a recoverable
  per-record reject (counted, raw bytes sidecarred, the 1-based element index reported
  as the reject's `line`) and the reader continues at the next boundary; *structural*
  problems (input that isn't an array, a truncated array, a trailing comma, trailing
  content after `]`) are reported once. Non-array input still fails eagerly at open
  with the "use --format jsonl?" hint.
- **Opt-in zstd "fat build" — `--features zstd`.** A `zstd` cargo feature on
  `shapeshift-parquet`, `shapeshift-iceberg`, and the CLI links the zstd codec, so
  `--compression zstd` works for **both** Parquet output and Iceberg data files
  (`cargo build -p shapeshift-cli --features zstd`; zstd level 3). DuckDB-verified: the
  written files' footers report `ZSTD` and read back correctly, Iceberg partition
  pruning intact. The musl-static **default build is unchanged** — its dependency graph
  contains no zstd at all (`cargo tree` verified) and `--compression zstd` still returns
  the clear error, which now points at the fat build.

- **Identity partitioning — `shape … --to iceberg --partition-by <cols>`.** `partition_by`
  is now physically applied instead of only validated and recorded: the table metadata
  carries a non-empty identity partition spec (`source-id`, transform `identity`, partition
  field-ids from 1000), and rows fan out to one data file per distinct partition tuple under
  Hive-style `data/<col>=<value>/` directories. Each data file's partition tuple is written
  into the manifest via a dynamic, per-spec Avro `partition` record (element field-ids
  preserved), and `partition-spec` / `partition-specs` / `default-spec-id` /
  `last-partition-id` are set in `metadata.json`. Works for **local and object-store** tables
  and composes with `--append` (an append must match the existing spec, or it is refused).
  Verified end-to-end with DuckDB, including partition **predicate pruning** — `WHERE
  region = …` reads only the matching files. The fan-out writer keeps one open writer per
  live partition (bounded-RAM per distinct value in a batch). Hidden (transform) partitioning
  shipped later in this release — see the entry above. In `infer` mode a `partition_by` column
  need not be pre-declared — the sink resolves it against the inferred schema.
- **Per-column statistics in Iceberg manifests.** Every data file now records
  `value_counts`, `null_value_counts`, and `lower_bounds` / `upper_bounds` per column —
  min/max computed while the data is written and serialized in Iceberg's little-endian
  single-value encoding (bool / long / double / date / timestamp / string; all-null
  columns emit no bound; NaN is excluded from float bounds). Readers skip whole files by
  predicate: verified with DuckDB, whose predicate queries return correct rows across a
  multi-file table (a wrong bound would drop rows, so correctness is the test). The
  manifest's `data_file` Avro schema gained the four optional `map<int, …>` stat fields
  with the spec's element field-ids.
- **Append-to-existing / multi-snapshot Iceberg — `shape … --to iceberg --append`.**
  Instead of only writing a fresh single-snapshot table, `--append` commits a new snapshot
  onto an existing one: it reads the current `metadata.json` and manifest list, carries the
  prior snapshot's `manifest_file` records forward, bumps the snapshot id + sequence number,
  chains `parent-snapshot-id`, and writes `v{N+1}.metadata.json` with `version-hint` → N+1.
  A full scan then reads every snapshot's rows; time-travel to an earlier snapshot reads only
  its rows (both DuckDB-verified). Works for **local and object-store** tables. At the time this
  landed, the schema had to match the existing table exactly; **additive schema evolution**
  shipped later in this release — see the entry above.
  This adds a small hand-rolled Avro **reader** to `shapeshift-iceberg` (the inverse of the
  manifest writer), the append-aware `build_metadata_artifacts` / `append_snapshot`, and an
  `append` flag on both Iceberg sinks. The CLI's "iceberg table metadata →" line now reports
  the actual metadata version written.
- **`shapeshift-objstore` — object-store Parquet sink (sixth crate).**
  `ObjectStoreParquetSink` wraps `shapeshift-parquet`'s `ParquetSink`: it shapes to a
  local temp Parquet file, then on `finish()` streams that file to S3 / GCS / Azure as a
  bounded-RAM **multipart upload** (8 MiB parts, on a current-thread tokio runtime).
  Built on `object_store` 0.14; stores are constructed from the destination URL with
  credentials from the environment (`s3://`, `gs://` / `gcs://`, `az://` / `azure://` /
  `abfs[s]://` / `adl://`, and `file://`). Aborts the upload and cleans up the temp file
  on any error.
- **CLI `object_store` feature (off by default).** `shapeshift shape --to parquet -o
  s3://bucket/key.parquet` routes through `ObjectStoreParquetSink` when built with
  `--features object_store`. The default musl-static binary links **no** cloud SDKs and
  refuses an object-store URL with a clear "needs a build with `--features object_store`"
  error.
- **Object-store Iceberg tables — `ObjectStoreIcebergSink`.** A URL `--output` with
  `--to iceberg` now lands a whole self-contained Iceberg v2 table (data Parquet +
  manifest + manifest-list + `metadata.json` + `version-hint.text`) in S3 / GCS / Azure:
  the data file streams up as a multipart upload, the metadata blobs go via `put`, and
  `version-hint.text` is written **last** so a snapshot only becomes visible once every
  file it references is in place. Every embedded path is **location-anchored** at the
  destination URI, so DuckDB `iceberg_scan` reads the table back at its location. Verified
  via a `file://` object-store backend — byte-identical to the local path; S3 / GCS /
  Azure share the same write path (not exercised in CI, which has no cloud credentials).
  `shapeshift inspect <url>` summarizes an object-store table. To support this, `shapeshift-iceberg` grew a backend-agnostic table
  builder (`IcebergDataWriter` + `build_metadata_artifacts`) that both the local and
  object-store sinks reuse; local tables now embed their real location rather than a
  `canonicalize`d path. A hosted **REST** catalog (catalog-managed relocation,
  multi-writer commits) is left to an external catalog.

### Changed

- **Append compatibility is validated up front (fail-fast).** An incompatible
  `--append` (schema or partition-spec mismatch) is now refused at sink `create()`,
  **before any data file is written or uploaded**, instead of after the full write — so
  a rejected append no longer leaves an orphaned Parquet under `data/`. Both the local
  and object-store sinks read + validate the prior table once at creation and reuse it at
  `finish()`.

### Notes / limits

- **Appends are single-writer.** `version-hint.text` is published with no lock, so
  concurrent `--append` writers to the same table can clobber each other's snapshot;
  multi-writer commits need a catalog (the roadmap REST catalog). Documented in
  `docs/SPEC.md` and the iceberg crate README.
- **A partitioned table needs at least one row.** A zero-row input with `--partition-by`
  has no partition to write and is refused with a clear error (an unpartitioned zero-row
  table still writes a valid empty table).

### Documentation

- **Relocating a table (copy-anywhere).** Embedded paths are absolute (location-anchored),
  so a moved/copied table fails a *default* reader — but DuckDB reads it in place with
  `iceberg_scan('<new path>', allow_moved_paths=true)`. Verified end-to-end for partitioned
  and multi-snapshot tables (predicate pruning still works). This is the OSS "copy-anywhere"
  story; writer-embedded *relative* paths are deliberately not pursued (non-standard in
  Iceberg, not read by default across engines), and **catalog-managed** relocation (any
  engine, no reader flag, plus multi-writer commits) is left to an external Iceberg REST catalog.
  Documented across README, ARCHITECTURE, SPEC, DESIGN, ROADMAP, SECURITY,
  and the crate READMEs.

## [0.1.0] - 2026-07-13

Initial public release. A standalone Cargo workspace (its own `Cargo.lock`) of five
crates, Rust edition 2021, `rust-version = 1.80`, Apache-2.0. Snappy-only Parquet
(`parquet` with `default-features = false`, features `["arrow", "snap"]`) keeps the
build musl-static and ships as a single binary.

### Added

- **Five-crate workspace.**
  - `shapeshift-core` — the engine, with no I/O backend dependencies: the dataset
    transform `Spec` model, JSON schema inference, the named `Transform` library, the
    `Shaper` (JSON value → Arrow `RecordBatch`), the `Sink` trait, and the MAR cost
    arithmetic.
  - `shapeshift-json` — streaming JSON source built on the simd-json SIMD lexer (via
    its serde bridge → `serde_json::Value`): `JsonlReader` (one object per line, bounded
    RAM, skips blank lines, preserves the raw bad line on a parse error) and
    `JsonArrayReader` (whole-document, non-streaming in v0.1).
  - `shapeshift-parquet` — `ParquetSink` (Arrow `RecordBatch` → Parquet, one row group
    flushed per batch for bounded RAM, Snappy default) plus `inspect()` read-back.
  - `shapeshift-iceberg` — `IcebergSink`, writing a self-contained Apache Iceberg v2
    table with no catalog server: `data/<uuid>.parquet` (carrying `PARQUET:field_id`),
    `metadata/<uuid>-m0.avro` (manifest), `metadata/snap-<id>-1-<uuid>.avro` (manifest
    list), `metadata/v1.metadata.json`, and `metadata/version-hint.text`. One
    unpartitioned append snapshot per run, plus `inspect()`.
  - `shapeshift-cli` — the single `shapeshift` binary.

- **`shapeshift infer`** — infer a `DatasetSpec` from a sample and print it (or write it
  with `-o`). Flags: `-i <input>`, `--format jsonl|json-array`, `--sample N` (default
  1000, `0` = all), `--no-flatten`, `--dataset NAME`, `-o spec.yaml`. Emits a
  ready-to-edit YAML `DatasetSpec`.

- **`shapeshift shape`** — shape a source into Parquet or Iceberg. Flags: `-s spec.yaml`,
  `-i input`, `-o output`, `--to parquet|iceberg`, `--format jsonl|json-array`,
  `--compression snappy|zstd|uncompressed`, `--dataset NAME`. With no `--spec` it infers
  on the fly (then `--input` and `--output` are required). Source parse errors are
  counted and sidecarred to `<output>.rejects.jsonl` (one JSON object per line:
  `{"line","error","raw"}`); the run never aborts on a bad line. Prints
  `rows_in=.. rows_out=.. rejected=.. parse_errors=.. row_groups=.. bytes=..`.

- **`shapeshift inspect <path>`** — auto-detects and summarizes either a Parquet file
  (rows / row-groups / columns) or an Iceberg table directory (format-version /
  table-uuid / current-snapshot-id / total-records / columns).

- **`shapeshift cost`** — MAR economics. Flags: `--rows N` or `-i input`,
  `--vendor-per-million P`, `--self-host-cost C`. Prints billable rows, vendor cost
  (`rows / 1e6 * P`), self-host cost, and dollars saved (with percent). Never guesses a
  price — you supply your plan's effective $/million MAR. Honest by design: a tiny job
  that doesn't amortize reports negative savings.

- **Global verbosity** `-v` / `-vv`. Logs go to stderr; command output to stdout.

- **Dataset transform Spec** (YAML or JSON) — the product boundary: write once per source
  shape, then shape unbounded rows. Covers `dataset`, `source`, `output`
  (`format`, `path`, `compression`, `partition_by`), `schema` (`infer` | `strict`,
  default `infer`), `columns` (`name`, `from`, `type`, `transform`, `required`), and
  `options` (`flatten` default true, `row_group_rows` default 50000, `infer_sample`
  default 1000).

- **Logical type system** — `bool` → Arrow `Boolean` → Iceberg `boolean`; `int64` →
  `Int64` → `long`; `float64` → `Float64` → `double`; `string` → `Utf8` → `string`;
  `date` → `Date32` → `date`; `timestamp` → `Timestamp(µs, no zone)` → `timestamp`;
  `json` → `Utf8` → `string` (a nested value kept as a JSON-encoded string).

- **Transforms** (value → value, applied before coercion): `lowercase`, `uppercase`,
  `trim`, `json_encode`, `to_string`, `dollars_to_cents` (number × 100 → int), `abs`,
  `empty_to_null`.

- **Path selection** — dotted (`user.name`), optional leading `$` / `$.`, a numeric
  segment as an array index (`tags.0`), `$` for the whole record; an unresolved path
  resolves to null.

- **Schema inference** — samples the first `infer_sample` records; `flatten` (default on)
  expands nested objects into dotted columns and makes arrays `json` leaves; detects
  `YYYY-MM-DD` → `date` and RFC3339 → `timestamp`; int + float in one field widen to
  `float64`; mixed scalar shapes widen to `string`; nested / array → `json`.

- **Coercion policy.**
  - _lenient_ (default) — a missing / null or uncoercible value in an optional column
    becomes null; a required column that is missing / null / uncoercible causes a soft
    row reject (row dropped and counted).
  - _strict_ — a required miss or any coercion failure aborts the run.
  - Coercion rules: int from an integral float or a numeric string; float from a number
    or numeric string; string stringifies scalars; date parses `YYYY-MM-DD` / RFC3339 (or
    an integer epoch-day); timestamp parses RFC3339 / zone-less ISO as UTC (or an integer
    epoch-millisecond).

- **Reject sidecar** — bad source lines are streamed to `<output>.rejects.jsonl` rather
  than aborting the run, so a single malformed record never fails a batch.

- **Hand-rolled field-id Avro encoder** (~150 LOC) — `shapeshift` emits the Iceberg Avro
  Object Container File itself (zig-zag varint longs, length-prefixed strings, unions,
  empty records) so the exact `field-id`-carrying schema JSON is preserved in the file
  header. The general Rust Avro crate drops those custom schema attributes on
  serialization, producing a table Iceberg readers cannot map (DuckDB fails with "No
  default expression in FieldId Map"). Emitting the container directly also removes a
  dependency, in keeping with the single-binary ethos.

- **DuckDB-verified round-trip** (DuckDB 1.5.4).
  - Parquet output reads via `read_parquet()` — types, dates / timestamps,
    nested-flattened columns, and nulls all correct.
  - The Iceberg v2 table reads via `iceberg_scan('<table dir>')` end-to-end, returning
    the same rows and aggregates as the Parquet path (verified identical: 4 rows,
    `sum(amount) = 115.49` on the sample).
  - The data Parquet carries `PARQUET:field_id` `1..N`, and the manifest's embedded Avro
    schema carries `field-id`.

- **Dependency floor** — `arrow-array` / `arrow-schema` 54, `parquet` 54, `simd-json`
  0.14, `chrono` 0.4 without the `clock` feature (the engine never reads the wall clock;
  all time is derived from the data, the sole wall-clock read being the Iceberg snapshot
  timestamp in the sink), `uuid` v4, `clap` 4 (derive), `serde`, `serde_json`,
  `serde_yaml`.

- **Quality gates** — ~3,400 Rust LOC across the five crates; 27 tests pass (25 `#[test]`
  + 2 doctests); `cargo clippy --workspace --all-targets -- -D warnings` is clean;
  `cargo fmt --check` is clean.

### Known limitations

- **Iceberg:** one unpartitioned append snapshot per run — no append-to-existing or
  multi-snapshot support yet. `partition_by` is validated and recorded but not applied (a
  warning states this). No per-column statistics in the manifest (optional in the spec).
  Table file paths are absolute, so the table is not yet relocatable (a catalog solves
  this).
- **Compression:** zstd is not compiled (musl-static build); `--compression zstd` returns
  a clear error — use `snappy` or `uncompressed`.
- **JSON array input** is whole-document (not streaming); JSONL is the unbounded,
  bounded-RAM path.
- **Output** is local filesystem only; object-store (S3 / GCS / Azure) output is roadmap.
- **Temporal integers:** integer timestamps read as epoch-milliseconds and integer dates
  as epoch-days; `timestamp` is zone-less (Iceberg `timestamp`, not `timestamptz`).
- **Not in the engine:** no incremental / CDC, no scheduling, and no connectors — those
  belong to an orchestrator.

[Unreleased]: https://github.com/lucheeseng827/shapeshift/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/lucheeseng827/shapeshift/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/lucheeseng827/shapeshift/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/lucheeseng827/shapeshift/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lucheeseng827/shapeshift/releases/tag/v0.1.0
