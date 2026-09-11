# shapeshift — Roadmap

Phase-based, forward-looking, and honest. This is a plan, **not a schedule** — there are no
dates and none are implied. Sizes and ordering are engineering judgement, revisable as users
tell us what matters. Anything marked *(roadmap)* is not built yet; anything checked is
in the v0.1 tree and, where the brief says so, verified. The design these phases extend is fixed
in [`./ARCHITECTURE.md`](./ARCHITECTURE.md) (the v0.1 limits are §13; the engine/orchestrator boundary is
§14).

Two rules govern everything below:

- **The OSS engine (Apache-2.0) stays a shaper, not an orchestrator.** It has no row cap and no
  telemetry, and it never will. Scheduling, run history, connectors, metering, and CDC are the
  orchestration layer's job, kept out of the engine on purpose
  ([ARCHITECTURE §14](./ARCHITECTURE.md)). The phases below only ever make the engine a
  better *shaper*; what stays out is listed in its own section at the end.
- **Additive, not a rewrite.** The six-crate split (I/O-free `core`, driver crates behind the
  `Sink` trait) was chosen so each roadmap item is a new driver, a new spec field, or a new
  manifest column — never a change to the engine's contract. New physical outputs are new crates
  implementing `Sink`; `core` does not move.

Performance note: shapeshift's numbers are **measured, on stated hardware**, with the
reproducible harness in [`benchmarks/`](./benchmarks/) — see
[`BENCHMARKS.md` §7](./BENCHMARKS.md#7-performance-measured). Numbers from one machine are never
presented as a promise for another; run the harness yourself to compare.

## Milestone summary

| Phase | Headline | Status |
|---|---|---|
| **0 — Foundation** | Standalone Cargo workspace, musl-static single binary, Snappy-only dep floor, hand-rolled Avro | ✅ **shipped (v0.1)** |
| **1 — v0.1 engine** | Dataset transform Spec → Arrow → Parquet **and** self-contained Iceberg v2, both read end-to-end by DuckDB | ✅ **shipped (v0.1)** |
| **2 — Iceberg write-path maturity** | Append-to-existing / multi-snapshot, partitioning, per-column stats, a catalog | ✅ **shipped** — append-to-existing / multi-snapshot, **additive schema evolution**, per-column statistics, **identity + hidden (transform) partitioning**, and the server-less Hadoop-style catalog; a REST catalog is out of scope |
| **3 — Reach & format coverage** | Object-store output, optional zstd fat build, streaming json-array, a real benchmark suite | ✅ **shipped** — object-store output (Parquet _and_ Iceberg), the zstd fat build (`--features zstd`), streaming json-array, and the measured benchmark suite ([`benchmarks/`](./benchmarks/)) |
| **4 — Trustworthy against a moving source** | Schema drift detection and mitigation: what the schema silently dropped, and five policies for what to do about it | ✅ **shipped** — `drift.policy` (`ignore`/`warn`/`rescue`/`quarantine`/`error`), a per-path report with the columns to add, measured at ~6% on a clean run |

Phases 2, 3, and 4 are independent of each other and can interleave — each only depends on the v0.1
core.

---

## Phase 0 — Foundation *(shipped, v0.1)*

The build discipline that makes the single-binary, no-runtime ethos real.

- [x] Standalone Cargo workspace with its own `Cargo.lock` (the Arrow / parquet / simd-json stack
      resolves independently of the shared monorepo), six member crates: `shapeshift-core`,
      `shapeshift-json`, `shapeshift-parquet`, `shapeshift-iceberg`, `shapeshift-objstore`,
      `shapeshift-cli`.
- [x] Rust edition 2021, `rust-version` 1.80, Apache-2.0.
- [x] Dependency floor chosen for a musl-static single binary: arrow-array / arrow-schema 54;
      parquet 54 with `default-features = false` + features `["arrow","snap"]` (Snappy only — **no**
      zstd/brotli C codecs); simd-json 0.14; chrono 0.4 **without** the `clock` feature (the engine
      never reads the wall clock — all time is derived from the data; the sole wall-clock read is
      the Iceberg snapshot timestamp in the sink); uuid v4; clap 4 (derive); serde / serde_json /
      serde_yaml.
- [x] **Hand-rolled Avro Object-Container-File encoder** (~150 LOC) instead of a general Avro crate
      — the field-id-carrying manifest schema is preserved verbatim, and one dependency is dropped
      (single-binary ethos). See [ARCHITECTURE](./ARCHITECTURE.md), "why hand-rolled Avro."
- [x] `core` has **no I/O backend deps** by design; `json` / `parquet` / `iceberg` are drivers that
      depend on `core` but never on each other. `objstore` is the sole exception — it wraps
      `parquet`'s `ParquetSink` to add a remote-upload tail, so it depends on `core` + `parquet`.
- [x] Green bar: ~7,900 Rust LOC across the six crates; **68 tests pass** (66 `#[test]` + 2
      doctests); `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt --check`
      clean.

## Phase 1 — v0.1 engine *(shipped, verified)*

Write the transform once, shape unbounded rows for free — and prove the output is real by reading
it back with an independent engine.

**Spec & engine**

- [x] The **dataset transform Spec** (YAML or JSON): `dataset` / `source` / `output` / `schema`
      (`infer` | `strict`) / `columns` / `options` — the product boundary.
- [x] **JSON schema inference** over the first `infer_sample` records: flatten (default on) expands
      nested objects into dotted columns and makes arrays `json` leaves; detects `YYYY-MM-DD` → date
      and RFC3339 → timestamp; int + float in one field widen to float64; mixed scalar shapes widen
      to string; nested / array → json. `infer --no-flatten` / `--sample N` (0 = all) supported.
- [x] The named **transform library** (value → value, applied before coercion): `lowercase`,
      `uppercase`, `trim`, `json_encode`, `to_string`, `dollars_to_cents`, `abs`, `empty_to_null`.
- [x] **Path selection**: dotted (`user.name`), optional leading `$`/`$.`, numeric segment = array
      index (`tags.0`), `$` = whole record; an unresolved path = null.
- [x] Logical **types → Arrow → Iceberg**: bool→Boolean→boolean, int64→Int64→long,
      float64→Float64→double, string→Utf8→string, date→Date32→date, timestamp→Timestamp(µs,no-zone)
      →timestamp, json→Utf8→string.
- [x] **Coercion policy**: lenient (default) — missing/null/uncoercible in an *optional* column → null;
      in a *required* column → a soft row reject (dropped, counted). strict — a required miss or any
      coercion failure aborts the run.
- [x] The **Shaper**: `serde_json::Value` → Arrow `RecordBatch` via the two-phase transactional push
      (a partially-built row can never leave a column ragged).

**Sources & sinks**

- [x] `shapeshift-json`: `JsonlReader` (one object per line, bounded RAM, skips blank lines,
      preserves the **raw bad line** on a parse error) + `JsonArrayReader` (whole-document).
- [x] `shapeshift-parquet`: `ParquetSink` (RecordBatch → Parquet, **one row group flushed per batch**
      = bounded RAM; Snappy default) + `inspect()` read-back.
- [x] `shapeshift-iceberg`: `IcebergSink` writes a **self-contained Apache Iceberg v2 table** with no
      catalog server — `data/<uuid>.parquet` (carrying `PARQUET:field_id`),
      `metadata/<uuid>-m0.avro` (manifest), `metadata/snap-<id>-1-<uuid>.avro` (manifest list),
      `metadata/v1.metadata.json`, `metadata/version-hint.text`; one unpartitioned append snapshot per
      run + `inspect()`.

**CLI & cost**

- [x] The single `shapeshift` binary: `infer`, `shape` (with on-the-fly inference when `--spec` is
      omitted), `inspect` (auto-detects Parquet file vs Iceberg table dir), `cost`. Global `-v`/`-vv`;
      logs → stderr, output → stdout.
- [x] Non-fatal ingest: source parse errors and required-column rejects are counted and sidecarred to
      `<output>.rejects.jsonl` (`{"line","error","raw"}` per line); the run never aborts on a bad line
      under the default policy.
- [x] **MAR cost arithmetic** (`cost`): billable rows, vendor cost (`rows/1e6 × --vendor-per-million`),
      self-host cost, and $ saved (+ %). Never guesses a price; prints a **negative** saving honestly
      for a job too small to amortize.

**Verified (DuckDB 1.5.4)**

- [x] Parquet output is read by DuckDB `read_parquet()` — types, dates/timestamps, nested-flattened
      columns, and nulls all correct.
- [x] The Iceberg v2 table is read by DuckDB `iceberg_scan('<table dir>')` **end-to-end** — same rows,
      same aggregates as the Parquet path (verified identical: 4 rows, `sum(amount) = 115.49` on the
      sample).
- [x] The data Parquet carries `PARQUET:field_id` 1..N; the manifest's embedded Avro schema carries
      `field-id` (the correctness fix the hand-rolled encoder exists for).

---

## OSS-core phases (Apache-2.0)

Everything below keeps the engine a self-hostable shaper with **no row cap and no telemetry**. These
phases closed the honest v0.1 limits — all three are now **shipped** — and none of them added a
scheduler, a connector, or a metering path (that boundary is the next section).

## Phase 2 — Iceberg write-path maturity *(shipped)*

This phase makes the table a first-class, evolvable Iceberg v2 dataset.

- [x] **Append-to-existing / multi-snapshot commits** — `shape … --to iceberg --append` commits a new
      append snapshot onto an existing table instead of only writing a fresh single-snapshot table: it
      reads the current `metadata.json` + manifest list (a hand-rolled Avro **reader** carries the prior
      `manifest_file` records forward), bumps the snapshot id + sequence number, chains
      `parent-snapshot-id`, and writes `v{N+1}.metadata.json` with `version-hint` → N+1. Works for
      both local and object-store tables; verified end-to-end with DuckDB — a full scan reads all
      snapshots' rows, and time-travel to an earlier snapshot reads only its rows.
- [x] **Additive schema evolution on append** — an append may **add optional columns**: existing
      columns keep their field-ids (matched by name, never position), new columns get fresh ids
      continuing from the table's `last-column-id`, and the metadata gains a new schema object with
      the next `schema-id` (`schemas` keeps the old ones, so prior snapshots stay readable — readers
      fill the new column with null for old rows; an unchanged schema is carried forward, never
      re-added). Non-additive changes — dropping, renaming, or re-typing a column, or a new
      *required* column — are refused fail-fast at `create`, and the evolved field-ids flow through
      the Parquet `PARQUET:field_id` stamps, the manifest stats keys, and the partition spec's
      source-ids. Works for local and object-store tables, and composes with partitioning
      (partition source-ids stay stable across evolutions). DuckDB-verified: old rows read the new
      column as null, new rows carry values, and partition pruning stays correct across evolved
      snapshots.
- [x] **Identity partitioning** — `shape … --to iceberg --partition-by <cols>` actually applies
      `partition_by`: it writes a non-empty identity partition spec (`source-id`, transform
      `identity`, partition field-ids from 1000) into the table metadata and fans rows out to one
      data file per distinct partition tuple under Hive-style `data/<col>=<value>/` directories, with
      each file's partition tuple recorded in the manifest (a dynamic per-spec Avro `partition`
      record). Composes with `--append`. Verified end-to-end with DuckDB, including partition
      predicate pruning (`WHERE region = …` reads only the matching files).
- [x] **Hidden partitioning** — Iceberg transform partitions so queries prune without the consumer
      knowing the physical layout: `bucket(N, col)` (spec-exact Murmur3, verified against the
      Iceberg spec's Appendix B test vectors), `truncate(W, col)`, and temporal
      `year|month|day|hour(col)` — expressed directly in `partition_by` / `--partition-by`
      alongside identity columns (`day(event_at), region`). The partition spec records the real
      transform strings (`day`, `bucket[16]`, …), the manifest's partition tuple stores the
      transform *result* type, temporal directories render human-readable
      (`event_at_day=2026-01-01/`), and appends must match the existing spec exactly (transform
      included). DuckDB-verified end-to-end: full scans, predicates, and appends across snapshots
      all correct for day / bucket / multi-transform tables.
- [x] **Per-column statistics in the manifest** — every data file records `value_counts`,
      `null_value_counts`, and `lower_bounds` / `upper_bounds` per column (computed while the data is
      written; bounds are the exact values in Iceberg's little-endian single-value encoding). Readers
      do file-level pushdown/skip: verified with DuckDB — predicate queries return correct rows across a
      multi-file table (a wrong bound would drop rows, so correctness is the test).
- [x] **Server-less Hadoop-style catalog (location-anchored tables)** — the object-store sink writes a
      table at a warehouse location (e.g. `s3://warehouse/db/table`) with the metadata `location` and
      **every** manifest / manifest-list / data-file path anchored there, plus a `version-hint.text`
      pointer. That makes the table discoverable and readable at its bucket location with **no catalog
      server** — the OSS half of "relocatable" (see Phase 3). Local tables likewise embed their real
      location rather than a `canonicalize`d path.
- [x] **Read-side relocation (moved / copied tables)** — a shapeshift table embeds absolute,
      location-anchored paths, so a moved copy fails a *default* reader; but DuckDB reads it in place
      with `iceberg_scan('<new path>', allow_moved_paths=true)` (verified for partitioned and
      multi-snapshot tables, predicate pruning intact). That is the OSS "copy-anywhere" story — no
      rewrite, no catalog. Writer-embedded *relative* paths are deliberately **not** pursued: they
      are non-standard in Iceberg and not resolved by default across engines, so they would trade a
      verifiable table for an unreadable one.
- **Out of scope: REST catalog + catalog-managed relocation + multi-writer append.** An external
      Iceberg REST catalog re-anchors a moved table's paths (so *any* engine reads it with no reader
      flag) and serialises concurrent appends; the engine does not implement one.

## Phase 3 — Reach & format coverage *(shipped)*

Widen where output can land and where input can come from — without breaking the musl-static default.

- [x] **Object-store output (S3 / GCS / Azure) — Parquet _and_ Iceberg** — shipped in
      **`shapeshift-objstore`**, new `Sink` implementations (nothing in `core` changed). A URL
      `--output` (`s3://` / `gs://` / `az://` / `file://`) lands **Parquet** (a bounded-RAM multipart
      upload) **and a whole self-contained Iceberg v2 table** — data + manifests + metadata, every
      path anchored at the destination, so DuckDB `iceberg_scan` reads it back at its location.
      Verified via a `file://` object-store backend — byte-identical to the local table; the cloud
      backends share the same write path (not exercised in CI, which has no credentials). `inspect
      <url>` summarizes an object-store table too.
      Credentials come from the `AWS_*` / `GOOGLE_*` / `AZURE_*` env vars; kept behind the CLI's
      **off-by-default `object_store` feature** so the default musl-static binary never links the
      cloud SDKs.
- [x] **Optional zstd via a fat build** — a `zstd` cargo feature on `shapeshift-parquet`,
      `shapeshift-iceberg`, and the CLI (`cargo build -p shapeshift-cli --features zstd`) links the
      zstd codec, so `--compression zstd` works for both Parquet output and Iceberg data files
      (DuckDB-verified: the file footers report ZSTD and read back correctly, partition pruning
      intact). The musl-static default is unchanged — zstd absent from its dependency graph
      (`cargo tree` verified), and `--compression zstd` still returns the clear error, now
      pointing at the fat build.
- [x] **True streaming json-array** — `JsonArrayReader` now streams: a small depth- and string-aware
      scanner finds element boundaries (`{`/`[` nest; strings may contain `,`/`]`/escapes), buffering
      **one element at a time** under the same per-record byte cap as JSONL, so a multi-GB array never
      materializes. Error semantics mirror JSONL: an invalid or oversized *element* is a recoverable
      per-record reject (reported with its 1-based element index; raw bytes preserved in the sidecar)
      and the reader recovers at the next boundary; *structural* problems (not an array, truncated
      array, trailing comma/garbage) are reported once. Non-array input still fails eagerly at open.
- [x] **A published benchmark suite** — [`benchmarks/bench.py`](./benchmarks/bench.py), a
      reproducible harness (python3 stdlib + cargo; deterministic formula datasets, per-child
      `wait4` RSS measurement) with *measured* numbers committed to
      [`BENCHMARKS.md` §7](./BENCHMARKS.md#7-performance-measured): on the shipped musl-static
      binary, peak RSS is **flat (9.8 → 13.3 MiB) across a 1.5 MB → 1.26 GB (~820×) input
      spread** (the bounded-RAM design claim, verified), ~140k rows/s on both output paths,
      3 ms cold start, and zstd 1.85× smaller than Snappy on the fat build. Also surfaced and documented the glibc-vs-musl
      allocator trade (glibc ~1.6× faster but retains freed buffers; not a leak, tunable via
      `MALLOC_MMAP_THRESHOLD_`).

## Phase 4 — Trustworthy against a moving source *(shipped)*

A spec is written once per source *shape*, which is the whole economic argument — and the whole
risk. Phase 4 closes the gap between "the spec you wrote" and "the source you have today", without
the engine growing any state.

- [x] **Schema drift detection** — the two losses that used to leave *no trace* are now found and
      counted per record: an **undeclared field** (in `schema: infer`, one that showed up after the
      `infer_sample` window; in `schema: strict`, one the spec never declared — either way its
      values were never selected) and a **type mismatch** in an optional column under the lenient
      policy (present, uncoercible, written as null, uncounted). Both used to report
      `rows_in == rows_out` and look like a clean run. The effective columns' source paths compile
      into a trie once; each record is walked against it, and the dotted path string is only built
      on a miss.
- [x] **A report that tells you what to change** — per drifted path: rows carrying it, the record it
      first appeared in, example values, and **the type inference would give it**, emitted as a
      ready-to-paste `columns:` block. `shape` prints a summary and writes
      `<output>.drift.json`; `serve` returns and renders it; embedders read `RunReport.drift`.
- [x] **Five mitigations, chosen per run** (`drift.policy`, or `--on-drift`): `ignore` (the v0.1 hot
      loop, no detection), **`warn`** (the default — report; rows written are identical to
      `ignore`), `rescue` (a `json` catch-all column, appended after every real column so enabling
      it is an *additive* Iceberg schema change, holding whatever the row would have lost),
      `quarantine` (a drifted row is rejected into the existing sidecar, so the table holds only
      exactly-on-schema rows), and `error` (fail at the first event — and a failed run never
      finalizes its sink, so there is no half-written table to consume).
- [x] **Bounded and measured, like the rest of the engine** — at most 1,000 distinct paths and
      `drift.max_examples` values per path (then the report says `truncated`), and the default costs
      **~6% of wall time on a clean 1M-row run with peak RSS unchanged**
      ([BENCHMARKS.md §7](./BENCHMARKS.md)).
- [ ] **Cross-run schema management** — propagating a new column into the destination on the next
      run, drift history, and alerting are **deliberately not here**. They are stateful and
      scheduled: an orchestrator's job ([ARCHITECTURE §14](./ARCHITECTURE.md)), listed in the
      out-of-scope section below.

---

## Out of scope (by design)

Everything above is the engine. The items below are deliberately **not** in it: each needs state,
a schedule, or a network surface the engine does not have. They belong to an orchestrator that runs
shapeshift as a step, and the engine's only contract to one is the CLI surface and the `SinkSummary`
it prints ([ARCHITECTURE §14](./ARCHITECTURE.md)).

- **Scheduling** — run a spec on a cadence or trigger. Compose shapeshift as a step under an
  orchestrator (such as **dagron**) rather than growing a scheduler inside the engine.
- **Incremental / CDC shaping** — shape only the rows changed since the last run (watermarks, change
  feeds). The engine stays a stateless, whole-source shaper; the
  [catalog demos](./examples/catalog-demos/README.md) show watermark and CDC patterns built around it.
- **Connectors** — source/sink connectors (databases, SaaS APIs) feeding the engine. Kept out of its
  dependency graph entirely.
- **Schema management across runs** — the stateful half of drift: remember each dataset's shape
  between runs, propagate an added column on the next run, keep the drift history, and alert on it.
  The engine already detects and reports drift per run
  ([Phase 4](#phase-4--trustworthy-against-a-moving-source-shipped)).
- **Run history** — retained run summaries. The `cost` command's arithmetic is available to anything
  that keeps them.

---

*shapeshift — © 2026 Nicholas Lu Chee Seng and the shapeshift contributors. Apache-2.0. Repo:
https://github.com/lucheeseng827/shapeshift*
