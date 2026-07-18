# shapeshift — Roadmap

Phase-based, forward-looking, and honest. This is a plan, **not a schedule** — there are no
dates and none are implied. Sizes and ordering are engineering judgement, revisable as the OSS
funnel tells us what matters. Anything marked *(roadmap)* is not built yet; anything checked is
in the v0.1 tree and, where the brief says so, verified. The design these phases extend is fixed
in [`./ARCHITECTURE.md`](./ARCHITECTURE.md) (the v0.1 limits are §13; the open-core boundary is
§14).

Two rules govern everything below:

- **The OSS engine (Apache-2.0) stays a shaper, not an orchestrator.** It has no row cap and no
  telemetry, and it never will. Scheduling, run history, connectors, metering, and CDC are the
  orchestration layer's job — they are hosted / commercial features, kept out of the core on purpose
  ([ARCHITECTURE §14](./ARCHITECTURE.md)). The OSS-core phases (2–3) only ever make the engine a
  better *shaper*; the hosted / commercial roadmap is a separate section, drawn with a hard line.
- **Additive, not a rewrite.** The six-crate split (I/O-free `core`, driver crates behind the
  `Sink` trait) was chosen so each roadmap item is a new driver, a new spec field, or a new
  manifest column — never a change to the engine's contract. New physical outputs are new crates
  implementing `Sink`; `core` does not move.

Performance note: shapeshift's numbers are **measured, on stated hardware**, with the
reproducible harness in [`benchmarks/`](./benchmarks/) — see
[`BENCHMARKS.md` §7](./BENCHMARKS.md#7-performance-measured). Numbers from one machine are never
presented as a promise for another; run the harness yourself to compare.

## Milestone summary

| Phase | Headline | Plane | Status |
|---|---|---|---|
| **0 — Foundation** | Standalone Cargo workspace, musl-static single binary, Snappy-only dep floor, hand-rolled Avro | OSS | ✅ **shipped (v0.1)** |
| **1 — v0.1 engine** | Dataset transform Spec → Arrow → Parquet **and** self-contained Iceberg v2, both read end-to-end by DuckDB | OSS | ✅ **shipped (v0.1)** |
| **2 — Iceberg write-path maturity** | Append-to-existing / multi-snapshot, partitioning, per-column stats, a catalog | OSS | ✅ **shipped** — append-to-existing / multi-snapshot, **additive schema evolution**, per-column statistics, **identity + hidden (transform) partitioning**, and the server-less Hadoop-style catalog; the hosted REST catalog is the commercial plane |
| **3 — Reach & format coverage** | Object-store output, optional zstd fat build, streaming json-array, a real benchmark suite | OSS | ✅ **shipped** — object-store output (Parquet _and_ Iceberg), the zstd fat build (`--features zstd`), streaming json-array, and the measured benchmark suite ([`benchmarks/`](./benchmarks/)) |
| **Hosted / commercial** | shapeshift Cloud (freemium) + commercial self-host: scheduling, connectors, CDC, MAR reporting *around* the unchanged engine | Hosted / commercial | ⏳ planned |

Phases 2 and 3 are independent of each other and can interleave — both only depend on the v0.1
core. The hosted / commercial plane is the business; it is a thin control plane *around* the engine and
can start whenever the OSS funnel justifies it, without blocking engine work.

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
phases closed the honest v0.1 limits — both are now **shipped** — and none of them added a
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
- [ ] **REST catalog + catalog-managed relocation + multi-writer append** — a commercial-edition feature: a
      hosted catalog re-anchors a moved table's paths (so *any* engine reads it with no reader flag)
      and serialises concurrent appends.

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

---

## Hosted / commercial roadmap (shapeshift Cloud + commercial license)

A hard line sits here. Everything above is the Apache-2.0 engine. Everything below is the paid open-core
plane, and it is built **around** the unchanged engine — the engine's only contract to it is the CLI
surface and the `SinkSummary` it prints ([ARCHITECTURE §14](./ARCHITECTURE.md)). Incremental/CDC,
connectors, and scheduling are **deliberately not in the OSS core**; they live here so the paid planes
add capability at the orchestration layer without ever needing to change — or cap — the engine.

- [ ] **shapeshift Cloud (hosted, freemium)** — a managed control plane to try shapeshift with zero
      setup: a hosted place to drop a source, keep a spec, and run it, with the same engine underneath.
- [ ] **Commercial license (paid self-host)** — the same managed control plane deployed
      **inside a team's own perimeter**, for teams who need the control plane but cannot use a hosted
      service. One-way dependency on the OSS crates; removing the commercial plane leaves a fully functional OSS engine.
- [ ] **Scheduling** — run a spec on a cadence / trigger. Composes shapeshift as a step under the lab's
      orchestrator (**dagron**) rather than growing a scheduler inside the engine.
- [ ] **Incremental / CDC shaping** — shape only the changed rows since the last run (watermarks,
      change feeds). This is the direct MAR counter-play at the control-plane layer; the OSS engine
      stays a stateless, whole-source shaper.
- [ ] **Connectors** — managed source/sink connectors (databases, SaaS APIs, object stores) feeding the
      engine, so users are not hand-wiring inputs. Kept out of the OSS core's dependency graph entirely.
- [ ] **Run history + MAR reporting** — retained run summaries and the "rows touched vs. what a MAR
      vendor would have billed" dashboard, built from the same honest cost arithmetic the OSS `cost`
      command already computes.

---

*shapeshift — © 2026 Nicholas Lu Chee Seng and the shapeshift contributors. Apache-2.0. Repo:
https://github.com/lucheeseng827/shapeshift*
