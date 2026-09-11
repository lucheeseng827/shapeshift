# Contributing to shapeshift

Thanks for your interest in shapeshift — the streaming JSON/JSONL → Parquet/Iceberg
shaper. The OSS engine is **Apache-2.0** and fully self-hostable: no row cap, no
telemetry, single static binary. Contributions land in this repository.

## Developer Certificate of Origin (DCO)

We use the [DCO](https://developercertificate.org/) rather than a CLA. By signing
off on your commits you certify that you wrote the patch, or otherwise have the
right to submit it under the project's Apache-2.0 license (inbound = outbound). The
patent grant comes from Apache-2.0 itself.

Sign off every commit:

```bash
git commit -s -m "your message"
```

This appends `Signed-off-by: Your Name <you@example.com>` from your `git` identity.

## Building & testing

shapeshift is its **own standalone Cargo workspace** with its own `Cargo.lock`.
Build and test it from this directory, not from the repo root:

```bash
cargo build   --workspace
cargo test    --workspace                                # 37 #[test] + 2 doctests
cargo fmt     --check                                     # or `cargo fmt --all` to fix
cargo clippy  --workspace --all-targets -- -D warnings    # warnings are errors
```

All four must pass before you open a PR — CI runs exactly these. Keep the tree
`fmt`-clean and clippy-clean; `-D warnings` is not negotiable.

Toolchain: Rust edition 2021, MSRV **1.80**. Don't reach for language or std
features newer than 1.80, and don't bump the `rust-version` casually.

### The single-binary / musl-static invariant

The release binary is a single musl-static executable. That is a hard constraint,
not a nice-to-have:

- `parquet` is pinned with `default-features = false, features = ["arrow", "snap"]`
  — Snappy only in the **default** build, deliberately **no zstd/brotli C codecs**.
  Do not add a dependency that pulls a C/C++ codec or a system library into the
  default feature set. (`--compression zstd` returning a clear error in the lean
  binary is intended behaviour, not a bug to "fix" by linking zstd by default —
  the sanctioned path is the opt-in `zstd` cargo feature, the "fat build".)
- `chrono` is used **without** the `clock` feature. The engine never reads the wall
  clock; all time is derived from the data. The only permitted wall-clock read in
  the whole tree is the Iceberg snapshot timestamp in `shapeshift-iceberg`. Don't
  add another.
- Prefer dropping a dependency over adding one (see the hand-rolled Avro encoder in
  `shapeshift-iceberg` for the ethos).

## Crate layout — where things go

Six crates, one closed workspace:

| Crate                 | Holds                                                                 |
|-----------------------|-----------------------------------------------------------------------|
| `shapeshift-core`     | The engine: the `Spec` / `DatasetSpec` model, JSON schema inference, the named `Transform` library, the `Shaper` (JSON value → Arrow `RecordBatch`), the `Sink` trait, and the MAR cost arithmetic. |
| `shapeshift-json`     | Streaming JSON **source** on the simd-json SIMD lexer: `JsonlReader`, `JsonArrayReader`. |
| `shapeshift-parquet`  | `ParquetSink` (one row group per batch) + `inspect()` read-back.       |
| `shapeshift-iceberg`  | `IcebergSink` (self-contained Iceberg v2 table, hand-rolled Avro) + `inspect()`. |
| `shapeshift-objstore` | `ObjectStoreParquetSink` + `ObjectStoreIcebergSink`: land a Parquet file / a whole Iceberg v2 table in S3/GCS/Azure (data via a bounded-RAM multipart upload, metadata via `put`, paths anchored at the destination), behind the CLI's `object_store` feature. |
| `shapeshift-cli`      | The single `shapeshift` binary.                                        |

Rule of thumb:

- **Engine / transform / inference / coercion logic → `shapeshift-core`.**
- **A new input format → `shapeshift-json`** (or a new source crate), producing
  `serde_json::Value` records.
- **A new output target → a sibling crate that implements the `Sink` trait**
  (`shapeshift-parquet` / `shapeshift-iceberg` are the reference implementations;
  `shapeshift-objstore` shows the wrap-another-sink pattern for a remote backend).
- Wiring a new format/target into the CLI's `--to` / `--format` flags → `shapeshift-cli`.

### `shapeshift-core` has no I/O-backend dependencies — keep it that way

This is the load-bearing invariant. `shapeshift-core` depends on the columnar core
(`arrow-array` / `arrow-schema`), `serde` / `serde_json` / `serde_yaml`, and
`chrono` — and nothing else. It must **not** depend on `parquet`, on `simd-json`,
on any filesystem/object-store backend, or on the network. The engine speaks Arrow
`RecordBatch` on the way out through the `Sink` trait and `serde_json::Value` on the
way in; backends and sources live in their own crates.

If a change to core would require adding an I/O or backend dependency, that is a
signal the code belongs in a sibling crate instead. `cargo tree -p shapeshift-core`
is the check.

## How to add a transform

Transforms are `value → value`, applied **before** type coercion. To add one:

1. `shapeshift-core/src/transform.rs` — add a variant to the `Transform` enum, its
   arm in `Transform::apply`, and its serde name (this is what appears as
   `transform: <name>` in a spec). Follow the existing pass-through convention: a
   transform that doesn't apply to a value returns it unchanged.
2. Add a unit test covering the applicable and the pass-through cases.
3. Document it in the spec reference (`ARCHITECTURE.md` / `docs/DESIGN.md` and the
   README transform list).

The existing set — `lowercase`, `uppercase`, `trim`, `json_encode`, `to_string`,
`dollars_to_cents`, `abs`, `empty_to_null` — is the pattern to match.

## How to add a logical type

Logical types thread from the spec through Arrow to Parquet and Iceberg. A new one
touches, in order:

1. `shapeshift-core/src/types.rs` — add a `ColumnType` variant, its `arrow_type()`
   mapping, and its `iceberg_type()` name.
2. `shapeshift-core/src/shaper.rs` — add the matching `ColBuilder` arm (the Arrow
   array builder) and a `coerce()` arm implementing the lenient/strict coercion
   policy for the new type.
3. `shapeshift-core/src/infer.rs` — teach inference to detect it (if it should be
   inferable) and to widen correctly when a field mixes shapes.
4. Add round-trip tests, and — if it reaches Iceberg — validate against DuckDB (see
   below). Update the type table in `ARCHITECTURE.md` and the README.

## Iceberg changes must be validated against DuckDB

The Iceberg writer is correctness-critical. Iceberg readers map manifest columns by
the `field-id` attributes embedded in the manifest's Avro schema — which is exactly
why shapeshift ships a hand-rolled Avro Object-Container-File encoder instead of the
general Avro crate (that crate drops the custom `field-id` attributes, producing a
table DuckDB rejects with "No default expression in FieldId Map"). Any change to
`shapeshift-iceberg` — the Avro encoder, the metadata JSON, the manifest/manifest-list,
or the type mapping — **must** be validated end-to-end:

```bash
# after `shapeshift shape --to iceberg -o <table dir> ...`
duckdb -c "SELECT count(*), <aggregate> FROM iceberg_scan('<table dir>');"
```

Confirm the row count and aggregates match the equivalent Parquet path exactly (the
reference sample reads identically both ways: 4 rows, `sum(amount) = 115.49`). A PR
that changes Iceberg output without a DuckDB `iceberg_scan` check will be sent back.
Verified against DuckDB 1.5.4.

## Expectations

- Add tests for new behaviour. Correctness-sensitive paths — path selection,
  coercion policy (lenient vs strict, row rejects), inference widening, the Avro
  encoder, and MAR cost arithmetic — need explicit tests.
- Keep docs honest: mark roadmap items clearly, don't add overclaims, and never
  quote a throughput number that wasn't measured — produce numbers with the
  reproducible harness (`benchmarks/bench.py`), state the hardware, and label
  anything unmeasured "illustrative / to be measured".
- Keep the public surface small and legible; prefer adding behind an existing seam
  (`Sink`, a source reader, a `Transform`) over widening core.
- Clear, conventional commit messages (`feat:`, `fix:`, `docs:`).

## License

By contributing you agree your work is licensed under **Apache-2.0**. See `LICENSE`
and `NOTICE`. Copyright the shapeshift authors — Nicholas Lu Chee Seng and the
shapeshift contributors.
