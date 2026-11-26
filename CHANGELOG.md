# Changelog

All notable changes to `shapeshift` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

`shapeshift` is a streaming JSON/JSONL → Parquet/Iceberg shaper. Copyright © 2026
Nicholas Lu Chee Seng and the shapeshift contributors. Licensed under Apache-2.0.

## [Unreleased]

## [0.5.0] - 2026-11-26

### Upgrading

Specs and command lines that worked in 0.4 work unchanged. Two things are new to look for:
`output.path` may now be left out when `output.catalog` names the table (without either,
validation reports it instead of the parser), and `--summary-json` gains `output.catalog`.

- `OutputSpec` gained `catalog: Option<CatalogSpec>`; a struct literal needs `catalog: None`.
  `CatalogSpec` gained `warehouse: Option<String>`.
- `shapeshift-objstore`'s catalog functions (`ObjectStoreIcebergSink::create_in_catalog`,
  `create_merge_in_catalog`, `find_run_in_catalog`, `compact_in_catalog`, `expire_in_catalog`)
  take a `&CatalogConnection` (URI, warehouse, `CatalogAuth`) where they took the URI;
  `CatalogConnection::new(uri)` is the old behaviour, and `CatalogAuth::from_env()` reads the
  credentials the CLI uses. `--summary-json` gains `output.catalog.warehouse`, and `warehouse`
  for `compact` and `expire`.
- `SinkSummary` gained `rebases` (0 for every sink but a catalog commit) and
  `earlier_attempt`; a struct literal needs them, or `..Default::default()`.
- `MetadataBuild` gained `tags: SnapshotTags` (`SnapshotTags::default()` records only the
  version), and `metadata::{MetadataInput, AppendInput}` gained `tags: &SnapshotTags`.
- A moved table's paths resolve through `Anchors` (`Anchors::of(&metadata)`):
  `LocalTableFiles::new` and `StoreFiles::open` take them where they took the `location`, and
  `LocalTableFiles::resolve` returns a `Result`; `plan_expiry` takes `&Anchors` after the table;
  `current_manifest_list_key` takes only the metadata. `IcebergInfo` gained `location` and
  `moved`.
- Every snapshot shapeshift writes now carries `shapeshift.version` in its summary (an
  ordinary summary property; readers show it and ignore it).
- The console's `POST /api/inspect` returns an Iceberg table's `current_snapshot_id` as a
  string, like every other snapshot id its API returns (it was a JSON number).
- `shapeshift-objstore` depends on `reqwest` directly: the same version and features
  `object_store` already links, so no crate is added to the object-store build.
- `DatasetSpec` gained `filter: Option<String>` (the spec's `where`); a struct literal needs
  `filter: None`. `PushOutcome` gained `Filtered`, so a `match` on it needs that arm, and
  `RunReport` gained `rows_filtered` and `filter`. `DatasetSpec::decode` reads a spec without
  validating it, for a caller that changes it first.
- **A record nested more than 512 levels deep** (objects and arrays together) is now a parse
  error: counted in `parse_errors=`, sidecarred, and skipped, as an unparseable line is. Nothing
  real nests that deep. `MAX_RECORD_DEPTH` is the limit.
- A path has at most as many segments as a record may nest (512, `MAX_PATH_SEGMENTS`). A
  column's `from:` path longer than that is refused when the spec loads, and so is a `where`
  path. A longer path could never resolve, and the drift tracker handled paths recursively, so
  a long enough one (a spec's text alone, sent to `serve`) aborted the process.
- `--summary-json` for `shape` gains `rows.filtered` (0 without a filter) and `where`
  (null without one). The text report adds `filtered=` only when the run has a `where`.
- `ManifestFileEntry` gained `partitions: Option<Vec<FieldSummary>>` (the manifest's partition
  summaries; `ManifestFileEntry::added` sets `None`), and is `PartialEq`; a struct literal needs
  the field. The manifest lists shapeshift writes carry the optional `partitions` field (null
  for its own manifests), which every Iceberg reader accepts.
- `shapeshift-iceberg` depends on `miniz_oxide` (pure Rust, with `adler2`) to inflate
  `deflate` manifests, and on `snap` directly (parquet already linked it). The lean binary
  stays free of C code.
- A write onto an existing table of format version 1 or 3 is now refused before anything is
  written (it used to fail late, or graft version 2 snapshots onto it).

### Added

- **Commits through an Iceberg REST catalog.** `shape … --catalog <uri> --table
  <namespace>.<name>` (or `output.catalog: { uri, table }` in a spec, with no `path`) writes to
  a table the catalog names, in a build with the `object_store` feature. The catalog decides
  the table's location (any store the object-store build writes to, or a local `file:` path)
  and is the commit point: the run asks it to add its snapshot only if the table is still the
  one it loaded and `main` has not moved (`assert-table-uuid`, `assert-ref-snapshot-id`). A
  table the catalog lacks is created empty, with the run's schema and partitioning, in a
  namespace that must exist; a table that already holds data needs `--append`. Added columns
  travel in the same commit (`add-schema`, asserting the schema and last field id they evolved
  from).
- **`shapeshift relocate` (S2).** A table copied or moved to a new place (any tool: `cp -r`, `aws s3
  sync`) is re-anchored there, so any Iceberg reader opens it with no flag, and a copy reads its own
  files rather than the original's. One new metadata version names a rewritten manifest list and
  manifest for each one that named a moved file (transcoded: decoded against its own schema and
  written back with every field another engine wrote kept), and a rewritten position delete file for
  each one whose rows name a moved data file (paths replaced, rows re-sorted; its size, bounds,
  column sizes and split offsets recorded anew). Data files are never touched, every snapshot is
  kept, nothing is overwritten (the commit guard; exit 75), and the superseded files are removed
  after the commit. Every file a snapshot reads is checked against one listing first: an incomplete
  copy is refused, naming what is missing, before anything is written. A move keeps the
  `table-uuid`; when a table still answers where this one was written, or that cannot be checked, it
  asks for `--moved` or `--copy` (a new `table-uuid`). `--dry-run` and `--summary-json`; local
  tables and object stores (`relocate_table`, `relocate_iceberg`, `plan_relocation`). Running it
  again does nothing. Checked by `verify_duckdb.py` after every scenario on local paths, `file://`
  and S3 (a moved table fails DuckDB's default read until relocated, then reads every snapshot;
  merges go on; moved, written to and moved again; a copy gets its own identity while the original
  reads as before) and by `verify_foreign.py` (a table PyIceberg and shapeshift wrote, relocated,
  read and appended to by PyIceberg, its partition summaries intact); a build that skips the
  delete-file rewrite is caught by deleted rows coming back. The docs no longer say a REST catalog
  re-anchors a moved table: none does.
- **`relocate --catalog <uri> --table <namespace>.<name>` (S2, R3).** After relocating, the table's
  metadata file is registered with the REST catalog (`POST …/register`), and `shape`, `compact` and
  `expire` then write through the catalog onto it, a local table at its plain path included (the
  catalog write path opens an absolute path as `file://`). A name the catalog already has is never
  overwritten: the same metadata file again is already done, so a run that relocated but failed to
  register is repeated, and so is a later version of it written through the catalog since (its
  metadata log names the file); any other table is refused, this one at its old place included. `--warehouse`, `--insecure-catalog` and the
  environment's credentials work as for `shape --catalog`; a dry run registers nothing. A
  `--catalog` URI that carries `user:password@` is refused before anything runs, a dry run
  included, since the URI is printed. A catalog's table at an `s3a://` or `s3n://` location
  (Spark's spellings, which a relocation keeps) now opens as S3 for `shape`, `compact` and
  `expire` through the catalog.
  `--summary-json` gains `metadata` and `catalog`. Checked against a stand-in catalog and, in CI,
  against Iceberg's reference REST fixture (plain path, `file://`, S3).
- **Catalog credentials, from the environment (S1).** A catalog that asks for credentials gets a
  bearer token from `SHAPESHIFT_CATALOG_TOKEN`, or OAuth2 client credentials from
  `SHAPESHIFT_CATALOG_CREDENTIAL` (`client_id:client_secret`), exchanged with the
  `client_credentials` grant at the catalog's `{uri}/v1/oauth/tokens` (or
  `SHAPESHIFT_CATALOG_OAUTH2_SERVER_URI`), scope `catalog` unless `SHAPESHIFT_CATALOG_SCOPE` says
  otherwise (`PRINCIPAL_ROLE:ALL` for Polaris). The token is fetched again once nine tenths of its
  life have passed. `--warehouse` (or `output.catalog.warehouse`) is sent as `?warehouse=` with `GET
  /v1/config`, for catalogs that hold several (Polaris, Lakekeeper); `shape`, `compact` and `expire`
  take it. Credentials never come from a spec: a `token:` there is refused with where it goes
  instead. A 401 or 403 names the variable to set, or says the one set was refused. No token or
  secret appears in an error, a log or `--summary-json`: a token endpoint's refusal is reported by
  status and a known OAuth error code only, and a catalog URI or token endpoint carrying
  `user:password@` is refused. Credentials are not sent over plain `http://` to another machine
  unless the run passes `--insecure-catalog` (a compose network; warned about, and never set by a
  variable or a spec); without credentials plain http is fine, and a catalog or token endpoint on
  this machine is never reached through an `HTTP_PROXY`. `expires_in` is read as a number or a
  numeric string, as some providers send it, and counted from before the token was asked for.
  Checked against a stand-in catalog that demands a token, issues short-lived ones and refuses
  expired ones; six mutants (a token never refreshed, a commit sent without one, a token request
  that follows a 307 and so re-sends the secret to its target, credentials sent over plain http, a
  loopback catalog reached through the proxy, `--insecure-catalog` ignored) are caught.
- **A commit's outcome is settled, never guessed.** A 409 is a refusal: exit 75, the run's
  files removed. A 5xx, a timeout or a dropped connection can hide a commit that landed, so the
  table is loaded again: this run's snapshot in it means committed; `main` moved elsewhere
  means refused (75, files removed); `main` unmoved without it means the commit may still be
  applied: the same commit is sent again (up to 3 times; every copy asserts the same head and
  adds the same snapshot, so at most one can apply), and if that never settles it, the run
  fails with an ordinary error that leaves its files in place. A copy that is refused is
  checked against the table, since the first attempt may have landed after all. A create
  whose answer was lost is settled the same way, by loading the table.
- **Appends retry instead of exiting 75.** An append the catalog refuses because another
  writer committed first is built again on the new head and committed again, up to 16 times,
  with a short random pause between: its data files are reused, and only its small manifest
  and manifest list are written again (the refused attempt's are removed). A table whose
  schema, partitioning, location or identity changed meanwhile is not rebased onto: that run
  still exits 75. Against Apache Iceberg's reference REST server, 12 writers starting
  together three times over all committed (up to 5 rebases each); without rebasing, 24 of 30
  runs of a 6-writer race exited 75.
- **Merge, compaction and expiry through the catalog.** `shape --catalog … --merge-on <key>`
  upserts into the catalog's table (creating it if the catalog has none), and `compact
  --catalog <uri> <namespace>.<name>` and `expire --catalog <uri> <namespace>.<name>` maintain
  it. A merge's delete files and a compaction's rewritten files go to the table's location like
  an append's, and their snapshot is committed with the same requirements. An expiry commits
  `remove-snapshots`, asserting `main` and every other branch and tag where it found them (it
  keeps the snapshots they point at), and deletes the files only the expired snapshots read once
  the catalog has taken it. None of the three is rebuilt when another writer commits first, since
  each was planned against the head it loaded: they exit 75, with nothing published and a merge's
  or compaction's files removed. A lost answer is settled by loading the table, as for an append;
  an expiry whose outcome stays unknown deletes nothing. A merge that cannot read a file because
  another writer moved `main` meanwhile (a compaction and an expiry remove files) exits 75 too.
- **Row filter `where`.** A spec's `where:` (or `shape --where`) names the source records the
  run takes, so one spec can take one kind of record out of a mixed stream: `where:
  record_type = 'gate'`. The expression is a subset of SQL's `WHERE` (`=`, `!=`, `<`, `<=`, `>`,
  `>=`, `in`, `not in`, `is [not] null`, `and`, `or`, `not`, single-quoted strings) over the paths
  `from:` takes, with SQL's three-valued logic. It is tested on the source record before
  shaping: a record it does not keep is counted as filtered (`filtered=` in the report,
  `rows.filtered` in `--summary-json`), never rejected, never seen by drift detection or
  inference, and a strict spec no longer aborts on another kind's row. The fields it reads are
  not reported as drift. A comparison of different kinds (`status = '500'` against a number) is
  never true, and the run counts and describes it. Mistakes are refused when the spec loads,
  naming the character and what to write instead (`==`, `<>`, `= null`, …). `infer --where`
  samples only the kept records and writes the filter into the spec. CI holds it to DuckDB's
  answer for the same text (`examples/where/verify_duckdb.py`, 300 generated expressions); it
  costs 2–5% on a run that keeps every row (BENCHMARKS).
- `shapeshift-core`: `Predicate` (parse, `test`, `explain_mismatch`), `Tested`,
  `MAX_WHERE_BYTES`, `FilterReport`, `Shaper::with_where` and `Shaper::filter_report`.
- **Writing into tables other engines made.** Manifests and manifest lists compressed with
  Avro `deflate` (Iceberg's `gzip`, the default of Iceberg's Java writers and of PyIceberg) or
  `snappy` are read; manifest lists are read by the schema they carry, so other engines'
  extra fields (`partitions`, `key_metadata`) and v1's field names read right, and the partition
  summaries are kept on every manifest carried into a new snapshot. Appending to, merging into,
  compacting and expiring a table PyIceberg made all work, with PyIceberg writing to it in
  between; DuckDB and PyIceberg read exactly the expected rows after every step
  (`examples/merge/verify_foreign.py`, in CI). Merging into or compacting a table whose data
  files are zstd (PyIceberg's and Java's default) needs `--features zstd`, and the lean build
  says so instead of "Disabled feature at compile time". `zstandard` manifests, encrypted
  manifests and format version 1 and 3 tables are refused by name.
- **Run ids: a retry never appends twice.** `shape --run-id <id>` names the run, for an
  orchestrator that may start it more than once (a retry, or another worker taking over from
  one that went quiet while it may still be running). The id is recorded in the snapshot
  (`shapeshift.run-id`), and checked three times: before anything is read (a run the table
  already holds is reported as done, with its snapshot, and nothing is written); when the sink
  finishes (an attempt that began after the run committed keeps nothing); and whenever a commit
  is refused (the snapshot that won may be an earlier attempt of the same run: then that is the
  run's commit, and this attempt's files are removed instead of rebased, refused or appended
  again). Local tables, tables in object stores and tables in a REST catalog. Against Apache
  Iceberg's reference REST server, 20 runs started as 4 concurrent attempts each, beside one
  another, landed exactly once each (60 attempts found an earlier one), late retries of every
  run read nothing, and DuckDB read each run's rows once. Parquet output refuses `--run-id`.
- Snapshot summaries record `shapeshift.version`, and `shapeshift.rebases` when a catalog append
  was rebuilt before it landed.
- **`shapeshift serve` shows what happened.** A **Runs** view lists every run the console
  started, drawn as its phases with its outcome (committed, conflict 75, failed 1) and counts,
  kept in `runs.jsonl` in the data dir; each run's id is recorded in the snapshot it commits.
  A **Tables** view reads a local table's history from its own metadata: every snapshot an
  event (append, overwrite, replace), schema changes, expiries (expired snapshots stay listed,
  marked), rows and files in and out, and the data files after each snapshot, each linked to
  the run that wrote it. New endpoints `GET /api/runs`, `GET /api/tables`, `POST /api/table`.
  The console now takes the look of the product site (its palette, square ruled panels, mono
  headings, a light theme), and lays out on a phone.
- `shapeshift-iceberg`: `table_events` (a table's history as events, from its current and
  earlier metadata), `TableEvent`, `EventCounts`, `Writer`, `SchemaChange`, `RemovedSnapshot`,
  and `read_local_history`.
- `--summary-json`: `run_id` (or null), and `table.earlier_attempt` (true when an earlier attempt
  of the run had committed `table.snapshot_id`; `version` and `metadata` are then null).
- `--summary-json`: `output.catalog` (`{uri, table}`, or null), and `table.rebases`. For a
  catalog commit `table` is present with `version` null (the catalog numbers its own metadata
  files), `metadata` the file the catalog wrote, and `rebases` how many times the append was
  built again onto a newer head (0 everywhere else). The text report says so too. `compact` and
  `expire` report `catalog` (the `--catalog` URI, or null), with `version` null for a catalog
  commit.
- `shapeshift-iceberg`: `TableIdent`, `catalog_commit`, `create_table_request`,
  `judge_reload`, `main_snapshot`, `rebase_blocked`. `PartitionDataFile` and `Stats` are
  `Clone`. For merge, compaction and expiry: `CatalogCommit` carries a `CommitEffect`
  (`AddsSnapshot` or `RemovesSnapshots`) and judges itself (`CatalogCommit::judge`), with
  `expiry_commit` and `judge_removal`. `shapeshift-objstore`:
  `ObjectStoreIcebergSink::create_in_catalog`, `ObjectStoreIcebergSink::create_merge_in_catalog`,
  `compact_in_catalog` and `expire_in_catalog`.
- Run ids in the crates: `shapeshift-iceberg` has `SnapshotTags`, `RunSnapshot`,
  `snapshot_of_run`, `validate_run_id`, `find_run_local`, `run_in`, `earlier_attempt_summary`,
  the `RUN_ID_KEY` / `VERSION_KEY` / `REBASES_KEY` names, and `IcebergSink::set_run_id`;
  `shapeshift-objstore` has `ObjectStoreIcebergSink::set_run_id`, `find_run` and
  `find_run_in_catalog`.

### Removed

- **The console's Cost page** and its `POST /api/cost`. Pricing a run is a one-off question,
  not part of shaping: `shapeshift cost` stays on the CLI, and the website has the same
  arithmetic as [a calculator](https://shapeshift.mancube.net/#cost).

### Fixed

- **A table moved, written to and moved again could no longer be merged into or compacted.** A
  write into a moved table records its files under the new directory and keeps the old
  `location`, so the table holds paths written at two places; only paths under `location` were
  re-anchored, and after a second move a merge or a compaction failed with "No such file". Every
  reader now resolves a path under any directory the table has been written at (its
  `location`, and the directory of every manifest list and logged metadata file) at the same
  key where the table is now, cut on a path-component boundary (`orders-archive/` is no longer
  taken for `orders/`); a recorded path that climbs out of the table (`..`) is refused.
  `expire` deletes the files written at every one of those places as the table's own: a copy's
  expired files are deleted in the copy (never the original's), where they used to be counted
  as outside the table and left behind. `inspect` now says when a table has moved, and a write
  into a moved table warns, since other engines' default readers still look for its files at
  the recorded place (0.5 S2, phase R1).
- **A deeply nested record crashed the run.** One line nested some tens of thousands of levels
  deep (a few thousand on a 2 MiB thread, as serve's workers have) overflowed the stack and
  aborted the whole process, instead of being reported as a bad line. Building the record,
  converting it, inferring from it and freeing it all recurse once per level. The readers now
  refuse a record past 512 levels before any of that runs, as a parse error the rest of the run
  survives. The check does not show in a run's time (BENCHMARKS, "The nesting limit").
- **Console: an Iceberg table's snapshot id read wrong in Inspect.** A 64-bit id sent as a JSON
  number lost its last digits in the browser (`…521819` shown as `…521800`), the id an engine's
  time travel needs. It is sent as a string now.
- **Console: the spec editor showed settings the run would not use.** The console's controls
  (dataset, source and output format, compression; the path is managed) win over the spec, but
  an inferred spec still said `format: parquet` after Output was set to Iceberg. The editor now
  follows the controls, after Infer and as they change.
- **An `object_store` build on a system without CA certificates** (an image on `scratch`,
  distroless-static or a slim base) failed every catalog and object-store call with only
  "builder error". It now says that no CA certificates were found, that they are needed even for
  plain http, and to install `ca-certificates` or point `SSL_CERT_FILE` at a PEM bundle. Any other
  client that cannot start shows its whole cause, not only "builder error".
- **`shapeshift serve` in a build without the console** said "unrecognized subcommand" and
  suggested `expire`. It now says the release binary and image are the lean build and how to
  build the console, and exits 2.
- A snapshot appended onto a table with no snapshot yet (`current-snapshot-id: -1`, as a
  catalog writes a table it created empty) recorded `parent-snapshot-id: -1`; it now records
  no parent.

## [0.4.0] - 2026-11-24

### Upgrading

YAML/JSON specs are unchanged. The CLI changes in two places: a run refused by the commit
guard exits 75, and a write without `--append` onto an existing table lands as its next
version. Its text reports are unchanged; scripts that parse them can move to
`--summary-json`. The rest affects Rust embedders.

- `ManifestEntry` gained `value_counts` and `null_counts` (read back so a rewritten
  manifest keeps every statistic), and `SnapshotCounts` gained the `removed_*` counts and
  `replace`; build either with `..Default::default()` where you can.
- A new snapshot no longer carries forward a manifest with no live file in it (the
  record a compaction leaves of the delete files it removed).
- **`ShapeError` gained `Conflict`** (the commit guard refused a commit; safe to retry).
  An exhaustive `match` on `ShapeError` needs an arm for it. `MetadataArtifacts` gained
  `version`, `MetadataBuild` gained `replaces` (`None` unless a fresh table replaces an
  existing one), and `ExpireCommit` gained `version`. An object-store caller that
  persists `MetadataArtifacts` itself must now create the metadata file only if absent
  (`PutMode::Create`) to keep the guard.
- **`TableFiles` now requires `Sync`** (several threads read through it at once) and
  gained `readers()`, how many files to read at once (default 1). `StoreFiles::new(store,
  base_key, location)` is now `StoreFiles::open(url, location)`: it builds an HTTP client of
  its own that only its own runtime drives. (A pooled keep-alive connection belongs to the
  runtime that opened it; reused from another runtime while that one is idle, its request
  is never sent and waits for the client's timeout.)
- `SinkSummary` gained `table_version` and `snapshot_id` (what a table sink committed;
  `None` for Parquet), and `CompactSummary` and `ExpireSummary` gained `version` (the
  table version committed). Struct literals need the new fields; the latter two derive
  `Default`.
- **CLI: a run refused by the commit guard exits with status 75** (`EX_TEMPFAIL`), not 1.
- **A write without `--append` onto an existing table** now commits a fresh table as the
  table's next version, instead of rewriting `v1.metadata.json` in place. Readers see the
  same result; nothing is overwritten. A table that 0.3 or earlier rewrote that way can
  carry older `v2…` files that do not continue from its `v1`; they are ignored on read,
  and the next write reports them with the steps to move them aside.

### Added

- **A table write can say where its rows came from, in each data file's footer.**
  `IcebergSink::set_footer` (and `PartitionedWriter::set_footer`, `IcebergDataWriter::set_footer`)
  takes a `FileFooter`: a function from a file's row count to key-value pairs, which every data
  file the write produces carries in its Parquet footer, made as that file closes. Any Parquet
  reader sees them. A merge's delete files carry none, and a compaction writes its new files
  without them: a compacted file holds rows of many writes. Nothing changes for a write that
  does not set one.
- **Compactions read ahead of their writer, and a small file is one request.** A compaction read
  every file it rewrites — footer, then each row group — one request after another. Up to 8
  readers (`SHAPESHIFT_READERS` overrides; local tables use up to 4) now each take the next file
  and fetch it ahead of the one writer, which still writes the rows in table order, so the files
  written are the same; about 16 MiB of fetched bytes waits for it (the row group it needs next is
  always fetched). A column chunk the footer's read already holds is not fetched again: that read
  takes a file's last 64 KiB, so a smaller file is one request, for merges too. **Measured**
  through `s3_proxy.py` at 20 ms per request on a 336-file partitioned table: compact 54.5 s →
  6.8 s (1,473 requests → 769), merge 11.8 s → 6.4 s (1,374 → 718); unpartitioned, compact
  3.29 s → 1.24 s. Peak RSS unchanged on those tables; up to 16 MiB more with big files (a
  19 MiB file: 31.9 → 44.1 MiB locally).
- **Object-store writes: one request per file, 8 in flight, and bulk deletes.** A table write
  uploaded every data and delete file as a multipart upload (create, part, complete) one after
  another. A file that fits in a part (8 MiB) is now one `PUT`, and the data files, delete files,
  manifests and manifest list go up 8 at a time — at most 16 MiB of file bytes in memory, what one
  multipart upload held before — then the guarded metadata file. A Parquet `--output` under 8 MiB
  is one `PUT` too. Expiry hands every key to the store's many-delete call (S3: one
  `DeleteObjects` request per thousand keys) and retries what it did not confirm one by one.
  **Measured** through `s3_proxy.py` at 20 ms per request on a 336-file partitioned table: append
  3.12 → 0.50 s, merge 18.2 → 12.7 s, expire of 771 files 58.5 → 5.2 s (773 requests → 3); peak RSS
  unchanged.
- **Merges read an object-store table several files at a time, and without `HEAD`s.** A
  merge waited on the store once per request: for every manifest, delete file and data file it
  read, one after another. The manifests, the delete files and the data files to scan are now
  each read 8 at a time (`SHAPESHIFT_READERS` overrides; local tables use up to 4), and every
  result is used in table order, so the delete files are the same whichever read finishes
  first. A file is opened at the size its manifest records, so its footer is the first request
  (a wrong recorded size costs one size lookup, then reads at the true one). **Measured**
  through a proxy adding 20 ms to every request (see BENCHMARKS): a merge into a 21-file table
  9.31 s → 1.84 s with 41 fewer requests; into a 336-file partitioned table 116.5 s → 18.0 s.
  Same rows replaced, same peak RSS. Compaction reads its manifests and delete files through
  the same pool, and expiry's planning its manifest lists and manifests (each once, however
  many snapshots carry it; the plan is the same whatever the reader count): expiring 771
  files of the 336-file table 5.11 s → 1.02 s, its plan 4.97 s → 0.87 s. CI's S3 checks now run through
  `examples/merge/s3_proxy.py`, which keeps connections open as S3 does (the S3 mock closes
  them, which once hid a compaction that stalled 30 s on a reused connection), and the
  DuckDB model-check fails any run that had to retry a request.
- **`--summary-json` on `shape`, `compact` and `expire`.** stdout carries one JSON
  document on one line in place of the text report: for `shape` the row counts, the
  rejects sidecar, the table version, snapshot and metadata file committed, and the merge
  and drift summaries; for `compact` and `expire` their counts and the version committed
  (null when there was nothing to do). A failed run prints
  `{"ok": false, "error": {"kind", "message"}}` instead, with the exit status unchanged —
  75 and `kind: "conflict"` when the commit guard refused it. Snapshot ids are strings (they
  overflow a double); every document carries `summary_version: 1`, which only a renamed,
  removed or redefined field bumps. **Verified** beyond its CLI tests: the DuckDB
  model-check now runs every write with it and fails when a summary disagrees with the table
  (the snapshot it names is not the newest, a merge's key counts differ from the model, an
  expiry names other snapshots than the ones that went — each caught with a deliberately
  wrong summary), and `race_writers.py` fails unless the versions concurrent runs report are
  1..N, each claimed by exactly one run.
- **The commit guard: concurrent writers can no longer lose each other's snapshots.**
  A commit creates `metadata/v{N+1}.metadata.json` only if no other writer has — locally a
  temp file hard-linked into place (which refuses to replace), on S3/GCS/Azure a
  conditional put (`If-None-Match: *`). Of two writers that built on the same version,
  the second gets `ShapeError::Conflict` (CLI exit 75), having removed every file it
  staged; running it again builds on the winner. An object-store commit that errors is
  checked against the stored metadata first — a 5xx or timeout can hide a put that landed —
  so a write never takes back files its own committed snapshot names; when the outcome
  stays unknown, its files are left in place. A write that fails because a compaction
  and an expiry removed the files it was reading is reported the same way. The hint is
  replaced atomically (temp + rename) after the commit and never moved backwards, and
  readers and writers follow committed versions past a stale hint — but only true
  successors, whose `metadata-log` names the version before. `append`, `--merge-on`,
  `compact` and `expire` are all guarded. **Verified** by
  [`examples/merge/race_writers.py`](./examples/merge/race_writers.py): writers merging
  into one table while a janitor compacts and expires it, retrying on exit 75 — DuckDB
  reads exactly what every writer's last round says, on local disk, `file://` and S3
  (moto honours `If-None-Match`), across seeds with up to 6 writers and 200 retries. The
  same race on a build without the guard lost 400-odd of ~580 rows whose writers had
  exited 0, and some runs crashed reading a half-written version hint. 6 crate tests (a
  two-writer race; merges racing a compaction and an expiry; a stale hint; a leftover
  file; a rewrite without `--append`), each guard rule shown to fail without it.
- **`shapeshift expire <table>`: let old snapshots go, and the files only they read.**
  Every commit keeps the table's whole history on disk, and a compaction keeps its inputs
  for the snapshots before it. `expire` drops the snapshots older than `--older-than`
  (default `5d`, Iceberg's own; `s`/`m`/`h`/`d`/`w`), except the current one, any a ref
  names, and the newest `--retain-last` (default 1) of the current history. It publishes
  the shorter history first, then deletes every manifest list, manifest, data and delete
  file only the expired snapshots referenced — never a file a retained snapshot reads as
  live, and never one whose path is outside the table's location (a copied table can name
  the original's files; those are counted and left). `--dry-run` reports and changes
  nothing. Works on object-store URLs too (`shapeshift_objstore::expire_iceberg`; the
  library entry points are `expire_table` and `plan_expiry`). Old `metadata.json` files are
  kept. **Verified**: the DuckDB model-check expires after every compaction, keeping the
  newest 1-3 snapshots, and time-travels every snapshot before it goes and every one left
  after, on local, `file://` and S3 (moto); a build that deletes files a retained snapshot
  needs fails it on a missing manifest. **Measured**: 30 merges and a compaction left a
  1M-row table at 27.1 MiB in 190 files; one expiry (0.01 s) brought it to 10.3 MiB in 38.
- **`shapeshift compact <table>`: apply every delete and pack small files, in one
  snapshot.** Every merge leaves a table more expensive to read: one more data file, one
  more delete file every reader must apply, one more manifest. Compaction rewrites
  **every data file that carries a delete** without its deleted rows, so afterwards the
  table has **no delete files**, and packs **small files** together per partition into
  files of up to `--target-file-size` (default `512MiB`, Iceberg's own; `0` only applies
  deletes). The commit is one Iceberg **`replace`** snapshot whose manifest list names a
  **single data manifest**: the files it wrote (ADDED), the ones it kept (EXISTING, with
  the sequence numbers they were first committed with), and the ones it removed
  (DELETED), plus a manifest recording the delete files it removed. Earlier snapshots are
  untouched, so time travel reads exactly what it did. Columns a file predates (additive
  schema evolution) are written as null. Nothing to do commits nothing. Reads go through
  the same seam as a merge, so `compact s3://…` (with `--features object_store`) runs the
  same code; the library entry points are `compact_table` and
  `shapeshift_objstore::compact_iceberg`.
  **Verified against DuckDB 2.0**: the model-check now compacts after every second merge
  and after the last, at a random target (pack everything, deletes only, or 16 KiB bins),
  and requires the same rows, no live delete file, merges that keep working on the
  compacted files, and time travel to every snapshot before and after — on local,
  `file://` and S3 (moto) tables. A build that kept the deleted rows fails it at the
  first compaction. **Measured**: after 30 daily merges into a 1M-row table, a DuckDB
  scan took 71 ms (31 data files, 30 delete files, 61 manifests); after a 0.7 s
  compaction that peaked at 23 MiB, 9 ms (7 ms before any merge), and the next merge
  0.15 s instead of 0.28 s.

### Fixed

- **A failed object-store upload no longer leaves the files already sent behind.** A write
  whose upload fails, or whose commit the guard refuses, removes every object it put.

## [0.3.0] - 2026-10-28

### Upgrading

Only Rust embedders are affected; YAML/JSON specs and the CLI are unchanged unless you
opt into the new flag.

- `SinkSummary` gained `merge: Option<MergeSummary>`, and `OutputSpec` gained
  `merge_on` (serde-defaulted, so specs on disk still parse). Struct literals need
  `merge: None` / `merge_on: Vec::new()`.
- `IcebergInfo` gained `total_position_deletes`, `MetadataBuild` gained `delete_files`,
  and `MetadataArtifacts` gained `delete_manifest` — an object-store caller of
  `build_metadata_artifacts` must persist it before the manifest list.
- **`current_data_files` now errors on a table that has delete files.** Listing a merged
  table's data files alone would resurrect the rows its deletes removed; use the new
  `current_scan_files`, which returns each data file with its deleted positions.

### Added

- **Upsert into an Iceberg table: `--merge-on <cols>` / `output.merge_on`.** Each row
  written replaces any row already in the table with the same key, so a source with
  updates — a daily snapshot, a change feed reshaped to JSONL — lands once per key instead
  of once per version. Within one write a repeated key keeps its **last** occurrence. A
  row with a null key is appended and matches nothing (SQL `MERGE` semantics), and the
  run warns about it. Composite keys (`--merge-on region,id`) work, the merge creates the
  table if there is none, and it composes with partitioning (a key may move between
  partitions) and with additive schema evolution.
  It is **merge-on-read**: the delta is appended as new data files, and the rows it
  supersedes are removed with Iceberg v2 **position deletes** — one small
  `(file_path, pos)` Parquet file per partition that lost rows, in a delete manifest. No
  existing data file is rewritten. Finding the superseded rows reads only the **key
  columns** of existing files, and a file whose manifest key bounds rule out every
  incoming key is never opened. A row already deleted by an earlier merge is neither
  matched nor deleted twice, so the delete counts stay exact.
  The run prints what the merge did — keys updated vs inserted, prior rows replaced,
  duplicates dropped, null-key rows, files scanned vs pruned — and embedders read it as
  `SinkSummary.merge`. `inspect` reports `live-records` once a table has deletes.
  **Verified against DuckDB 2.0's `iceberg_scan`** by
  [`examples/merge/verify_duckdb.py`](./examples/merge/verify_duckdb.py): six randomized
  scenarios (unpartitioned; identity partitions with keys moving between them; hidden
  partitions with a composite key; files of many row groups; climbing keys that prune;
  files past 64 KiB with a composite key whose columns are adjacent and delete files of
  tens of thousands of rows, read back by the next merge),
  each checked against a plain-Python model after every merge and time-travelled to every
  snapshot — with a negative control (`--append` in place of the merge fails it). Measured
  on one machine (see [BENCHMARKS.md §7](./BENCHMARKS.md#7-performance-measured)): a
  100,000-row delta with 50,000 updates merges into a 1M-row table in **0.37 s** against
  **0.14 s** to append it, peak RSS 33 MiB against 14 MiB; when the updates touch only
  the newest rows of a 10-file table, 9 of the 10 files are pruned (0.24 s).
  Single writer, like `--append`. Delete files accumulate until a compaction command
  exists — `inspect` shows how many rows they hide. The local console (`serve`) merges
  too when a submitted spec sets `output.merge_on`, and its inspect reports live rows.
- **Merge into an object-store table** (`--merge-on` with an `s3://` / `gs://` / `az://` /
  `file://` `--output`, in a build with `--features object_store`). Same semantics and the
  same code as a local merge: only the source of the bytes differs. The existing table
  is read with as few requests as the work allows: the manifests and delete files whole,
  and for each data file the merge must scan one `HEAD`, one ranged `GET` for the
  footer, and then **one ranged `GET` per row group for the key columns' chunks only** —
  never the other columns, and never a request per page. The delete files are staged
  and uploaded with the data files, before the manifest that lists them, so the
  snapshot still becomes visible only when `version-hint.text` lands last.
  **Verified over the S3 protocol**: `verify_duckdb.py --tables s3://…` runs all six
  scenarios against an S3-compatible server (moto), with DuckDB reading the tables back
  through `httpfs`, time travel included; the negative control fails there too. A merge
  into a 1M-row, 20-row-group table sent exactly 1 `HEAD` + 21 ranged `GET`s for the
  existing data file. How much of the file those GETs carry depends on how wide the table
  is: 6.0 of 8.7 MB on that narrow benchmark table (unique `int64` ids barely compress;
  its other columns are near-constant), under a quarter on a table with one wide column.
  `inspect <url>` reports the live row count.
- **`TableFiles`** (`shapeshift-iceberg`): the read seam a merge goes through —
  whole-file reads, sizes, and byte ranges of a table's existing files —
  with `LocalTableFiles` (local disk) and `StoreFiles` (`shapeshift-objstore`). The merge
  write path is `MergeWriter`, shared by both sinks; `ObjectStoreIcebergSink::create_merge`
  is the object-store constructor.
- **`current_scan_files`** (`shapeshift-iceberg`): a table's live data files, each with
  the positions its delete files remove — what a reader needs to return exactly the live
  rows.

### Changed

- **Manifests are read against the schema they carry.** The manifest reader used to
  assume shapeshift's unpartitioned byte layout; it now parses the Avro schema in each
  manifest's own header and decodes against it, so a partition tuple reads correctly for
  any partition spec. `current_data_files` therefore lists a **partitioned** table's
  committed files exactly, where it used to report a mismatch and leave the caller to scan
  the directory. A compressed manifest (another engine's `deflate`) is refused by name
  rather than misread.
- Snapshot summaries count data and delete files apart, as the Iceberg spec's keys do:
  `total-records` is every row in a live data file, `total-position-deletes` the rows
  deleted out of them, and a snapshot that deletes rows is an `overwrite`.

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

[Unreleased]: https://github.com/lucheeseng827/shapeshift/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/lucheeseng827/shapeshift/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/lucheeseng827/shapeshift/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/lucheeseng827/shapeshift/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/lucheeseng827/shapeshift/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/lucheeseng827/shapeshift/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/lucheeseng827/shapeshift/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/lucheeseng827/shapeshift/releases/tag/v0.1.0
