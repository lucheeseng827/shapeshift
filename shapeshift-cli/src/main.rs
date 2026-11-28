//! `shapeshift` — infer a dataset spec from a sample of the input, shape JSONL, JSON, CSV, TSV or
//! XML (or a pulled source) into Parquet or an Iceberg table, inspect the output, and price a
//! run against Fivetran-style MAR billing.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::Value;

use shapeshift_core::{
    estimate_mar, infer_columns, CatalogSpec, ChangeReport, ColumnSpec, Compression, CsvOptions,
    CsvTypes, DatasetSpec, DriftPolicy, DriftReport, FilterReport, MarInputs, MergeSummary,
    OutputFormat, OutputSpec, Predicate, PushOutcome, Record, RecordBatch, SchemaMode, ShapeError,
    Shaper, Sink, SinkSummary, SourceFormat, SourceSpec, XmlOptions,
};
use shapeshift_iceberg::IcebergSink;
use shapeshift_json::{
    is_rereadable, is_stdin, open_source, JsonError, OwnedTape, RecordSource, TapeJsonArrayReader,
    TapeJsonlReader, TapeRecord, TapeSource,
};
use shapeshift_parquet::ParquetSink;

mod mcp;

/// shapeshift — a streaming JSON, CSV and XML → Parquet/Iceberg shaper.
#[derive(Parser)]
#[command(name = "shapeshift", version, about, long_about = None)]
struct Cli {
    /// Increase log verbosity (-v = debug, -vv = trace).
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Infer a dataset spec from a sample of the input and print it (ready to edit).
    Infer(InferArgs),
    /// Shape JSONL, JSON, CSV, TSV or XML (or a pulled source) into Parquet or an Iceberg table.
    Shape(Box<ShapeArgs>),
    /// Inspect a Parquet file or Iceberg table (schema + row count).
    Inspect(InspectArgs),
    /// Compact an Iceberg table: apply every delete and pack small files, in one
    /// `replace` snapshot.
    Compact(CompactArgs),
    /// Expire old snapshots of an Iceberg table and delete the files only they read.
    Expire(ExpireArgs),
    /// Re-anchor an Iceberg table that was copied or moved, so every path it records names
    /// where it is now and any engine reads it with no flag.
    Relocate(RelocateArgs),
    /// Price a shape against a managed vendor's MAR (Monthly Active Rows) rate.
    Cost(CostArgs),
    /// Convert CSV, TSV, XML or a JSON array to JSON Lines: one record per line, the
    /// records shapeshift would read.
    Convert(ConvertArgs),
    /// Serve shapeshift's tools to an AI agent over the Model Context Protocol (stdio).
    /// Read-only: it looks at files under --root, or at a running console (--console).
    Mcp(McpArgs),
    /// Serve a local web console over the shaper (infer/shape/inspect/cost).
    /// Requires a build with `--features serve`.
    #[cfg(feature = "serve")]
    Serve(ServeArgs),
    /// A build without the console still knows the word, so `serve` says how to get it
    /// instead of clap suggesting `expire`. Hidden from --help: this build has no console.
    #[cfg(not(feature = "serve"))]
    #[command(hide = true, disable_help_flag = true)]
    Serve {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true, hide = true)]
        _args: Vec<String>,
    },
}

#[derive(Copy, Clone, ValueEnum)]
enum CliSourceFormat {
    Jsonl,
    JsonArray,
    Csv,
    Tsv,
    Xml,
}
impl From<CliSourceFormat> for SourceFormat {
    fn from(v: CliSourceFormat) -> Self {
        match v {
            CliSourceFormat::Jsonl => SourceFormat::Jsonl,
            CliSourceFormat::JsonArray => SourceFormat::JsonArray,
            CliSourceFormat::Csv => SourceFormat::Csv,
            CliSourceFormat::Tsv => SourceFormat::Tsv,
            CliSourceFormat::Xml => SourceFormat::Xml,
        }
    }
}

/// How to read a CSV, TSV or XML input when there is no spec to say (with a spec, its
/// `source.csv` / `source.xml` say it). The same flags on every command that reads one.
#[derive(clap::Args, Clone, Default)]
struct SourceOpts {
    /// CSV/TSV: the field separator (default `,` for csv, a tab for tsv).
    #[arg(long, value_name = "CHAR")]
    delimiter: Option<char>,
    /// CSV/TSV: the first row is data, not column names.
    #[arg(long)]
    no_header: bool,
    /// CSV/TSV: the column names, comma-separated, in order (instead of the header's).
    #[arg(long, value_delimiter = ',', value_name = "NAMES")]
    columns: Vec<String>,
    /// CSV/TSV: read every field as a string, with no number or boolean detection.
    #[arg(long)]
    all_strings: bool,
    /// XML: the element that is one record: `/Orders/Order` from the root, or `Order` at
    /// any depth.
    #[arg(long, value_name = "PATH")]
    xml_record: Option<String>,
}

impl SourceOpts {
    /// Whether any of these flags was given.
    fn given(&self) -> bool {
        self.delimiter.is_some()
            || self.no_header
            || !self.columns.is_empty()
            || self.all_strings
            || self.xml_record.is_some()
    }

    /// The source these flags describe, for `format`, checked as a spec's would be.
    fn spec(&self, format: SourceFormat, path: Option<String>) -> Result<SourceSpec> {
        let mut s = SourceSpec::of(format);
        s.path = path;
        if self.delimiter.is_some()
            || self.no_header
            || !self.columns.is_empty()
            || self.all_strings
        {
            s.csv = Some(CsvOptions {
                header: !self.no_header,
                columns: self.columns.clone(),
                delimiter: self.delimiter,
                types: if self.all_strings {
                    CsvTypes::Strings
                } else {
                    CsvTypes::Auto
                },
                ..CsvOptions::default()
            });
        }
        if let Some(record) = &self.xml_record {
            s.xml = Some(XmlOptions {
                record: record.clone(),
            });
        }
        s.validate().map_err(|e| {
            anyhow!(
                "{}",
                e.to_string()
                    .replace("source.xml.record", "--xml-record")
                    .replace("`source.xml`", "--xml-record")
                    .replace("source.csv.columns", "--columns")
                    .replace("source.csv.delimiter", "--delimiter")
                    .replace(
                        "`source.csv`",
                        "--delimiter, --no-header, --columns and --all-strings",
                    )
            )
        })?;
        Ok(s)
    }
}

#[derive(Parser)]
struct ConvertArgs {
    /// Input file, or `-` for standard input.
    #[arg(short, long)]
    input: PathBuf,
    /// Input format.
    #[arg(long, value_enum)]
    format: CliSourceFormat,
    #[command(flatten)]
    source: SourceOpts,
    /// Write here (replaced whole, only once every record is read); default standard output.
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Leave out a record that does not parse (reported on standard error, with its line)
    /// instead of stopping at the first.
    #[arg(long)]
    skip_bad: bool,
    /// Stop after this many records.
    #[arg(long)]
    limit: Option<u64>,
}

#[derive(Copy, Clone, ValueEnum)]
enum CliOutputFormat {
    Parquet,
    Iceberg,
}
impl From<CliOutputFormat> for OutputFormat {
    fn from(v: CliOutputFormat) -> Self {
        match v {
            CliOutputFormat::Parquet => OutputFormat::Parquet,
            CliOutputFormat::Iceberg => OutputFormat::Iceberg,
        }
    }
}

#[derive(Copy, Clone, ValueEnum)]
enum CliCompression {
    Snappy,
    Zstd,
    Uncompressed,
}
impl From<CliCompression> for Compression {
    fn from(v: CliCompression) -> Self {
        match v {
            CliCompression::Snappy => Compression::Snappy,
            CliCompression::Zstd => Compression::Zstd,
            CliCompression::Uncompressed => Compression::Uncompressed,
        }
    }
}

#[derive(Copy, Clone, ValueEnum)]
enum CliDriftPolicy {
    Ignore,
    Warn,
    Rescue,
    Quarantine,
    Error,
}
impl From<CliDriftPolicy> for DriftPolicy {
    fn from(v: CliDriftPolicy) -> Self {
        match v {
            CliDriftPolicy::Ignore => DriftPolicy::Ignore,
            CliDriftPolicy::Warn => DriftPolicy::Warn,
            CliDriftPolicy::Rescue => DriftPolicy::Rescue,
            CliDriftPolicy::Quarantine => DriftPolicy::Quarantine,
            CliDriftPolicy::Error => DriftPolicy::Error,
        }
    }
}

#[derive(Parser)]
struct InferArgs {
    /// Input file to sample, or `-` for standard input.
    #[arg(short, long)]
    input: PathBuf,
    /// Input format.
    #[arg(long, value_enum, default_value_t = CliSourceFormat::Jsonl)]
    format: CliSourceFormat,
    #[command(flatten)]
    source: SourceOpts,
    /// Records to sample (0 = whole file).
    #[arg(long, default_value_t = 1000)]
    sample: usize,
    /// Do not flatten nested objects into dotted columns.
    #[arg(long)]
    no_flatten: bool,
    /// Dataset name (defaults to the input file stem).
    #[arg(long)]
    dataset: Option<String>,
    /// Sample only the records this keeps, and write it into the spec as `where`: a
    /// subset of SQL's WHERE over source paths, e.g. `record_type = 'gate'` (see the
    /// spec reference).
    #[arg(long = "where", value_name = "EXPR")]
    filter: Option<String>,
    /// Write the spec here instead of printing it.
    #[arg(short, long)]
    out: Option<PathBuf>,
}

#[derive(Parser)]
struct ShapeArgs {
    /// Dataset spec (YAML/JSON). Optional — omit to infer from --input on the fly.
    #[arg(short, long)]
    spec: Option<PathBuf>,
    /// Input file, or `-` for standard input (overrides the spec's source path).
    #[arg(short, long)]
    input: Option<PathBuf>,
    /// Output path (overrides the spec). A file/dir for parquet; a table dir for iceberg.
    #[arg(short, long)]
    output: Option<String>,
    /// Output format (overrides the spec).
    #[arg(long, value_enum)]
    to: Option<CliOutputFormat>,
    /// Input format when inferring without a spec.
    #[arg(long, value_enum, default_value_t = CliSourceFormat::Jsonl)]
    format: CliSourceFormat,
    #[command(flatten)]
    source: SourceOpts,
    /// Compression (overrides the spec).
    #[arg(long, value_enum)]
    compression: Option<CliCompression>,
    /// Dataset name when inferring without a spec.
    #[arg(long)]
    dataset: Option<String>,
    /// Append a new Iceberg snapshot onto an existing table (schema must match) instead
    /// of writing a fresh single-snapshot table. Iceberg only.
    #[arg(long)]
    append: bool,
    /// Partition the Iceberg table (overrides the spec's `partition_by`). Each entry is
    /// a column (identity) or a hidden-transform expression: `bucket(N, col)`,
    /// `truncate(W, col)`, `year|month|day|hour(col)`. Comma-separate entries or repeat
    /// the flag — commas inside `(...)` do not split. Iceberg only; partition on
    /// low-cardinality transforms.
    #[arg(long)]
    partition_by: Vec<String>,
    /// Shape only the source records this keeps (overrides the spec's `where`): a subset
    /// of SQL's WHERE over source paths, e.g. `record_type = 'gate' and status >= 500`.
    /// A record it does not keep is counted as filtered, not rejected, and never reaches
    /// inference, drift detection or the table.
    #[arg(long = "where", value_name = "EXPR")]
    filter: Option<String>,
    /// Merge (upsert) on these key columns instead of appending (overrides the spec's
    /// `merge_on`): each row replaces any row already in the table with the same key, and
    /// a key repeated in the input keeps its last occurrence. Creates the table if there
    /// is none. Comma-separate a composite key or repeat the flag. Iceberg only (a local
    /// table, or an object-store URL in a build with the `object_store` feature).
    #[arg(long, value_delimiter = ',')]
    merge_on: Vec<String>,
    /// Commit to an Iceberg table through the REST catalog at this URI
    /// (`http(s)://host:port`) instead of writing a table directory: the catalog names the
    /// table's location and is its commit point (overrides the spec's `output.catalog`).
    /// Needs --table and implies --to iceberg. A table the catalog lacks is created empty
    /// in an existing namespace; one that holds data needs --append or --merge-on. An
    /// append another writer beat to the commit is rebuilt onto the new head and committed
    /// again; a merge is not (exit 75). A build with the `object_store` feature.
    #[arg(long, requires = "table", conflicts_with = "output")]
    catalog: Option<String>,
    /// The catalog's table, `<namespace>.<name>` (with --catalog).
    #[arg(long, requires = "catalog", conflicts_with = "output")]
    table: Option<String>,
    /// Which of the catalog's warehouses holds the table (overrides the spec's
    /// `output.catalog.warehouse`): a Polaris catalog's name, a Lakekeeper warehouse. The
    /// catalog's credentials come from the environment: SHAPESHIFT_CATALOG_TOKEN, or
    /// SHAPESHIFT_CATALOG_CREDENTIAL (client_id:client_secret, exchanged for a token).
    #[arg(long)]
    warehouse: Option<String>,
    /// Insecure mode: let the catalog's credentials go over plain `http://` to another
    /// machine (a compose network's `http://polaris:8181`). Without it they go only over
    /// https, or to this machine, since anyone on the path could read the token or secret.
    /// For a network trusted on its own terms; each run warns.
    #[arg(long)]
    insecure_catalog: bool,
    /// What to do when the source's shape outgrows the schema (overrides the spec's
    /// `drift.policy`): `ignore` (no detection), `warn` (default — count and report
    /// what the schema drops), `rescue` (keep the dropped values in a json catch-all
    /// column), `quarantine` (reject drifted rows so the output stays on-schema), or
    /// `error` (fail the run on the first drift).
    #[arg(long, value_enum)]
    on_drift: Option<CliDriftPolicy>,
    /// Name of the catch-all column under `--on-drift rescue` (default `_rescued`).
    #[arg(long)]
    rescue_column: Option<String>,
    /// Overlap encoding/compressing a row group with shaping the next one, on a writer
    /// thread. On by default (`auto`); never changes the output, only who waits.
    #[arg(long, value_enum, default_value_t = CliPipeline::Auto)]
    pipeline: CliPipeline,
    /// Name this run, for an orchestrator that may start it more than once (a retry, or
    /// another worker taking over from one that went quiet). The id is recorded in the
    /// snapshot (`shapeshift.run-id`). A run whose id the table already holds commits
    /// nothing and reports that snapshot, so a retry can never append the rows twice.
    /// Iceberg tables only; 1–128 letters, digits, `.`, `_`, `:` or `-`.
    #[arg(long, value_name = "ID", value_parser = parse_run_id)]
    run_id: Option<String>,
    /// Print one JSON summary to stdout instead of the text report (`summary_version: 1`;
    /// see the README). On failure stdout carries a JSON error instead; the exit status
    /// is unchanged.
    #[arg(long)]
    summary_json: bool,
    /// On exit, write the run's numbers to this file in the Prometheus text format (for
    /// node_exporter's textfile collector or any file scraper), replacing it whole. A failed
    /// run writes it too. Off unless given: nothing is kept or written otherwise.
    #[arg(long, value_name = "PATH")]
    metrics_file: Option<PathBuf>,
    /// Pull the records from this http(s) endpoint, or the rows from this
    /// `mysql://user@host/db` or `postgres://user@host/db` (`postgresql://`), instead of a
    /// file (sets the spec's `source.http.url`, `source.mysql.url` or `source.postgres.url`).
    /// An endpoint's paging, credentials and cursor come from the spec's `source.http`;
    /// without a spec it is one request whose JSON body is the records (an array, or
    /// `--records`). A database reads `--query` or `--source-table`. A build with the
    /// `http`, `mysql` or `postgres` feature.
    #[arg(long, value_name = "URL", conflicts_with = "input")]
    from: Option<String>,
    /// Where a JSON response's records are, as a path (`data.items`), with --from
    /// (overrides the spec's `source.http.records`).
    #[arg(long, value_name = "PATH")]
    records: Option<String>,
    /// The `SELECT` a database source runs (overrides `source.mysql.query` or
    /// `source.postgres.query`). With
    /// --watermark it names `:cursor`: `select … where updated_at >= :cursor order by
    /// updated_at`.
    #[arg(long, value_name = "SQL", conflicts_with = "source_table")]
    query: Option<String>,
    /// The table a database source reads, by --watermark and its primary key (overrides
    /// `source.mysql.table` or `source.postgres.table`).
    #[arg(long, value_name = "TABLE")]
    source_table: Option<String>,
    /// The column a database source's position follows (`updated_at`, an id): the run reads
    /// what is new since the table's position (overrides `source.mysql.cursor` or
    /// `source.postgres.cursor`).
    #[arg(long, value_name = "COLUMN")]
    watermark: Option<String>,
    /// A database source's first lower bound (`2020-01-01`, `0`), for when the table records
    /// no position yet (overrides `source.mysql.start` or `source.postgres.start`).
    #[arg(long, value_name = "VALUE")]
    start: Option<String>,
    /// Stop an incremental source's run after this many new records (re-read ones are
    /// not counted). Without a tiebreak the run reads on to the end of the group of
    /// records sharing the last value. The next run goes on from there.
    #[arg(long, value_name = "N")]
    max_records: Option<u64>,
    /// Stop an incremental source's run after this many pages, counted from the first
    /// with a new record.
    #[arg(long, value_name = "N")]
    max_pages: Option<u64>,
    /// Stop an incremental source's run once this long has passed since its first new
    /// record (`30s`, `10m`).
    #[arg(long, value_name = "DURATION")]
    max_wait: Option<String>,
    /// Insecure mode: let an endpoint's credentials go over plain `http://` to another
    /// machine, or connect to a database on another machine without TLS. Without it they go
    /// only over https (TLS), or to this machine.
    #[arg(long)]
    insecure_source: bool,
    /// The only hosts the source may connect to (`host` or `host:port`, comma-separated),
    /// whatever its spec says: the first URL, each redirect and page, a database's hosts as its
    /// driver reads them. For a runner that holds runs to a list of its own (a worker's
    /// `--allow-source`). Unset: no limit.
    #[arg(long, value_name = "HOSTS", value_delimiter = ',')]
    source_hosts: Vec<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
enum CliPipeline {
    /// The measured default: on.
    Auto,
    On,
    Off,
}

impl CliPipeline {
    /// On everywhere, now that the tape reader removed the reason it wasn't.
    ///
    /// This default was previously *off* on musl, and the reasoning was sound at the
    /// time: musl's malloc takes a single global lock, so a second allocating thread
    /// contended rather than scaled, and the overlap measured −9% there against +19% on
    /// glibc. What changed is not the allocator but how much the shaper asks of it —
    /// parsing onto a reusable tape stopped allocating per record, so there is no longer
    /// a lock to fight over. The same A/B now reads **+10% on musl** and +22% on glibc
    /// (BENCHMARKS.md §7).
    fn enabled(self) -> bool {
        match self {
            CliPipeline::On | CliPipeline::Auto => true,
            CliPipeline::Off => false,
        }
    }
}

#[derive(Parser)]
struct InspectArgs {
    /// A Parquet file or an Iceberg table directory.
    path: PathBuf,
}

#[derive(Parser)]
struct CompactArgs {
    /// The Iceberg table: a directory, or an `s3://` / `gs://` / `az://` / `file://` URL
    /// (in a build with `--features object_store`); with --catalog, `<namespace>.<name>`.
    table: String,
    /// Maintain the table through the REST catalog at this URI (`http(s)://host:port`);
    /// TABLE is then the catalog's `<namespace>.<name>`. The table's files are read and
    /// written where the catalog says, and the commit goes through the catalog, asserting
    /// the table and its refs are as read: another writer's commit in between refuses it
    /// (exit 75), and it is not retried. A build with the `object_store` feature.
    #[arg(long)]
    catalog: Option<String>,
    /// Which of the catalog's warehouses holds the table (with --catalog). Credentials
    /// come from the environment: SHAPESHIFT_CATALOG_TOKEN, or SHAPESHIFT_CATALOG_CREDENTIAL.
    #[arg(long, requires = "catalog")]
    warehouse: Option<String>,
    /// Insecure mode: let the catalog's credentials go over plain `http://` to another
    /// machine (with --catalog). Without it they go only over https, or to this machine.
    #[arg(long, requires = "catalog")]
    insecure_catalog: bool,
    /// Files smaller than this are packed together within their partition, into files
    /// of up to about this size (`512MiB`, `64M`, `1048576`). `0` packs nothing: only
    /// the files carrying deletes are rewritten.
    #[arg(long, default_value = "512MiB", value_parser = parse_size)]
    target_file_size: u64,
    /// Codec of the rewritten files.
    #[arg(long, value_enum, default_value_t = CliCompression::Snappy)]
    compression: CliCompression,
    /// Print one JSON summary to stdout instead of the text report (`summary_version: 1`;
    /// see the README). On failure stdout carries a JSON error instead; the exit status
    /// is unchanged.
    #[arg(long)]
    summary_json: bool,
    /// On exit, write the run's numbers to this file in the Prometheus text format (for
    /// node_exporter's textfile collector or any file scraper), replacing it whole. A failed
    /// run writes it too. Off unless given: nothing is kept or written otherwise.
    #[arg(long, value_name = "PATH")]
    metrics_file: Option<PathBuf>,
}

#[derive(Parser)]
struct ExpireArgs {
    /// The Iceberg table: a directory, or an `s3://` / `gs://` / `az://` / `file://` URL
    /// (in a build with `--features object_store`); with --catalog, `<namespace>.<name>`.
    table: String,
    /// Maintain the table through the REST catalog at this URI (`http(s)://host:port`);
    /// TABLE is then the catalog's `<namespace>.<name>`. The table's files are read and
    /// written where the catalog says, and the commit goes through the catalog, asserting
    /// the table and its refs are as read: another writer's commit in between refuses it
    /// (exit 75), and it is not retried. A build with the `object_store` feature.
    #[arg(long)]
    catalog: Option<String>,
    /// Which of the catalog's warehouses holds the table (with --catalog). Credentials
    /// come from the environment: SHAPESHIFT_CATALOG_TOKEN, or SHAPESHIFT_CATALOG_CREDENTIAL.
    #[arg(long, requires = "catalog")]
    warehouse: Option<String>,
    /// Insecure mode: let the catalog's credentials go over plain `http://` to another
    /// machine (with --catalog). Without it they go only over https, or to this machine.
    #[arg(long, requires = "catalog")]
    insecure_catalog: bool,
    /// Expire snapshots committed longer ago than this (`5d`, `12h`, `30m`, `0s`).
    /// Iceberg's own default is five days.
    #[arg(long, default_value = "5d", value_parser = parse_duration_ms)]
    older_than: i64,
    /// Always keep the newest N snapshots of the current history, however old.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    retain_last: u32,
    /// Report what would be expired and deleted, and change nothing.
    #[arg(long)]
    dry_run: bool,
    /// Print one JSON summary to stdout instead of the text report (`summary_version: 1`;
    /// see the README). On failure stdout carries a JSON error instead; the exit status
    /// is unchanged.
    #[arg(long)]
    summary_json: bool,
    /// On exit, write the run's numbers to this file in the Prometheus text format (for
    /// node_exporter's textfile collector or any file scraper), replacing it whole. A failed
    /// run writes it too. Off unless given: nothing is kept or written otherwise.
    #[arg(long, value_name = "PATH")]
    metrics_file: Option<PathBuf>,
}

#[derive(Parser)]
struct RelocateArgs {
    /// The Iceberg table where it is now, after any tool copied or moved it there: a
    /// directory, or an `s3://` / `gs://` / `az://` / `file://` URL (in a build with
    /// `--features object_store`). Its data files are not copied, read or rewritten.
    table: String,
    /// The original is being retired: the table keeps its identity (`table-uuid`). Needed
    /// only when a table still answers where this one was written, or that cannot be
    /// checked; otherwise a relocation is a move.
    #[arg(long, conflicts_with = "copy")]
    moved: bool,
    /// Both tables live on: give this one a new `table-uuid`, so the two can be told apart.
    #[arg(long)]
    copy: bool,
    /// Report what would be rewritten and removed, and change nothing.
    #[arg(long)]
    dry_run: bool,
    /// Then register the relocated table with the REST catalog at this URI
    /// (`http(s)://host:port`), as --table: the catalog records where its metadata file is.
    /// A name the catalog already has for another metadata file is refused, never
    /// overwritten; the same file again is already done. Credentials come from the
    /// environment, as for `shape --catalog`. A build with the `object_store` feature.
    #[arg(long, requires = "register_as")]
    catalog: Option<String>,
    /// The name to register the table under, `<namespace>.<name>` (with --catalog).
    #[arg(long = "table", id = "register_as", requires = "catalog")]
    register_as: Option<String>,
    /// Which of the catalog's warehouses to register it in (with --catalog).
    #[arg(long, requires = "catalog")]
    warehouse: Option<String>,
    /// Insecure mode: let the catalog's credentials go over plain `http://` to another
    /// machine (with --catalog).
    #[arg(long, requires = "catalog")]
    insecure_catalog: bool,
    /// Print one JSON summary to stdout instead of the text report (`summary_version: 1`;
    /// see the README). On failure stdout carries a JSON error instead; the exit status
    /// is unchanged.
    #[arg(long)]
    summary_json: bool,
    /// On exit, write the run's numbers to this file in the Prometheus text format (for
    /// node_exporter's textfile collector or any file scraper), replacing it whole. A failed
    /// run writes it too. Off unless given: nothing is kept or written otherwise.
    #[arg(long, value_name = "PATH")]
    metrics_file: Option<PathBuf>,
}

/// A run id (`--run-id`), checked as the snapshot will record it.
fn parse_run_id(s: &str) -> std::result::Result<String, String> {
    shapeshift_iceberg::validate_run_id(s)
        .map(|()| s.to_string())
        .map_err(|e| e.to_string())
}

/// A duration in milliseconds: a number and a unit (`s`, `m`, `h`, `d`, `w`).
fn parse_duration_ms(s: &str) -> std::result::Result<i64, String> {
    shapeshift_core::parse_duration_ms(s)
}

/// A byte size: a number with an optional binary suffix (`K`/`KiB`, `M`/`MiB`,
/// `G`/`GiB`; `B` and a trailing `B` are optional).
fn parse_size(s: &str) -> std::result::Result<u64, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("`{s}` is not a size (try 512MiB, 64M or 1048576)"))?;
    let shift = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "k" | "kb" | "kib" => 10,
        "m" | "mb" | "mib" => 20,
        "g" | "gb" | "gib" => 30,
        _ => return Err(format!("`{s}`: unknown size unit (use K, M or G)")),
    };
    n.checked_mul(1u64 << shift)
        .ok_or_else(|| format!("`{s}` is too large"))
}

#[cfg(feature = "serve")]
#[derive(Parser)]
struct ServeArgs {
    /// Address to bind the console. Keep it on loopback unless you front it with
    /// your own auth/proxy — the server has no authentication of its own.
    #[arg(long, default_value = "127.0.0.1:8087")]
    addr: String,
    /// Working directory for shaped output, reject sidecars, and temp input.
    #[arg(long, default_value = "shapeshift-data")]
    data_dir: PathBuf,
    /// Extra `Host` header value to accept (repeatable). Loopback names and the bind
    /// IP are always allowed; add the hostname you reach the console by behind a proxy.
    #[arg(long = "allow-host")]
    allow_host: Vec<String>,
    /// Disable the `Host`-header allow-list entirely (only behind a trusted proxy).
    #[arg(long)]
    allow_any_host: bool,
    /// Where push connectors take records: a second listener that serves only
    /// `POST /v1/ingest/<connector>`, each post with its connector's token
    /// (e.g. `0.0.0.0:8088`). The console stays where --addr puts it.
    #[arg(long)]
    ingest_addr: Option<String>,
    /// A REST catalog for the console's Catalog page to browse, read-only (a build with
    /// `object_store`). Its credentials come from the environment, as for `shape --catalog`.
    #[arg(long = "catalog")]
    catalog: Option<String>,
    /// Which of the catalog's warehouses to browse (with --catalog).
    #[arg(long, requires = "catalog")]
    warehouse: Option<String>,
    /// Let the catalog's credentials go over plain http to another machine (with --catalog).
    #[arg(long, requires = "catalog")]
    insecure_catalog: bool,
    /// Let datasets pull from the sources their specs name (an endpoint, a database). Off by
    /// default: a spec names a host and the environment variables whose values it sends
    /// there, and the console has no login, so start it only where nobody else can reach it.
    #[arg(long)]
    allow_sources: bool,
    /// Serve the console's metrics at `GET /metrics` on this address, in the Prometheus text
    /// format (e.g. `127.0.0.1:9187`): a listener of its own that serves nothing else and asks
    /// no login, so keep it on a network you trust. Off unless given: nothing is counted.
    #[arg(long)]
    metrics_addr: Option<String>,
}

#[derive(Parser)]
struct McpArgs {
    /// The directory the tools read under. Every path an agent gives is resolved inside it,
    /// symlinks included; anything outside is refused. Required unless --console is given: an
    /// agent's client may start the server anywhere, `/` or a home directory among them.
    #[arg(
        long,
        value_name = "DIR",
        conflicts_with = "console",
        required_unless_present = "console"
    )]
    root: Option<PathBuf>,
    /// Front a running console instead (`http://127.0.0.1:8087`): its datasets, runs, alerts,
    /// connectors and tables, read through its API. An IP address or `localhost`, never a name
    /// to look up (a lookup could hang past the server's deadline).
    #[arg(long, value_name = "URL")]
    console: Option<String>,
}

#[derive(Parser)]
struct CostArgs {
    /// Count billable rows by streaming this input, or `-` for standard input
    /// (alternative to --rows).
    #[arg(short, long)]
    input: Option<PathBuf>,
    /// Input format for --input.
    #[arg(long, value_enum, default_value_t = CliSourceFormat::Jsonl)]
    format: CliSourceFormat,
    #[command(flatten)]
    source: SourceOpts,
    /// Billable row count (alternative to --input).
    #[arg(long)]
    rows: Option<u64>,
    /// The managed vendor's effective price per million MAR (from your own plan).
    #[arg(long)]
    vendor_per_million: f64,
    /// What running shapeshift cost you (compute + storage), for a net figure.
    #[arg(long, default_value_t = 0.0)]
    self_host_cost: f64,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let level = match cli.verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
        )
        .with_writer(std::io::stderr)
        .init();

    // Under `--summary-json`, stdout carries exactly one JSON document: the summary, or
    // the error when the run fails.
    let json_command = match &cli.cmd {
        Cmd::Shape(a) if a.summary_json => Some("shape"),
        Cmd::Compact(a) if a.summary_json => Some("compact"),
        Cmd::Expire(a) if a.summary_json => Some("expire"),
        Cmd::Relocate(a) if a.summary_json => Some("relocate"),
        _ => None,
    };
    let metrics = match &cli.cmd {
        Cmd::Shape(a) => a.metrics_file.clone().map(|p| (p, "shape")),
        Cmd::Compact(a) => a.metrics_file.clone().map(|p| (p, "compact")),
        Cmd::Expire(a) => a.metrics_file.clone().map(|p| (p, "expire")),
        Cmd::Relocate(a) => a.metrics_file.clone().map(|p| (p, "relocate")),
        _ => None,
    };
    // A `shape` run's dataset, as far as its flags and spec file say before the run reads them:
    // a run that fails before its spec is settled still reports under its dataset.
    if let (Cmd::Shape(a), Some(_)) = (&cli.cmd, &metrics) {
        if let Some(d) = early_dataset(a) {
            report_dataset(&d);
        }
    }
    let started = std::time::Instant::now();
    let result = match cli.cmd {
        Cmd::Infer(a) => cmd_infer(a),
        Cmd::Shape(a) => cmd_shape(*a),
        Cmd::Inspect(a) => cmd_inspect(a),
        Cmd::Compact(a) => cmd_compact(a),
        Cmd::Expire(a) => cmd_expire(a),
        Cmd::Relocate(a) => cmd_relocate(a),
        Cmd::Cost(a) => cmd_cost(a),
        Cmd::Convert(a) => cmd_convert(a),
        Cmd::Mcp(a) => mcp::run(a.root, a.console),
        #[cfg(feature = "serve")]
        Cmd::Serve(a) => cmd_serve(a),
        #[cfg(not(feature = "serve"))]
        Cmd::Serve { .. } => {
            eprintln!("{SERVE_NOT_BUILT}");
            std::process::exit(2);
        }
    };
    if let Some((path, command)) = &metrics {
        let status = exit_code(&result);
        // The run's own outcome stands: a metrics file that cannot be written is said, not
        // made the run's failure.
        if let Err(e) = write_run_metrics(path, command, status, started.elapsed()) {
            eprintln!("warning: --metrics-file {}: {e}", path.display());
        }
    }
    let Err(e) = &result else {
        return result;
    };
    // Another writer committed first: nothing of this run was published, and running it
    // again will work. Its own exit status lets an orchestrator retry it and nothing else.
    let code = exit_code(&result);
    if let Some(command) = json_command {
        println!("{}", error_json(command, e));
        eprintln!("Error: {e:?}");
        std::process::exit(code);
    }
    if code == EXIT_CONFLICT {
        eprintln!("Error: {e}");
        std::process::exit(EXIT_CONFLICT);
    }
    result
}

/// The exit status a run's result gives: 0, [`EXIT_CONFLICT`] when another writer committed
/// first, 1 for any other failure. The metrics file and the process exit both read it.
fn exit_code(result: &Result<()>) -> i32 {
    match result {
        Ok(()) => 0,
        Err(e)
            if matches!(
                e.downcast_ref::<ShapeError>(),
                Some(ShapeError::Conflict(_))
            ) =>
        {
            EXIT_CONFLICT
        }
        Err(_) => 1,
    }
}

/// A `shape` run's dataset before its spec is parsed: `--dataset`, the spec file's `dataset`
/// key (read as plain YAML, so a spec that fails to parse whole still names it), or the input's
/// name, as the run itself would choose it.
fn early_dataset(a: &ShapeArgs) -> Option<String> {
    if let Some(d) = &a.dataset {
        return Some(d.clone());
    }
    if let Some(p) = &a.spec {
        let text = std::fs::read_to_string(p).ok()?;
        let v: serde_yaml::Value = serde_yaml::from_str(&text).ok()?;
        return v.get("dataset")?.as_str().map(str::to_string);
    }
    match (&a.input, &a.from) {
        (Some(i), _) => Some(stem_of(i)),
        (None, Some(url)) => Some(stem_of(&PathBuf::from(endpoint_name(url)))),
        _ => None,
    }
}

/// What a run leaves for `--metrics-file`: its dataset, once its spec is read, and its summary
/// document (`--summary-json`'s), once it finishes. Set only when the flag asks for it.
static RUN_REPORT: std::sync::Mutex<(Option<String>, Option<Value>)> =
    std::sync::Mutex::new((None, None));

fn report_dataset(name: &str) {
    if let Ok(mut r) = RUN_REPORT.lock() {
        r.0 = Some(name.to_string());
    }
}

fn report_summary(doc: &Value) {
    if let Ok(mut r) = RUN_REPORT.lock() {
        r.1 = Some(doc.clone());
    }
}

/// The run's numbers (the README's "Monitor it" lists them), written whole to `path`. A failed
/// run writes its status, end time and duration; its last success time is carried from the
/// file it replaces, so a run failing since Tuesday reads as such.
fn write_run_metrics(
    path: &Path,
    command: &str,
    status: i32,
    took: std::time::Duration,
) -> Result<()> {
    use shapeshift_core::metrics::{read_labels, read_sample, write_atomic, Registry};
    let (dataset, doc) = RUN_REPORT.lock().map(|r| r.clone()).unwrap_or_default();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let previous = std::fs::read_to_string(path).ok();
    // `shape` is labelled by its dataset and command, always the same two, so a failed run
    // replaces the series a successful one wrote rather than starting others. Its dataset is
    // the run's, else the one the file it replaces names, else empty. The table commands are
    // labelled by command alone.
    let dataset = match (command, dataset) {
        ("shape", Some(d)) => Some(d),
        ("shape", None) => Some(
            previous
                .as_deref()
                .and_then(|t| read_labels(t, "shapeshift_run_last_exit_status"))
                .filter(|l| l.iter().any(|(k, v)| k == "command" && v == "shape"))
                .and_then(|l| l.into_iter().find(|(k, _)| k == "dataset").map(|(_, v)| v))
                .unwrap_or_default(),
        ),
        _ => None,
    };
    let (names, values): (Vec<&str>, Vec<&str>) = match &dataset {
        Some(d) => (vec!["dataset", "command"], vec![d.as_str(), command]),
        None => (vec!["command"], vec![command]),
    };
    let pairs: Vec<(&str, &str)> = names.iter().copied().zip(values.iter().copied()).collect();
    let mut r = Registry::new();
    let gauge = |r: &mut Registry, name: &str, help: &str, v: f64| -> Result<()> {
        r.gauge(name, help, &names)?;
        r.set(name, &values, v)?;
        Ok(())
    };
    gauge(
        &mut r,
        "shapeshift_run_last_exit_status",
        "The last run's exit status: 0, 75 (another writer committed first) or 1.",
        f64::from(status),
    )?;
    gauge(
        &mut r,
        "shapeshift_run_last_end_timestamp_seconds",
        "When the last run ended, in Unix seconds.",
        now,
    )?;
    gauge(
        &mut r,
        "shapeshift_run_last_duration_seconds",
        "How long the last run took.",
        took.as_secs_f64(),
    )?;
    // A run that found its `--run-id` already committed read and wrote nothing: it succeeds,
    // but it is no news of the data, so it neither advances the success time nor counts.
    let replayed = doc
        .as_ref()
        .is_some_and(|d| d["table"]["earlier_attempt"] == Value::Bool(true));
    let last_success = if status == 0 && !replayed {
        Some(now)
    } else {
        previous.as_deref().and_then(|text| {
            read_sample(
                text,
                "shapeshift_run_last_success_timestamp_seconds",
                &pairs,
            )
        })
    };
    if let Some(t) = last_success {
        gauge(
            &mut r,
            "shapeshift_run_last_success_timestamp_seconds",
            "When a run last succeeded, in Unix seconds.",
            t,
        )?;
    }
    if let Some(doc) = doc.filter(|_| status == 0 && !replayed) {
        let n = |v: &Value| v.as_f64().unwrap_or(0.0);
        if let Some(rows) = doc.get("rows").filter(|v| v.is_object()) {
            for (key, help) in [
                ("in", "Records the last run read."),
                ("out", "Rows the last run wrote."),
                ("rejected", "Records the last run rejected."),
                ("filtered", "Records the last run's `where` left out."),
                ("deleted", "Rows the last run's change feed deleted."),
                (
                    "stale",
                    "Changes the last run found older than the table's.",
                ),
            ] {
                // A count the summary does not give is left out, not written as 0.
                if let Some(v) = rows.get(key).and_then(Value::as_f64) {
                    gauge(&mut r, &format!("shapeshift_run_last_rows_{key}"), help, v)?;
                }
            }
        }
        if let Some(b) = doc.get("bytes").filter(|v| v.is_number()) {
            gauge(
                &mut r,
                "shapeshift_run_last_bytes_written",
                "Bytes the last run wrote.",
                n(b),
            )?;
        }
        if let Some(src) = doc.get("source").filter(|v| v.is_object()) {
            gauge(
                &mut r,
                "shapeshift_source_last_records_new",
                "Records new to the table in the last pull.",
                n(&src["new"]),
            )?;
            gauge(
                &mut r,
                "shapeshift_source_last_records_replayed",
                "Records the last pull read again (its replay).",
                n(&src["replayed"]),
            )?;
            // When the cursor is a time: the lag is now minus this.
            let at = src["position"]
                .get("at")
                .or_else(|| src["position"].get("inside"));
            if let Some(t) = at
                .and_then(|v| v.as_str())
                .and_then(shapeshift_core::metrics::unix_seconds)
            {
                gauge(&mut r, "shapeshift_source_last_position_timestamp_seconds", "The source position after the last pull, when its cursor is a time, in Unix seconds.", t)?;
            }
        }
    }
    r.gauge(
        "shapeshift_build_info",
        "The shapeshift build that wrote this file.",
        &["version"],
    )?;
    r.set("shapeshift_build_info", &[env!("CARGO_PKG_VERSION")], 1.0)?;
    write_atomic(path, &r.render())?;
    Ok(())
}

/// What `serve` says in a build without the console (the release binary and image are lean).
#[cfg(not(feature = "serve"))]
const SERVE_NOT_BUILT: &str = "error: this shapeshift was built without the web console (`serve`).

The release binary and the container image are the lean build. To get the console, build from
source with the `serve` feature:

  git clone https://github.com/lucheeseng827/shapeshift && cd shapeshift
  cargo build --release -p shapeshift-cli --features serve
  ./target/release/shapeshift serve        # then open http://127.0.0.1:8087/

Every other command works in this build.";

/// Exit status of a run refused by the commit guard (`EX_TEMPFAIL`: try again).
const EXIT_CONFLICT: i32 = 75;

/// Version of the `--summary-json` documents. Fields may be added within a version;
/// renaming or removing one, or changing its meaning, bumps it.
const SUMMARY_VERSION: u32 = 1;

/// The `--summary-json` document of a failed run. `kind` names the engine error
/// (`conflict` is the one worth retrying), or `other` for anything else.
fn error_json(command: &str, e: &anyhow::Error) -> Value {
    let kind = match e.downcast_ref::<ShapeError>() {
        Some(ShapeError::Spec(_)) => "spec",
        Some(ShapeError::Schema(_)) => "schema",
        Some(ShapeError::Record(_)) => "record",
        Some(ShapeError::Drift(_)) => "drift",
        Some(ShapeError::Arrow(_)) => "arrow",
        Some(ShapeError::Sink(_)) => "sink",
        Some(ShapeError::Conflict(_)) => "conflict",
        Some(ShapeError::Source(_)) => "source",
        None => "other",
    };
    serde_json::json!({
        "summary_version": SUMMARY_VERSION,
        "command": command,
        "ok": false,
        "error": { "kind": kind, "message": format!("{e:#}") },
    })
}

#[cfg(feature = "serve")]
fn cmd_serve(a: ServeArgs) -> Result<()> {
    let cfg = shapeshift_serve::ServeConfig {
        addr: a.addr,
        data_dir: a.data_dir,
        allowed_hosts: a.allow_host,
        allow_any_host: a.allow_any_host,
        ingest_addr: a.ingest_addr,
        catalog_uri: a.catalog,
        catalog_warehouse: a.warehouse,
        insecure_catalog: a.insecure_catalog,
        allow_sources: a.allow_sources,
        metrics_addr: a.metrics_addr,
    };
    shapeshift_serve::serve(cfg).map_err(|e| anyhow!("{e}"))
}

/// Derive a dataset name from a file path stem.
fn stem_of(path: &Path) -> String {
    if is_stdin(path) {
        return "stdin".to_string();
    }
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("dataset")
        .to_string()
}

/// Read up to `n` well-formed records (0 = all) that `filter` keeps, for inference; parse
/// errors are skipped here and surfaced properly during the shaping pass.
fn sample_records(
    path: &Path,
    source: &SourceSpec,
    n: usize,
    filter: Option<&Predicate>,
) -> Result<Vec<Value>> {
    let reader =
        open_source(path, source).map_err(|e| anyhow!("opening {}: {e}", path.display()))?;
    let mut out = Vec::new();
    // Inference tolerates malformed *lines* — they are surfaced properly in the
    // shaping pass — but a genuine I/O error must not silently yield a partial
    // sample (which would produce a spec inferred from a truncated file).
    for item in reader {
        match item {
            Ok(v) => {
                if filter.is_some_and(|f| !f.test(&v).keep) {
                    continue;
                }
                out.push(v);
                if n != 0 && out.len() >= n {
                    break;
                }
            }
            Err(JsonError::Parse { .. }) => {}
            Err(JsonError::Io(e)) => return Err(anyhow!("reading {}: {e}", path.display())),
        }
    }
    Ok(out)
}

/// How many malformed records the streaming sampler will hold for replay while it hunts
/// for its sample. A stream whose head is mostly unparseable would otherwise grow the
/// replay buffer without bound; past this it stops sampling and infers from what it has.
/// Nothing is dropped — the reader carries on from where sampling stopped.
const MAX_REPLAY_REJECTS: usize = 10_000;

/// Infer the output columns from the head of a source **without losing the head**,
/// returning those columns plus a reader that yields every record.
///
/// `shape` used to sample by opening the input, then open it a second time to shape it.
/// That is fine for a file and silently truncated every non-seekable input — a pipe, a
/// FIFO, `/dev/stdin` (the recipe the docs themselves gave) — because the sampling pass
/// consumed bytes the shaping pass never saw, and the run still reported success.
///
/// So the two cases are served differently, and deliberately:
///
/// - **A rereadable source** (a regular file) is sampled and then reopened, exactly as
///   before. Nothing needs buffering, so nothing is: peak memory is the sample alone,
///   whatever `infer_sample` is set to.
/// - **A stream** cannot be reopened, so the head is chained back in front of the
///   remainder. That buffer is bounded — at most `infer_sample` records plus
///   [`MAX_REPLAY_REJECTS`] malformed ones — because bounded RAM is the whole promise.
///   `infer_sample: 0` ("sample every record") is the one thing that cannot be bounded
///   here, and is refused rather than quietly buffering the whole stream.
///
/// Either way, parse errors met while sampling are still counted and sidecarred by the
/// shaping pass, in their original order.
///
/// With a `filter`, inference sees only the records it keeps, so the schema is theirs
/// alone. A stream's records the filter drops are buffered for replay like malformed
/// ones (the shaping pass counts them), under the same bound.
fn infer_and_chain(
    path: &Path,
    pulled: &mut Option<RecordSource>,
    source: &SourceSpec,
    n: usize,
    flatten: bool,
    filter: Option<&Predicate>,
    row: Option<&str>,
) -> Result<(Vec<ColumnSpec>, usize, ShapeSource)> {
    // A change feed's columns are its rows', not the envelope's.
    let row_of = |v: &'_ Value| -> Option<Value> {
        match row {
            None => Some(v.clone()),
            Some(p) => shapeshift_core::select(v, p)
                .filter(|r| !r.is_null())
                .cloned(),
        }
    };
    // An endpoint's records are a stream that is already open.
    let mut pulled = pulled.take();
    let mut tape = |what: &str| -> Result<Box<dyn TapeSource + Send>> {
        if let Some(r) = pulled.take() {
            return Ok(Box::new(OwnedTape::new(r)));
        }
        open_tape_reader(path, source).map_err(|e| anyhow!("{what} {}: {e}", display_input(path)))
    };

    if !is_pulled(source) && is_rereadable(path) {
        // A file is sampled and then reopened, exactly as before — that is what keeps
        // `infer_sample: 0` bounded and lets inference scan past a malformed head. The
        // shaping pass that follows is the one that gets the tape.
        let samples = sample_records(path, source, n, filter)?;
        let rows: Vec<Value> = samples.iter().filter_map(row_of).collect();
        let columns = infer_columns(rows.iter(), flatten);
        return Ok((
            columns,
            samples.len(),
            ShapeSource {
                reader: tape("reading")?,
                prefix: Vec::new(),
            },
        ));
    }

    if n == 0 {
        bail!(
            "`infer_sample: 0` (sample every record) cannot read from {}: a stream is read \
             once, so sampling all of it would have to buffer the whole input. Set a \
             positive `infer_sample` (the default is 1000), or shape from a file.",
            display_input(path)
        );
    }

    // A stream is read once, so the sample is taken through the same reader that will
    // shape the rest: owned values while inference needs to *keep* what it sees, then
    // the tape for everything after.
    let mut reader = tape("opening")?;

    let mut prefix: Vec<std::result::Result<Value, JsonError>> = Vec::new();
    // Which buffered records inference may use: parsed, and kept by the filter.
    let mut sampled: Vec<bool> = Vec::new();
    // Records buffered without being sampled — malformed, or dropped by the filter — are
    // bounded together, since both are held for replay.
    let (mut good, mut malformed, mut dropped) = (0usize, 0usize, 0usize);
    while good < n && malformed + dropped < MAX_REPLAY_REJECTS {
        match reader.next_owned() {
            None => break,
            Some(Ok(v)) => {
                // A record the filter drops is replayed (and counted) but not sampled.
                let keep = !filter.is_some_and(|f| !f.test(&v).keep);
                if keep {
                    good += 1;
                } else {
                    dropped += 1;
                }
                sampled.push(keep);
                prefix.push(Ok(v));
            }
            // Inference skips a malformed line; shaping still reports it, so it stays.
            Some(Err(e @ JsonError::Parse { .. })) => {
                malformed += 1;
                sampled.push(false);
                prefix.push(Err(e));
            }
            Some(Err(JsonError::Io(e))) => {
                return Err(anyhow!("reading {}: {e}", display_input(path)))
            }
        }
    }
    if good == 0 && malformed + dropped >= MAX_REPLAY_REJECTS {
        match filter {
            Some(f) if dropped > 0 => bail!(
                "`where {f}` kept none of the first {} records from {} ({dropped} filtered, \
                 {malformed} unparseable). A stream is sampled from its head, and no more than \
                 that is held back, so there is nothing to infer a schema from: declare the \
                 columns (`schema: strict`), or shape the input from a file.",
                malformed + dropped,
                display_input(path)
            ),
            _ => bail!(
                "the first {malformed} records from {} were all unparseable, so there is \
                 nothing to infer a schema from — is the input really {}? Pass a spec with \
                 `schema: strict` to shape it anyway.",
                display_input(path),
                source.format.name()
            ),
        }
    }
    // The bound stopped the hunt before the sample filled: say so, since the schema then
    // rests on fewer records than `infer_sample` asked for.
    if good < n && malformed + dropped >= MAX_REPLAY_REJECTS {
        eprintln!(
            "note: inferred the schema from {good} record(s), not the {n} `infer_sample` asks \
             for: {} more from the head of {} were held back unsampled ({dropped} filtered, \
             {malformed} unparseable), which is as many as a stream may hold. A column that \
             appears only later is not in the schema; declare it, or shape from a file.",
            malformed + dropped,
            display_input(path)
        );
    }
    // Infer straight off the buffered records — no second copy of the sample.
    let kept = prefix
        .iter()
        .zip(&sampled)
        .filter(|(_, keep)| **keep)
        .filter_map(|(r, _)| r.as_ref().ok())
        .filter_map(row_of)
        .collect::<Vec<_>>();
    let columns = infer_columns(kept.iter(), flatten);
    Ok((columns, good, ShapeSource { reader, prefix }))
}

/// Open the tape-backed reader for `path` (or standard input) as `source` says.
///
/// The array reader validates the opening `[` eagerly, so a document that is not an
/// array fails here — at open time, naming the file — rather than at the first record.
fn open_tape_reader(path: &Path, source: &SourceSpec) -> Result<Box<dyn TapeSource + Send>> {
    // CSV, TSV and XML read into owned values, which are then taped.
    if !matches!(source.format, SourceFormat::Jsonl | SourceFormat::JsonArray) {
        let records = open_source(path, source)
            .map_err(|e| anyhow!("reading {}: {e}", display_input(path)))?;
        return Ok(Box::new(OwnedTape::new(records)));
    }
    // Both failures name the input. Strict mode calls this directly, with no wrapper of
    // its own, so a missing file or a `json-array` document that is not an array would
    // otherwise print `No such file or directory` — or the array reader's complaint —
    // with nothing to say which file it meant.
    let inner: Box<dyn std::io::BufRead + Send> = if is_stdin(path) {
        Box::new(std::io::BufReader::new(std::io::stdin()))
    } else {
        Box::new(std::io::BufReader::new(
            std::fs::File::open(path)
                .with_context(|| format!("opening {}", display_input(path)))?,
        ))
    };
    Ok(match source.format {
        SourceFormat::JsonArray => Box::new(
            TapeJsonArrayReader::new(inner)
                .map_err(|e| anyhow!("reading {}: {e}", display_input(path)))?,
        ),
        SourceFormat::Jsonl => Box::new(TapeJsonlReader::new(inner)),
        SourceFormat::Csv | SourceFormat::Tsv | SourceFormat::Xml => unreachable!("taped above"),
    })
}

/// Where a shape run's records come from: a reusable tape, whichever framing produced
/// it, plus whatever inference had to keep before the tape took over.
///
/// Every supported input is taped, so there is no second record model here to choose
/// between. If a format arrives that cannot be — one needing the whole document
/// resident, say — this grows a variant then, and the compiler will find every place
/// that has to care.
struct ShapeSource {
    reader: Box<dyn TapeSource + Send>,
    /// Owned values inference had to *keep* while sampling a stream. They replay first,
    /// so the run still sees every record exactly once, in order.
    prefix: Vec<std::result::Result<Value, JsonError>>,
}

/// The state a shape run threads through every record.
///
/// This exists so the two record models cannot drift apart: an owned `Value` and a
/// borrowed tape cursor both implement `Record`, `push` is generic over it, and every
/// other decision — rejects, drift, row-group flushing, the counters — happens here,
/// once, for both.
struct ShapeLoop<'a> {
    shaper: &'a mut Shaper,
    sink: &'a mut Box<dyn ShapeWriter>,
    rejects: &'a mut Option<std::io::BufWriter<std::fs::File>>,
    reject_path: &'a Path,
    drift_path: &'a Path,
    dataset: &'a str,
    rg: usize,
    rows_in: u64,
    rows_out: u64,
    rows_rejected: u64,
    parse_errors: u64,
    batches: u64,
}

impl ShapeLoop<'_> {
    /// Shape one record.
    fn record<'r, R: Record<'r>>(&mut self, rec: R) -> Result<()> {
        self.rows_in += 1;
        let outcome = match self.shaper.push(rec) {
            Ok(o) => o,
            Err(e) => {
                // A fail-closed policy (`schema: strict`, or `drift.policy: error`)
                // stops here. Persist what the run learned first — the report is how
                // you fix the spec it just failed against.
                if let Some(r) = self.shaper.drift_report() {
                    if let Err(w) = write_drift_report(self.drift_path, self.dataset, r) {
                        tracing::warn!("could not write the drift report: {w}");
                    }
                }
                return Err(e.into());
            }
        };
        match outcome {
            PushOutcome::Appended => self.rows_out += 1,
            // Counted by the shaper's filter report; nothing to keep or replay.
            PushOutcome::Filtered => {}
            // A change feed's `skip` op: counted by the shaper's change report.
            PushOutcome::Skipped => {}
            PushOutcome::Rejected(reason) => {
                self.rows_rejected += 1;
                tracing::debug!(reason, "row rejected");
                // A valid JSON row that failed shaping (e.g. a required column was
                // null) is preserved for replay, like a parse error — a lenient run
                // never silently drops data. Materializing the record is confined to
                // this path, which is about to write it out anyway.
                let raw = rec.to_owned_value();
                write_reject(
                    self.rejects,
                    self.reject_path,
                    &serde_json::json!({"error": reason, "raw": raw}),
                )?;
            }
        }
        if self.shaper.pending() >= self.rg {
            if let Some(batch) = self.shaper.flush().map_err(anyhow::Error::from)? {
                self.sink.write_batch(batch)?;
                self.batches += 1;
            }
        }
        Ok(())
    }

    /// A record the source could not hand over: a malformed line is counted and
    /// sidecarred, an I/O failure ends the run.
    fn source_error(&mut self, e: JsonError) -> Result<()> {
        match e {
            JsonError::Parse { line, message, raw } => {
                self.parse_errors += 1;
                write_reject(
                    self.rejects,
                    self.reject_path,
                    &serde_json::json!({"line": line, "error": message, "raw": raw}),
                )
            }
            JsonError::Io(e) => Err(anyhow!("read error: {e}")),
        }
    }

    /// `(rows_in, rows_out, rejected, parse_errors, batches)`. Consumes the loop so the
    /// borrows it holds end before the caller finalizes the run.
    fn into_totals(self) -> (u64, u64, u64, u64, u64) {
        (
            self.rows_in,
            self.rows_out,
            self.rows_rejected,
            self.parse_errors,
            self.batches,
        )
    }
}

/// How the shape loop writes row groups: straight through, or via a writer thread. Both
/// take the batch by value, so the threaded one can hand ownership across without a copy.
trait ShapeWriter {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    fn set_source_position(&mut self, kind: &str, position: &Value) -> Result<()>;
    fn finish(&mut self) -> Result<SinkSummary>;
}

/// The single-threaded path: encode and write inline, exactly as before.
struct DirectSink(Box<dyn Sink + Send>);

impl ShapeWriter for DirectSink {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.0.write_batch(&batch).map_err(anyhow::Error::from)
    }
    fn set_source_position(&mut self, kind: &str, position: &Value) -> Result<()> {
        self.0
            .set_source_position(kind, position)
            .map_err(anyhow::Error::from)
    }
    fn finish(&mut self) -> Result<SinkSummary> {
        self.0.finish().map_err(anyhow::Error::from)
    }
}

impl ShapeWriter for PipelinedSink {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        PipelinedSink::write_batch(self, batch)
    }
    fn set_source_position(&mut self, kind: &str, position: &Value) -> Result<()> {
        PipelinedSink::set_source_position(self, kind, position)
    }
    fn finish(&mut self) -> Result<SinkSummary> {
        PipelinedSink::finish(self)
    }
}

/// Row groups the writer may fall behind by. One in flight is enough to keep both
/// threads busy; more only raises peak memory, which is bounded by row-group size and
/// must stay that way.
const WRITER_QUEUE_DEPTH: usize = 1;

/// What the shape loop sends the writer. Finalizing is an **explicit** message, never
/// implied by the queue closing: a fail-closed run (`schema: strict`, `drift.policy:
/// error`) abandons the sink precisely so no reader can open a partial result, and a
/// writer that finalized on disconnect would hand back a complete file instead.
enum WriterMsg {
    Batch(RecordBatch),
    /// A pulled source's position, for the commit `Finish` makes.
    Position(String, Value),
    Finish,
}

/// Where the sink currently lives.
enum WriterState {
    /// Still on this thread. A run with a single row group — every small input, and the
    /// 4-row cold-start case — never leaves this state, so it pays nothing for a thread
    /// it would not have overlapped anything with.
    Inline(Box<dyn Sink + Send>),
    Threaded {
        tx: Option<std::sync::mpsc::SyncSender<WriterMsg>>,
        handle: Option<std::thread::JoinHandle<Result<Option<SinkSummary>>>>,
    },
    /// Collected — `finish` (or `drop`) already took the result.
    Done,
}

/// Runs a [`Sink`] on its own thread, so encoding and compressing one row group overlaps
/// with shaping the next.
///
/// **What crosses the thread boundary matters more than that anything does.** The first
/// cut of this put the *source* on the far thread, handing `serde_json::Value`s over — a
/// tree of small heap allocations per record, allocated on one core and freed on the
/// other. That measured ~2x *slower* than single-threaded on both glibc and musl:
/// cross-thread frees fight the allocator, and the consumer chases pointers into memory
/// another core just wrote. An Arrow `RecordBatch` is the opposite shape — a handful of
/// large contiguous buffers behind an `Arc` — so it is nearly free to hand over and the
/// receiver reads it sequentially.
///
/// Memory stays bounded by the row group, not the input: at most `WRITER_QUEUE_DEPTH + 1`
/// batches exist at once.
struct PipelinedSink {
    state: WriterState,
    batches: u64,
}

impl PipelinedSink {
    /// Hold the sink; the thread is spawned on the *second* row group, once there is
    /// actually something to overlap. Spawning eagerly cost ~0.35 ms on a 3 ms cold
    /// start for no gain, since a one-row-group run has nothing to run alongside.
    fn spawn(sink: Box<dyn Sink + Send>) -> Self {
        PipelinedSink {
            state: WriterState::Inline(sink),
            batches: 0,
        }
    }

    fn start_thread(sink: Box<dyn Sink + Send>) -> WriterState {
        let (tx, rx) = std::sync::mpsc::sync_channel::<WriterMsg>(WRITER_QUEUE_DEPTH);
        let handle = std::thread::spawn(move || -> Result<Option<SinkSummary>> {
            let mut sink = sink;
            for msg in &rx {
                match msg {
                    WriterMsg::Batch(b) => sink.write_batch(&b).map_err(anyhow::Error::from)?,
                    WriterMsg::Position(kind, p) => sink
                        .set_source_position(&kind, &p)
                        .map_err(anyhow::Error::from)?,
                    // Asked to commit: close the footer / write the table metadata.
                    WriterMsg::Finish => {
                        return sink.finish().map(Some).map_err(anyhow::Error::from)
                    }
                }
            }
            // The queue closed without a `Finish`: the run was abandoned. Leave the sink
            // unfinalized, and report that nothing was committed.
            Ok(None)
        });
        WriterState::Threaded {
            tx: Some(tx),
            handle: Some(handle),
        }
    }

    /// Hand over a row group. The first is written inline; from the second on it goes to
    /// the writer thread. A closed queue means the writer already failed, and its error
    /// is the real one — surface that rather than "channel closed".
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.batches += 1;
        if let WriterState::Inline(_) = self.state {
            if self.batches == 1 {
                let WriterState::Inline(sink) = &mut self.state else {
                    unreachable!("just matched Inline")
                };
                return sink.write_batch(&batch).map_err(anyhow::Error::from);
            }
            // A second row group: there is real work to overlap now.
            let WriterState::Inline(sink) = std::mem::replace(&mut self.state, WriterState::Done)
            else {
                unreachable!("just matched Inline")
            };
            self.state = Self::start_thread(sink);
        }
        let WriterState::Threaded { tx, .. } = &self.state else {
            return Err(anyhow!("the writer was already collected"));
        };
        let tx = tx.as_ref().expect("sender live until finish");
        match tx.send(WriterMsg::Batch(batch)) {
            Ok(()) => Ok(()),
            Err(_) => Err(self
                .collect(None)
                .err()
                .unwrap_or_else(|| anyhow!("the writer stopped before the run finished"))),
        }
    }

    /// Hand over a pulled source's position, ahead of the commit.
    fn set_source_position(&mut self, kind: &str, position: &Value) -> Result<()> {
        match &mut self.state {
            WriterState::Inline(sink) => sink
                .set_source_position(kind, position)
                .map_err(anyhow::Error::from),
            WriterState::Threaded { tx, .. } => {
                let tx = tx.as_ref().expect("sender live until finish");
                match tx.send(WriterMsg::Position(kind.to_string(), position.clone())) {
                    Ok(()) => Ok(()),
                    Err(_) => Err(self
                        .collect(None)
                        .err()
                        .unwrap_or_else(|| anyhow!("the writer stopped before the run finished"))),
                }
            }
            WriterState::Done => Err(anyhow!("the writer was already collected")),
        }
    }

    /// Commit: finalize the sink (asking the writer thread to, if there is one).
    fn finish(&mut self) -> Result<SinkSummary> {
        self.collect(Some(WriterMsg::Finish))?
            .ok_or_else(|| anyhow!("the writer stopped before it could finalize the output"))
    }

    /// Wind the sink up. `last` is `Some(Finish)` to commit and `None` to abandon —
    /// abandoning must never finalize, so a failed run leaves nothing a reader can open.
    fn collect(&mut self, last: Option<WriterMsg>) -> Result<Option<SinkSummary>> {
        match std::mem::replace(&mut self.state, WriterState::Done) {
            WriterState::Inline(mut sink) => match last {
                Some(_) => sink.finish().map(Some).map_err(anyhow::Error::from),
                None => Ok(None),
            },
            WriterState::Threaded { tx, handle } => {
                if let (Some(tx), Some(msg)) = (tx.as_ref(), last) {
                    let _ = tx.send(msg);
                }
                drop(tx);
                match handle {
                    Some(h) => h
                        .join()
                        .map_err(|_| anyhow!("the writer thread panicked"))?,
                    None => Err(anyhow!("the writer was already collected")),
                }
            }
            WriterState::Done => Err(anyhow!("the writer was already collected")),
        }
    }
}

impl Drop for PipelinedSink {
    fn drop(&mut self) {
        // Never a `Finish` here: reaching drop without an explicit commit means the run
        // was abandoned. Wind up without finalizing, and wait, so the output is left
        // unopenable rather than half-written by a detached thread racing the exit.
        let _ = self.collect(None);
    }
}

/// How an input path should read in a message: `-` is standard input.
fn display_input(path: &Path) -> String {
    if is_stdin(path) {
        "<stdin>".to_string()
    } else {
        path.display().to_string()
    }
}

fn cmd_infer(a: InferArgs) -> Result<()> {
    let dataset = a.dataset.clone().unwrap_or_else(|| stem_of(&a.input));
    let filter = a
        .filter
        .as_deref()
        .map(Predicate::parse)
        .transpose()
        .map_err(anyhow::Error::from)?;
    let source = a.source.spec(
        a.format.into(),
        Some(a.input.to_string_lossy().into_owned()),
    )?;
    let spec = infer_spec(&a.input, source, a.sample, filter, &dataset, !a.no_flatten)?;
    let yaml = spec.to_yaml().map_err(|e| anyhow!("{e}"))?;
    match a.out {
        Some(p) => {
            std::fs::write(&p, yaml).with_context(|| format!("writing {}", p.display()))?;
            eprintln!(
                "wrote spec for dataset `{dataset}` ({} columns) to {}",
                spec.columns.len(),
                p.display()
            );
        }
        None => print!("{yaml}"),
    }
    Ok(())
}

/// A spec inferred from a sample of `input`: what `shapeshift infer` prints, and the MCP
/// server's `shapeshift_infer` answers.
fn infer_spec(
    input: &Path,
    source: SourceSpec,
    sample: usize,
    filter: Option<Predicate>,
    dataset: &str,
    flatten: bool,
) -> Result<DatasetSpec> {
    let samples = sample_records(input, &source, sample, filter.as_ref())?;
    if samples.is_empty() {
        match &filter {
            Some(f) => bail!(
                "`where {f}` kept none of the records read from {}, so there is nothing to \
                 infer a schema from",
                input.display()
            ),
            None => bail!("no records read from {}", input.display()),
        }
    }
    let columns = infer_columns(samples.iter(), flatten);
    Ok(DatasetSpec {
        dataset: dataset.to_string(),
        filter: filter.map(|f| f.text().to_string()),
        changes: None,
        source,
        output: OutputSpec {
            format: OutputFormat::Parquet,
            path: format!("{dataset}.parquet"),
            compression: Compression::Snappy,
            partition_by: Vec::new(),
            merge_on: Vec::new(),
            catalog: None,
            apply: None,
        },
        schema: SchemaMode::Infer,
        columns,
        drift: Default::default(),
        options: Default::default(),
    })
}

/// Split one `--partition-by` value on TOP-LEVEL commas only, so `region,tier` still
/// separates but the comma inside `bucket(16, id)` does not.
fn split_partition_entries(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut cur = String::new();
    for ch in s.chars() {
        match ch {
            '(' => {
                depth += 1;
                cur.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                cur.push(ch);
            }
            ',' if depth == 0 => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

fn cmd_shape(a: ShapeArgs) -> Result<()> {
    if a.spec.is_some() && a.source.given() {
        bail!(
            "--delimiter, --no-header, --columns, --all-strings and --xml-record describe an \
             input when there is no spec; with --spec, set them in its `source.csv` or \
             `source.xml`"
        );
    }
    // 1. Assemble the effective spec (from file, or synthesized from inference).
    let mut spec = match &a.spec {
        Some(p) => {
            let text =
                std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?;
            // Validated below, once the flags have had their say: `--where` can stand in
            // for a `where` the file gets wrong, as `--output` can for its path.
            DatasetSpec::decode(&text).map_err(anyhow::Error::from)?
        }
        None => {
            let input = match (&a.input, &a.from) {
                (Some(i), _) => i.clone(),
                (None, Some(url)) => PathBuf::from(endpoint_name(url)),
                (None, None) => bail!("--input (or --from) is required when no --spec is given"),
            };
            // A catalog names the table's location, so it stands in for --output.
            let output = match (&a.output, &a.catalog) {
                (Some(o), _) => o.clone(),
                (None, Some(_)) => String::new(),
                (None, None) => {
                    bail!("--output (or --catalog) is required when no --spec is given")
                }
            };
            let default_format = if a.catalog.is_some() {
                OutputFormat::Iceberg
            } else {
                OutputFormat::Parquet
            };
            let dataset = a.dataset.clone().unwrap_or_else(|| stem_of(&input));
            DatasetSpec {
                dataset,
                filter: None,
                changes: None,
                source: match &a.from {
                    Some(url) if shapeshift_core::DbKind::of_url(url).is_some() => {
                        let db = shapeshift_core::DbSourceSpec {
                            url: url.clone(),
                            ..Default::default()
                        };
                        match shapeshift_core::DbKind::of_url(url) {
                            Some(shapeshift_core::DbKind::Postgres) => SourceSpec {
                                postgres: Some(db),
                                ..SourceSpec::default()
                            },
                            _ => SourceSpec {
                                mysql: Some(db),
                                ..SourceSpec::default()
                            },
                        }
                    }
                    // The endpoint's body format and options: --format csv/tsv/xml with
                    // their flags; otherwise a JSON body.
                    Some(_) => {
                        let mut s = a.source.spec(a.format.into(), None)?;
                        let format = match s.format {
                            SourceFormat::Csv => shapeshift_core::HttpFormat::Csv,
                            SourceFormat::Tsv => shapeshift_core::HttpFormat::Tsv,
                            SourceFormat::Xml => shapeshift_core::HttpFormat::Xml,
                            SourceFormat::Jsonl | SourceFormat::JsonArray => {
                                shapeshift_core::HttpFormat::Json
                            }
                        };
                        s.format = SourceFormat::Jsonl;
                        s.http = Some(shapeshift_core::HttpSourceSpec {
                            format,
                            ..Default::default()
                        });
                        s
                    }
                    None => a
                        .source
                        .spec(a.format.into(), Some(input.to_string_lossy().into_owned()))?,
                },
                output: OutputSpec {
                    format: a.to.map(Into::into).unwrap_or(default_format),
                    path: output,
                    compression: a.compression.map(Into::into).unwrap_or(Compression::Snappy),
                    partition_by: Vec::new(),
                    merge_on: Vec::new(),
                    catalog: None,
                    apply: None,
                },
                schema: SchemaMode::Infer,
                columns: Vec::new(),
                drift: Default::default(),
                options: Default::default(),
            }
        }
    };

    // 2. Apply CLI overrides onto a spec loaded from file.
    if let Some(input) = &a.input {
        if is_pulled(&spec.source) {
            bail!("--input names a file, and the spec reads an endpoint or a database: give one");
        }
        spec.source.path = Some(input.to_string_lossy().into_owned());
    }
    if let Some(url) = &a.from {
        spec.source.path = None;
        if let Some(kind) = shapeshift_core::DbKind::of_url(url) {
            // A database's rows have no file format: what the spec said for a file goes.
            spec.source.http = None;
            spec.source.format = SourceFormat::Jsonl;
            spec.source.csv = None;
            spec.source.xml = None;
            // The spec's database settings carry over to the URL's kind.
            let db = spec.source.mysql.take().or(spec.source.postgres.take());
            let mut db = db.unwrap_or_default();
            db.url = url.clone();
            match kind {
                shapeshift_core::DbKind::Mysql => spec.source.mysql = Some(db),
                shapeshift_core::DbKind::Postgres => spec.source.postgres = Some(db),
            }
        } else {
            spec.source.mysql = None;
            spec.source.postgres = None;
            spec.source.http.get_or_insert_with(Default::default).url = url.clone();
        }
    }
    let database_flags = [
        ("--query", a.query.is_some()),
        ("--source-table", a.source_table.is_some()),
        ("--watermark", a.watermark.is_some()),
        ("--start", a.start.is_some()),
    ];
    match spec.source.mysql.as_mut().or(spec.source.postgres.as_mut()) {
        Some(m) => {
            if let Some(q) = &a.query {
                m.query = Some(q.clone());
                m.table = None;
            }
            if let Some(t) = &a.source_table {
                m.table = Some(t.clone());
                m.query = None;
                // A table's tiebreak is its primary key.
                m.tiebreak = None;
            }
            if let Some(w) = &a.watermark {
                m.cursor = Some(w.clone());
            }
            if let Some(st) = &a.start {
                m.start = Some(st.clone());
            }
        }
        None => {
            if let Some((flag, _)) = database_flags.iter().find(|(_, set)| *set) {
                bail!("{flag} is for a database source: it goes with --from mysql://… or postgres://…");
            }
        }
    }
    if let Some(r) = &a.records {
        match spec.source.http.as_mut() {
            Some(h) => h.records = Some(r.clone()),
            None => bail!("--records is where an endpoint's records are: it goes with --from"),
        }
    }
    if let Some(output) = &a.output {
        spec.output.path = output.clone();
    }
    if let Some(fmt) = a.to {
        spec.output.format = fmt.into();
    }
    // --catalog takes the table out of the spec's directory and into the catalog: the
    // spec's path no longer applies, and the output is an Iceberg table unless --to says
    // otherwise (which validation then refuses).
    if let (Some(uri), Some(table)) = (&a.catalog, &a.table) {
        spec.output.catalog = Some(CatalogSpec {
            uri: uri.clone(),
            table: table.clone(),
            warehouse: None,
        });
        spec.output.path.clear();
        if a.to.is_none() {
            spec.output.format = OutputFormat::Iceberg;
        }
    }
    if let Some(w) = &a.warehouse {
        match spec.output.catalog.as_mut() {
            Some(c) => c.warehouse = Some(w.clone()),
            None => bail!(
                "--warehouse names a warehouse of a REST catalog; this run writes to none (give \
                 --catalog, or `output.catalog` in the spec)"
            ),
        }
    }
    if a.insecure_catalog && spec.output.catalog.is_none() {
        bail!(
            "--insecure-catalog lets a REST catalog's credentials go over plain http; this run \
             writes to no catalog (give --catalog, or `output.catalog` in the spec)"
        );
    }
    if let Some(c) = a.compression {
        spec.output.compression = c.into();
    }
    if !a.partition_by.is_empty() {
        spec.output.partition_by = a
            .partition_by
            .iter()
            .flat_map(|s| split_partition_entries(s))
            .collect();
    }
    if !a.merge_on.is_empty() {
        spec.output.merge_on = a.merge_on.iter().map(|k| k.trim().to_string()).collect();
    }
    if let Some(w) = &a.filter {
        spec.filter = Some(w.clone());
    }
    if let Some(p) = a.on_drift {
        spec.drift.policy = p.into();
    }
    if let Some(c) = &a.rescue_column {
        spec.drift.rescue_column = c.clone();
    }
    if a.metrics_file.is_some() {
        report_dataset(&spec.dataset);
    }
    spec.validate().map_err(anyhow::Error::from)?;

    // 3. Resolve the input: a path, or an endpoint (named in messages without its query).
    let database = spec.source.mysql.as_ref().or(spec.source.postgres.as_ref());
    let input = match (&spec.source.http, database, &spec.source.path) {
        (Some(h), _, _) => endpoint_name(&h.url),
        (None, Some(m), _) => endpoint_name(&m.url),
        (None, None, Some(p)) => p.clone(),
        (None, None, None) => bail!("no input path (set source.path in the spec or pass --input)"),
    };
    let input = PathBuf::from(input);
    let source = spec.source.clone();
    let bounds = shapeshift_core::Bounds {
        max_records: a.max_records,
        max_pages: a.max_pages,
        max_wait: a
            .max_wait
            .as_deref()
            .map(|w| {
                shapeshift_core::duration_ms(w)
                    .map(|ms| std::time::Duration::from_millis(ms.max(0) as u64))
            })
            .transpose()
            .map_err(anyhow::Error::from)?,
    };
    if !is_pulled(&spec.source) && (bounds.any() || a.insecure_source || !a.source_hosts.is_empty())
    {
        bail!(
            "--max-records, --max-pages, --max-wait, --insecure-source and --source-hosts apply to an endpoint \
             or a database (--from, or `source.http` / `source.mysql` / `source.postgres`)"
        );
    }

    if !spec.output.partition_by.is_empty() && spec.output.format != OutputFormat::Iceberg {
        bail!("`partition_by` applies only to Iceberg tables (--to iceberg)");
    }
    if a.append && spec.output.format != OutputFormat::Iceberg {
        bail!("--append applies only to Iceberg tables (--to iceberg)");
    }
    let merging = !spec.output.merge_on.is_empty();
    // A changelog is the table's history: each run adds to it, never replaces it.
    let append = a.append || spec.output.apply == Some(shapeshift_core::ApplyMode::Changelog);
    let run_id = a.run_id.as_deref();

    // A run that an earlier attempt already committed is done: report its snapshot and
    // read nothing. (An attempt that commits while this one runs is caught at commit.)
    if let Some(id) = run_id {
        if spec.output.format != OutputFormat::Iceberg {
            bail!(
                "--run-id applies only to Iceberg tables (--to iceberg): a Parquet file has no \
                 history to record the run in"
            );
        }
        if let Some(found) = find_run(&spec, a.insecure_catalog, id)? {
            let summary = shapeshift_iceberg::earlier_attempt_summary(&found);
            let primary = PathBuf::from(match &spec.output.catalog {
                Some(c) => &c.table,
                None => &spec.output.path,
            });
            if a.summary_json || a.metrics_file.is_some() {
                // Nothing was read, so nothing was filtered; the spec's `where` still shows.
                let filter = spec.filter.as_ref().map(|expr| FilterReport {
                    expr: expr.trim().to_string(),
                    ..FilterReport::default()
                });
                let doc = shape_json(
                    &spec,
                    &primary,
                    [0; 5],
                    &summary,
                    None,
                    None,
                    filter.as_ref(),
                    None,
                    run_id,
                );
                report_summary(&doc);
                if a.summary_json {
                    println!("{doc}");
                    return Ok(());
                }
            }
            if !a.summary_json {
                println!(
                    "run `{id}` is already in {}: an earlier attempt committed snapshot {} \
                     (sequence number {}). Nothing was read or written.",
                    primary.display(),
                    found.snapshot_id,
                    found.sequence_number
                );
            }
            return Ok(());
        }
    }

    // An endpoint: the table's recorded position, then the records from it.
    let access = SourceAccess {
        insecure: a.insecure_source,
        hosts: a.source_hosts.clone(),
    };
    let mut pulled = open_pulled(&spec, &bounds, &access, a.insecure_catalog)?;
    let mut records = pulled
        .as_mut()
        .map(|p| p.records.take().expect("just opened"));

    // 4. Build the shaper (inferring in Infer mode). The source is opened **once**:
    // inference reads the head and hands back a reader that still yields every record,
    // so a pipe or `-` shapes the same rows a regular file would.
    let (inferred, reader) = if spec.schema == SchemaMode::Infer {
        let filter = spec
            .filter
            .as_deref()
            .map(Predicate::parse)
            .transpose()
            .map_err(anyhow::Error::from)?;
        let (columns, sampled, reader) = infer_and_chain(
            &input,
            &mut records,
            &source,
            spec.options.infer_sample,
            spec.options.flatten,
            filter.as_ref(),
            spec.changes.as_ref().and_then(|c| c.row.as_deref()),
        )?;
        // Only when it kept nothing (records it kept that carry no field to infer from
        // are the same case they are without a filter), and nothing is declared either.
        if let (Some(f), 0, true) = (&filter, sampled, spec.columns.is_empty()) {
            bail!(
                "`where {f}` kept none of the records sampled from {} (`infer_sample` is {}), \
                 so there is no schema to infer: check the expression, or declare the \
                 columns",
                display_input(&input),
                spec.options.infer_sample
            );
        }
        (columns, Some(reader))
    } else {
        (Vec::new(), None)
    };
    let mut shaper = Shaper::from_spec(&spec, &inferred).map_err(anyhow::Error::from)?;
    // The table's schema: a change feed's batches carry one more column, which marks the
    // deletes for the merge sink and is never written.
    let schema = shaper.table_schema();
    let order_by: &[String] = spec.changes.as_ref().map_or(&[], |c| c.order_by.as_slice());
    // How long a delete is remembered against `order_by` (`changes.tombstone_retention`).
    let retention_ms = match &spec.changes {
        Some(c) => c.retention_ms().map_err(anyhow::Error::from)?,
        None => shapeshift_iceberg::DEFAULT_TOMBSTONE_RETENTION_MS,
    };

    // 5. Resolve the output + create the sink. A catalog table counts as remote output:
    // its sidecars are named after the dataset in the working directory.
    let output_is_url = spec.output.catalog.is_some() || looks_like_url(&spec.output.path);
    let (sink, primary): (Box<dyn Sink + Send>, PathBuf) = match spec.output.format {
        OutputFormat::Iceberg if spec.output.catalog.is_some() => {
            let cat = spec.output.catalog.as_ref().expect("checked");
            let s = objstore_catalog_sink(
                CatalogTarget {
                    spec: cat,
                    insecure: a.insecure_catalog,
                },
                schema.clone(),
                spec.output.compression,
                append,
                &spec.output.partition_by,
                MergeOn {
                    keys: &spec.output.merge_on,
                    order_by,
                    retention_ms,
                },
                run_id,
            )?;
            (s, PathBuf::from(&cat.table))
        }
        OutputFormat::Parquet if output_is_url => {
            let s =
                objstore_parquet_sink(&spec.output.path, schema.clone(), spec.output.compression)?;
            (s, PathBuf::from(&spec.output.path))
        }
        OutputFormat::Parquet => {
            let file = resolve_parquet_path(&spec.output.path, &spec.dataset);
            let s = ParquetSink::create(&file, schema.clone(), spec.output.compression)
                .map_err(anyhow::Error::from)?;
            (Box::new(s), file)
        }
        OutputFormat::Iceberg if output_is_url => {
            let s = objstore_iceberg_sink(
                &spec.output.path,
                schema.clone(),
                spec.output.compression,
                append,
                &spec.output.partition_by,
                MergeOn {
                    keys: &spec.output.merge_on,
                    order_by,
                    retention_ms,
                },
                run_id,
            )?;
            (s, PathBuf::from(&spec.output.path))
        }
        OutputFormat::Iceberg => {
            let dir = PathBuf::from(&spec.output.path);
            let mut s = if merging {
                IcebergSink::create_merge(
                    &dir,
                    schema.clone(),
                    spec.output.compression,
                    &spec.output.partition_by,
                    &spec.output.merge_on,
                )
            } else {
                IcebergSink::create(
                    &dir,
                    schema.clone(),
                    spec.output.compression,
                    append,
                    &spec.output.partition_by,
                )
            }
            .map_err(anyhow::Error::from)?;
            if !order_by.is_empty() {
                s.order_changes_by(order_by, retention_ms)
                    .map_err(anyhow::Error::from)?;
            }
            if let Some(id) = run_id {
                s.set_run_id(id).map_err(anyhow::Error::from)?;
            }
            (Box::new(s), dir)
        }
    };

    // 6. Stream + shape, sidecarring source parse errors. In Infer mode the reader is
    // the one inference already started; in Strict mode nothing has read the source yet.
    let reader = match reader {
        Some(r) => r,
        // Strict mode: nothing has read the source yet, so it goes straight onto the
        // tape with no prefix to replay.
        None => ShapeSource {
            reader: match records.take() {
                Some(r) => Box::new(OwnedTape::new(r)),
                None => open_tape_reader(&input, &source)?,
            },
            prefix: Vec::new(),
        },
    };
    // The reject sidecar is always local. For a URL output, name it after the
    // dataset in the cwd (a `<url>.rejects.jsonl` local file can't be created).
    let reject_path = if output_is_url {
        PathBuf::from(format!("{}.rejects.jsonl", spec.dataset))
    } else {
        PathBuf::from(format!("{}.rejects.jsonl", spec.output.path))
    };
    // The drift report follows the same rule, and is written only if drift is seen.
    let drift_path = if output_is_url {
        PathBuf::from(format!("{}.drift.json", spec.dataset))
    } else {
        PathBuf::from(format!("{}.drift.json", spec.output.path))
    };
    // Start clean: a stale sidecar from a previous run must not sit beside fresh
    // output (a clean rerun would otherwise leave old rejects behind).
    for stale in [&reject_path, &drift_path] {
        match std::fs::remove_file(stale) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).with_context(|| format!("removing {}", stale.display())),
        }
    }
    let mut rejects: Option<std::io::BufWriter<std::fs::File>> = None;
    let rg = spec.options.row_group_rows.max(1);

    // Encoding and compressing a row group is substantial, and it used to take turns
    // with shaping on one core. Unless asked not to, hand finished batches to a writer
    // thread so the two overlap. Row order, row-group boundaries, and the bytes written
    // are unchanged — only who is idle changes.
    let mut sink: Box<dyn ShapeWriter> = if a.pipeline.enabled() {
        Box::new(PipelinedSink::spawn(sink))
    } else {
        Box::new(DirectSink(sink))
    };

    // The per-record work lives in one place so both record models run *exactly* the
    // same logic: an owned `serde_json::Value` and a borrowed tape cursor both satisfy
    // `Record`, and `push` is generic over it.
    let mut run = ShapeLoop {
        shaper: &mut shaper,
        sink: &mut sink,
        rejects: &mut rejects,
        reject_path: &reject_path,
        drift_path: &drift_path,
        dataset: &spec.dataset,
        rg,
        rows_in: 0,
        rows_out: 0,
        rows_rejected: 0,
        parse_errors: 0,
        batches: 0,
    };

    // Records parsed onto a reusable tape, whose strings point back into the read
    // buffer: nothing is allocated per record. Whatever inference had to keep replays
    // first, as owned values — both models implement `Record`, so the loop is the same.
    let ShapeSource { mut reader, prefix } = reader;
    for item in prefix {
        match item {
            Ok(v) => run.record(&v)?,
            Err(e) => run.source_error(e)?,
        }
    }
    while let Some(item) = reader.next_tape() {
        match item {
            Ok(tape) => run.record(TapeRecord::new(tape.as_value()))?,
            Err(e) => run.source_error(e)?,
        }
    }

    let (rows_in, rows_out, rows_rejected, parse_errors, mut batches) = run.into_totals();
    if let Some(batch) = shaper.flush().map_err(anyhow::Error::from)? {
        sink.write_batch(batch)?;
        batches += 1;
    }
    if let Some(mut w) = rejects {
        // Fail closed: a lost flush must not report success.
        w.flush()
            .with_context(|| format!("flushing {}", reject_path.display()))?;
    }
    // The endpoint's position goes into the same commit as the rows it read.
    let pull = match &pulled {
        Some(p) => {
            let (position, progress) = p
                .source
                .outcome()
                .ok_or_else(|| anyhow!("the endpoint was not read to the end of its run"))?;
            if let Some(pos) = &position {
                let recorded = shapeshift_core::recorded_position(pos, p.source.as_ref());
                sink.set_source_position(p.source.kind(), &recorded)?;
            }
            Some((p.source.kind(), p.start.clone(), position, progress))
        }
        None => None,
    };
    let summary: SinkSummary = sink.finish()?;

    // 7. Report. The full drift report goes beside the output either way.
    let filter = shaper.filter_report().cloned();
    let changes = shaper.change_report().cloned();
    let drift = shaper.into_drift_report();
    let drift_report = match &drift {
        Some(report) if !report.is_clean() => {
            write_drift_report(&drift_path, &spec.dataset, report)?;
            Some(drift_path.as_path())
        }
        _ => None,
    };
    if a.summary_json || a.metrics_file.is_some() {
        let rejects = (parse_errors + rows_rejected > 0).then_some(reject_path.as_path());
        let doc = shape_json(
            &spec,
            &primary,
            [rows_in, rows_out, rows_rejected, parse_errors, batches],
            &summary,
            rejects,
            drift.as_ref().map(|r| (r, drift_report)),
            filter.as_ref(),
            changes.as_ref(),
            run_id,
        );
        let mut doc = doc;
        if let Some(p) = &pull {
            doc["source"] = pull_json(p);
        }
        report_summary(&doc);
        if a.summary_json {
            println!("{doc}");
            return Ok(());
        }
    }
    println!(
        "shaped `{}` → {} ({:?})",
        spec.dataset,
        primary.display(),
        spec.output.format
    );
    // `filtered=` appears only with a `where`, so a run without one reports as it did.
    let filtered = filter
        .as_ref()
        .map(|f| format!(" filtered={}", f.filtered))
        .unwrap_or_default();
    println!(
        "rows_in={rows_in} rows_out={rows_out}{filtered} rejected={rows_rejected} \
         parse_errors={parse_errors} row_groups={batches} bytes={}",
        summary.bytes
    );
    if let Some(f) = &filter {
        print_filter(f, rows_in);
    }
    if let Some(p) = &pull {
        print_pull(p);
    }
    if let Some(c) = &changes {
        println!(
            "changes: {} insert(s), {} update(s), {} delete(s){}",
            c.inserts,
            c.updates,
            c.deletes,
            if c.skipped > 0 {
                format!(", {} skipped", c.skipped)
            } else {
                String::new()
            }
        );
    }
    if parse_errors > 0 || rows_rejected > 0 {
        println!(
            "malformed / rejected rows → {} ({} parse, {} shaped-out)",
            reject_path.display(),
            parse_errors,
            rows_rejected
        );
    }
    if spec.output.format == OutputFormat::Iceberg {
        // The sink reports the metadata file it actually wrote (v1 fresh, vN on append).
        if let Some(meta) = summary
            .files
            .iter()
            .find(|p| p.to_string_lossy().ends_with(".metadata.json"))
        {
            println!("iceberg table metadata → {}", meta.display());
        }
        if summary.earlier_attempt {
            println!(
                "an earlier attempt of run `{}` had already committed snapshot {}: this one \
                 committed nothing, and its files were removed",
                run_id.unwrap_or_default(),
                summary.snapshot_id.unwrap_or_default()
            );
        } else if summary.rebases > 0 {
            println!(
                "another writer committed first: rebuilt onto the new head {} time(s), then \
                 committed",
                summary.rebases
            );
        }
    }
    if let Some(m) = &summary.merge {
        print_merge(m);
    }
    if let (Some(report), Some(path)) = (&drift, drift_report) {
        print_drift(report);
        println!("full drift report → {}", path.display());
    }
    Ok(())
}

/// The `--summary-json` document of a shape run. Snapshot ids are strings: they are
/// 64-bit, past what a JSON number holds exactly in most parsers.
#[allow(clippy::too_many_arguments)]
fn shape_json(
    spec: &DatasetSpec,
    primary: &Path,
    [rows_in, rows_out, rejected, parse_errors, row_groups]: [u64; 5],
    summary: &SinkSummary,
    rejects: Option<&Path>,
    drift: Option<(&DriftReport, Option<&Path>)>,
    filter: Option<&FilterReport>,
    changes: Option<&ChangeReport>,
    run_id: Option<&str>,
) -> Value {
    // A catalog commit has a snapshot but no `vN` of its own (the catalog numbers its
    // metadata files), so `version` is null there.
    let table = summary.snapshot_id.map(|_| {
        let metadata = summary
            .files
            .iter()
            .find(|p| p.to_string_lossy().ends_with(".metadata.json"));
        serde_json::json!({
            "version": summary.table_version,
            "snapshot_id": summary.snapshot_id.map(|id| id.to_string()),
            "rebases": summary.rebases,
            "metadata": metadata.map(|p| p.display().to_string()),
            "earlier_attempt": summary.earlier_attempt,
        })
    });
    serde_json::json!({
        "summary_version": SUMMARY_VERSION,
        "command": "shape",
        "ok": true,
        "dataset": spec.dataset,
        "run_id": run_id,
        "output": {
            "format": spec.output.format,
            "path": primary.display().to_string(),
            "compression": spec.output.compression,
            "catalog": spec.output.catalog.as_ref().map(|c| serde_json::json!({
                "uri": c.uri,
                "table": c.table,
                "warehouse": c.warehouse,
            })),
        },
        "rows": {
            "in": rows_in,
            "out": rows_out,
            "rejected": rejected,
            "filtered": filter.map_or(0, |f| f.filtered),
            "parse_errors": parse_errors,
            "skipped": changes.map_or(0, |c| c.skipped),
            "deleted": summary.merge.as_ref().map_or(0, |m| m.rows_deleted),
            "stale": summary.merge.as_ref().map_or(0, |m| m.changes_stale),
        },
        // Present when the spec reads a change feed (`changes:`).
        "changes": changes.map(|c| serde_json::json!({
            "inserts": c.inserts,
            "updates": c.updates,
            "deletes": c.deletes,
            "skipped": c.skipped,
        })),
        // Present when the run had a `where`.
        "where": filter.map(|f| serde_json::json!({
            "expr": f.expr,
            "filtered": f.filtered,
            "type_mismatches": f.type_mismatches,
            "first_mismatch": f.first_mismatch,
        })),
        "row_groups": row_groups,
        "bytes": summary.bytes,
        "rejects": rejects.map(|p| serde_json::json!({
            "path": p.display().to_string(),
            "rows": rejected + parse_errors,
        })),
        "table": table,
        "merge": summary.merge.as_ref().map(merge_json),
        // Present whenever drift was checked (any policy but `ignore`), so a clean run
        // reads differently from an unchecked one.
        "drift": drift.map(|(r, report)| serde_json::json!({
            "policy": r.policy,
            "rows_scanned": r.rows_scanned,
            "rows_with_drift": r.rows_with_drift,
            "rows_quarantined": r.rows_quarantined,
            "rows_rescued": r.rows_rescued,
            "new_fields": r.new_fields.keys().collect::<Vec<_>>(),
            "type_mismatches": r.type_mismatches.keys().collect::<Vec<_>>(),
            "truncated": r.truncated,
            "report": report.map(|p| p.display().to_string()),
        })),
    })
}

/// What the `where` did, when it needs saying: a filter that kept nothing, and
/// comparisons between values of different kinds, which are never true.
fn print_filter(f: &FilterReport, rows_in: u64) {
    if rows_in > 0 && f.filtered == rows_in {
        println!(
            "note: `where {}` kept none of the {rows_in} record(s)",
            f.expr
        );
    }
    if f.type_mismatches > 0 {
        println!(
            "note: in {} record(s) `where` compared values of different kinds, which is never \
             true; the first: {}",
            f.type_mismatches,
            f.first_mismatch.as_deref().unwrap_or("?")
        );
    }
}

fn merge_json(m: &MergeSummary) -> Value {
    serde_json::json!({
        "key_columns": m.key_columns,
        "distinct_keys": m.distinct_keys,
        "keys_updated": m.keys_updated,
        "keys_inserted": m.keys_inserted,
        "prior_rows_replaced": m.prior_rows_replaced,
        "duplicates_in_input": m.duplicates_in_input,
        "null_key_rows": m.null_key_rows,
        "data_files_scanned": m.data_files_scanned,
        "data_files_pruned": m.data_files_pruned,
        "delete_files": m.delete_files,
        "position_deletes": m.position_deletes,
        "keys_deleted": m.keys_deleted,
        "rows_deleted": m.rows_deleted,
        "changes_stale": m.changes_stale,
        "order_by": m.order_by,
        "tombstones": {
            "written": m.tombstones_written,
            "retired": m.tombstones_retired,
            "trimmed": m.tombstones_trimmed,
            "live": m.tombstones_live,
        },
    })
}

/// Summarize a merge: what the key matched, what it replaced, and what it cost to find.
fn print_merge(m: &MergeSummary) {
    println!(
        "merged on ({}): {} key(s) — {} updated, {} inserted; {} prior row(s) replaced",
        m.key_columns.join(", "),
        m.distinct_keys,
        m.keys_updated,
        m.keys_inserted,
        m.prior_rows_replaced
    );
    if m.keys_deleted > 0 || m.rows_deleted > 0 {
        println!(
            "  {} key(s) deleted; {} live row(s) removed",
            m.keys_deleted, m.rows_deleted
        );
    }
    if m.changes_stale > 0 {
        println!(
            "  {} change(s) were stale (not newer than the row by {}), and the table kept its row",
            m.changes_stale,
            m.order_by.join(", ")
        );
    }
    if !m.order_by.is_empty() {
        println!(
            "  tombstones: {} written, {} retired, {} trimmed past the retention; {} remembered",
            m.tombstones_written, m.tombstones_retired, m.tombstones_trimmed, m.tombstones_live
        );
    }
    if m.duplicates_in_input > 0 {
        println!(
            "  {} earlier occurrence(s) of a repeated key in the input were dropped (the last one wins)",
            m.duplicates_in_input
        );
    }
    if m.null_key_rows > 0 {
        println!(
            "  warning: {} row(s) have a null key — appended, but they match nothing and can \
             never be updated by a later merge",
            m.null_key_rows
        );
    }
    println!(
        "  scanned {} data file(s), pruned {} by key bounds; {} position delete(s) in {} delete file(s)",
        m.data_files_scanned, m.data_files_pruned, m.position_deletes, m.delete_files
    );
}

/// Lines shown for individual drifted paths before the rest is left to the sidecar.
const DRIFT_LINES: usize = 5;

/// Summarize drift on the terminal: what was lost, how often, from when — and the
/// column declaration that would keep it.
fn print_drift(r: &DriftReport) {
    let mut what = Vec::new();
    if !r.new_fields.is_empty() {
        what.push(format!("{} undeclared field(s)", r.new_fields.len()));
    }
    if !r.type_mismatches.is_empty() {
        what.push(format!(
            "{} column(s) no longer coercing",
            r.type_mismatches.len()
        ));
    }
    println!(
        "schema drift (policy: {}): {} in {} of {} rows",
        r.policy,
        what.join(", "),
        r.rows_with_drift,
        r.rows_scanned
    );
    for (path, f) in r.new_fields.iter().take(DRIFT_LINES) {
        println!(
            "  new field `{path}` — {} row(s), first at record {}, suggest type {}{}",
            f.count,
            f.first_record,
            f.suggested_type,
            example_of(&f.examples)
        );
    }
    for (col, m) in r.type_mismatches.iter().take(DRIFT_LINES) {
        println!(
            "  column `{col}` ({}) — {} value(s) written as null, first at record {}, \
             now looks like {}{}",
            m.declared_type,
            m.count,
            m.first_record,
            m.suggested_type,
            example_of(&m.examples)
        );
    }
    let shown = r.new_fields.len().min(DRIFT_LINES) + r.type_mismatches.len().min(DRIFT_LINES);
    let total = r.new_fields.len() + r.type_mismatches.len();
    if total > shown {
        println!("  … and {} more (in the report)", total - shown);
    }
    if r.truncated {
        println!(
            "  (only the first {} distinct paths were tracked)",
            shapeshift_core::MAX_TRACKED_PATHS
        );
    }
    match r.policy {
        DriftPolicy::Rescue => println!(
            "  rescued into the catch-all column on {} row(s)",
            r.rows_rescued
        ),
        DriftPolicy::Quarantine => println!(
            "  {} row(s) quarantined — not written, sidecarred with the rejects",
            r.rows_quarantined
        ),
        // `warn` wrote every row exactly as `ignore` would; say so, so nobody reads a
        // drift report as "rows were dropped".
        _ => println!("  every row was written as-is; nothing was dropped for drift"),
    }
    if !r.new_fields.is_empty() {
        if let Ok(yaml) = serde_yaml::to_string(&r.suggested_columns()) {
            println!("  to keep the new fields, add to the spec's `columns:`");
            for line in yaml.lines().take(DRIFT_LINES * 3) {
                println!("    {line}");
            }
        }
    }
}

fn example_of(examples: &[Value]) -> String {
    match examples.first() {
        Some(v) => format!(", e.g. {v}"),
        None => String::new(),
    }
}

/// Write the full drift report beside the output, so a run's shape story outlives its
/// terminal scrollback.
fn write_drift_report(path: &Path, dataset: &str, report: &DriftReport) -> Result<()> {
    let doc = serde_json::json!({
        "dataset": dataset,
        "drift": report,
        "suggested_columns": report.suggested_columns(),
    });
    let text = serde_json::to_string_pretty(&doc).context("serializing the drift report")?;
    std::fs::write(path, text).with_context(|| format!("writing {}", path.display()))
}

/// A parquet `--output` may be a file or a directory; a directory (existing, or a
/// trailing separator) gets `<dataset>.parquet` appended.
/// Append one JSON record to the reject sidecar, creating it lazily on first use.
/// Fails closed — a write error is propagated, never swallowed.
fn write_reject(
    rejects: &mut Option<std::io::BufWriter<std::fs::File>>,
    reject_path: &Path,
    rec: &serde_json::Value,
) -> Result<()> {
    let w = match rejects.as_mut() {
        Some(w) => w,
        None => {
            let f = std::fs::File::create(reject_path)
                .with_context(|| format!("creating {}", reject_path.display()))?;
            *rejects = Some(std::io::BufWriter::new(f));
            rejects.as_mut().unwrap()
        }
    };
    writeln!(w, "{rec}").with_context(|| format!("writing {}", reject_path.display()))?;
    Ok(())
}

/// Heuristic: does `--output` name an object-store URL (`scheme://…`) rather than a
/// local path? Feature-independent, so the default binary can still recognize a URL
/// and give a clear "rebuild with the feature" error instead of trying to create a
/// bogus local file. The `shapeshift-objstore` crate validates the actual scheme.
fn looks_like_url(s: &str) -> bool {
    s.contains("://")
}

/// Build an object-store Parquet sink for a URL `--output`. Present only when built
/// `--features object_store`; the default binary errors clearly instead of linking
/// the cloud SDKs.
#[cfg(feature = "object_store")]
fn objstore_parquet_sink(
    url: &str,
    schema: shapeshift_core::SchemaRef,
    compression: Compression,
) -> Result<Box<dyn Sink + Send>> {
    let s = shapeshift_objstore::ObjectStoreParquetSink::create(url, schema, compression)
        .map_err(anyhow::Error::from)?;
    Ok(Box::new(s))
}

#[cfg(not(feature = "object_store"))]
fn objstore_parquet_sink(
    url: &str,
    _schema: shapeshift_core::SchemaRef,
    _compression: Compression,
) -> Result<Box<dyn Sink + Send>> {
    bail!(
        "object-store output ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

/// Build an object-store Iceberg sink for a URL `--output` (the table location) — a
/// merge when `merge_on` names key columns. Feature-gated like the Parquet variant, so the
/// default binary links no cloud SDKs.
#[cfg(feature = "object_store")]
fn objstore_iceberg_sink(
    url: &str,
    schema: shapeshift_core::SchemaRef,
    compression: Compression,
    append: bool,
    partition_by: &[String],
    merge: MergeOn<'_>,
    run_id: Option<&str>,
) -> Result<Box<dyn Sink + Send>> {
    use shapeshift_objstore::ObjectStoreIcebergSink;
    let (merge_on, order_by, retention_ms) = (merge.keys, merge.order_by, merge.retention_ms);
    let mut s = if merge_on.is_empty() {
        ObjectStoreIcebergSink::create(url, schema, compression, append, partition_by)
    } else {
        ObjectStoreIcebergSink::create_merge(url, schema, compression, partition_by, merge_on)
    }
    .map_err(anyhow::Error::from)?;
    if !order_by.is_empty() {
        s.order_changes_by(order_by, retention_ms)
            .map_err(anyhow::Error::from)?;
    }
    if let Some(id) = run_id {
        s.set_run_id(id).map_err(anyhow::Error::from)?;
    }
    Ok(Box::new(s))
}

#[cfg(not(feature = "object_store"))]
fn objstore_iceberg_sink(
    url: &str,
    _schema: shapeshift_core::SchemaRef,
    _compression: Compression,
    _append: bool,
    _partition_by: &[String],
    _merge: MergeOn<'_>,
    _run_id: Option<&str>,
) -> Result<Box<dyn Sink + Send>> {
    bail!(
        "object-store Iceberg output ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

/// Build a sink committing to a table through a REST catalog (`--catalog`), merging on
/// `merge_on` when it names key columns. Feature-gated like the object-store sinks: the
/// catalog's tables live in object stores (or `file:`).
#[cfg(feature = "object_store")]
fn objstore_catalog_sink(
    catalog: CatalogTarget<'_>,
    schema: shapeshift_core::SchemaRef,
    compression: Compression,
    append: bool,
    partition_by: &[String],
    merge: MergeOn<'_>,
    run_id: Option<&str>,
) -> Result<Box<dyn Sink + Send>> {
    use shapeshift_objstore::ObjectStoreIcebergSink;
    let conn = catalog.connection()?;
    let (conn, table) = (&conn, &catalog.spec.table);
    let (merge_on, order_by, retention_ms) = (merge.keys, merge.order_by, merge.retention_ms);
    let mut s = if merge_on.is_empty() {
        ObjectStoreIcebergSink::create_in_catalog(
            conn,
            table,
            schema,
            compression,
            append,
            partition_by,
        )
    } else {
        ObjectStoreIcebergSink::create_merge_in_catalog(
            conn,
            table,
            schema,
            compression,
            partition_by,
            merge_on,
        )
    }
    .map_err(anyhow::Error::from)?;
    if !order_by.is_empty() {
        s.order_changes_by(order_by, retention_ms)
            .map_err(anyhow::Error::from)?;
    }
    if let Some(id) = run_id {
        s.set_run_id(id).map_err(anyhow::Error::from)?;
    }
    Ok(Box::new(s))
}

#[cfg(not(feature = "object_store"))]
fn objstore_catalog_sink(
    catalog: CatalogTarget<'_>,
    _schema: shapeshift_core::SchemaRef,
    _compression: Compression,
    _append: bool,
    _partition_by: &[String],
    _merge: MergeOn<'_>,
    _run_id: Option<&str>,
) -> Result<Box<dyn Sink + Send>> {
    // The URI is not repeated: it may carry credentials, which are refused only when used.
    bail!(
        "committing {} through a REST catalog needs a build with `--features object_store`",
        catalog.spec.table
    )
}

/// What a table sink merges on: the key, and how a change feed's changes are ordered
/// (`changes.order_by`; empty for arrival order, or a plain merge).
// Read only where an object store can be written: a build with `object_store`.
#[cfg_attr(not(feature = "object_store"), allow(dead_code))]
#[derive(Clone, Copy)]
struct MergeOn<'a> {
    keys: &'a [String],
    order_by: &'a [String],
    retention_ms: i64,
}

/// A REST catalog as this run reaches it: the spec's `output.catalog` (or --catalog), and
/// whether --insecure-catalog lets its credentials go over plain http.
#[derive(Clone, Copy)]
struct CatalogTarget<'a> {
    spec: &'a CatalogSpec,
    // Read only where a catalog can be reached: a build with `object_store`.
    #[cfg_attr(not(feature = "object_store"), allow(dead_code))]
    insecure: bool,
}

#[cfg(feature = "object_store")]
impl CatalogTarget<'_> {
    fn connection(&self) -> Result<shapeshift_objstore::CatalogConnection> {
        catalog_connection(
            &self.spec.uri,
            self.spec.warehouse.as_deref(),
            self.insecure,
        )
    }
}

/// An endpoint as messages and default dataset names show it: no query, no user info.
fn endpoint_name(url: &str) -> String {
    let no_query = url.split(['?', '#']).next().unwrap_or(url);
    match no_query.split_once("://") {
        Some((scheme, rest)) => {
            // User info is in the authority only; a path may hold an `@` (`/users/@me`).
            let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
            let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
            if path.is_empty() && !rest.contains('/') {
                format!("{scheme}://{host}")
            } else {
                format!("{scheme}://{host}/{path}")
            }
        }
        None => no_query.to_string(),
    }
}

/// An endpoint being read: the source, its records until the run takes them, and the
/// position it started from.
struct Pulled {
    source: Box<dyn shapeshift_core::Source>,
    records: Option<RecordSource>,
    start: Option<shapeshift_core::Position>,
}

/// What a pull did: its kind, where it started, what it commits, and its counts.
type PullReport = (
    &'static str,
    Option<shapeshift_core::Position>,
    Option<shapeshift_core::Position>,
    shapeshift_core::Progress,
);

fn pull_json((kind, start, end, p): &PullReport) -> Value {
    serde_json::json!({
        "kind": kind,
        "from": start.as_ref().map(|s| s.to_json()),
        "position": end.as_ref().map(|e| e.to_json()),
        "new": p.new,
        "replayed": p.replayed,
        "already_read": p.skipped,
        "bounded": p.bounded,
    })
}

fn print_pull((kind, start, end, p): &PullReport) {
    let at = |x: &Option<shapeshift_core::Position>| {
        x.as_ref()
            .map_or("none".to_string(), |x| x.to_json().to_string())
    };
    // A source read whole has no position to show.
    let position = if start.is_none() && end.is_none() {
        String::new()
    } else {
        format!("; position {} → {}", at(start), at(end))
    };
    println!(
        "  source {kind}: {} new, {} re-read, {} already read{position}{}",
        p.new,
        p.replayed,
        p.skipped,
        if p.bounded {
            " (stopped at a bound: the next run goes on from there)"
        } else if start.is_some() && p.new == 0 {
            " (nothing new)"
        } else {
            ""
        }
    );
}

/// Whether a source is pulled (an endpoint, a database) rather than read from a file.
fn is_pulled(s: &SourceSpec) -> bool {
    s.http.is_some() || s.mysql.is_some() || s.postgres.is_some()
}

/// What a run allows its pulled source beyond the spec: credentials over plain http or a
/// database without TLS (`--insecure-source`), and the only hosts it may reach
/// (`--source-hosts`; empty: any).
struct SourceAccess {
    insecure: bool,
    // Read by the sources, which a build without them does not have.
    #[cfg_attr(
        not(any(feature = "http", feature = "mysql", feature = "postgres")),
        allow(dead_code)
    )]
    hosts: Vec<String>,
}

/// The pulled source the spec names, if any, opened from the table's recorded position.
fn open_pulled(
    spec: &DatasetSpec,
    bounds: &shapeshift_core::Bounds,
    access: &SourceAccess,
    insecure_catalog: bool,
) -> Result<Option<Pulled>> {
    if access.insecure {
        eprintln!(
            "warning: --insecure-source: the source's credentials may go over the network \
             unencrypted"
        );
    }
    if let Some(h) = &spec.source.http {
        return open_endpoint(spec, h, bounds, access, insecure_catalog).map(Some);
    }
    if let Some(m) = &spec.source.mysql {
        return open_database(spec, m, bounds, access, insecure_catalog).map(Some);
    }
    if let Some(p) = &spec.source.postgres {
        return open_postgres(spec, p, bounds, access, insecure_catalog).map(Some);
    }
    Ok(None)
}

/// Read the table's position for `source`, and open it from there.
#[cfg(any(feature = "http", feature = "mysql", feature = "postgres"))]
fn open_from_position(
    spec: &DatasetSpec,
    mut source: Box<dyn shapeshift_core::Source>,
    incremental: bool,
    bounds: &shapeshift_core::Bounds,
    insecure_catalog: bool,
) -> Result<Pulled> {
    let recorded = match incremental {
        true => source_position(spec, insecure_catalog, source.kind())?,
        false => None,
    };
    // A position is a position in one source: another one's would skip or repeat rows.
    let start =
        shapeshift_core::resume_from(recorded, source.as_ref()).map_err(anyhow::Error::from)?;
    let records = source
        .open(start.clone(), bounds.clone())
        .map_err(anyhow::Error::from)?;
    let records: RecordSource = Box::new(
        records.map(|r| r.map_err(|e| JsonError::Io(std::io::Error::other(e.to_string())))),
    );
    Ok(Pulled {
        source,
        records: Some(records),
        start,
    })
}

/// Open the database at the table's position.
#[cfg(feature = "mysql")]
fn open_database(
    spec: &DatasetSpec,
    mysql: &shapeshift_core::MysqlSourceSpec,
    bounds: &shapeshift_core::Bounds,
    access: &SourceAccess,
    insecure_catalog: bool,
) -> Result<Pulled> {
    let source = shapeshift_source_mysql::MysqlSource::new(
        mysql,
        shapeshift_source_mysql::MysqlOptions {
            insecure: access.insecure,
            hosts: access.hosts.clone(),
        },
    )
    .map_err(anyhow::Error::from)?;
    open_from_position(
        spec,
        Box::new(source),
        mysql.incremental(),
        bounds,
        insecure_catalog,
    )
}

/// Open the Postgres database at the table's position.
#[cfg(feature = "postgres")]
fn open_postgres(
    spec: &DatasetSpec,
    pg: &shapeshift_core::PostgresSourceSpec,
    bounds: &shapeshift_core::Bounds,
    access: &SourceAccess,
    insecure_catalog: bool,
) -> Result<Pulled> {
    let source = shapeshift_source_postgres::PostgresSource::new(
        pg,
        shapeshift_source_postgres::PostgresOptions {
            insecure: access.insecure,
            hosts: access.hosts.clone(),
        },
    )
    .map_err(anyhow::Error::from)?;
    open_from_position(
        spec,
        Box::new(source),
        pg.incremental(),
        bounds,
        insecure_catalog,
    )
}

#[cfg(not(feature = "postgres"))]
fn open_postgres(
    _spec: &DatasetSpec,
    pg: &shapeshift_core::PostgresSourceSpec,
    _bounds: &shapeshift_core::Bounds,
    _access: &SourceAccess,
    _insecure_catalog: bool,
) -> Result<Pulled> {
    bail!(
        "reading a Postgres database ({}) needs a build with `--features postgres` (kept out \
         of the default binary so it links no database client)",
        endpoint_name(&pg.url)
    )
}

#[cfg(not(feature = "mysql"))]
fn open_database(
    _spec: &DatasetSpec,
    mysql: &shapeshift_core::MysqlSourceSpec,
    _bounds: &shapeshift_core::Bounds,
    _access: &SourceAccess,
    _insecure_catalog: bool,
) -> Result<Pulled> {
    bail!(
        "reading a database ({}) needs a build with `--features mysql` (kept out of the \
         default binary so it links no database client)",
        endpoint_name(&mysql.url)
    )
}

/// Open the endpoint at the table's position.
#[cfg(feature = "http")]
fn open_endpoint(
    spec: &DatasetSpec,
    http: &shapeshift_core::HttpSourceSpec,
    bounds: &shapeshift_core::Bounds,
    access: &SourceAccess,
    insecure_catalog: bool,
) -> Result<Pulled> {
    let source = shapeshift_source_http::HttpSource::new(
        http,
        spec.source.csv.clone(),
        spec.source.xml.clone(),
        shapeshift_source_http::HttpOptions {
            insecure: access.insecure,
            backoff: None,
            hosts: access.hosts.clone(),
        },
    )
    .map_err(anyhow::Error::from)?;
    open_from_position(
        spec,
        Box::new(source),
        http.incremental.is_some(),
        bounds,
        insecure_catalog,
    )
}

#[cfg(not(feature = "http"))]
fn open_endpoint(
    _spec: &DatasetSpec,
    http: &shapeshift_core::HttpSourceSpec,
    _bounds: &shapeshift_core::Bounds,
    _access: &SourceAccess,
    _insecure_catalog: bool,
) -> Result<Pulled> {
    bail!(
        "reading an endpoint ({}) needs a build with `--features http` (kept out of the \
         default binary so it links no HTTP client)",
        endpoint_name(&http.url)
    )
}

/// The position the table records for a `kind` source, wherever the table is.
#[cfg_attr(
    not(any(feature = "http", feature = "mysql", feature = "postgres")),
    allow(dead_code)
)]
fn source_position(
    spec: &DatasetSpec,
    insecure_catalog: bool,
    kind: &str,
) -> Result<Option<Value>> {
    if let Some(cat) = &spec.output.catalog {
        let target = CatalogTarget {
            spec: cat,
            insecure: insecure_catalog,
        };
        return objstore_source_position_in_catalog(target, kind);
    }
    if looks_like_url(&spec.output.path) {
        return objstore_source_position(&spec.output.path, kind);
    }
    shapeshift_iceberg::source_position_local(&spec.output.path, kind).map_err(anyhow::Error::from)
}

#[cfg(feature = "object_store")]
fn objstore_source_position(url: &str, kind: &str) -> Result<Option<Value>> {
    shapeshift_objstore::source_position(url, kind).map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
#[cfg_attr(
    not(any(feature = "http", feature = "mysql", feature = "postgres")),
    allow(dead_code)
)]
fn objstore_source_position(url: &str, _kind: &str) -> Result<Option<Value>> {
    bail!(
        "object-store Iceberg output ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

#[cfg(feature = "object_store")]
fn objstore_source_position_in_catalog(
    catalog: CatalogTarget<'_>,
    kind: &str,
) -> Result<Option<Value>> {
    let conn = catalog.connection()?;
    shapeshift_objstore::source_position_in_catalog(&conn, &catalog.spec.table, kind)
        .map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
#[cfg_attr(
    not(any(feature = "http", feature = "mysql", feature = "postgres")),
    allow(dead_code)
)]
fn objstore_source_position_in_catalog(
    catalog: CatalogTarget<'_>,
    _kind: &str,
) -> Result<Option<Value>> {
    bail!(
        "a REST catalog ({}) needs a build with `--features object_store`",
        catalog.spec.uri
    )
}

/// The snapshot run `run_id` already committed to the spec's Iceberg output, if any: a
/// local table, a table in an object store, or one a REST catalog names.
fn find_run(
    spec: &DatasetSpec,
    insecure_catalog: bool,
    run_id: &str,
) -> Result<Option<shapeshift_iceberg::RunSnapshot>> {
    if let Some(cat) = &spec.output.catalog {
        let target = CatalogTarget {
            spec: cat,
            insecure: insecure_catalog,
        };
        return objstore_find_run_in_catalog(target, run_id);
    }
    if looks_like_url(&spec.output.path) {
        return objstore_find_run(&spec.output.path, run_id);
    }
    shapeshift_iceberg::find_run_local(&spec.output.path, run_id).map_err(anyhow::Error::from)
}

#[cfg(feature = "object_store")]
fn objstore_find_run(url: &str, run_id: &str) -> Result<Option<shapeshift_iceberg::RunSnapshot>> {
    shapeshift_objstore::find_run(url, run_id).map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
fn objstore_find_run(url: &str, _run_id: &str) -> Result<Option<shapeshift_iceberg::RunSnapshot>> {
    bail!(
        "object-store Iceberg output ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

#[cfg(feature = "object_store")]
fn objstore_find_run_in_catalog(
    catalog: CatalogTarget<'_>,
    run_id: &str,
) -> Result<Option<shapeshift_iceberg::RunSnapshot>> {
    let conn = catalog.connection()?;
    shapeshift_objstore::find_run_in_catalog(&conn, &catalog.spec.table, run_id)
        .map_err(anyhow::Error::from)
}

/// The catalog at `uri`, its `warehouse`, the credentials the environment names
/// (`SHAPESHIFT_CATALOG_TOKEN`, or `SHAPESHIFT_CATALOG_CREDENTIAL`), and whether
/// --insecure-catalog lets them go over plain http to another machine.
#[cfg(feature = "object_store")]
fn catalog_connection(
    uri: &str,
    warehouse: Option<&str>,
    insecure: bool,
) -> Result<shapeshift_objstore::CatalogConnection> {
    let mut conn =
        shapeshift_objstore::CatalogConnection::from_env(uri, warehouse.map(str::to_string))
            .map_err(anyhow::Error::from)?;
    conn.allow_plain_http = insecure;
    Ok(conn)
}

#[cfg(not(feature = "object_store"))]
fn objstore_find_run_in_catalog(
    catalog: CatalogTarget<'_>,
    _run_id: &str,
) -> Result<Option<shapeshift_iceberg::RunSnapshot>> {
    // The URI is not repeated: it may carry credentials, which are refused only when used.
    bail!(
        "committing {} through a REST catalog needs a build with `--features object_store`",
        catalog.spec.table
    )
}

/// Summarize an Iceberg table living in an object store (`inspect <url>`). Feature-gated
/// like the sinks.
#[cfg(feature = "object_store")]
fn objstore_inspect_iceberg(url: &str) -> Result<shapeshift_iceberg::IcebergInfo> {
    shapeshift_objstore::inspect_iceberg(url).map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
fn objstore_inspect_iceberg(url: &str) -> Result<shapeshift_iceberg::IcebergInfo> {
    bail!(
        "inspecting an object-store table ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

/// Compact an Iceberg table living in an object store. Feature-gated like the sinks.
#[cfg(feature = "object_store")]
fn objstore_compact_iceberg(
    url: &str,
    options: &shapeshift_iceberg::CompactOptions,
) -> Result<shapeshift_iceberg::CompactSummary> {
    shapeshift_objstore::compact_iceberg(url, options).map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
fn objstore_compact_iceberg(
    url: &str,
    _options: &shapeshift_iceberg::CompactOptions,
) -> Result<shapeshift_iceberg::CompactSummary> {
    bail!(
        "compacting an object-store table ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

/// Compact a table through a REST catalog (`compact --catalog`). Feature-gated like the
/// sinks.
#[cfg(feature = "object_store")]
fn objstore_compact_in_catalog(
    uri: &str,
    warehouse: Option<&str>,
    insecure: bool,
    table: &str,
    options: &shapeshift_iceberg::CompactOptions,
) -> Result<shapeshift_iceberg::CompactSummary> {
    let conn = catalog_connection(uri, warehouse, insecure)?;
    shapeshift_objstore::compact_in_catalog(&conn, table, options).map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
fn objstore_compact_in_catalog(
    _uri: &str,
    _warehouse: Option<&str>,
    _insecure: bool,
    _table: &str,
    _options: &shapeshift_iceberg::CompactOptions,
) -> Result<shapeshift_iceberg::CompactSummary> {
    bail!("compacting through a REST catalog needs a build with `--features object_store`")
}

/// Expire snapshots through a REST catalog (`expire --catalog`). Feature-gated like the
/// sinks.
#[cfg(feature = "object_store")]
fn objstore_expire_in_catalog(
    uri: &str,
    warehouse: Option<&str>,
    insecure: bool,
    table: &str,
    options: &shapeshift_iceberg::ExpireOptions,
) -> Result<shapeshift_iceberg::ExpireSummary> {
    let conn = catalog_connection(uri, warehouse, insecure)?;
    shapeshift_objstore::expire_in_catalog(&conn, table, options).map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
fn objstore_expire_in_catalog(
    _uri: &str,
    _warehouse: Option<&str>,
    _insecure: bool,
    _table: &str,
    _options: &shapeshift_iceberg::ExpireOptions,
) -> Result<shapeshift_iceberg::ExpireSummary> {
    bail!("expiring through a REST catalog needs a build with `--features object_store`")
}

/// Expire snapshots of an Iceberg table living in an object store. Feature-gated like the
/// sinks.
#[cfg(feature = "object_store")]
fn objstore_expire_iceberg(
    url: &str,
    options: &shapeshift_iceberg::ExpireOptions,
) -> Result<shapeshift_iceberg::ExpireSummary> {
    shapeshift_objstore::expire_iceberg(url, options).map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
fn objstore_expire_iceberg(
    url: &str,
    _options: &shapeshift_iceberg::ExpireOptions,
) -> Result<shapeshift_iceberg::ExpireSummary> {
    bail!(
        "expiring an object-store table ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

/// Relocate an Iceberg table living in an object store. Feature-gated like the sinks.
#[cfg(feature = "object_store")]
fn objstore_relocate_iceberg(
    url: &str,
    options: &shapeshift_iceberg::RelocateOptions,
) -> Result<shapeshift_iceberg::RelocateSummary> {
    shapeshift_objstore::relocate_iceberg(url, options).map_err(anyhow::Error::from)
}

#[cfg(not(feature = "object_store"))]
fn objstore_relocate_iceberg(
    url: &str,
    _options: &shapeshift_iceberg::RelocateOptions,
) -> Result<shapeshift_iceberg::RelocateSummary> {
    bail!(
        "relocating an object-store table ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

fn resolve_parquet_path(out: &str, dataset: &str) -> PathBuf {
    let p = PathBuf::from(out);
    let is_dir = out.ends_with('/') || out.ends_with(std::path::MAIN_SEPARATOR) || p.is_dir();
    if is_dir {
        p.join(format!("{dataset}.parquet"))
    } else {
        p
    }
}

fn print_iceberg_info(label: &str, info: &shapeshift_iceberg::IcebergInfo) {
    println!("iceberg table: {label}");
    println!("  format-version: {}", info.format_version);
    println!("  table-uuid: {}", info.table_uuid);
    if info.moved {
        // shapeshift reads it where it is; other engines look where the metadata says.
        println!(
            "  moved: its metadata says it is at {}; shapeshift reads and writes it here, but \
             other engines' default readers look for its files there",
            info.location
        );
    }
    println!("  current-snapshot-id: {}", info.current_snapshot_id);
    if info.total_position_deletes > 0 {
        println!(
            "  live-records: {} ({} written, {} removed by position deletes)",
            info.live_records(),
            info.total_records,
            info.total_position_deletes
        );
    } else {
        println!("  total-records: {}", info.total_records);
    }
    println!("  columns ({}):", info.fields.len());
    for (n, t) in &info.fields {
        println!("    {n}: {t}");
    }
}

fn cmd_inspect(a: InspectArgs) -> Result<()> {
    let path_str = a.path.to_string_lossy();
    if looks_like_url(&path_str) {
        // A URL points at an object-store Iceberg table (read via version-hint).
        let info = objstore_inspect_iceberg(&path_str)?;
        print_iceberg_info(&path_str, &info);
    } else if a.path.is_dir() {
        let info = shapeshift_iceberg::inspect(&a.path).map_err(anyhow::Error::from)?;
        print_iceberg_info(&a.path.display().to_string(), &info);
    } else {
        let info = shapeshift_parquet::inspect(&a.path).map_err(anyhow::Error::from)?;
        println!("parquet file: {}", a.path.display());
        println!("  rows: {}", info.rows);
        println!("  row-groups: {}", info.row_groups);
        println!("  columns ({}):", info.columns.len());
        for (n, t) in &info.columns {
            println!("    {n}: {t}");
        }
    }
    Ok(())
}

fn cmd_compact(a: CompactArgs) -> Result<()> {
    let options = shapeshift_iceberg::CompactOptions {
        target_file_size: a.target_file_size,
        compression: a.compression.into(),
        ..shapeshift_iceberg::CompactOptions::default()
    };
    let s = if let Some(uri) = &a.catalog {
        objstore_compact_in_catalog(
            uri,
            a.warehouse.as_deref(),
            a.insecure_catalog,
            &a.table,
            &options,
        )?
    } else if looks_like_url(&a.table) {
        objstore_compact_iceberg(&a.table, &options)?
    } else {
        let dir = PathBuf::from(&a.table);
        if !dir.join("metadata/version-hint.text").exists() {
            bail!(
                "no Iceberg table at {} (no metadata/version-hint.text)",
                dir.display()
            );
        }
        shapeshift_iceberg::compact_table(&dir, &options).map_err(anyhow::Error::from)?
    };
    let doc = compact_json(&a, &s);
    report_summary(&doc);
    if a.summary_json {
        println!("{doc}");
    } else {
        print_compact(&a.table, &s);
    }
    Ok(())
}

fn compact_json(a: &CompactArgs, s: &shapeshift_iceberg::CompactSummary) -> Value {
    serde_json::json!({
        "summary_version": SUMMARY_VERSION,
        "command": "compact",
        "ok": true,
        "table": a.table,
        "catalog": a.catalog,
        "warehouse": a.warehouse,
        "committed": s.snapshot_id.is_some(),
        "version": s.version,
        "snapshot_id": s.snapshot_id.map(|id| id.to_string()),
        "data_files": {
            "rewritten": s.data_files_rewritten,
            "written": s.data_files_written,
            "kept": s.data_files_kept,
        },
        "delete_files_removed": s.delete_files_removed,
        "position_deletes_applied": s.position_deletes_applied,
        "rows_rewritten": s.rows_rewritten,
        "bytes": { "rewritten": s.bytes_rewritten, "written": s.bytes_written },
        "partitions_compacted": s.partitions_compacted,
        "options": {
            "target_file_size": a.target_file_size,
            "compression": Compression::from(a.compression),
        },
    })
}

fn print_compact(table: &str, s: &shapeshift_iceberg::CompactSummary) {
    let Some(id) = s.snapshot_id else {
        println!(
            "nothing to compact in {table}: no delete files, and no partition with two or \
             more files below the target size ({} data file(s) left as they are)",
            s.data_files_kept
        );
        return;
    };
    println!("compacted {table} → snapshot {id} (replace)");
    println!(
        "  rewrote {} data file(s) ({}) into {} ({}); kept {} as they were",
        s.data_files_rewritten,
        human_bytes(s.bytes_rewritten),
        s.data_files_written,
        human_bytes(s.bytes_written),
        s.data_files_kept
    );
    println!(
        "  applied {} position delete(s) from {} delete file(s) — the table has none left",
        s.position_deletes_applied, s.delete_files_removed
    );
    println!(
        "  {} live row(s) rewritten across {} partition(s); earlier snapshots still read their own files",
        s.rows_rewritten, s.partitions_compacted
    );
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

fn cmd_expire(a: ExpireArgs) -> Result<()> {
    let options = shapeshift_iceberg::ExpireOptions {
        older_than_ms: shapeshift_iceberg::snapshot_timestamp_ms().saturating_sub(a.older_than),
        retain_last: a.retain_last as usize,
        dry_run: a.dry_run,
    };
    let s = if let Some(uri) = &a.catalog {
        objstore_expire_in_catalog(
            uri,
            a.warehouse.as_deref(),
            a.insecure_catalog,
            &a.table,
            &options,
        )?
    } else if looks_like_url(&a.table) {
        objstore_expire_iceberg(&a.table, &options)?
    } else {
        let dir = PathBuf::from(&a.table);
        if !dir.join("metadata/version-hint.text").exists() {
            bail!(
                "no Iceberg table at {} (no metadata/version-hint.text)",
                dir.display()
            );
        }
        shapeshift_iceberg::expire_table(&dir, &options).map_err(anyhow::Error::from)?
    };
    let doc = expire_json(&a, &s);
    report_summary(&doc);
    if a.summary_json {
        println!("{doc}");
    } else {
        print_expire(&a, &s);
    }
    Ok(())
}

/// Whether a URI names a user (`user:password@host`) in its authority.
fn uri_has_userinfo(uri: &str) -> bool {
    let rest = uri.split_once("://").map_or(uri, |(_, r)| r);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    authority.contains('@')
}

fn cmd_relocate(a: RelocateArgs) -> Result<()> {
    // The catalog URI is printed (errors, a dry run, the summary), so one that carries
    // credentials is refused before anything runs, as the catalog client refuses it.
    if a.catalog.as_deref().is_some_and(uri_has_userinfo) {
        bail!(
            "the catalog URI carries credentials (`user:password@`); put them in \
             SHAPESHIFT_CATALOG_TOKEN or SHAPESHIFT_CATALOG_CREDENTIAL instead"
        );
    }
    let options = shapeshift_iceberg::RelocateOptions {
        identity: match (a.moved, a.copy) {
            (true, _) => Some(shapeshift_iceberg::Identity::Moved),
            (_, true) => Some(shapeshift_iceberg::Identity::Copy),
            _ => None,
        },
        dry_run: a.dry_run,
    };
    let s = if looks_like_url(&a.table) {
        objstore_relocate_iceberg(&a.table, &options)?
    } else {
        let dir = PathBuf::from(&a.table);
        if !dir.join("metadata/version-hint.text").exists() {
            bail!(
                "no Iceberg table at {} (no metadata/version-hint.text)",
                dir.display()
            );
        }
        shapeshift_iceberg::relocate_table(&dir, &options).map_err(anyhow::Error::from)?
    };
    // Registered once the table is where its metadata says (never in a dry run): the
    // catalog then reads paths that are the table's own.
    let registered = match (&a.catalog, &a.register_as, a.dry_run) {
        (Some(uri), Some(name), false) => Some(
            objstore_register_in_catalog(
                uri,
                a.warehouse.as_deref(),
                a.insecure_catalog,
                name,
                &s.metadata,
            )
            .with_context(|| {
                format!(
                    "registering {name} with the catalog at {uri} (the relocation itself {}; run \
                     relocate again with --catalog to register it)",
                    if s.committed {
                        "is committed"
                    } else {
                        "had nothing to do"
                    }
                )
            })?,
        ),
        _ => None,
    };
    let doc = relocate_json(&a, &s, registered);
    report_summary(&doc);
    if a.summary_json {
        println!("{doc}");
    } else {
        print_relocate(&a, &s, registered);
    }
    Ok(())
}

/// Register a relocated table with a REST catalog: `true` if this run registered it, `false`
/// if the catalog already named it at the same metadata file. Feature-gated like the sinks.
#[cfg(feature = "object_store")]
fn objstore_register_in_catalog(
    uri: &str,
    warehouse: Option<&str>,
    insecure: bool,
    table: &str,
    metadata: &str,
) -> Result<bool> {
    let conn = catalog_connection(uri, warehouse, insecure)?;
    Ok(
        shapeshift_objstore::register_in_catalog(&conn, table, metadata)
            .map_err(anyhow::Error::from)?
            == shapeshift_objstore::Registration::Registered,
    )
}

#[cfg(not(feature = "object_store"))]
fn objstore_register_in_catalog(
    _uri: &str,
    _warehouse: Option<&str>,
    _insecure: bool,
    _table: &str,
    _metadata: &str,
) -> Result<bool> {
    bail!("registering with a REST catalog needs a build with `--features object_store`")
}

/// In a dry run, the counts are what a real run would rewrite and remove.
fn relocate_json(
    a: &RelocateArgs,
    s: &shapeshift_iceberg::RelocateSummary,
    registered: Option<bool>,
) -> Value {
    serde_json::json!({
        "summary_version": SUMMARY_VERSION,
        "command": "relocate",
        "ok": true,
        "table": a.table,
        "dry_run": a.dry_run,
        "committed": s.committed,
        "version": s.version,
        "already_anchored": s.already_anchored,
        "from": s.from,
        "to": s.to,
        "table_uuid": s.table_uuid,
        "new_uuid": s.new_uuid,
        "undecided": s.undecided,
        "snapshots": s.snapshots,
        "rewritten": {
            "manifest_lists": s.manifest_lists_rewritten,
            "manifests": s.manifests_rewritten,
            "delete_files": s.delete_files_rewritten,
        },
        "data_files": s.data_files,
        "files_outside_table": s.files_outside_table,
        "superseded": {
            "files": s.superseded,
            "deleted": s.superseded_deleted,
            "not_deleted": s.superseded_not_deleted,
        },
        "metadata": s.metadata,
        "catalog": a.catalog.as_ref().map(|uri| serde_json::json!({
            "uri": uri,
            "table": a.register_as,
            "warehouse": a.warehouse,
            // null in a dry run: nothing was registered.
            "registered": registered,
        })),
    })
}

fn print_relocate(
    a: &RelocateArgs,
    s: &shapeshift_iceberg::RelocateSummary,
    registered: Option<bool>,
) {
    let catalog = || {
        let (Some(uri), Some(name)) = (&a.catalog, &a.register_as) else {
            return;
        };
        match registered {
            Some(true) => println!(
                "  registered {name} with the catalog at {uri} ({})",
                s.metadata
            ),
            Some(false) => println!(
                "  {name} was already registered with the catalog at {uri}, at {}",
                s.metadata
            ),
            None => println!(
                "  would register {name} with the catalog at {uri} ({})",
                s.metadata
            ),
        }
    };
    if s.already_anchored {
        println!(
            "{} is already anchored where it is ({}): nothing to relocate",
            a.table, s.to
        );
        catalog();
        return;
    }
    let (relocate, rewrite, remove) = if a.dry_run {
        ("would relocate", "would rewrite", "would remove")
    } else {
        ("relocated", "rewrote", "removed")
    };
    println!(
        "{relocate} {}: {} → {} ({} snapshot(s) kept{}){}",
        a.table,
        s.from,
        s.to,
        s.snapshots,
        if s.new_uuid {
            format!("; a copy, new table-uuid {}", s.table_uuid)
        } else {
            String::new()
        },
        if a.dry_run {
            " (dry run: nothing changed)"
        } else {
            ""
        }
    );
    println!(
        "  {rewrite} {} manifest list(s), {} manifest(s), {} delete file(s); {} data file(s) \
         unchanged",
        s.manifest_lists_rewritten, s.manifests_rewritten, s.delete_files_rewritten, s.data_files
    );
    let removed = if a.dry_run {
        s.superseded
    } else {
        s.superseded_deleted
    };
    if removed > 0 {
        println!("  {remove} {removed} superseded metadata and delete file(s)");
    }
    if s.files_outside_table > 0 {
        println!(
            "  left {} file(s) as recorded: their paths are outside every place the table \
             was written at",
            s.files_outside_table
        );
    }
    if s.superseded_not_deleted > 0 {
        println!(
            "  warning: {} superseded file(s) could not be removed; nothing names them any \
             more",
            s.superseded_not_deleted
        );
    }
    catalog();
    if let Some(why) = &s.undecided {
        println!(
            "  note: {why}; a real run needs --moved (the original is being retired) or \
             --copy (both live on)"
        );
    }
}

/// In a dry run, `deleted` counts what a real run would delete.
fn expire_json(a: &ExpireArgs, s: &shapeshift_iceberg::ExpireSummary) -> Value {
    serde_json::json!({
        "summary_version": SUMMARY_VERSION,
        "command": "expire",
        "ok": true,
        "table": a.table,
        "catalog": a.catalog,
        "warehouse": a.warehouse,
        "dry_run": a.dry_run,
        "committed": s.committed,
        "version": s.version,
        "snapshots": {
            "expired": s.expired_snapshots.iter().map(i64::to_string).collect::<Vec<_>>(),
            "retained": s.retained_snapshots,
        },
        "deleted": {
            "data_files": s.data_files_deleted,
            "delete_files": s.delete_files_deleted,
            "manifests": s.manifests_deleted,
            "manifest_lists": s.manifest_lists_deleted,
            "statistics_files": s.statistics_files_deleted,
            "bytes": s.bytes_deleted,
        },
        "files_outside_table": s.files_outside_table,
        "files_not_deleted": s.files_not_deleted,
        "options": { "older_than_ms": a.older_than, "retain_last": a.retain_last },
    })
}

fn print_expire(a: &ExpireArgs, s: &shapeshift_iceberg::ExpireSummary) {
    if s.expired_snapshots.is_empty() {
        println!(
            "nothing to expire in {}: {} snapshot(s), none older than {} outside the newest {}",
            a.table,
            s.retained_snapshots,
            human_duration(a.older_than),
            a.retain_last
        );
        return;
    }
    let (expire, delete) = if a.dry_run {
        ("would expire", "would delete")
    } else {
        ("expired", "deleted")
    };
    println!(
        "{expire} {} snapshot(s) of {}; kept {}{}",
        s.expired_snapshots.len(),
        a.table,
        s.retained_snapshots,
        if a.dry_run {
            " (dry run: nothing changed)"
        } else {
            ""
        }
    );
    println!(
        "  {delete} {} data file(s), {} delete file(s), {} manifest(s), {} manifest list(s) — {}",
        s.data_files_deleted,
        s.delete_files_deleted,
        s.manifests_deleted,
        s.manifest_lists_deleted,
        human_bytes(s.bytes_deleted)
    );
    if s.statistics_files_deleted > 0 {
        println!(
            "  {delete} {} tombstone file(s) only expired snapshots named",
            s.statistics_files_deleted
        );
    }
    if s.files_outside_table > 0 {
        println!(
            "  left {} file(s) in place: their paths are outside the table's location",
            s.files_outside_table
        );
    }
    if s.files_not_deleted > 0 {
        println!(
            "  warning: {} file(s) could not be deleted; no snapshot references them any more",
            s.files_not_deleted
        );
    }
}

fn human_duration(ms: i64) -> String {
    for (unit, size) in [
        ("w", 7 * 86_400_000),
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
    ] {
        if ms >= size && ms % size == 0 {
            return format!("{}{unit}", ms / size);
        }
    }
    format!("{}s", ms / 1000)
}

/// `shapeshift convert`: the input's records as JSON Lines.
///
/// A record that does not parse stops the run, naming its line, unless `--skip-bad`; an
/// I/O error always does. Written to a file, the output appears only when the whole input
/// has been read (a temporary file renamed into place), so a failed run leaves nothing
/// half-written behind.
fn cmd_convert(a: ConvertArgs) -> Result<()> {
    let source = a.source.spec(a.format.into(), None)?;
    // What a parse error's number counts, in this format.
    let unit = match source.format {
        SourceFormat::Jsonl | SourceFormat::Csv | SourceFormat::Tsv => "line",
        SourceFormat::JsonArray => "element",
        SourceFormat::Xml => "record",
    };
    let records = open_source(&a.input, &source)
        .map_err(|e| anyhow!("opening {}: {e}", display_input(&a.input)))?;
    let dest = a.output.as_ref().filter(|p| p.as_os_str() != "-");
    let (mut out, tmp): (Box<dyn Write>, Option<PathBuf>) = match dest {
        None => (
            Box::new(std::io::BufWriter::new(std::io::stdout().lock())),
            None,
        ),
        Some(p) => {
            let name = p
                .file_name()
                .ok_or_else(|| anyhow!("--output {} names no file", p.display()))?;
            let mut tmp_name = std::ffi::OsString::from(".");
            tmp_name.push(name);
            tmp_name.push(format!(".{}.tmp", std::process::id()));
            let tmp = p.with_file_name(tmp_name);
            // A new file only: never write through something already at that name.
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)
                .with_context(|| format!("creating {}", tmp.display()))?;
            (Box::new(std::io::BufWriter::new(file)), Some(tmp))
        }
    };
    let result = (|| -> Result<(u64, u64)> {
        let (mut written, mut skipped) = (0u64, 0u64);
        for item in records {
            if a.limit.is_some_and(|l| written >= l) {
                break;
            }
            match item {
                Ok(v) => {
                    serde_json::to_writer(&mut out, &v)?;
                    out.write_all(b"\n")?;
                    written += 1;
                }
                Err(JsonError::Parse { line, message, .. }) if a.skip_bad => {
                    eprintln!(
                        "skipped: {}: {unit} {line}: {message}",
                        display_input(&a.input)
                    );
                    skipped += 1;
                }
                Err(JsonError::Parse { line, message, .. }) => bail!(
                    "{}: {unit} {line}: {message} (--skip-bad leaves such records out)",
                    display_input(&a.input)
                ),
                Err(e) => bail!("reading {}: {e}", display_input(&a.input)),
            }
        }
        out.flush()?;
        Ok((written, skipped))
    })();
    drop(out);
    let (written, skipped) = match result {
        Ok(counts) => counts,
        Err(e) => {
            if let Some(tmp) = tmp {
                let _ = std::fs::remove_file(tmp);
            }
            return Err(e);
        }
    };
    if let (Some(tmp), Some(dest)) = (tmp, dest) {
        if let Err(e) = std::fs::rename(&tmp, dest) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e).with_context(|| format!("moving the output into {}", dest.display()));
        }
    }
    eprintln!(
        "converted {written} record{} from {}{}",
        if written == 1 { "" } else { "s" },
        display_input(&a.input),
        if skipped > 0 {
            format!(", left out {skipped} that did not parse")
        } else {
            String::new()
        }
    );
    Ok(())
}

fn cmd_cost(a: CostArgs) -> Result<()> {
    let rows = match (a.rows, &a.input) {
        (Some(r), _) => r,
        (None, Some(input)) => {
            let source = a.source.spec(a.format.into(), None)?;
            let reader = open_source(input, &source).map_err(|e| anyhow!("{e}"))?;
            let mut n = 0u64;
            // Skip malformed lines, but never price from a truncated/failed read.
            for item in reader {
                match item {
                    Ok(_) => n += 1,
                    Err(JsonError::Parse { .. }) => {}
                    Err(JsonError::Io(e)) => {
                        return Err(anyhow!("reading {}: {e}", input.display()))
                    }
                }
            }
            n
        }
        (None, None) => bail!("pass either --rows or --input"),
    };
    let report = estimate_mar(MarInputs {
        rows,
        vendor_per_million: a.vendor_per_million,
        self_host_cost: a.self_host_cost,
    });
    println!("billable rows (MAR-equivalent): {}", report.rows);
    println!(
        "vendor:     ${:.2}  (@ ${:.2}/million MAR)",
        report.vendor_cost, a.vendor_per_million
    );
    println!("self-host:  ${:.2}", report.self_host_cost);
    match report.saved_fraction {
        Some(frac) => println!(
            "saved:      ${}  ({:.1}%)",
            cents(report.saved),
            frac * 100.0
        ),
        None => println!("saved:      ${}", cents(report.saved)),
    }
    Ok(())
}

/// Dollars to two places, with no sign on an amount that rounds to zero (`-0.004` is `0.00`,
/// not `-0.00`); a loss of a cent or more keeps its minus (`-2.00`).
fn cents(dollars: f64) -> String {
    let s = format!("{dollars:.2}");
    if s == "-0.00" {
        "0.00".into()
    } else {
        s
    }
}

#[cfg(test)]
mod endpoint_name_tests {
    #[test]
    fn user_info_and_query_are_dropped_and_a_path_keeps_its_at() {
        let n = super::endpoint_name;
        assert_eq!(
            n("https://api.example.com/users/@me?key=1"),
            "https://api.example.com/users/@me"
        );
        assert_eq!(
            n("https://u:p@api.example.com/v1#x"),
            "https://api.example.com/v1"
        );
        assert_eq!(n("https://api.example.com"), "https://api.example.com");
    }
}

#[cfg(test)]
mod cents_tests {
    use super::cents;

    #[test]
    fn a_loss_under_a_cent_has_no_minus_and_a_real_one_keeps_it() {
        assert_eq!(cents(-0.004), "0.00");
        assert_eq!(cents(-0.0), "0.00");
        assert_eq!(cents(0.0), "0.00");
        assert_eq!(cents(-2.0), "-2.00");
        assert_eq!(cents(29998.0), "29998.00");
        assert_eq!(cents(-0.005001), "-0.01");
    }
}
