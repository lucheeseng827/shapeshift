//! `shapeshift` — infer a dataset spec from sample JSON, shape JSONL/JSON into
//! Parquet or an Iceberg table, inspect the output, and price a run against
//! Fivetran-style MAR billing.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::Value;

use shapeshift_core::{
    estimate_mar, infer_columns, Compression, DatasetSpec, MarInputs, OutputFormat, OutputSpec,
    PushOutcome, SchemaMode, Shaper, Sink, SinkSummary, SourceFormat, SourceSpec,
};
use shapeshift_iceberg::IcebergSink;
use shapeshift_json::{open_reader, JsonError};
use shapeshift_parquet::ParquetSink;

/// shapeshift — a streaming JSON→Parquet/Iceberg shaper.
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
    /// Shape input JSON/JSONL into Parquet or an Iceberg table.
    Shape(ShapeArgs),
    /// Inspect a Parquet file or Iceberg table (schema + row count).
    Inspect(InspectArgs),
    /// Price a shape against a managed vendor's MAR (Monthly Active Rows) rate.
    Cost(CostArgs),
    /// Serve a local web console over the shaper (infer/shape/inspect/cost).
    /// Requires a build with `--features serve`.
    #[cfg(feature = "serve")]
    Serve(ServeArgs),
}

#[derive(Copy, Clone, ValueEnum)]
enum CliSourceFormat {
    Jsonl,
    JsonArray,
}
impl From<CliSourceFormat> for SourceFormat {
    fn from(v: CliSourceFormat) -> Self {
        match v {
            CliSourceFormat::Jsonl => SourceFormat::Jsonl,
            CliSourceFormat::JsonArray => SourceFormat::JsonArray,
        }
    }
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

#[derive(Parser)]
struct InferArgs {
    /// Input file to sample.
    #[arg(short, long)]
    input: PathBuf,
    /// Input format.
    #[arg(long, value_enum, default_value_t = CliSourceFormat::Jsonl)]
    format: CliSourceFormat,
    /// Records to sample (0 = whole file).
    #[arg(long, default_value_t = 1000)]
    sample: usize,
    /// Do not flatten nested objects into dotted columns.
    #[arg(long)]
    no_flatten: bool,
    /// Dataset name (defaults to the input file stem).
    #[arg(long)]
    dataset: Option<String>,
    /// Write the spec here instead of printing it.
    #[arg(short, long)]
    out: Option<PathBuf>,
}

#[derive(Parser)]
struct ShapeArgs {
    /// Dataset spec (YAML/JSON). Optional — omit to infer from --input on the fly.
    #[arg(short, long)]
    spec: Option<PathBuf>,
    /// Input file (overrides the spec's source path).
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
}

#[derive(Parser)]
struct InspectArgs {
    /// A Parquet file or an Iceberg table directory.
    path: PathBuf,
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
}

#[derive(Parser)]
struct CostArgs {
    /// Count billable rows by streaming this input (alternative to --rows).
    #[arg(short, long)]
    input: Option<PathBuf>,
    /// Input format for --input.
    #[arg(long, value_enum, default_value_t = CliSourceFormat::Jsonl)]
    format: CliSourceFormat,
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

    match cli.cmd {
        Cmd::Infer(a) => cmd_infer(a),
        Cmd::Shape(a) => cmd_shape(a),
        Cmd::Inspect(a) => cmd_inspect(a),
        Cmd::Cost(a) => cmd_cost(a),
        #[cfg(feature = "serve")]
        Cmd::Serve(a) => cmd_serve(a),
    }
}

#[cfg(feature = "serve")]
fn cmd_serve(a: ServeArgs) -> Result<()> {
    let cfg = shapeshift_serve::ServeConfig {
        addr: a.addr,
        data_dir: a.data_dir,
        allowed_hosts: a.allow_host,
        allow_any_host: a.allow_any_host,
    };
    shapeshift_serve::serve(cfg).map_err(|e| anyhow!("{e}"))
}

/// Derive a dataset name from a file path stem.
fn stem_of(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("dataset")
        .to_string()
}

/// Read up to `n` well-formed records (0 = all) for inference; parse errors are
/// skipped here and surfaced properly during the shaping pass.
fn sample_records(path: &Path, format: SourceFormat, n: usize) -> Result<Vec<Value>> {
    let reader =
        open_reader(path, format).map_err(|e| anyhow!("opening {}: {e}", path.display()))?;
    let mut out = Vec::new();
    // Inference tolerates malformed *lines* — they are surfaced properly in the
    // shaping pass — but a genuine I/O error must not silently yield a partial
    // sample (which would produce a spec inferred from a truncated file).
    for item in reader {
        match item {
            Ok(v) => {
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

fn cmd_infer(a: InferArgs) -> Result<()> {
    let dataset = a.dataset.clone().unwrap_or_else(|| stem_of(&a.input));
    let samples = sample_records(&a.input, a.format.into(), a.sample)?;
    if samples.is_empty() {
        bail!("no records read from {}", a.input.display());
    }
    let columns = infer_columns(samples.iter(), !a.no_flatten);
    let spec = DatasetSpec {
        dataset: dataset.clone(),
        source: SourceSpec {
            format: a.format.into(),
            path: Some(a.input.to_string_lossy().into_owned()),
        },
        output: OutputSpec {
            format: OutputFormat::Parquet,
            path: format!("{dataset}.parquet"),
            compression: Compression::Snappy,
            partition_by: Vec::new(),
        },
        schema: SchemaMode::Infer,
        columns,
        options: Default::default(),
    };
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
    // 1. Assemble the effective spec (from file, or synthesized from inference).
    let mut spec = match &a.spec {
        Some(p) => {
            let text =
                std::fs::read_to_string(p).with_context(|| format!("reading {}", p.display()))?;
            DatasetSpec::parse(&text).map_err(|e| anyhow!("{e}"))?
        }
        None => {
            let input = a
                .input
                .clone()
                .ok_or_else(|| anyhow!("--input is required when no --spec is given"))?;
            let output = a
                .output
                .clone()
                .ok_or_else(|| anyhow!("--output is required when no --spec is given"))?;
            let dataset = a.dataset.clone().unwrap_or_else(|| stem_of(&input));
            DatasetSpec {
                dataset,
                source: SourceSpec {
                    format: a.format.into(),
                    path: Some(input.to_string_lossy().into_owned()),
                },
                output: OutputSpec {
                    format: a.to.map(Into::into).unwrap_or(OutputFormat::Parquet),
                    path: output,
                    compression: a.compression.map(Into::into).unwrap_or(Compression::Snappy),
                    partition_by: Vec::new(),
                },
                schema: SchemaMode::Infer,
                columns: Vec::new(),
                options: Default::default(),
            }
        }
    };

    // 2. Apply CLI overrides onto a spec loaded from file.
    if let Some(input) = &a.input {
        spec.source.path = Some(input.to_string_lossy().into_owned());
    }
    if let Some(output) = &a.output {
        spec.output.path = output.clone();
    }
    if let Some(fmt) = a.to {
        spec.output.format = fmt.into();
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
    spec.validate().map_err(|e| anyhow!("{e}"))?;

    // 3. Resolve the input path.
    let input =
        spec.source.path.clone().ok_or_else(|| {
            anyhow!("no input path (set source.path in the spec or pass --input)")
        })?;
    let input = PathBuf::from(input);
    let src_format = spec.source.format;

    if !spec.output.partition_by.is_empty() && spec.output.format != OutputFormat::Iceberg {
        bail!("`partition_by` applies only to Iceberg tables (--to iceberg)");
    }
    if a.append && spec.output.format != OutputFormat::Iceberg {
        bail!("--append applies only to Iceberg tables (--to iceberg)");
    }

    // 4. Build the shaper (inferring in Infer mode).
    let inferred = if spec.schema == SchemaMode::Infer {
        let samples = sample_records(&input, src_format, spec.options.infer_sample)?;
        infer_columns(samples.iter(), spec.options.flatten)
    } else {
        Vec::new()
    };
    let mut shaper = Shaper::from_spec(&spec, &inferred).map_err(|e| anyhow!("{e}"))?;
    let schema = shaper.schema();

    // 5. Resolve the output + create the sink.
    let output_is_url = looks_like_url(&spec.output.path);
    let (mut sink, primary): (Box<dyn Sink>, PathBuf) = match spec.output.format {
        OutputFormat::Parquet if output_is_url => {
            let s =
                objstore_parquet_sink(&spec.output.path, schema.clone(), spec.output.compression)?;
            (s, PathBuf::from(&spec.output.path))
        }
        OutputFormat::Parquet => {
            let file = resolve_parquet_path(&spec.output.path, &spec.dataset);
            let s = ParquetSink::create(&file, schema.clone(), spec.output.compression)
                .map_err(|e| anyhow!("{e}"))?;
            (Box::new(s), file)
        }
        OutputFormat::Iceberg if output_is_url => {
            let s = objstore_iceberg_sink(
                &spec.output.path,
                schema.clone(),
                spec.output.compression,
                a.append,
                &spec.output.partition_by,
            )?;
            (s, PathBuf::from(&spec.output.path))
        }
        OutputFormat::Iceberg => {
            let dir = PathBuf::from(&spec.output.path);
            let s = IcebergSink::create(
                &dir,
                schema.clone(),
                spec.output.compression,
                a.append,
                &spec.output.partition_by,
            )
            .map_err(|e| anyhow!("{e}"))?;
            (Box::new(s), dir)
        }
    };

    // 6. Stream + shape, sidecarring source parse errors.
    let reader = open_reader(&input, src_format).map_err(|e| anyhow!("{e}"))?;
    // The reject sidecar is always local. For a URL output, name it after the
    // dataset in the cwd (a `<url>.rejects.jsonl` local file can't be created).
    let reject_path = if output_is_url {
        PathBuf::from(format!("{}.rejects.jsonl", spec.dataset))
    } else {
        PathBuf::from(format!("{}.rejects.jsonl", spec.output.path))
    };
    // Start clean: a stale sidecar from a previous run must not sit beside fresh
    // output (a clean rerun would otherwise leave old rejects behind).
    match std::fs::remove_file(&reject_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("removing {}", reject_path.display())),
    }
    let mut rejects: Option<std::io::BufWriter<std::fs::File>> = None;
    let rg = spec.options.row_group_rows.max(1);

    let mut rows_in = 0u64;
    let mut rows_out = 0u64;
    let mut rows_rejected = 0u64;
    let mut parse_errors = 0u64;
    let mut batches = 0u64;

    for item in reader {
        match item {
            Ok(v) => {
                rows_in += 1;
                match shaper.push(&v).map_err(|e| anyhow!("{e}"))? {
                    PushOutcome::Appended => rows_out += 1,
                    PushOutcome::Rejected(reason) => {
                        rows_rejected += 1;
                        tracing::debug!(reason, "row rejected");
                        // A valid JSON row that failed shaping (e.g. a required column
                        // was null) is preserved for replay, like a parse error — a
                        // lenient run never silently drops data.
                        write_reject(
                            &mut rejects,
                            &reject_path,
                            &serde_json::json!({"error": reason, "raw": v}),
                        )?;
                    }
                }
                if shaper.pending() >= rg {
                    if let Some(batch) = shaper.flush().map_err(|e| anyhow!("{e}"))? {
                        sink.write_batch(&batch).map_err(|e| anyhow!("{e}"))?;
                        batches += 1;
                    }
                }
            }
            Err(JsonError::Parse { line, message, raw }) => {
                parse_errors += 1;
                write_reject(
                    &mut rejects,
                    &reject_path,
                    &serde_json::json!({"line": line, "error": message, "raw": raw}),
                )?;
            }
            Err(JsonError::Io(e)) => return Err(anyhow!("read error: {e}")),
        }
    }
    if let Some(batch) = shaper.flush().map_err(|e| anyhow!("{e}"))? {
        sink.write_batch(&batch).map_err(|e| anyhow!("{e}"))?;
        batches += 1;
    }
    if let Some(mut w) = rejects {
        // Fail closed: a lost flush must not report success.
        w.flush()
            .with_context(|| format!("flushing {}", reject_path.display()))?;
    }
    let summary: SinkSummary = sink.finish().map_err(|e| anyhow!("{e}"))?;

    // 7. Report.
    println!(
        "shaped `{}` → {} ({:?})",
        spec.dataset,
        primary.display(),
        spec.output.format
    );
    println!(
        "rows_in={rows_in} rows_out={rows_out} rejected={rows_rejected} parse_errors={parse_errors} \
         row_groups={batches} bytes={}",
        summary.bytes
    );
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
    }
    Ok(())
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
) -> Result<Box<dyn Sink>> {
    let s = shapeshift_objstore::ObjectStoreParquetSink::create(url, schema, compression)
        .map_err(|e| anyhow!("{e}"))?;
    Ok(Box::new(s))
}

#[cfg(not(feature = "object_store"))]
fn objstore_parquet_sink(
    url: &str,
    _schema: shapeshift_core::SchemaRef,
    _compression: Compression,
) -> Result<Box<dyn Sink>> {
    bail!(
        "object-store output ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

/// Build an object-store Iceberg sink for a URL `--output` (the table location).
/// Feature-gated like the Parquet variant, so the default binary links no cloud SDKs.
#[cfg(feature = "object_store")]
fn objstore_iceberg_sink(
    url: &str,
    schema: shapeshift_core::SchemaRef,
    compression: Compression,
    append: bool,
    partition_by: &[String],
) -> Result<Box<dyn Sink>> {
    let s = shapeshift_objstore::ObjectStoreIcebergSink::create(
        url,
        schema,
        compression,
        append,
        partition_by,
    )
    .map_err(|e| anyhow!("{e}"))?;
    Ok(Box::new(s))
}

#[cfg(not(feature = "object_store"))]
fn objstore_iceberg_sink(
    url: &str,
    _schema: shapeshift_core::SchemaRef,
    _compression: Compression,
    _append: bool,
    _partition_by: &[String],
) -> Result<Box<dyn Sink>> {
    bail!(
        "object-store Iceberg output ({url}) needs a build with `--features object_store` \
         (kept out of the default musl-static binary so it never links the cloud SDKs)"
    )
}

/// Summarize an Iceberg table living in an object store (`inspect <url>`). Feature-gated
/// like the sinks.
#[cfg(feature = "object_store")]
fn objstore_inspect_iceberg(url: &str) -> Result<shapeshift_iceberg::IcebergInfo> {
    shapeshift_objstore::inspect_iceberg(url).map_err(|e| anyhow!("{e}"))
}

#[cfg(not(feature = "object_store"))]
fn objstore_inspect_iceberg(url: &str) -> Result<shapeshift_iceberg::IcebergInfo> {
    bail!(
        "inspecting an object-store table ({url}) needs a build with `--features object_store` \
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
    println!("  current-snapshot-id: {}", info.current_snapshot_id);
    println!("  total-records: {}", info.total_records);
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
        let info = shapeshift_iceberg::inspect(&a.path).map_err(|e| anyhow!("{e}"))?;
        print_iceberg_info(&a.path.display().to_string(), &info);
    } else {
        let info = shapeshift_parquet::inspect(&a.path).map_err(|e| anyhow!("{e}"))?;
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

fn cmd_cost(a: CostArgs) -> Result<()> {
    let rows = match (a.rows, &a.input) {
        (Some(r), _) => r,
        (None, Some(input)) => {
            let reader = open_reader(input, a.format.into()).map_err(|e| anyhow!("{e}"))?;
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
        Some(frac) => println!("saved:      ${:.2}  ({:.1}%)", report.saved, frac * 100.0),
        None => println!("saved:      ${:.2}", report.saved),
    }
    Ok(())
}
