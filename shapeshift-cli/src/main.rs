//! `shapeshift` — infer a dataset spec from sample JSON, shape JSONL/JSON into
//! Parquet or an Iceberg table, inspect the output, and price a run against
//! Fivetran-style MAR billing.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::Value;

use shapeshift_core::{
    estimate_mar, infer_columns, ColumnSpec, Compression, DatasetSpec, DriftPolicy, DriftReport,
    MarInputs, OutputFormat, OutputSpec, PushOutcome, Record, RecordBatch, SchemaMode, Shaper,
    Sink, SinkSummary, SourceFormat, SourceSpec,
};
use shapeshift_iceberg::IcebergSink;
use shapeshift_json::{
    is_rereadable, is_stdin, open_reader, JsonError, TapeJsonArrayReader, TapeJsonlReader,
    TapeRecord, TapeSource,
};
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
    /// Count billable rows by streaming this input, or `-` for standard input
    /// (alternative to --rows).
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
    if is_stdin(path) {
        return "stdin".to_string();
    }
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
fn infer_and_chain(
    path: &Path,
    format: SourceFormat,
    n: usize,
    flatten: bool,
) -> Result<(Vec<ColumnSpec>, ShapeSource)> {
    let tape = |what: &str| {
        open_tape_reader(path, format).map_err(|e| anyhow!("{what} {}: {e}", display_input(path)))
    };

    if is_rereadable(path) {
        // A file is sampled and then reopened, exactly as before — that is what keeps
        // `infer_sample: 0` bounded and lets inference scan past a malformed head. The
        // shaping pass that follows is the one that gets the tape.
        let samples = sample_records(path, format, n)?;
        let columns = infer_columns(samples.iter(), flatten);
        return Ok((
            columns,
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
    let (mut good, mut rejects) = (0usize, 0usize);
    while good < n && rejects < MAX_REPLAY_REJECTS {
        match reader.next_owned() {
            None => break,
            Some(Ok(v)) => {
                good += 1;
                prefix.push(Ok(v));
            }
            // Inference skips a malformed line; shaping still reports it, so it stays.
            Some(Err(e @ JsonError::Parse { .. })) => {
                rejects += 1;
                prefix.push(Err(e));
            }
            Some(Err(JsonError::Io(e))) => {
                return Err(anyhow!("reading {}: {e}", display_input(path)))
            }
        }
    }
    if good == 0 && rejects >= MAX_REPLAY_REJECTS {
        bail!(
            "the first {rejects} records from {} were all unparseable, so there is nothing to \
             infer a schema from — is the input really {:?}? Pass a spec with `schema: strict` \
             to shape it anyway.",
            display_input(path),
            format
        );
    }
    // Infer straight off the buffered records — no second copy of the sample.
    let columns = infer_columns(prefix.iter().filter_map(|r| r.as_ref().ok()), flatten);
    Ok((columns, ShapeSource { reader, prefix }))
}

/// Open the tape-backed reader for `path` (or standard input) in `format`.
///
/// The array reader validates the opening `[` eagerly, so a document that is not an
/// array fails here — at open time, naming the file — rather than at the first record.
fn open_tape_reader(path: &Path, format: SourceFormat) -> Result<Box<dyn TapeSource + Send>> {
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
    Ok(match format {
        SourceFormat::JsonArray => Box::new(
            TapeJsonArrayReader::new(inner)
                .map_err(|e| anyhow!("reading {}: {e}", display_input(path)))?,
        ),
        SourceFormat::Jsonl => Box::new(TapeJsonlReader::new(inner)),
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
                return Err(anyhow!("{e}"));
            }
        };
        match outcome {
            PushOutcome::Appended => self.rows_out += 1,
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
            if let Some(batch) = self.shaper.flush().map_err(|e| anyhow!("{e}"))? {
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
    fn finish(&mut self) -> Result<SinkSummary>;
}

/// The single-threaded path: encode and write inline, exactly as before.
struct DirectSink(Box<dyn Sink + Send>);

impl ShapeWriter for DirectSink {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.0.write_batch(&batch).map_err(|e| anyhow!("{e}"))
    }
    fn finish(&mut self) -> Result<SinkSummary> {
        self.0.finish().map_err(|e| anyhow!("{e}"))
    }
}

impl ShapeWriter for PipelinedSink {
    fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        PipelinedSink::write_batch(self, batch)
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
                    WriterMsg::Batch(b) => sink.write_batch(&b).map_err(|e| anyhow!("{e}"))?,
                    // Asked to commit: close the footer / write the table metadata.
                    WriterMsg::Finish => {
                        return sink.finish().map(Some).map_err(|e| anyhow!("{e}"))
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
                return sink.write_batch(&batch).map_err(|e| anyhow!("{e}"));
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
                Some(_) => sink.finish().map(Some).map_err(|e| anyhow!("{e}")),
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
        drift: Default::default(),
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
                drift: Default::default(),
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
    if let Some(p) = a.on_drift {
        spec.drift.policy = p.into();
    }
    if let Some(c) = &a.rescue_column {
        spec.drift.rescue_column = c.clone();
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

    // 4. Build the shaper (inferring in Infer mode). The source is opened **once**:
    // inference reads the head and hands back a reader that still yields every record,
    // so a pipe or `-` shapes the same rows a regular file would.
    let (inferred, reader) = if spec.schema == SchemaMode::Infer {
        let (columns, reader) = infer_and_chain(
            &input,
            src_format,
            spec.options.infer_sample,
            spec.options.flatten,
        )?;
        (columns, Some(reader))
    } else {
        (Vec::new(), None)
    };
    let mut shaper = Shaper::from_spec(&spec, &inferred).map_err(|e| anyhow!("{e}"))?;
    let schema = shaper.schema();

    // 5. Resolve the output + create the sink.
    let output_is_url = looks_like_url(&spec.output.path);
    let (sink, primary): (Box<dyn Sink + Send>, PathBuf) = match spec.output.format {
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

    // 6. Stream + shape, sidecarring source parse errors. In Infer mode the reader is
    // the one inference already started; in Strict mode nothing has read the source yet.
    let reader = match reader {
        Some(r) => r,
        // Strict mode: nothing has read the source yet, so it goes straight onto the
        // tape with no prefix to replay.
        None => ShapeSource {
            reader: open_tape_reader(&input, src_format)?,
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
    if let Some(batch) = shaper.flush().map_err(|e| anyhow!("{e}"))? {
        sink.write_batch(batch)?;
        batches += 1;
    }
    if let Some(mut w) = rejects {
        // Fail closed: a lost flush must not report success.
        w.flush()
            .with_context(|| format!("flushing {}", reject_path.display()))?;
    }
    let summary: SinkSummary = sink.finish()?;

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
    if let Some(report) = shaper.into_drift_report() {
        if !report.is_clean() {
            print_drift(&report);
            write_drift_report(&drift_path, &spec.dataset, &report)?;
            println!("full drift report → {}", drift_path.display());
        }
    }
    Ok(())
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
        .map_err(|e| anyhow!("{e}"))?;
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

/// Build an object-store Iceberg sink for a URL `--output` (the table location).
/// Feature-gated like the Parquet variant, so the default binary links no cloud SDKs.
#[cfg(feature = "object_store")]
fn objstore_iceberg_sink(
    url: &str,
    schema: shapeshift_core::SchemaRef,
    compression: Compression,
    append: bool,
    partition_by: &[String],
) -> Result<Box<dyn Sink + Send>> {
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
) -> Result<Box<dyn Sink + Send>> {
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
