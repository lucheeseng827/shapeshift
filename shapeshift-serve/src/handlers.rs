//! The console's JSON API — thin wrappers over the OSS shaper's verbs
//! (`infer`, `shape`, `inspect`), plus the run log and table history. Every handler reuses the exact engine
//! the CLI uses; nothing here is a second implementation of the shaping logic.
//!
//! The server is single-user and keeps nothing beyond the files it writes: shaped
//! output and reject sidecars land under the configured data directory, `inspect`
//! reads them (or any local table) back, and every shape is a run in the run log
//! (`runs.jsonl`), whose snapshot carries the run's id. `runs`, `tables` and `table`
//! read those back; a table's history comes from its own metadata. Named specs live in
//! [`crate::datasets`]. There is no run queue and no metering — that is orchestration, and
//! out of scope here.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use shapeshift_core::{
    infer_columns, Compression, DatasetSpec, DriftPolicy, DriftReport, OutputFormat, OutputSpec,
    Predicate, PushOutcome, SchemaMode, Shaper, SourceFormat, SourceSpec,
};
use shapeshift_iceberg::{parse_metadata_json, read_local_history, table_events, IcebergSink};
use shapeshift_json::{open_source, JsonError};
use shapeshift_parquet::ParquetSink;

use crate::alerts::Alerts;
use crate::catalog::{CatalogCache, CatalogSettings};
use crate::connectors::Connectors;
use crate::datasets::Datasets;
use crate::http::{Request, Response};
use crate::runs::{Run, RunLog};

/// Shared server state: where output lands, plus a monotonic counter for unique
/// temp-file names (so concurrent requests never clobber each other's input).
pub struct Ctx {
    data_dir: PathBuf,
    counter: AtomicU64,
    /// Datasets may pull from sources (`serve --allow-sources`).
    allow_sources: std::sync::atomic::AtomicBool,
    /// The runs this console started (`runs.jsonl` in the data dir).
    runs: RunLog,
    /// The datasets it keeps (`datasets/` in the data dir).
    datasets: Datasets,
    /// Alert rules, targets and incidents (`alerts.json` in the data dir).
    alerts: Alerts,
    /// Push connectors (`connectors.json`, and their spools under `spool/`).
    connectors: Connectors,
    /// The REST catalog the Catalog page browses, if the console was started with one.
    catalog: std::sync::OnceLock<CatalogSettings>,
    /// Its last listing (a listing walks the whole catalog).
    catalog_cache: CatalogCache,
    /// The console's metrics, when `--metrics-addr` asked for them.
    metrics: std::sync::OnceLock<std::sync::Arc<crate::metrics::Live>>,
}

/// Cap on reject rows returned inline to the console (all rows are still counted and
/// written to the sidecar file — this only bounds the JSON payload).
const REJECT_SAMPLE_CAP: usize = 200;

impl Ctx {
    /// Create the context and ensure the `tmp/` and `outputs/` working dirs exist.
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(data_dir.join("tmp"))
            .with_context(|| format!("creating {}/tmp", data_dir.display()))?;
        std::fs::create_dir_all(data_dir.join("outputs"))
            .with_context(|| format!("creating {}/outputs", data_dir.display()))?;
        let runs = RunLog::open(&data_dir)?;
        let datasets = Datasets::open(&data_dir)?;
        let alerts = Alerts::open(&data_dir)?;
        let connectors = Connectors::open(&data_dir)?;
        Ok(Ctx {
            data_dir,
            counter: AtomicU64::new(0),
            allow_sources: std::sync::atomic::AtomicBool::new(false),
            runs,
            datasets,
            alerts,
            connectors,
            catalog: std::sync::OnceLock::new(),
            catalog_cache: CatalogCache::default(),
            metrics: std::sync::OnceLock::new(),
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Keep the console's metrics from now on (`--metrics-addr`).
    pub(crate) fn enable_metrics(&self) -> std::sync::Arc<crate::metrics::Live> {
        let live = self
            .metrics
            .get_or_init(|| std::sync::Arc::new(crate::metrics::Live::new()))
            .clone();
        let _ = self.runs.metrics.set(live.clone());
        live
    }

    pub(crate) fn metrics(&self) -> Option<&crate::metrics::Live> {
        self.metrics.get().map(|m| m.as_ref())
    }

    pub(crate) fn runs(&self) -> &RunLog {
        &self.runs
    }

    pub(crate) fn datasets(&self) -> &Datasets {
        &self.datasets
    }

    pub(crate) fn alerts(&self) -> &Alerts {
        &self.alerts
    }

    pub(crate) fn connectors(&self) -> &Connectors {
        &self.connectors
    }

    pub(crate) fn catalog(&self) -> Option<&CatalogSettings> {
        self.catalog.get()
    }

    pub(crate) fn catalog_cache(&self) -> &CatalogCache {
        &self.catalog_cache
    }

    pub(crate) fn set_catalog(&self, s: CatalogSettings) {
        let _ = self.catalog.set(s);
    }

    pub(crate) fn set_allow_sources(&self, allow: bool) {
        self.allow_sources.store(allow, Ordering::SeqCst);
    }

    /// Whether datasets may pull from the sources their specs name (`serve --allow-sources`).
    pub(crate) fn allow_sources(&self) -> bool {
        self.allow_sources.load(Ordering::SeqCst)
    }

    fn tmp_dir(&self) -> PathBuf {
        self.data_dir.join("tmp")
    }

    fn out_dir(&self) -> PathBuf {
        self.data_dir.join("outputs")
    }

    fn next_id(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Resolve a user-supplied path: absolute as-is, relative against the data dir.
    fn resolve(&self, p: &str) -> PathBuf {
        let path = PathBuf::from(p);
        if path.is_absolute() {
            path
        } else {
            self.data_dir.join(path)
        }
    }
}

// ---- infer ------------------------------------------------------------------

#[derive(Deserialize)]
struct InferReq {
    /// Inline records in the selected format (JSONL or a JSON array).
    #[serde(default)]
    input: String,
    #[serde(default)]
    format: SourceFormat,
    #[serde(default = "default_true")]
    flatten: bool,
    #[serde(default = "default_sample")]
    sample: usize,
    #[serde(default)]
    dataset: Option<String>,
}

#[derive(Serialize)]
struct InferResp {
    dataset: String,
    /// The inferred spec, ready to edit and hand back to `/api/shape`.
    spec_yaml: String,
    columns: Vec<Value>,
    sampled: usize,
}

pub fn infer(ctx: &Ctx, req: &Request) -> Response {
    let r: InferReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    match do_infer(ctx, r) {
        Ok(resp) => Response::json(200, &resp),
        Err(e) => Response::error(400, e.to_string()),
    }
}

fn do_infer(ctx: &Ctx, r: InferReq) -> Result<InferResp> {
    if r.input.trim().is_empty() {
        bail!("no input — paste some records to infer a spec");
    }
    if r.format == SourceFormat::Xml {
        // The form has no field for the record path yet: say so, not the CLI's flag.
        bail!("xml input needs a record path, which only a spec can give here: shape it with a spec whose `source.xml.record` names the element that is one record");
    }
    let dataset = clean_dataset(r.dataset.as_deref());
    let input_path = write_temp_input(ctx, &r.input, r.format)?;
    let samples = sample_records(&input_path, &SourceSpec::of(r.format), r.sample, None)?;
    if samples.is_empty() {
        bail!("no well-formed records were read from the input");
    }
    let columns = infer_columns(samples.iter(), r.flatten);
    let spec = DatasetSpec {
        dataset: dataset.clone(),
        filter: None,
        changes: None,
        source: SourceSpec::of(r.format),
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
    };
    let spec_yaml = spec.to_yaml().map_err(|e| anyhow!("{e}"))?;
    let columns = spec
        .columns
        .iter()
        .map(|c| serde_json::to_value(c).unwrap_or(Value::Null))
        .collect();
    // The temp input has served its purpose.
    let _ = std::fs::remove_file(&input_path);
    Ok(InferResp {
        dataset,
        spec_yaml,
        columns,
        sampled: samples.len(),
    })
}

// ---- shape ------------------------------------------------------------------

#[derive(Deserialize)]
pub(crate) struct ShapeReq {
    /// Inline records; ignored when `input_path` is given.
    #[serde(default)]
    pub(crate) input: Option<String>,
    /// A server-side input file to shape instead of inline text (for large inputs).
    #[serde(default)]
    pub(crate) input_path: Option<String>,
    /// An edited spec (YAML/JSON). When omitted, the schema is inferred on the fly.
    #[serde(default)]
    pub(crate) spec_yaml: Option<String>,
    #[serde(default)]
    pub(crate) format: SourceFormat,
    #[serde(default = "default_output_format")]
    pub(crate) to: OutputFormat,
    #[serde(default)]
    pub(crate) compression: Compression,
    #[serde(default)]
    pub(crate) dataset: Option<String>,
    #[serde(default)]
    pub(crate) partition_by: Vec<String>,
    #[serde(default)]
    pub(crate) append: bool,
    /// What to do about schema drift. Overrides the spec's `drift.policy`; absent
    /// means "whatever the spec says" (which defaults to `warn`).
    #[serde(default)]
    pub(crate) on_drift: Option<DriftPolicy>,
    /// The run id the snapshot records, when the caller keeps one for a retry (a connector's
    /// batch): a run the table already holds is found and not appended twice. Never read from
    /// a request; absent, the run log's own id.
    #[serde(skip)]
    pub(crate) run_id: Option<String>,
}

#[derive(Serialize)]
struct RejectRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    line: Option<u64>,
    error: String,
    raw: Value,
}

/// One drifted path, flattened for the console.
#[derive(Serialize)]
struct DriftRow {
    path: String,
    count: u64,
    first_record: u64,
    suggested_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    declared_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    example: Option<Value>,
}

/// The run's drift story, trimmed to [`DRIFT_ROW_CAP`] rows per kind: the console shows
/// a summary and the engine's report carries the rest. Note that "the rest" is itself
/// bounded — the engine tracks at most `MAX_TRACKED_PATHS` distinct paths — so when
/// `truncated` is set neither this response nor the engine's report is a complete
/// inventory of what drifted.
#[derive(Serialize)]
struct DriftResp {
    policy: String,
    rows_scanned: u64,
    rows_with_drift: u64,
    rows_quarantined: u64,
    rows_rescued: u64,
    /// The engine hit its per-run path cap: the `_total` counts below are how many
    /// distinct paths were *tracked*, not how many drifted.
    truncated: bool,
    new_fields_total: usize,
    type_mismatches_total: usize,
    new_fields: Vec<DriftRow>,
    type_mismatches: Vec<DriftRow>,
    /// The columns to add to the spec to stop losing the new fields, as YAML.
    #[serde(skip_serializing_if = "Option::is_none")]
    suggested_columns_yaml: Option<String>,
}

/// Drifted paths the console shows per kind before deferring to the report.
const DRIFT_ROW_CAP: usize = 25;

fn drift_resp(r: &DriftReport) -> DriftResp {
    let new_fields = r
        .new_fields
        .iter()
        .take(DRIFT_ROW_CAP)
        .map(|(path, f)| DriftRow {
            path: path.clone(),
            count: f.count,
            first_record: f.first_record,
            suggested_type: f.suggested_type.to_string(),
            declared_type: None,
            example: f.examples.first().cloned(),
        })
        .collect();
    let type_mismatches = r
        .type_mismatches
        .iter()
        .take(DRIFT_ROW_CAP)
        .map(|(col, m)| DriftRow {
            path: col.clone(),
            count: m.count,
            first_record: m.first_record,
            suggested_type: m.suggested_type.to_string(),
            declared_type: Some(m.declared_type.to_string()),
            example: m.examples.first().cloned(),
        })
        .collect();
    DriftResp {
        policy: r.policy.to_string(),
        rows_scanned: r.rows_scanned,
        rows_with_drift: r.rows_with_drift,
        rows_quarantined: r.rows_quarantined,
        rows_rescued: r.rows_rescued,
        truncated: r.truncated,
        new_fields_total: r.new_fields.len(),
        type_mismatches_total: r.type_mismatches.len(),
        new_fields,
        type_mismatches,
        suggested_columns_yaml: (!r.new_fields.is_empty())
            .then(|| serde_yaml::to_string(&r.suggested_columns()).ok())
            .flatten(),
    }
}

#[derive(Serialize)]
struct ShapeResp {
    /// This run's id in the run log, also recorded in the snapshot it commits.
    run_id: String,
    dataset: String,
    format: String,
    output_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_path: Option<String>,
    rows_in: u64,
    rows_out: u64,
    rejected: u64,
    /// Records the spec's `where` did not keep (0 without one).
    filtered: u64,
    /// Records in which the `where` compared values of different kinds, which is never
    /// true (`status = '500'` against a number), and the first such comparison.
    #[serde(skip_serializing_if = "Option::is_none")]
    type_mismatches: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_mismatch: Option<String>,
    parse_errors: u64,
    row_groups: u64,
    bytes: u64,
    rejects_total: u64,
    rejects_path: Option<String>,
    rejects_sample: Vec<RejectRow>,
    /// The snapshot an Iceberg write committed (a string: it is 64-bit), and the version.
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    table_version: Option<u64>,
    /// The table already held this run's snapshot, so nothing was written.
    earlier_attempt: bool,
    /// What the source did that the schema does not cover — absent under
    /// `drift.policy: ignore`, or when the run matched its schema exactly.
    #[serde(skip_serializing_if = "Option::is_none")]
    drift: Option<DriftResp>,
    /// A pulled source: its kind, the position it started from and committed, and its counts.
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<Value>,
}

pub fn shape(ctx: &Ctx, req: &Request) -> Response {
    let r: ShapeReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    run_shape(ctx, r, json!({ "via": "shape" }))
}

/// Run one shape, as a run in the log: the Shape form's, or a dataset's `Run now`. `extra` is
/// added to the run's record (`via`: which of the two started it).
pub(crate) fn run_shape(ctx: &Ctx, r: ShapeReq, extra: Value) -> Response {
    // Every shape is a run in the log, from the moment it starts.
    let mut fields = json!({
        "command": "shape",
        "dataset": clean_dataset(r.dataset.as_deref()),
        "format": format!("{:?}", r.to).to_lowercase(),
        "append": r.append,
    });
    if let (Some(f), Some(x)) = (fields.as_object_mut(), extra.as_object()) {
        f.extend(x.clone());
    }
    let mut run = ctx.runs.start(fields, "shaping");
    let resp = match do_shape(ctx, r, &mut run) {
        Ok(resp) => {
            run.succeed(json!({
                "rows_in": resp.rows_in,
                "rows_out": resp.rows_out,
                "rejected": resp.rejected,
                "parse_errors": resp.parse_errors,
                "row_groups": resp.row_groups,
                "bytes": resp.bytes,
                "snapshot_id": resp.snapshot_id,
                "table_version": resp.table_version,
                "rejects_path": resp.rejects_path,
                "earlier_attempt": resp.earlier_attempt,
                "source": resp.source,
            }));
            Response::json(200, &resp)
        }
        Err(e) => {
            let message = format!("{e:#}");
            run.fail(&e);
            Response::error(400, message)
        }
    };
    // A run's outcome can open or resolve an incident: evaluate the rules now.
    ctx.alerts.poke();
    resp
}

fn do_shape(ctx: &Ctx, r: ShapeReq, run: &mut Run<'_>) -> Result<ShapeResp> {
    let dataset = clean_dataset(r.dataset.as_deref());

    // 1. Assemble the effective spec (parsed, or synthesized for on-the-fly inference).
    let mut spec = match r
        .spec_yaml
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(text) => DatasetSpec::parse(text).map_err(anyhow::Error::from)?,
        None if r.format == SourceFormat::Xml => bail!(
            "xml input needs a record path, which only a spec can give here: add a spec whose \
             `source.xml.record` names the element that is one record"
        ),
        None => DatasetSpec {
            dataset: dataset.clone(),
            filter: None,
            changes: None,
            source: SourceSpec::of(r.format),
            output: OutputSpec {
                format: r.to,
                path: String::new(),
                compression: r.compression,
                partition_by: Vec::new(),
                merge_on: Vec::new(),
                catalog: None,
                apply: None,
            },
            schema: SchemaMode::Infer,
            columns: Vec::new(),
            drift: Default::default(),
            options: Default::default(),
        },
    };

    // 2. Apply the console's controls onto the spec (they always win — the form is
    //    the source of truth for where/how the output lands).
    spec.dataset = dataset.clone();
    // A pulled source (an endpoint, a database) has no file format for the form to set.
    let pulling = crate::pulled::is_pulled(&spec.source);
    if pulling && !ctx.allow_sources() {
        bail!(
            "this console pulls from no sources: a spec's source names a host and the \
             environment variables whose values it sends there, and the console has no login. \
             Start it with `shapeshift serve --allow-sources` where only you can reach it"
        );
    }
    if !pulling {
        spec.source.format = r.format;
    }
    // A spec's CSV or XML options must still fit the format the form chose.
    spec.source.validate().map_err(anyhow::Error::from)?;
    if spec.changes.is_some() {
        bail!(
            "a change feed (`changes:`) is applied from the command line (`shapeshift shape`) in \
             this release; the console shapes records as they are"
        );
    }
    spec.output.format = r.to;
    spec.output.compression = r.compression;
    if !r.partition_by.is_empty() {
        spec.output.partition_by = r
            .partition_by
            .iter()
            .flat_map(|s| split_partition_entries(s))
            .collect();
    }
    if let Some(p) = r.on_drift {
        spec.drift.policy = p;
    }

    // 3. Compute a managed output path under the data dir (never a caller-chosen path).
    let (output_path, is_iceberg) = match spec.output.format {
        OutputFormat::Parquet => (ctx.out_dir().join(format!("{dataset}.parquet")), false),
        OutputFormat::Iceberg => (ctx.out_dir().join(&dataset), true),
    };
    spec.output.path = output_path.to_string_lossy().into_owned();
    spec.validate()
        .map_err(|e| anyhow::Error::from(e).context("spec"))?;
    run.set(
        "output",
        json!(output_path
            .strip_prefix(ctx.data_dir())
            .unwrap_or(&output_path)
            .to_string_lossy()),
    );
    if !spec.output.merge_on.is_empty() {
        run.set("merge_on", json!(spec.output.merge_on));
    }

    if !spec.output.partition_by.is_empty() && !is_iceberg {
        bail!("`partition_by` applies only to Iceberg output");
    }
    if r.append && !is_iceberg {
        bail!("`append` applies only to Iceberg output");
    }

    let merging = !spec.output.merge_on.is_empty();
    if pulling && crate::pulled::incremental(&spec.source) && !r.append && !merging {
        bail!(
            "an incremental source keeps its position in the table, which replacing the table \
             would drop: turn on Append, or merge on a key (`output.merge_on`)"
        );
    }

    // 4. Resolve the input: a server-side path, inline text written to a temp file, or (a
    //    pulled source) nothing: the records come from the source.
    let (input_path, temp_input) = match r.input_path.as_deref().filter(|s| !s.is_empty()) {
        _ if pulling => (PathBuf::new(), false),
        Some(p) => (ctx.resolve(p), false),
        None => {
            let text = r
                .input
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| anyhow!("provide `input` records or an `input_path`"))?;
            (write_temp_input(ctx, text, r.format)?, true)
        }
    };

    // A pulled source is opened once, from the position its table records; inference reads
    // the head of its records, which the shaping pass then replays.
    let (pulled, mut pulled_records) = if pulling {
        let (p, records) = crate::pulled::open(&spec, &output_path)?
            .ok_or_else(|| anyhow!("the spec names no source to pull"))?;
        (Some(p), Some(records))
    } else {
        (None, None)
    };
    let mut pulled_head: Vec<Value> = Vec::new();

    // 5. Build the shaper (inferring in Infer mode).
    let inferred =
        if let (Some(records), SchemaMode::Infer) = (pulled_records.as_mut(), spec.schema) {
            let filter = spec.filter.as_deref().map(Predicate::parse).transpose()?;
            let n = spec.options.infer_sample.max(1);
            // Records the filter drops are held too (the shaping pass counts them), so the
            // head is bounded by what is read, not only by what is kept.
            let max_read = n.saturating_add(10_000);
            let mut kept = Vec::new();
            while kept.len() < n && pulled_head.len() < max_read {
                match records.next() {
                    Some(Ok(v)) => {
                        if filter.as_ref().map_or(true, |f| f.test(&v).keep) {
                            kept.push(v.clone());
                        }
                        pulled_head.push(v);
                    }
                    Some(Err(e)) => return Err(anyhow!("reading the source: {e}")),
                    None => break,
                }
            }
            if let (Some(f), true) = (&filter, kept.is_empty()) {
                bail!(
                    "`where {f}` kept none of the {} records sampled from the source, so there is \
                     no schema to infer: check the expression, or declare every column \
                     (`schema: strict`)",
                    pulled_head.len()
                );
            }
            infer_columns(kept.iter(), spec.options.flatten)
        } else if spec.schema == SchemaMode::Infer {
            // Inference sees only the records the run keeps, so the schema is theirs alone.
            let filter = spec.filter.as_deref().map(Predicate::parse).transpose()?;
            let samples = sample_records(
                &input_path,
                &spec.source,
                spec.options.infer_sample,
                filter.as_ref(),
            )?;
            if let (Some(f), true) = (&filter, samples.is_empty() && spec.columns.is_empty()) {
                bail!(
                "`where {f}` kept none of the records sampled, so there is no schema to infer: \
                 check the expression, or declare the columns"
            );
            }
            infer_columns(samples.iter(), spec.options.flatten)
        } else {
            Vec::new()
        };
    let mut shaper = Shaper::from_spec(&spec, &inferred).map_err(anyhow::Error::from)?;
    let schema = shaper.schema();

    // 6. For a fresh (non-append) Iceberg write, start from a clean table dir so a
    //    rerun never mixes stale data files. Safe: the path is always inside out_dir.
    //    A merge (`output.merge_on` in the spec) upserts into the table that is there,
    //    as the CLI's does, so it never clears it.
    if is_iceberg && !r.append && !merging && output_path.exists() {
        std::fs::remove_dir_all(&output_path)
            .with_context(|| format!("clearing {}", output_path.display()))?;
    }

    // 7. Create the sink (local filesystem only — object-store URLs are CLI-only).
    let mut sink: Box<dyn shapeshift_core::Sink> = if is_iceberg {
        let mut s = if merging {
            IcebergSink::create_merge(
                &output_path,
                schema,
                spec.output.compression,
                &spec.output.partition_by,
                &spec.output.merge_on,
            )
        } else {
            IcebergSink::create(
                &output_path,
                schema,
                spec.output.compression,
                r.append,
                &spec.output.partition_by,
            )
        }
        .map_err(anyhow::Error::from)?;
        // The snapshot records which of this console's runs wrote it.
        let snapshot_run = r.run_id.clone().unwrap_or_else(|| run.id());
        s.set_run_id(&snapshot_run).map_err(anyhow::Error::from)?;
        Box::new(s)
    } else {
        Box::new(
            ParquetSink::create(&output_path, schema, spec.output.compression)
                .map_err(anyhow::Error::from)?,
        )
    };

    // 8. Stream + shape, collecting rejects (a bounded sample for the UI, all of them
    //    to the sidecar file — exactly the CLI's fail-open behavior).
    let reject_path = PathBuf::from(format!("{}.rejects.jsonl", output_path.display()));
    let _ = std::fs::remove_file(&reject_path);
    let mut reject_writer: Option<std::io::BufWriter<std::fs::File>> = None;
    let mut rejects_sample: Vec<RejectRow> = Vec::new();

    let rg = spec.options.row_group_rows.max(1);
    let mut rows_in = 0u64;
    let mut rows_out = 0u64;
    let mut rejected = 0u64;
    let mut parse_errors = 0u64;
    let mut batches = 0u64;

    let reader: shapeshift_json::RecordSource = match pulled_records.take() {
        Some(rest) => Box::new(
            std::mem::take(&mut pulled_head)
                .into_iter()
                .map(Ok)
                .chain(rest),
        ),
        None => open_source(&input_path, &spec.source).map_err(|e| anyhow!("{e}"))?,
    };
    for item in reader {
        match item {
            Ok(v) => {
                rows_in += 1;
                match shaper.push(&v).map_err(anyhow::Error::from)? {
                    PushOutcome::Appended => rows_out += 1,
                    PushOutcome::Filtered | PushOutcome::Skipped => {}
                    PushOutcome::Rejected(reason) => {
                        rejected += 1;
                        let rec = json!({ "error": reason, "raw": v });
                        write_reject(&mut reject_writer, &reject_path, &rec)?;
                        if rejects_sample.len() < REJECT_SAMPLE_CAP {
                            rejects_sample.push(RejectRow {
                                line: None,
                                error: reason,
                                raw: v,
                            });
                        }
                    }
                }
                if shaper.pending() >= rg {
                    if let Some(batch) = shaper.flush().map_err(anyhow::Error::from)? {
                        sink.write_batch(&batch).map_err(anyhow::Error::from)?;
                        batches += 1;
                    }
                }
            }
            Err(JsonError::Parse { line, message, raw }) => {
                parse_errors += 1;
                let rec = json!({ "line": line, "error": message, "raw": raw });
                write_reject(&mut reject_writer, &reject_path, &rec)?;
                if rejects_sample.len() < REJECT_SAMPLE_CAP {
                    rejects_sample.push(RejectRow {
                        line: Some(line),
                        error: message,
                        raw: Value::String(raw),
                    });
                }
            }
            Err(JsonError::Io(e)) => return Err(anyhow!("read error: {e}")),
        }
    }
    if let Some(batch) = shaper.flush().map_err(anyhow::Error::from)? {
        sink.write_batch(&batch).map_err(anyhow::Error::from)?;
        batches += 1;
    }
    if let Some(mut w) = reject_writer {
        use std::io::Write as _;
        w.flush()?;
    }
    // The source's position goes into the same commit as the rows it read.
    let source_report = match &pulled {
        Some(p) => {
            let (end, progress) = p
                .source
                .outcome()
                .ok_or_else(|| anyhow!("the source was not read to the end of its run"))?;
            if let Some(pos) = &end {
                sink.set_source_position(
                    p.source.kind(),
                    &shapeshift_core::recorded_position(pos, p.source.as_ref()),
                )
                .map_err(anyhow::Error::from)?;
            }
            Some(crate::pulled::report(p, &end, &progress))
        }
        None => None,
    };
    run.phase(if is_iceberg { "committing" } else { "closing" });
    let summary = sink.finish().map_err(anyhow::Error::from)?;
    let filter_report = shaper.filter_report().cloned();
    let drift = shaper.into_drift_report();

    if temp_input {
        let _ = std::fs::remove_file(&input_path);
    }

    let metadata_path = if is_iceberg {
        summary
            .files
            .iter()
            .find(|p| p.to_string_lossy().ends_with(".metadata.json"))
            .map(|p| p.to_string_lossy().into_owned())
    } else {
        None
    };
    let rejects_total = rejected + parse_errors;

    Ok(ShapeResp {
        run_id: run.id(),
        dataset,
        format: format!("{:?}", spec.output.format).to_lowercase(),
        output_path: output_path.to_string_lossy().into_owned(),
        metadata_path,
        rows_in,
        rows_out,
        rejected,
        filtered: filter_report.as_ref().map_or(0, |f| f.filtered),
        // Present only for a run with a `where`.
        type_mismatches: filter_report.as_ref().map(|f| f.type_mismatches),
        first_mismatch: filter_report.and_then(|f| f.first_mismatch),
        parse_errors,
        row_groups: batches,
        bytes: summary.bytes,
        rejects_total,
        rejects_path: (rejects_total > 0).then(|| reject_path.to_string_lossy().into_owned()),
        rejects_sample,
        drift: drift.as_ref().filter(|r| !r.is_clean()).map(drift_resp),
        snapshot_id: summary.snapshot_id.map(|id| id.to_string()),
        table_version: summary.table_version,
        earlier_attempt: summary.earlier_attempt,
        source: source_report,
    })
}

// ---- inspect ----------------------------------------------------------------

#[derive(Deserialize)]
struct InspectReq {
    path: String,
}

pub fn inspect(ctx: &Ctx, req: &Request) -> Response {
    let r: InspectReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    match do_inspect(ctx, &r.path) {
        Ok(v) => Response::json(200, &v),
        Err(e) => Response::error(400, e.to_string()),
    }
}

fn do_inspect(ctx: &Ctx, path: &str) -> Result<Value> {
    if path.trim().is_empty() {
        bail!("provide a `path` to a Parquet file or an Iceberg table directory");
    }
    let resolved = ctx.resolve(path.trim());
    if !resolved.exists() {
        bail!("no such path: {}", resolved.display());
    }
    if resolved.is_dir() {
        let info = shapeshift_iceberg::inspect(&resolved).map_err(|e| anyhow!("{e}"))?;
        Ok(json!({
            "kind": "iceberg",
            "path": resolved.to_string_lossy(),
            "format_version": info.format_version,
            "table_uuid": info.table_uuid,
            // A string, like every other snapshot id this API returns: Iceberg's are 64-bit,
            // and a JSON number that large loses its last digits in a JavaScript client.
            "current_snapshot_id": info.current_snapshot_id.to_string(),
            "total_records": info.total_records,
            "position_deletes": info.total_position_deletes,
            "live_records": info.live_records(),
            "columns": info.fields.iter().map(|(n, t)| json!({ "name": n, "type": t })).collect::<Vec<_>>(),
        }))
    } else {
        let info = shapeshift_parquet::inspect(&resolved).map_err(|e| anyhow!("{e}"))?;
        Ok(json!({
            "kind": "parquet",
            "path": resolved.to_string_lossy(),
            "rows": info.rows,
            "row_groups": info.row_groups,
            "columns": info.columns.iter().map(|(n, t)| json!({ "name": n, "type": t })).collect::<Vec<_>>(),
        }))
    }
}

// ---- runs and tables (read-only) ---------------------------------------------

/// The runs this console started, newest first.
pub fn runs(ctx: &Ctx) -> Response {
    Response::json(200, &json!({ "runs": ctx.runs.list() }))
}

/// The Iceberg tables under the data dir's `outputs/`, most recently changed first.
pub fn tables(ctx: &Ctx) -> Response {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(ctx.out_dir()) {
        for e in entries.flatten() {
            let dir = e.path();
            if !dir.join("metadata").is_dir() {
                continue;
            }
            let Ok(Some((current, _))) = read_local_history(&dir, 0) else {
                continue;
            };
            let rel = dir.strip_prefix(ctx.data_dir()).unwrap_or(&dir);
            out.push(table_card(&rel.to_string_lossy(), &current));
        }
    }
    out.sort_by_key(|t| std::cmp::Reverse(t["last_updated_ms"].as_i64().unwrap_or(0)));
    Response::json(200, &json!({ "tables": out }))
}

#[derive(Deserialize)]
struct TableReq {
    path: String,
}

/// One table's history as events, read from its metadata files (a local table directory:
/// one under the data dir's `outputs/`, or any path given).
pub fn table(ctx: &Ctx, req: &Request) -> Response {
    let r: TableReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let dir = ctx.resolve(r.path.trim());
    match read_local_history(&dir, 200) {
        Ok(Some((current, earlier))) => {
            Response::json(200, &describe_table(&r.path, &current, &earlier))
        }
        Ok(None) => Response::error(404, format!("no Iceberg table at {}", dir.display())),
        Err(e) => Response::error(400, format!("{e}")),
    }
}

/// What the console shows of a table: its card, schema, partitioning and history as events,
/// from its current metadata and the earlier versions it could read (none for a table loaded
/// from a catalog, whose history is the snapshots its current metadata lists).
pub(crate) fn describe_table(label: &str, current: &Value, earlier: &[Value]) -> Value {
    let mut card = table_card(label, current);
    let info = parse_metadata_json(&current.to_string()).ok();
    card["schema"] = json!(info
        .map(|i| i
            .fields
            .iter()
            .map(|(n, t)| json!({ "name": n, "type": t }))
            .collect::<Vec<_>>())
        .unwrap_or_default());
    // A partition field is shown as its transform of the source column it reads
    // (`day(event_at)`), not of its own name (`event_at_day`).
    let columns = column_names(current);
    card["partition"] = json!(current["partition-specs"]
        .as_array()
        .and_then(|specs| specs
            .iter()
            .find(|s| s["spec-id"] == current["default-spec-id"]))
        .and_then(|s| s["fields"].as_array())
        .map(|f| f
            .iter()
            .map(|f| format!(
                "{}({})",
                f["transform"].as_str().unwrap_or("?"),
                f["source-id"]
                    .as_i64()
                    .and_then(|id| columns.get(&id).cloned())
                    .or_else(|| f["name"].as_str().map(str::to_string))
                    .unwrap_or_else(|| "?".into())
            ))
            .collect::<Vec<_>>())
        .unwrap_or_default());
    card["versions_read"] = json!(earlier.len() + 1);
    card["events"] = json!(table_events(current, earlier)
        .iter()
        .map(|e| e.to_json())
        .collect::<Vec<_>>());
    card
}

/// The current schema's columns by field id (nested struct fields as `parent.child`).
fn column_names(meta: &Value) -> std::collections::HashMap<i64, String> {
    fn walk(fields: &Value, prefix: &str, out: &mut std::collections::HashMap<i64, String>) {
        for f in fields.as_array().into_iter().flatten() {
            let (Some(id), Some(name)) = (f["id"].as_i64(), f["name"].as_str()) else {
                continue;
            };
            let full = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}.{name}")
            };
            if f["type"]["type"] == "struct" {
                walk(&f["type"]["fields"], &full, out);
            }
            out.insert(id, full);
        }
    }
    let mut out = std::collections::HashMap::new();
    if let Some(schema) = meta["schemas"].as_array().and_then(|a| {
        a.iter()
            .find(|s| s["schema-id"] == meta["current-schema-id"])
    }) {
        walk(&schema["fields"], "", &mut out);
    }
    out
}

/// What a table list shows of one table, from its current metadata.
fn table_card(path: &str, meta: &Value) -> Value {
    let current = meta["current-snapshot-id"].as_i64().filter(|&id| id != -1);
    let head = current.and_then(|id| {
        meta["snapshots"]
            .as_array()?
            .iter()
            .find(|s| s["snapshot-id"].as_i64() == Some(id))
    });
    let num = |k: &str| -> i64 {
        head.and_then(|s| s["summary"][k].as_str())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    };
    json!({
        "path": path,
        "location": meta["location"],
        "table_uuid": meta["table-uuid"],
        "format_version": meta["format-version"],
        "last_updated_ms": meta["last-updated-ms"],
        "current_snapshot_id": current.map(|id| id.to_string()),
        "snapshots": meta["snapshots"].as_array().map_or(0, |a| a.len()),
        "schema_id": meta["current-schema-id"],
        // Equality deletes (another engine's) match by value: with any, the live count is unknown.
        "rows": (num("total-equality-deletes") == 0)
            .then(|| num("total-records") - num("total-position-deletes")),
        "data_files": num("total-data-files"),
        "delete_files": num("total-delete-files"),
    })
}

// ---- health -----------------------------------------------------------------

pub fn health(ctx: &Ctx) -> Response {
    Response::json(
        200,
        &json!({
            "status": "ok",
            "service": "shapeshift-serve",
            "version": env!("CARGO_PKG_VERSION"),
            "data_dir": ctx.data_dir().to_string_lossy(),
        }),
    )
}

// ---- shared helpers ---------------------------------------------------------

pub(crate) fn parse_body<T: for<'de> Deserialize<'de>>(req: &Request) -> Result<T, Response> {
    serde_json::from_slice(&req.body)
        .map_err(|e| Response::error(400, format!("invalid JSON body: {e}")))
}

fn default_true() -> bool {
    true
}
fn default_sample() -> usize {
    1000
}
fn default_output_format() -> OutputFormat {
    OutputFormat::Parquet
}

/// Sanitize a dataset name into a safe single path segment (letters/digits/_/-),
/// falling back to `dataset`. Prevents a name from escaping the output directory.
pub(crate) fn clean_dataset(name: Option<&str>) -> String {
    let raw = name
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("dataset");
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches(['_', '-', '.']).to_string();
    if trimmed.is_empty() {
        "dataset".to_string()
    } else {
        trimmed
    }
}

/// Write inline records to a unique temp file whose extension matches the format
/// (so the file says what it holds), returning its path.
fn write_temp_input(ctx: &Ctx, text: &str, format: SourceFormat) -> Result<PathBuf> {
    let ext = match format {
        SourceFormat::Jsonl => "jsonl",
        SourceFormat::JsonArray => "json",
        SourceFormat::Csv => "csv",
        SourceFormat::Tsv => "tsv",
        SourceFormat::Xml => "xml",
    };
    let path = ctx.tmp_dir().join(format!("in-{}.{ext}", ctx.next_id()));
    std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

/// Read up to `n` well-formed records (0 = all) that `filter` keeps, for inference;
/// malformed *lines* are tolerated here (surfaced during the shaping pass) but a real I/O
/// error aborts.
fn sample_records(
    path: &Path,
    source: &SourceSpec,
    n: usize,
    filter: Option<&Predicate>,
) -> Result<Vec<Value>> {
    let reader =
        open_source(path, source).map_err(|e| anyhow!("opening {}: {e}", path.display()))?;
    let mut out = Vec::new();
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

/// Split one `partition_by` entry on TOP-LEVEL commas only, so `region,tier` splits
/// but the comma inside `bucket(16, id)` does not. Mirrors the CLI.
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

/// Append one JSON record to the reject sidecar, creating it lazily. Fails closed.
fn write_reject(
    rejects: &mut Option<std::io::BufWriter<std::fs::File>>,
    reject_path: &Path,
    rec: &Value,
) -> Result<()> {
    use std::io::Write as _;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_dataset_sanitizes_and_defaults() {
        assert_eq!(clean_dataset(Some("orders")), "orders");
        assert_eq!(clean_dataset(Some("my_events-2")), "my_events-2");
        // Path-escaping characters are neutralised to a single safe segment.
        assert_eq!(clean_dataset(Some("../../etc/passwd")), "etc_passwd");
        assert_eq!(clean_dataset(Some("a/b.c")), "a_b_c");
        // Empty / all-separator / None fall back to the default.
        assert_eq!(clean_dataset(Some("   ")), "dataset");
        assert_eq!(clean_dataset(Some("___")), "dataset");
        assert_eq!(clean_dataset(None), "dataset");
    }

    #[test]
    fn split_partition_entries_respects_top_level_commas() {
        assert_eq!(split_partition_entries("region"), vec!["region"]);
        assert_eq!(
            split_partition_entries("region, tier"),
            vec!["region", "tier"]
        );
        // The comma inside bucket(...) must NOT split the entry.
        assert_eq!(
            split_partition_entries("day(event_at), bucket(16, id)"),
            vec!["day(event_at)", "bucket(16, id)"]
        );
        assert!(split_partition_entries("  ,  ").is_empty());
    }
}
