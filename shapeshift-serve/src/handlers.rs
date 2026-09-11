//! The console's JSON API — thin wrappers over the OSS shaper's four verbs
//! (`infer`, `shape`, `inspect`, `cost`). Every handler reuses the exact engine
//! the CLI uses; nothing here is a second implementation of the shaping logic.
//!
//! The server is single-user and stateless beyond the files it writes: shaped
//! output and reject sidecars land under the configured data directory, and
//! `inspect` reads them (or any local table) back. There is no run queue, no
//! persistence of specs, and no metering — that is orchestration, and out of scope
//! here.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use shapeshift_core::{
    estimate_mar, infer_columns, Compression, DatasetSpec, DriftPolicy, DriftReport, MarInputs,
    OutputFormat, OutputSpec, PushOutcome, SchemaMode, Shaper, SourceFormat, SourceSpec,
};
use shapeshift_iceberg::IcebergSink;
use shapeshift_json::{open_reader, JsonError};
use shapeshift_parquet::ParquetSink;

use crate::http::{Request, Response};

/// Shared server state: where output lands, plus a monotonic counter for unique
/// temp-file names (so concurrent requests never clobber each other's input).
pub struct Ctx {
    data_dir: PathBuf,
    counter: AtomicU64,
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
        Ok(Ctx {
            data_dir,
            counter: AtomicU64::new(0),
        })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
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
    let dataset = clean_dataset(r.dataset.as_deref());
    let input_path = write_temp_input(ctx, &r.input, r.format)?;
    let samples = sample_records(&input_path, r.format, r.sample)?;
    if samples.is_empty() {
        bail!("no well-formed records were read from the input");
    }
    let columns = infer_columns(samples.iter(), r.flatten);
    let spec = DatasetSpec {
        dataset: dataset.clone(),
        source: SourceSpec {
            format: r.format,
            path: None,
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
struct ShapeReq {
    /// Inline records; ignored when `input_path` is given.
    #[serde(default)]
    input: Option<String>,
    /// A server-side input file to shape instead of inline text (for large inputs).
    #[serde(default)]
    input_path: Option<String>,
    /// An edited spec (YAML/JSON). When omitted, the schema is inferred on the fly.
    #[serde(default)]
    spec_yaml: Option<String>,
    #[serde(default)]
    format: SourceFormat,
    #[serde(default = "default_output_format")]
    to: OutputFormat,
    #[serde(default)]
    compression: Compression,
    #[serde(default)]
    dataset: Option<String>,
    #[serde(default)]
    partition_by: Vec<String>,
    #[serde(default)]
    append: bool,
    /// What to do about schema drift. Overrides the spec's `drift.policy`; absent
    /// means "whatever the spec says" (which defaults to `warn`).
    #[serde(default)]
    on_drift: Option<DriftPolicy>,
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
    dataset: String,
    format: String,
    output_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_path: Option<String>,
    rows_in: u64,
    rows_out: u64,
    rejected: u64,
    parse_errors: u64,
    row_groups: u64,
    bytes: u64,
    rejects_total: u64,
    rejects_path: Option<String>,
    rejects_sample: Vec<RejectRow>,
    /// What the source did that the schema does not cover — absent under
    /// `drift.policy: ignore`, or when the run matched its schema exactly.
    #[serde(skip_serializing_if = "Option::is_none")]
    drift: Option<DriftResp>,
}

pub fn shape(ctx: &Ctx, req: &Request) -> Response {
    let r: ShapeReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    match do_shape(ctx, r) {
        Ok(resp) => Response::json(200, &resp),
        Err(e) => Response::error(400, e.to_string()),
    }
}

fn do_shape(ctx: &Ctx, r: ShapeReq) -> Result<ShapeResp> {
    let dataset = clean_dataset(r.dataset.as_deref());

    // 1. Assemble the effective spec (parsed, or synthesized for on-the-fly inference).
    let mut spec = match r
        .spec_yaml
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(text) => DatasetSpec::parse(text).map_err(|e| anyhow!("spec: {e}"))?,
        None => DatasetSpec {
            dataset: dataset.clone(),
            source: SourceSpec {
                format: r.format,
                path: None,
            },
            output: OutputSpec {
                format: r.to,
                path: String::new(),
                compression: r.compression,
                partition_by: Vec::new(),
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
    spec.source.format = r.format;
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
    spec.validate().map_err(|e| anyhow!("spec: {e}"))?;

    if !spec.output.partition_by.is_empty() && !is_iceberg {
        bail!("`partition_by` applies only to Iceberg output");
    }
    if r.append && !is_iceberg {
        bail!("`append` applies only to Iceberg output");
    }

    // 4. Resolve the input: a server-side path, or inline text written to a temp file.
    let (input_path, temp_input) = match r.input_path.as_deref().filter(|s| !s.is_empty()) {
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

    // 5. Build the shaper (inferring in Infer mode).
    let inferred = if spec.schema == SchemaMode::Infer {
        let samples = sample_records(&input_path, spec.source.format, spec.options.infer_sample)?;
        infer_columns(samples.iter(), spec.options.flatten)
    } else {
        Vec::new()
    };
    let mut shaper = Shaper::from_spec(&spec, &inferred).map_err(|e| anyhow!("{e}"))?;
    let schema = shaper.schema();

    // 6. For a fresh (non-append) Iceberg write, start from a clean table dir so a
    //    rerun never mixes stale data files. Safe: the path is always inside out_dir.
    if is_iceberg && !r.append && output_path.exists() {
        std::fs::remove_dir_all(&output_path)
            .with_context(|| format!("clearing {}", output_path.display()))?;
    }

    // 7. Create the sink (local filesystem only — object-store URLs are CLI-only).
    let mut sink: Box<dyn shapeshift_core::Sink> = if is_iceberg {
        Box::new(
            IcebergSink::create(
                &output_path,
                schema,
                spec.output.compression,
                r.append,
                &spec.output.partition_by,
            )
            .map_err(|e| anyhow!("{e}"))?,
        )
    } else {
        Box::new(
            ParquetSink::create(&output_path, schema, spec.output.compression)
                .map_err(|e| anyhow!("{e}"))?,
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

    let reader = open_reader(&input_path, spec.source.format).map_err(|e| anyhow!("{e}"))?;
    for item in reader {
        match item {
            Ok(v) => {
                rows_in += 1;
                match shaper.push(&v).map_err(|e| anyhow!("{e}"))? {
                    PushOutcome::Appended => rows_out += 1,
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
                    if let Some(batch) = shaper.flush().map_err(|e| anyhow!("{e}"))? {
                        sink.write_batch(&batch).map_err(|e| anyhow!("{e}"))?;
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
    if let Some(batch) = shaper.flush().map_err(|e| anyhow!("{e}"))? {
        sink.write_batch(&batch).map_err(|e| anyhow!("{e}"))?;
        batches += 1;
    }
    if let Some(mut w) = reject_writer {
        use std::io::Write as _;
        w.flush()?;
    }
    let summary = sink.finish().map_err(|e| anyhow!("{e}"))?;
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
        dataset,
        format: format!("{:?}", spec.output.format).to_lowercase(),
        output_path: output_path.to_string_lossy().into_owned(),
        metadata_path,
        rows_in,
        rows_out,
        rejected,
        parse_errors,
        row_groups: batches,
        bytes: summary.bytes,
        rejects_total,
        rejects_path: (rejects_total > 0).then(|| reject_path.to_string_lossy().into_owned()),
        rejects_sample,
        drift: drift.as_ref().filter(|r| !r.is_clean()).map(drift_resp),
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
            "current_snapshot_id": info.current_snapshot_id,
            "total_records": info.total_records,
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

// ---- cost -------------------------------------------------------------------

#[derive(Deserialize)]
struct CostReq {
    rows: u64,
    vendor_per_million: f64,
    #[serde(default)]
    self_host_cost: f64,
}

pub fn cost(_ctx: &Ctx, req: &Request) -> Response {
    let r: CostReq = match parse_body(req) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let report = estimate_mar(MarInputs {
        rows: r.rows,
        vendor_per_million: r.vendor_per_million,
        self_host_cost: r.self_host_cost,
    });
    Response::json(
        200,
        &json!({
            "rows": report.rows,
            "vendor_cost": report.vendor_cost,
            "self_host_cost": report.self_host_cost,
            "saved": report.saved,
            "saved_fraction": report.saved_fraction,
            "vendor_per_million": r.vendor_per_million,
        }),
    )
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

fn parse_body<T: for<'de> Deserialize<'de>>(req: &Request) -> Result<T, Response> {
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
fn clean_dataset(name: Option<&str>) -> String {
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
/// (so `open_reader` picks the right reader), returning its path.
fn write_temp_input(ctx: &Ctx, text: &str, format: SourceFormat) -> Result<PathBuf> {
    let ext = match format {
        SourceFormat::Jsonl => "jsonl",
        SourceFormat::JsonArray => "json",
    };
    let path = ctx.tmp_dir().join(format!("in-{}.{ext}", ctx.next_id()));
    std::fs::write(&path, text).with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

/// Read up to `n` well-formed records (0 = all) for inference; malformed *lines* are
/// tolerated here (surfaced during the shaping pass) but a real I/O error aborts.
fn sample_records(path: &Path, format: SourceFormat, n: usize) -> Result<Vec<Value>> {
    let reader =
        open_reader(path, format).map_err(|e| anyhow!("opening {}: {e}", path.display()))?;
    let mut out = Vec::new();
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
