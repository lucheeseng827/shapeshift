//! The declarative **dataset transform spec** — shapeshift's answer to
//! per-row (MAR) billing. One small YAML/JSON document describes how a stream of
//! JSON records becomes one columnar table: which fields to select, what logical
//! type each becomes, which named transform to apply, and where/how to write it.
//!
//! The spec is intentionally *dataset-scoped*, not row-scoped: you pay the cost of
//! writing it once per source shape, then shape unbounded rows for free on your own
//! hardware. It is the whole product boundary — everything else is an engine that
//! executes this document.

use serde::{Deserialize, Serialize};

use crate::changes::{ApplyMode, ChangesSpec, CHANGE_COLUMN, OP_COLUMN};
use crate::drift::{DriftPolicy, DriftSpec};
use crate::transform::Transform;
use crate::types::ColumnType;

/// A complete dataset shaping spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DatasetSpec {
    /// Logical table name (used for the Iceberg table dir / Parquet file stem when
    /// the output path is a directory).
    pub dataset: String,

    /// Which source records the run takes (`where:`): a subset of SQL's `WHERE` over
    /// `from:`-style paths, tested on each record before it is shaped. A record it does
    /// not keep is counted as filtered, not rejected. See [`crate::Predicate`].
    #[serde(rename = "where", default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// The records are a feed of changes (inserts, updates, deletes) rather than rows:
    /// what says which, where the row is, and what orders two changes to a key. See
    /// [`ChangesSpec`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changes: Option<ChangesSpec>,

    /// How to read the input.
    #[serde(default)]
    pub source: SourceSpec,

    /// How/where to write the output.
    pub output: OutputSpec,

    /// Schema handling. `infer` (default) lets undeclared columns ride along;
    /// `strict` writes *only* declared columns and errors on a required miss.
    #[serde(default)]
    pub schema: SchemaMode,

    /// Explicit column declarations. In `infer` mode these override/augment the
    /// inferred schema; in `strict` mode they *are* the schema.
    #[serde(default)]
    pub columns: Vec<ColumnSpec>,

    /// What the run does when the source's shape outgrows this schema. Defaults to
    /// `warn` — detected and reported, never silent.
    #[serde(default, skip_serializing_if = "DriftSpec::is_default")]
    pub drift: DriftSpec,

    /// Dataset-wide shaping options.
    #[serde(default)]
    pub options: Options,
}

/// Input description.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    /// `jsonl` (one JSON object per line, the default), `json-array` (a single
    /// top-level array), `csv`, `tsv`, or `xml` (see [`CsvOptions`], [`XmlOptions`]).
    #[serde(default)]
    pub format: SourceFormat,

    /// Optional input path. The CLI's `--input` overrides this; leaving it unset
    /// means "read from the path passed on the command line / stdin".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// How a `csv` or `tsv` source is read. Absent: the defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csv: Option<CsvOptions>,

    /// How an `xml` source is read. Required for `xml`: it names the repeating element.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xml: Option<XmlOptions>,

    /// Pull the records from a REST endpoint instead of a file (`shape --from <url>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<crate::pull::HttpSourceSpec>,

    /// Pull the rows from a MySQL query or table instead of a file (`shape --from mysql://…`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mysql: Option<crate::pull::MysqlSourceSpec>,

    /// Pull the rows from a Postgres query or table instead of a file
    /// (`shape --from postgres://…`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres: Option<crate::pull::PostgresSourceSpec>,
}

impl Default for SourceSpec {
    fn default() -> Self {
        SourceSpec {
            format: SourceFormat::Jsonl,
            path: None,
            csv: None,
            xml: None,
            http: None,
            mysql: None,
            postgres: None,
        }
    }
}

impl SourceSpec {
    /// A source in `format` with default options and no path.
    pub fn of(format: SourceFormat) -> Self {
        SourceSpec {
            format,
            ..SourceSpec::default()
        }
    }

    /// Whether the records come from a pulled source (an endpoint or a database), not a file.
    pub fn pulls(&self) -> bool {
        self.http.is_some() || self.mysql.is_some() || self.postgres.is_some()
    }

    /// The environment variables a pulled source reads its credentials from: those
    /// `source.http.auth` and `source.http.soap` name, and a database's password variable. A
    /// spec never holds a secret, so this is everything an orchestrator must provide a run with.
    /// Sorted, each once.
    pub fn credential_env(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        if let Some(h) = &self.http {
            if let Some(a) = &h.auth {
                names.extend(
                    [
                        a.bearer_env.as_deref(),
                        a.basic_env.as_deref(),
                        a.header_env.as_ref().map(|e| e.env.as_str()),
                        a.query_env.as_ref().map(|e| e.env.as_str()),
                    ]
                    .into_iter()
                    .flatten()
                    .map(str::to_string),
                );
            }
            if let Some(s) = &h.soap {
                names.extend(
                    [s.username_env.as_deref(), s.password_env.as_deref()]
                        .into_iter()
                        .flatten()
                        .map(str::to_string),
                );
            }
        }
        if self.mysql.is_some() {
            names.push(crate::pull::DbKind::Mysql.password_env().to_string());
        }
        if self.postgres.is_some() {
            names.push(crate::pull::DbKind::Postgres.password_env().to_string());
        }
        names.sort();
        names.dedup();
        names
    }
}

impl SourceSpec {
    /// Options that fit the format, and every option well formed.
    pub fn validate(&self) -> crate::error::Result<()> {
        use crate::error::ShapeError;
        let databases = [
            (self.mysql.as_ref(), crate::pull::DbKind::Mysql),
            (self.postgres.as_ref(), crate::pull::DbKind::Postgres),
        ];
        if databases.iter().filter(|(d, _)| d.is_some()).count() > 1 {
            return Err(ShapeError::Spec(
                "a source is one database: `source.mysql` or `source.postgres`, not both".into(),
            ));
        }
        if let Some((Some(m), kind)) = databases.into_iter().find(|(d, _)| d.is_some()) {
            if self.path.is_some() || self.http.is_some() {
                return Err(ShapeError::Spec(
                    "a source is one of a file (`source.path`), an endpoint (`source.http`) or a \
                     database (`source.mysql`, `source.postgres`)"
                        .into(),
                ));
            }
            if self.format != SourceFormat::Jsonl || self.csv.is_some() || self.xml.is_some() {
                return Err(ShapeError::Spec(
                    "a database's rows have no file format: `source.format`, `source.csv` and \
                     `source.xml` do not apply to a database source"
                        .into(),
                ));
            }
            return m.validate_as(kind);
        }
        if let Some(h) = &self.http {
            if self.path.is_some() {
                return Err(ShapeError::Spec(
                    "a source is a file (`source.path`) or an endpoint (`source.http`), not both"
                        .into(),
                ));
            }
            if self.format != SourceFormat::Jsonl {
                return Err(ShapeError::Spec(
                    "an endpoint's body format is `source.http.format`, not `source.format`".into(),
                ));
            }
            h.validate()?;
            // The body's format decides which of the format options apply.
            let body = match h.body_format() {
                crate::pull::HttpFormat::Csv => SourceFormat::Csv,
                crate::pull::HttpFormat::Tsv => SourceFormat::Tsv,
                crate::pull::HttpFormat::Xml => SourceFormat::Xml,
                _ => SourceFormat::JsonArray,
            };
            return SourceSpec {
                format: body,
                http: None,
                ..self.clone()
            }
            .validate();
        }
        let f = self.format.name();
        match self.format {
            SourceFormat::Csv | SourceFormat::Tsv => {}
            _ if self.csv.is_some() => {
                return Err(ShapeError::Spec(format!(
                    "`source.csv` applies to csv and tsv sources, not {f}"
                )))
            }
            _ => {}
        }
        match (self.format, &self.xml) {
            (SourceFormat::Xml, None) => {
                return Err(ShapeError::Spec(
                    "an xml source needs `source.xml.record`: the repeating element, like `/Orders/Order`"
                        .into(),
                ))
            }
            (SourceFormat::Xml, Some(x)) => {
                let path = x.record.trim().trim_start_matches('/');
                if path.is_empty() || path.split('/').any(|s| s.trim().is_empty()) {
                    return Err(ShapeError::Spec(format!(
                        "`source.xml.record` {:?} is not a path of element names",
                        x.record
                    )));
                }
            }
            (_, Some(_)) => {
                return Err(ShapeError::Spec(format!(
                    "`source.xml` applies to xml sources, not {f}"
                )))
            }
            _ => {}
        }
        if let Some(c) = &self.csv {
            for (what, ch) in [("delimiter", c.delimiter), ("quote", Some(c.quote))] {
                if let Some(ch) = ch {
                    if !ch.is_ascii() || ch == '\n' || ch == '\r' {
                        return Err(ShapeError::Spec(format!(
                            "`source.csv.{what}` must be one ASCII character other than a line break"
                        )));
                    }
                }
            }
            // The default delimiter counts too: `quote: ","` on a csv source would make
            // every comma both.
            let delimiter = c.delimiter.unwrap_or(if self.format == SourceFormat::Tsv {
                '\t'
            } else {
                ','
            });
            if delimiter == c.quote {
                return Err(ShapeError::Spec(
                    "`source.csv.delimiter` and `quote` must differ".into(),
                ));
            }
            let mut seen = std::collections::HashSet::new();
            for name in &c.columns {
                if name.trim().is_empty() {
                    return Err(ShapeError::Spec(
                        "`source.csv.columns` has an empty name".into(),
                    ));
                }
                if !seen.insert(name) {
                    return Err(ShapeError::Spec(format!(
                        "`source.csv.columns` names `{name}` twice"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// How a CSV or TSV file becomes records: one record per row, an object keyed by the
/// column names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CsvOptions {
    /// The first row names the columns (the default). With `columns` also given, that row
    /// is skipped and `columns` names them instead.
    #[serde(default = "yes")]
    pub header: bool,
    /// The column names, in order. Required with `header: false` unless the default names
    /// (`column_1`, `column_2`, …) will do.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// The field separator, one byte: `,` for csv and a tab for tsv unless set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<char>,
    /// The quote character, one byte (default `"`).
    #[serde(default = "double_quote")]
    pub quote: char,
    /// Values read as null (default: the empty field).
    #[serde(default = "empty_is_null")]
    pub null_values: Vec<String>,
    /// `auto` (the default): an unquoted-looking integer, decimal, `true` or `false`
    /// arrives as a JSON number or boolean, so inference sees types. `strings`: every
    /// field arrives as a string, for the spec's types to coerce.
    #[serde(default)]
    pub types: CsvTypes,
}

impl Default for CsvOptions {
    fn default() -> Self {
        CsvOptions {
            header: true,
            columns: Vec::new(),
            delimiter: None,
            quote: '"',
            null_values: empty_is_null(),
            types: CsvTypes::Auto,
        }
    }
}

fn yes() -> bool {
    true
}
fn double_quote() -> char {
    '"'
}
fn empty_is_null() -> Vec<String> {
    vec![String::new()]
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum CsvTypes {
    #[default]
    Auto,
    Strings,
}

/// How an XML document becomes records: each element at `record` is one record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct XmlOptions {
    /// The repeating element, as a path of element names: `/Orders/Order` from the root,
    /// or `Order` / `Orders/Order` at any depth. Namespace prefixes are ignored
    /// (`soap:Body` is `Body`).
    pub record: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SourceFormat {
    #[default]
    Jsonl,
    JsonArray,
    /// Comma-separated values (RFC 4180 quoting).
    Csv,
    /// Tab-separated values.
    Tsv,
    /// XML, one record per element at `source.xml.record`.
    Xml,
}

impl SourceFormat {
    /// The name the spec and the command line use.
    pub fn name(self) -> &'static str {
        match self {
            SourceFormat::Jsonl => "jsonl",
            SourceFormat::JsonArray => "json-array",
            SourceFormat::Csv => "csv",
            SourceFormat::Tsv => "tsv",
            SourceFormat::Xml => "xml",
        }
    }
}

/// Output description.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    /// `parquet` (a single file, or `<path>/<dataset>.parquet` when `path` is a
    /// directory) or `iceberg` (an Iceberg v2 table rooted at `path`).
    pub format: OutputFormat,

    /// Destination path. For `parquet`, a file or directory; for `iceberg`, the
    /// table root directory. Left empty when `catalog` names the table: the catalog
    /// decides where it lives.
    #[serde(default)]
    pub path: String,

    /// Parquet page compression. Snappy by default (musl-static, no C codecs).
    #[serde(default)]
    pub compression: Compression,

    /// Iceberg partition fields: identity columns (`region`) and/or hidden-transform
    /// expressions — `bucket(N, col)`, `truncate(W, col)`, `year|month|day|hour(col)`
    /// (see [`partition_source_column`] for how validation reads the source column).
    /// For Parquet output this is recorded in the spec but a single file is still
    /// written — Hive-style directory partitioning for Parquet is on the roadmap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<String>,

    /// Iceberg only: **merge** (upsert) on these key columns instead of appending. Each
    /// written row replaces any row already in the table with the same key, and when the
    /// input carries a key more than once the last occurrence wins. Creates the table if
    /// there is none. Empty (the default) = a plain write.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merge_on: Vec<String>,

    /// Iceberg only: commit through this REST catalog instead of a table directory. The
    /// catalog names the table's location and is its commit point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog: Option<CatalogSpec>,

    /// With `changes:`: `upsert` (the default) keeps the current state on `merge_on`;
    /// `changelog` appends every change with an `_op` column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply: Option<ApplyMode>,
}

/// A spec error, with a word added when it is a catalog credential written into the spec:
/// say where credentials go instead of only that the key is unknown.
fn secret_hint(error: String) -> String {
    let secret = [
        "token",
        "credential",
        "client_secret",
        "client_id",
        "password",
    ]
    .iter()
    .any(|k| error.contains(&format!("unknown field `{k}`")));
    if secret && error.contains("`warehouse`") {
        format!(
            "{error}. A catalog's credentials never go in a spec, which is meant to be \
             committed: set SHAPESHIFT_CATALOG_TOKEN, or SHAPESHIFT_CATALOG_CREDENTIAL \
             (client_id:client_secret), in the environment"
        )
    } else {
        error
    }
}

/// An Iceberg REST catalog and a table in it (`output.catalog`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CatalogSpec {
    /// The catalog's base URI, `http(s)://host:port` (the client adds `/v1/…`).
    pub uri: String,
    /// The table, `<namespace>.<name>`; a namespace may have several levels
    /// (`analytics.raw.events`).
    pub table: String,
    /// Which of the catalog's warehouses holds the table (sent as `?warehouse=` when the
    /// client asks for the catalog's config): a Polaris catalog's name, a Lakekeeper
    /// warehouse. A catalog with one needs none.
    ///
    /// The catalog's credentials are deliberately not here: a spec is meant to be
    /// committed, so they come from the environment (`SHAPESHIFT_CATALOG_TOKEN`, or
    /// `SHAPESHIFT_CATALOG_CREDENTIAL`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum OutputFormat {
    Parquet,
    Iceberg,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Compression {
    #[default]
    Snappy,
    Zstd,
    Uncompressed,
}

/// Schema strictness.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SchemaMode {
    /// Infer the schema from a sample and let undeclared columns through
    /// (declarations refine it). The forgiving default for messy sources.
    #[default]
    Infer,
    /// Only declared columns are written; a required column that is missing/null
    /// aborts the run. The contract mode for downstream stability.
    Strict,
}

/// One output column: where it comes from, what it becomes, and how it is
/// transformed on the way.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    /// Output column name.
    pub name: String,

    /// Source path into each record. Dotted (`user.name`), optionally `$`-prefixed
    /// (`$.user.name`); numeric segments index into arrays (`tags.0`). Defaults to
    /// `name` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,

    /// Target logical type.
    #[serde(rename = "type")]
    pub ty: ColumnType,

    /// Optional named transform applied to the selected value before coercion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<Transform>,

    /// When true, a missing/null value rejects the row (lenient) or aborts
    /// (strict). When false (default), it is written as null.
    #[serde(default)]
    pub required: bool,
}

impl ColumnSpec {
    /// The effective source path (`from` or, failing that, `name`).
    pub fn source_path(&self) -> &str {
        self.from.as_deref().unwrap_or(&self.name)
    }
}

/// Dataset-wide shaping options.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Options {
    /// Flatten nested objects into dotted column names during inference
    /// (`{"user":{"name":..}}` → `user.name`). Arrays become JSON-encoded string
    /// leaves. Applies to inference only; explicit columns always win.
    #[serde(default = "default_true")]
    pub flatten: bool,

    /// Rows to buffer before flushing a RecordBatch to the sink. Bounds RAM — a
    /// 100M-row shape never holds more than this many rows at once.
    #[serde(default = "default_row_group")]
    pub row_group_rows: usize,

    /// Records to sample when inferring the schema (0 = scan the whole input).
    #[serde(default = "default_sample")]
    pub infer_sample: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            flatten: true,
            row_group_rows: default_row_group(),
            infer_sample: default_sample(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_row_group() -> usize {
    50_000
}
fn default_sample() -> usize {
    1_000
}

impl DatasetSpec {
    /// Parse a spec from YAML or JSON (auto-detected: a leading `{` is JSON).
    pub fn parse(text: &str) -> crate::error::Result<Self> {
        let spec = DatasetSpec::decode(text)?;
        spec.validate()?;
        Ok(spec)
    }

    /// Read a spec from YAML or JSON **without** validating it: for a caller that
    /// changes it first (the CLI's flags override what the file says) and then calls
    /// [`DatasetSpec::validate`] on the result.
    pub fn decode(text: &str) -> crate::error::Result<Self> {
        let trimmed = text.trim_start();
        let decoded = if trimmed.starts_with('{') {
            serde_json::from_str(text).map_err(|e| e.to_string())
        } else {
            serde_yaml::from_str(text).map_err(|e| e.to_string())
        };
        let mut spec: DatasetSpec =
            decoded.map_err(|e| crate::error::ShapeError::Spec(secret_hint(e)))?;
        spec.apply_preset();
        Ok(spec)
    }

    /// Fill in what `changes.preset` knows and the spec left out, and add the preset's
    /// version columns unless the spec declares columns of those names. Runs when a spec is
    /// read; running it again changes nothing.
    pub fn apply_preset(&mut self) {
        let Some(changes) = self.changes.as_mut() else {
            return;
        };
        changes.apply_preset();
        let Some(p) = changes.preset else { return };
        for (name, ty, from) in p.columns() {
            if !self.columns.iter().any(|c| c.name == *name) {
                self.columns.push(ColumnSpec {
                    name: name.to_string(),
                    from: Some(from.to_string()),
                    ty: *ty,
                    transform: None,
                    required: false,
                });
            }
        }
    }

    /// Serialize to YAML (the canonical on-disk form).
    pub fn to_yaml(&self) -> crate::error::Result<String> {
        serde_yaml::to_string(self).map_err(|e| crate::error::ShapeError::Spec(e.to_string()))
    }

    /// Structural checks beyond serde's shape validation.
    pub fn validate(&self) -> crate::error::Result<()> {
        use crate::error::ShapeError;
        if self.dataset.trim().is_empty() {
            return Err(ShapeError::Spec("`dataset` must not be empty".into()));
        }
        if let Some(w) = &self.filter {
            crate::predicate::Predicate::parse(w)?;
        }
        self.source.validate()?;
        if self.schema == SchemaMode::Strict && self.columns.is_empty() {
            return Err(ShapeError::Spec(
                "strict schema mode requires at least one declared column".into(),
            ));
        }
        // Duplicate output column names are a hard error — they would collide in the
        // Arrow schema.
        let mut seen = std::collections::HashSet::new();
        for c in &self.columns {
            if !seen.insert(&c.name) {
                return Err(ShapeError::Spec(format!(
                    "duplicate column name `{}`",
                    c.name
                )));
            }
            if crate::value::segments(c.source_path()).len() > crate::value::MAX_PATH_SEGMENTS {
                return Err(ShapeError::Spec(format!(
                    "column `{}`: its path has more than {} segments",
                    c.name,
                    crate::value::MAX_PATH_SEGMENTS
                )));
            }
        }
        // The rescue column is a real output column, so it needs a real name — and one
        // that cannot collide with a declared one. (A collision with an *inferred*
        // column is only knowable once the sample is read; the shaper checks that.)
        if self.drift.policy == DriftPolicy::Rescue {
            let rescue = self.drift.rescue_column.trim();
            if rescue.is_empty() {
                return Err(ShapeError::Spec(
                    "`drift.rescue_column` must not be empty under `policy: rescue`".into(),
                ));
            }
            if self.columns.iter().any(|c| c.name == rescue) {
                return Err(ShapeError::Spec(format!(
                    "`drift.rescue_column` `{rescue}` collides with a declared column \
                     (pick another name)"
                )));
            }
        }
        // A catalog table lives where the catalog says, so it takes no path; any other
        // output needs one.
        match &self.output.catalog {
            Some(c) => {
                if self.output.format != OutputFormat::Iceberg {
                    return Err(ShapeError::Spec(
                        "`output.catalog` applies only to Iceberg tables (`format: iceberg`)"
                            .into(),
                    ));
                }
                if !self.output.path.trim().is_empty() {
                    return Err(ShapeError::Spec(
                        "`output.path` and `output.catalog` both name the table: the catalog \
                         decides its location, so drop the path"
                            .into(),
                    ));
                }
                if c.uri.trim().is_empty() || c.table.trim().is_empty() {
                    return Err(ShapeError::Spec(
                        "`output.catalog` needs both a `uri` and a `table`".into(),
                    ));
                }
                if c.warehouse.as_deref().is_some_and(|w| w.trim().is_empty()) {
                    return Err(ShapeError::Spec(
                        "`output.catalog.warehouse` is empty: name the warehouse, or leave the \
                         key out"
                            .into(),
                    ));
                }
            }
            None if self.output.path.trim().is_empty() => {
                return Err(ShapeError::Spec(
                    "`output.path` is required (or `output.catalog` for an Iceberg table a \
                     REST catalog names)"
                        .into(),
                ));
            }
            None => {}
        }
        // In strict schema mode, partition columns must be among the declared columns.
        // In infer mode `columns` may be only a partial pin-list (the rest ride along from
        // the inferred schema), so partition columns are resolved against the full inferred
        // schema by the sink instead — don't reject them here. A `partition_by` entry may
        // be a transform expression (`day(event_at)`, `bucket(16, id)`); the declared-column
        // check applies to its SOURCE column — the Iceberg sink validates the transform
        // itself (name, arity, type applicability).
        if self.schema == SchemaMode::Strict {
            for p in &self.output.partition_by {
                let col = partition_source_column(p);
                if !self.columns.iter().any(|c| c.name == col) {
                    return Err(ShapeError::Spec(format!(
                        "partition column `{col}` is not a declared column (declare it to partition by it)"
                    )));
                }
            }
        }
        // A merge key is matched against rows already in an Iceberg table, so it needs
        // one; its columns follow the same declared-column rule as partition columns (the
        // sink checks the key's types once the schema is known).
        if !self.output.merge_on.is_empty() {
            if self.output.format != OutputFormat::Iceberg {
                return Err(ShapeError::Spec(
                    "`output.merge_on` applies only to Iceberg tables (`format: iceberg`)".into(),
                ));
            }
            for (i, k) in self.output.merge_on.iter().enumerate() {
                if self.output.merge_on[..i].contains(k) {
                    return Err(ShapeError::Spec(format!(
                        "`output.merge_on` names `{k}` twice"
                    )));
                }
                if self.schema == SchemaMode::Strict && !self.columns.iter().any(|c| &c.name == k) {
                    return Err(ShapeError::Spec(format!(
                        "merge key `{k}` is not a declared column (declare it to merge on it)"
                    )));
                }
            }
        }
        self.validate_pull()?;
        self.validate_changes()
    }

    /// An incremental source, against the output (RELEASE-0.6 M4, rule 3).
    fn validate_pull(&self) -> crate::error::Result<()> {
        use crate::error::ShapeError;
        // The cursor field, and whether it is strict.
        // The cursor field, whether it is strict, and where a spec says so.
        let (from, strict, strict_field) = match (&self.source.http, &self.source.mysql) {
            (Some(h), _) => match &h.incremental {
                Some(i) => (i.from.clone(), i.strict, "source.http.incremental.strict"),
                None => return Ok(()),
            },
            (None, Some(m)) => match &m.cursor {
                Some(c) => (c.clone(), m.strict, "source.mysql.strict"),
                None => return Ok(()),
            },
            (None, None) => match self
                .source
                .postgres
                .as_ref()
                .and_then(|p| p.cursor.as_ref().map(|c| (c, p)))
            {
                Some((c, p)) => (c.clone(), p.strict, "source.postgres.strict"),
                None => return Ok(()),
            },
        };
        if self.output.format != OutputFormat::Iceberg {
            return Err(ShapeError::Spec(
                "an incremental source records its position in the table it writes: it needs \
                 `output.format: iceberg` (a Parquet file has no snapshot to keep it in)"
                    .into(),
            ));
        }
        if !strict && self.output.merge_on.is_empty() {
            return Err(ShapeError::Spec(format!(
                "an incremental source re-reads the records at its position's value \
                 (`{}`), so they would land twice: set `output.merge_on` (replayed rows upsert \
                 onto themselves), or declare `{strict_field}: true` if the field is unique \
                 and only grows, like an auto-increment id",
                from
            )));
        }
        if self.output.apply == Some(crate::changes::ApplyMode::Changelog) && !strict {
            return Err(ShapeError::Spec(format!(
                "a changelog appends every change it reads, so a replayed record would be \
                 appended twice: an incremental source into a changelog needs \
                 `{strict_field}: true`"
            )));
        }
        Ok(())
    }

    /// `changes:` and `output.apply`, against the rest of the spec.
    fn validate_changes(&self) -> crate::error::Result<()> {
        use crate::error::ShapeError;
        if self.columns.iter().any(|c| c.name == CHANGE_COLUMN) {
            return Err(ShapeError::Spec(format!(
                "`{CHANGE_COLUMN}` is a name shapeshift keeps for itself: rename the column"
            )));
        }
        let Some(changes) = &self.changes else {
            if self.output.apply.is_some() {
                return Err(ShapeError::Spec(
                    "`output.apply` says how to apply a feed of changes: it needs a `changes:` block"
                        .into(),
                ));
            }
            return Ok(());
        };
        changes.validate()?;
        match self.output.apply.unwrap_or_default() {
            ApplyMode::Upsert => {
                if self.output.merge_on.is_empty() {
                    return Err(ShapeError::Spec(
                        "`changes:` applies each change to a key: set `output.merge_on` (or \
                         `output.apply: changelog` to keep every change instead)"
                            .into(),
                    ));
                }
                if changes.key_from.is_some() {
                    for c in &changes.order_by {
                        let declared = self.columns.iter().find(|d| &d.name == c);
                        if declared.is_some_and(|d| !d.source_path().starts_with('$')) {
                            return Err(ShapeError::Spec(crate::shaper::key_from_version(c)));
                        }
                    }
                }
                if self.schema == SchemaMode::Strict {
                    for c in &changes.order_by {
                        if !self.columns.iter().any(|d| &d.name == c) {
                            return Err(ShapeError::Spec(format!(
                                "`changes.order_by` column `{c}` is not a declared column"
                            )));
                        }
                    }
                }
            }
            ApplyMode::Changelog => {
                if self.output.format != OutputFormat::Iceberg {
                    return Err(ShapeError::Spec(
                        "`output.apply: changelog` adds each run's changes to an Iceberg table \
                         (`format: iceberg`)"
                            .into(),
                    ));
                }
                if !self.output.merge_on.is_empty() {
                    return Err(ShapeError::Spec(
                        "`output.apply: changelog` appends every change, so it matches no key: \
                         drop `output.merge_on`"
                            .into(),
                    ));
                }
                if !changes.order_by.is_empty() {
                    return Err(ShapeError::Spec(
                        "`changes.order_by` decides which change to a key wins, and a changelog \
                         keeps them all: drop it (the columns stay in the table)"
                            .into(),
                    ));
                }
                if self.columns.iter().any(|c| c.name == OP_COLUMN) {
                    return Err(ShapeError::Spec(format!(
                        "`apply: changelog` adds the column `{OP_COLUMN}`, and the spec declares \
                         one: rename it"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// The source column named by a `partition_by` entry — the entry itself for identity
/// (`region`), or the last parenthesized argument for a transform expression
/// (`day(event_at)` → `event_at`, `bucket(16, id)` → `id`). Full transform parsing
/// (name, arity, type applicability) is the Iceberg sink's job; this is just enough
/// for spec validation to know which column is being partitioned on.
pub fn partition_source_column(expr: &str) -> &str {
    match (expr.find('('), expr.rfind(')')) {
        (Some(o), Some(c)) if c > o => expr[o + 1..c].rsplit(',').next().unwrap_or("").trim(),
        _ => expr.trim(),
    }
}
