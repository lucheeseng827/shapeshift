//! Partitioning: route a table's rows to per-partition data files so an Iceberg reader
//! can skip whole partitions by predicate.
//!
//! Supports **identity** partitioning (`region`) and Iceberg's **hidden (transform)**
//! partitions — `bucket(N, col)` (spec-exact Murmur3), `truncate(W, col)`, and the
//! temporal `year|month|day|hour(col)` — so queries prune without the consumer knowing
//! the physical layout.
//!
//! The [`PartitionedWriter`] fans out — one open [`IcebergDataWriter`](crate::IcebergDataWriter)
//! per distinct partition-key combination — and each row goes to the file for its
//! **transformed** key. An **unpartitioned** table (`partition_by` empty) is the
//! degenerate one-file case, so both sinks drive this one writer.
//!
//! **RAM note:** fan-out holds one open writer (one row-group buffer) per live partition,
//! so partition on *low-cardinality* transforms. Rows still stream — nothing buffers the
//! whole dataset.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt32Array,
};
use arrow_schema::{DataType, TimeUnit};
use arrow_select::take::take;
use uuid::Uuid;

use shapeshift_core::{Compression, Result, SchemaRef, ShapeError};

use crate::stats::Stats;
use crate::{IceField, IcebergDataWriter};

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// One column's identity-partition value for a data file (all rows in the file share it).
/// Doubles as the hash key that groups rows — NaN/`-0.0` compare by bit pattern.
#[derive(Clone, Debug)]
pub enum PartVal {
    Null,
    Bool(bool),
    Long(i64),
    Int(i32),
    Double(f64),
    Str(String),
}

impl PartVal {
    fn disc(&self) -> u8 {
        match self {
            PartVal::Null => 0,
            PartVal::Bool(_) => 1,
            PartVal::Long(_) => 2,
            PartVal::Int(_) => 3,
            PartVal::Double(_) => 4,
            PartVal::Str(_) => 5,
        }
    }
    /// A filesystem-safe rendering for the Hive-style `name=value` path segment.
    fn path_segment(&self) -> String {
        let raw = match self {
            PartVal::Null => "__NULL__".to_string(),
            PartVal::Bool(b) => b.to_string(),
            PartVal::Long(n) => n.to_string(),
            PartVal::Int(n) => n.to_string(),
            PartVal::Double(d) => d.to_string(),
            PartVal::Str(s) => s.clone(),
        };
        raw.chars()
            .map(|c| {
                if c.is_alphanumeric() || matches!(c, '-' | '.' | '_') {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    }
}

impl PartialEq for PartVal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PartVal::Null, PartVal::Null) => true,
            (PartVal::Bool(a), PartVal::Bool(b)) => a == b,
            (PartVal::Long(a), PartVal::Long(b)) => a == b,
            (PartVal::Int(a), PartVal::Int(b)) => a == b,
            (PartVal::Double(a), PartVal::Double(b)) => a.to_bits() == b.to_bits(),
            (PartVal::Str(a), PartVal::Str(b)) => a == b,
            _ => false,
        }
    }
}
impl Eq for PartVal {}
impl Hash for PartVal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.disc().hash(state);
        match self {
            PartVal::Null => {}
            PartVal::Bool(b) => b.hash(state),
            PartVal::Long(n) => n.hash(state),
            PartVal::Int(n) => n.hash(state),
            PartVal::Double(d) => d.to_bits().hash(state),
            PartVal::Str(s) => s.hash(state),
        }
    }
}

/// An Iceberg partition transform. `Identity` keeps the source value; the rest are the
/// **hidden** transforms — the reader prunes on the transformed value without knowing
/// the physical layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Transform {
    Identity,
    /// `bucket(N, col)` — `(murmur3_x86_32(encoded value) & i32::MAX) % N`, per the
    /// Iceberg spec's Appendix B encodings. Result type: int.
    Bucket(u32),
    /// `truncate(W, col)` — ints floor to a multiple of `W`; strings keep the first `W`
    /// code points. Result type: the source type.
    Truncate(u32),
    /// Years since 1970 (date | timestamp → int).
    Year,
    /// Months since 1970-01 (date | timestamp → int).
    Month,
    /// Days since 1970-01-01 (date | timestamp → int).
    Day,
    /// Hours since 1970-01-01T00 (timestamp → int).
    Hour,
}

impl Transform {
    /// The transform string recorded in the table's partition spec
    /// (`identity`, `bucket[16]`, `truncate[4]`, `year`, …).
    pub fn spec_str(&self) -> String {
        match self {
            Transform::Identity => "identity".into(),
            Transform::Bucket(n) => format!("bucket[{n}]"),
            Transform::Truncate(w) => format!("truncate[{w}]"),
            Transform::Year => "year".into(),
            Transform::Month => "month".into(),
            Transform::Day => "day".into(),
            Transform::Hour => "hour".into(),
        }
    }

    /// The transform's result type for a given source type (validated by the parser).
    fn result_type(&self, source: &str) -> String {
        match self {
            Transform::Identity | Transform::Truncate(_) => source.to_string(),
            _ => "int".into(),
        }
    }

    /// Source types this transform accepts (Iceberg spec's applicability table).
    fn accepts(&self, source: &str) -> bool {
        match self {
            Transform::Identity => {
                matches!(
                    source,
                    "boolean" | "long" | "double" | "string" | "date" | "timestamp"
                )
            }
            Transform::Bucket(_) => matches!(source, "long" | "string" | "date" | "timestamp"),
            Transform::Truncate(_) => matches!(source, "long" | "string"),
            Transform::Year | Transform::Month | Transform::Day => {
                matches!(source, "date" | "timestamp")
            }
            Transform::Hour => source == "timestamp",
        }
    }

    /// Apply the transform to a source value (null → null, per the Iceberg spec).
    /// Source-type validity was checked at resolve time, so mismatches here are bugs.
    fn apply(&self, v: PartVal, source_is_date: bool) -> PartVal {
        if matches!(v, PartVal::Null) {
            return PartVal::Null;
        }
        match self {
            Transform::Identity => v,
            Transform::Bucket(n) => {
                let hash = match &v {
                    // Spec Appendix B: int/long/date/timestamp hash as the 8-byte
                    // little-endian long; strings hash their UTF-8 bytes.
                    PartVal::Long(x) => murmur3_x86_32(&x.to_le_bytes()),
                    PartVal::Int(x) => murmur3_x86_32(&(*x as i64).to_le_bytes()),
                    PartVal::Str(s) => murmur3_x86_32(s.as_bytes()),
                    other => unreachable!("bucket over unsupported value {other:?}"),
                };
                PartVal::Int((hash & i32::MAX) % (*n as i32))
            }
            Transform::Truncate(w) => match v {
                PartVal::Long(x) => PartVal::Long(x - x.rem_euclid(*w as i64)),
                PartVal::Str(s) => PartVal::Str(s.chars().take(*w as usize).collect()),
                other => unreachable!("truncate over unsupported value {other:?}"),
            },
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
                // date arrives as Int(days since epoch); timestamp as Long(µs since epoch).
                let (days, micros) = match v {
                    PartVal::Int(d) if source_is_date => (d as i64, None),
                    PartVal::Long(us) => (us.div_euclid(86_400_000_000), Some(us)),
                    other => unreachable!("temporal transform over unsupported value {other:?}"),
                };
                let out = match self {
                    Transform::Day => days,
                    Transform::Hour => micros
                        .expect("hour() applies to timestamps only")
                        .div_euclid(3_600_000_000),
                    Transform::Year => civil_from_days(days).0 - 1970,
                    Transform::Month => {
                        let (y, m, _) = civil_from_days(days);
                        (y - 1970) * 12 + (m as i64 - 1)
                    }
                    _ => unreachable!(),
                };
                PartVal::Int(out as i32)
            }
        }
    }
}

/// Murmur3 x86 32-bit, seed 0 — the hash the Iceberg spec mandates for `bucket`.
/// Verified against the spec's Appendix B test vectors (see tests).
///
/// NOT a security control: this is a **non-cryptographic partitioning hash**, required
/// byte-for-byte by the Iceberg spec so every engine buckets a value identically —
/// substituting a cryptographic hash (SHA-256 etc.) would make the table unreadable
/// to other engines. Nothing here protects secrets or integrity.
fn murmur3_x86_32(data: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let mut h: u32 = 0;
    let chunks = data.len() / 4;
    for i in 0..chunks {
        let mut k = u32::from_le_bytes(data[4 * i..4 * i + 4].try_into().unwrap());
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h = (h ^ k)
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe654_6b64);
    }
    let rem = &data[4 * chunks..];
    if !rem.is_empty() {
        let mut k: u32 = 0;
        for (i, &b) in rem.iter().enumerate() {
            k |= (b as u32) << (8 * i);
        }
        k = k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        h ^= k;
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

/// Proleptic-Gregorian (year, month, day) from days since 1970-01-01.
/// Howard Hinnant's `civil_from_days`; exact for negative (pre-1970) days too.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// A partition field — a transform over a source column — resolved against the table
/// schema.
#[derive(Clone)]
pub struct PartitionField {
    /// Partition field name (`region`, `event_at_day`, `id_bucket`, …).
    pub name: String,
    /// Schema field-id of the source column.
    pub source_id: i32,
    /// Partition field id (Iceberg convention: `1000 + position`).
    pub field_id: i32,
    /// **Result** type of the transform (identity/truncate → source type; bucket and
    /// the temporal transforms → int). This is the type the manifest's `partition`
    /// record stores.
    pub iceberg_type: String,
    /// The source column's index in the record batch (for row routing).
    pub source_index: usize,
    /// The transform applied to the source value.
    pub transform: Transform,
    /// Whether the source column is a `date` (its values arrive as epoch days, not µs).
    source_is_date: bool,
}

impl PartitionField {
    /// The Hive-style `name=value` path segment for a transformed value — temporal
    /// transforms render human-readable (`2026`, `2026-01`, `2026-01-14`,
    /// `2026-01-14-13`), everything else renders the raw value. Cosmetic only:
    /// readers use the manifest's partition tuple, never the path.
    fn path_segment(&self, v: &PartVal) -> String {
        match (&self.transform, v) {
            (Transform::Year, PartVal::Int(y)) => format!("{}", 1970 + *y as i64),
            (Transform::Month, PartVal::Int(mo)) => {
                let mo = *mo as i64;
                format!(
                    "{:04}-{:02}",
                    1970 + mo.div_euclid(12),
                    mo.rem_euclid(12) + 1
                )
            }
            (Transform::Day, PartVal::Int(d)) => {
                let (y, m, d) = civil_from_days(*d as i64);
                format!("{y:04}-{m:02}-{d:02}")
            }
            (Transform::Hour, PartVal::Int(h)) => {
                let h = *h as i64;
                let (y, m, d) = civil_from_days(h.div_euclid(24));
                format!("{y:04}-{m:02}-{d:02}-{:02}", h.rem_euclid(24))
            }
            _ => v.path_segment(),
        }
    }
}

/// Parse one `partition_by` entry: a bare column name (identity) or a transform
/// expression — `bucket(N, col)`, `truncate(W, col)`, `year|month|day|hour(col)`.
pub fn parse_partition_expr(expr: &str) -> Result<(Transform, String)> {
    let expr = expr.trim();
    let (open, close) = match (expr.find('('), expr.rfind(')')) {
        (Some(o), Some(c)) if c > o => (o, c),
        (None, None) => return Ok((Transform::Identity, expr.to_string())),
        _ => return Err(err(format!("malformed partition expression `{expr}`"))),
    };
    if close != expr.len() - 1 {
        return Err(err(format!("malformed partition expression `{expr}`")));
    }
    // The grammar has no nesting — any parenthesis between the outermost pair
    // (`day(event_at))`, `day((event_at))`) is malformed, not a column name.
    if expr[open + 1..close].contains(['(', ')']) {
        return Err(err(format!("malformed partition expression `{expr}`")));
    }
    let func = expr[..open].trim();
    let args: Vec<&str> = expr[open + 1..close].split(',').map(str::trim).collect();
    let one = |args: &[&str]| -> Result<String> {
        match args {
            [col] if !col.is_empty() => Ok(col.to_string()),
            _ => Err(err(format!(
                "`{func}(col)` takes exactly one column, got `{expr}`"
            ))),
        }
    };
    let n_and_col = |args: &[&str]| -> Result<(u32, String)> {
        match args {
            [n, col] if !col.is_empty() => {
                let n: u32 = n.parse().map_err(|_| {
                    err(format!(
                        "`{func}(N, col)` needs a positive integer N, got `{n}`"
                    ))
                })?;
                if n == 0 {
                    return Err(err(format!("`{func}(N, col)` needs N ≥ 1")));
                }
                Ok((n, col.to_string()))
            }
            _ => Err(err(format!(
                "`{func}(N, col)` takes (N, column), got `{expr}`"
            ))),
        }
    };
    match func {
        "bucket" => {
            let (n, col) = n_and_col(&args)?;
            Ok((Transform::Bucket(n), col))
        }
        "truncate" => {
            let (w, col) = n_and_col(&args)?;
            Ok((Transform::Truncate(w), col))
        }
        "year" => Ok((Transform::Year, one(&args)?)),
        "month" => Ok((Transform::Month, one(&args)?)),
        "day" => Ok((Transform::Day, one(&args)?)),
        "hour" => Ok((Transform::Hour, one(&args)?)),
        other => Err(err(format!(
            "unknown partition transform `{other}` \
             (supported: identity, bucket(N, col), truncate(W, col), year, month, day, hour)"
        ))),
    }
}

/// Resolve `partition_by` entries (bare columns or transform expressions) against the
/// table's fields. Errors if a source column is missing or a transform does not apply
/// to its type.
pub fn resolve_partition_fields(
    ice_fields: &[IceField],
    partition_by: &[String],
) -> Result<Vec<PartitionField>> {
    let mut out: Vec<PartitionField> = Vec::with_capacity(partition_by.len());
    for (pos, expr) in partition_by.iter().enumerate() {
        let (transform, col) = parse_partition_expr(expr)?;
        let (idx, f) = ice_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name == col)
            .ok_or_else(|| err(format!("partition_by column `{col}` is not in the schema")))?;
        if !transform.accepts(&f.iceberg_type) {
            return Err(err(format!(
                "partition transform `{}` does not apply to `{col}` ({}) — see the \
                 Iceberg transform applicability rules",
                transform.spec_str(),
                f.iceberg_type
            )));
        }
        // Java Iceberg's naming convention: identity keeps the column name; transforms
        // suffix it (`event_at_day`, `id_bucket`, `name_trunc`).
        let name = match &transform {
            Transform::Identity => col.clone(),
            Transform::Bucket(_) => format!("{col}_bucket"),
            Transform::Truncate(_) => format!("{col}_trunc"),
            Transform::Year => format!("{col}_year"),
            Transform::Month => format!("{col}_month"),
            Transform::Day => format!("{col}_day"),
            Transform::Hour => format!("{col}_hour"),
        };
        if out.iter().any(|p| p.name == name) {
            return Err(err(format!("duplicate partition field `{name}`")));
        }
        out.push(PartitionField {
            name,
            source_id: f.id,
            field_id: 1000 + pos as i32,
            iceberg_type: transform.result_type(&f.iceberg_type),
            source_index: idx,
            source_is_date: f.iceberg_type == "date",
            transform,
        });
    }
    Ok(out)
}

/// The partition value of `arr` at row `i`.
fn part_val_at(arr: &dyn Array, i: usize) -> PartVal {
    if arr.is_null(i) {
        return PartVal::Null;
    }
    macro_rules! v {
        ($ty:ty) => {
            arr.as_any().downcast_ref::<$ty>().unwrap().value(i)
        };
    }
    match arr.data_type() {
        DataType::Boolean => PartVal::Bool(v!(BooleanArray)),
        DataType::Int64 => PartVal::Long(v!(Int64Array)),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            PartVal::Long(v!(TimestampMicrosecondArray))
        }
        DataType::Float64 => PartVal::Double(v!(Float64Array)),
        DataType::Date32 => PartVal::Int(v!(Date32Array)),
        DataType::Utf8 => PartVal::Str(v!(StringArray).to_string()),
        // `resolve_partition_fields` only admits the Iceberg types above, so any other
        // Arrow type here is a bug in that resolver — fail loudly rather than silently
        // routing the row into a wrong (null) partition.
        other => unreachable!("unsupported partition column arrow type: {other:?}"),
    }
}

/// One finished data file: its table-relative key, partition values, row count, byte
/// size, and per-column statistics.
pub struct PartitionDataFile {
    pub data_key: String,
    pub partition: Vec<PartVal>,
    pub rows: u64,
    pub size: i64,
    pub stats: Stats,
}

struct OpenPartition {
    data_key: String,
    partition: Vec<PartVal>,
    writer: IcebergDataWriter,
}

/// Routes rows to one data file per distinct partition key. Writes local files under
/// `base_dir` (the table dir for a local sink, a temp dir for an object-store sink).
pub struct PartitionedWriter {
    base_dir: PathBuf,
    schema: SchemaRef,
    compression: Compression,
    /// The table's Iceberg fields — carries the (possibly evolved) field-ids every
    /// data file's Parquet stamps and stats are keyed by.
    ice_fields: Vec<IceField>,
    fields: Vec<PartitionField>,
    open: HashMap<Vec<PartVal>, OpenPartition>,
}

impl PartitionedWriter {
    pub fn new(
        base_dir: &Path,
        schema: SchemaRef,
        compression: Compression,
        ice_fields: Vec<IceField>,
        fields: Vec<PartitionField>,
    ) -> Result<Self> {
        let mut w = PartitionedWriter {
            base_dir: base_dir.to_path_buf(),
            schema,
            compression,
            ice_fields,
            fields,
            open: HashMap::new(),
        };
        // Unpartitioned tables always have exactly one data file — create it eagerly so a
        // zero-row shape still writes a valid (empty) table, as before partitioning.
        if w.fields.is_empty() {
            w.partition_writer(&[])?;
        }
        Ok(w)
    }

    /// Route each row to the data file for its partition key (one file when unpartitioned).
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.fields.is_empty() {
            let op = self.partition_writer(&[])?;
            return op.writer.write_batch(batch);
        }
        // Group row indices by their TRANSFORMED partition key — identity keeps the
        // source value; hidden transforms (bucket/truncate/temporal) map it first.
        let mut groups: HashMap<Vec<PartVal>, Vec<u32>> = HashMap::new();
        for row in 0..batch.num_rows() {
            let key: Vec<PartVal> = self
                .fields
                .iter()
                .map(|f| {
                    let raw = part_val_at(batch.column(f.source_index).as_ref(), row);
                    f.transform.apply(raw, f.source_is_date)
                })
                .collect();
            groups.entry(key).or_default().push(row as u32);
        }
        // Take each group's rows into a sub-batch and write it to that partition's file.
        for (key, rows) in groups {
            let idx = UInt32Array::from(rows);
            let cols = batch
                .columns()
                .iter()
                .map(|c| take(c.as_ref(), &idx, None).map_err(err))
                .collect::<Result<Vec<_>>>()?;
            let sub = RecordBatch::try_new(batch.schema(), cols)?;
            let op = self.partition_writer(&key)?;
            op.writer.write_batch(&sub)?;
        }
        Ok(())
    }

    fn partition_writer(&mut self, key: &[PartVal]) -> Result<&mut OpenPartition> {
        if !self.open.contains_key(key) {
            let data_key = self.data_key(key);
            let path = self.base_dir.join(&data_key);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).map_err(err)?;
            }
            let writer = IcebergDataWriter::create_with_fields(
                &path,
                self.schema.clone(),
                self.compression,
                self.ice_fields.clone(),
            )?;
            self.open.insert(
                key.to_vec(),
                OpenPartition {
                    data_key,
                    partition: key.to_vec(),
                    writer,
                },
            );
        }
        Ok(self.open.get_mut(key).unwrap())
    }

    fn data_key(&self, key: &[PartVal]) -> String {
        let mut path = String::from("data");
        for (f, v) in self.fields.iter().zip(key) {
            path.push('/');
            path.push_str(&f.name);
            path.push('=');
            path.push_str(&f.path_segment(v));
        }
        format!("{path}/{}.parquet", Uuid::new_v4())
    }

    /// Close every partition's file and return their descriptors. Consumes the open
    /// writers (a second call returns an empty list).
    pub fn finish(&mut self) -> Result<Vec<PartitionDataFile>> {
        let mut out = Vec::with_capacity(self.open.len());
        for (_, mut op) in std::mem::take(&mut self.open) {
            let rows = op.writer.close()?;
            let stats = op.writer.column_stats();
            let size = std::fs::metadata(self.base_dir.join(&op.data_key))
                .map(|m| m.len())
                .unwrap_or(0) as i64;
            out.push(PartitionDataFile {
                data_key: op.data_key,
                partition: op.partition,
                rows,
                size,
                stats,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn ice(name: &str, ty: &str, id: i32) -> IceField {
        IceField {
            id,
            name: name.into(),
            iceberg_type: ty.into(),
            required: false,
        }
    }

    #[test]
    fn resolves_partition_fields_or_errors() {
        let fields = vec![ice("id", "long", 1), ice("region", "string", 2)];
        let p = resolve_partition_fields(&fields, &["region".to_string()]).unwrap();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].source_id, 2);
        assert_eq!(p[0].field_id, 1000); // partition ids start at 1000
        assert_eq!(p[0].source_index, 1);
        assert!(resolve_partition_fields(&fields, &["nope".to_string()]).is_err());
    }

    #[test]
    fn routes_rows_to_one_file_per_partition() {
        let dir = std::env::temp_dir().join(format!("ss-part-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let ids = Int64Array::from(vec![1, 2, 3]);
        let region = StringArray::from(vec!["us", "eu", "us"]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(region)]).unwrap();

        let ice_fields = vec![ice("id", "long", 1), ice("region", "string", 2)];
        let fields = resolve_partition_fields(&ice_fields, &["region".to_string()]).unwrap();
        let mut w =
            PartitionedWriter::new(&dir, schema, Compression::Snappy, ice_fields, fields).unwrap();
        w.write_batch(&batch).unwrap();
        let files = w.finish().unwrap();

        // Two partitions: "us" (2 rows) and "eu" (1 row), each a single string tuple.
        assert_eq!(files.len(), 2);
        let mut rows: Vec<u64> = files.iter().map(|f| f.rows).collect();
        rows.sort_unstable();
        assert_eq!(rows, vec![1, 2]);
        for f in &files {
            assert_eq!(f.partition.len(), 1);
            assert!(matches!(f.partition[0], PartVal::Str(_)));
            assert!(f.data_key.contains("region="));
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn murmur3_matches_the_iceberg_spec_test_vectors() {
        // Appendix B of the Iceberg spec pins these exact hashes; bucket correctness
        // (cross-engine!) depends on byte-for-byte agreement.
        assert_eq!(murmur3_x86_32(&34i64.to_le_bytes()), 2017239379); // int + long 34
        assert_eq!(murmur3_x86_32(b"iceberg"), 1210000089); // string
        assert_eq!(murmur3_x86_32(&17486i64.to_le_bytes()), -653330422); // date 2017-11-16
        assert_eq!(
            murmur3_x86_32(&1510871468000000i64.to_le_bytes()),
            -2047944441 // timestamp 2017-11-16T22:31:08
        );
    }

    #[test]
    fn transforms_apply_per_spec() {
        // bucket: (hash & i32::MAX) % N over the spec encoding.
        let b = Transform::Bucket(16);
        assert_eq!(
            b.apply(PartVal::Long(34), false),
            PartVal::Int((2017239379 & i32::MAX) % 16)
        );
        // date 17486 hashes as a long, then buckets.
        assert_eq!(
            b.apply(PartVal::Int(17486), true),
            PartVal::Int((-653330422i32 & i32::MAX) % 16)
        );
        // truncate ints floor to a multiple of W — including negatives.
        let t = Transform::Truncate(10);
        assert_eq!(t.apply(PartVal::Long(37), false), PartVal::Long(30));
        assert_eq!(t.apply(PartVal::Long(-1), false), PartVal::Long(-10));
        // truncate strings keep the first W code points (not bytes).
        let t4 = Transform::Truncate(4);
        assert_eq!(
            t4.apply(PartVal::Str("héllo world".into()), false),
            PartVal::Str("héll".into())
        );
        // temporal over a date (epoch days): 2017-11-16 = day 17486.
        assert_eq!(
            Transform::Day.apply(PartVal::Int(17486), true),
            PartVal::Int(17486)
        );
        assert_eq!(
            Transform::Year.apply(PartVal::Int(17486), true),
            PartVal::Int(47)
        );
        assert_eq!(
            Transform::Month.apply(PartVal::Int(17486), true),
            PartVal::Int(47 * 12 + 10)
        );
        // temporal over a timestamp (µs): 2017-11-16T22:31:08.
        let us = 1510871468000000i64;
        assert_eq!(
            Transform::Day.apply(PartVal::Long(us), false),
            PartVal::Int(17486)
        );
        assert_eq!(
            Transform::Hour.apply(PartVal::Long(us), false),
            PartVal::Int(17486 * 24 + 22)
        );
        // pre-1970 floors correctly: 1969-12-31T23:00:00 is day -1, hour -1, year -1.
        let pre = -3_600_000_000i64;
        assert_eq!(
            Transform::Day.apply(PartVal::Long(pre), false),
            PartVal::Int(-1)
        );
        assert_eq!(
            Transform::Hour.apply(PartVal::Long(pre), false),
            PartVal::Int(-1)
        );
        assert_eq!(
            Transform::Year.apply(PartVal::Long(pre), false),
            PartVal::Int(-1)
        );
        assert_eq!(
            Transform::Month.apply(PartVal::Long(pre), false),
            PartVal::Int(-1)
        );
        // null in → null out, for every transform.
        assert_eq!(b.apply(PartVal::Null, false), PartVal::Null);
        assert_eq!(Transform::Day.apply(PartVal::Null, false), PartVal::Null);
    }

    #[test]
    fn parses_partition_expressions_and_rejects_bad_ones() {
        assert_eq!(
            parse_partition_expr("region").unwrap(),
            (Transform::Identity, "region".into())
        );
        assert_eq!(
            parse_partition_expr("bucket(16, id)").unwrap(),
            (Transform::Bucket(16), "id".into())
        );
        assert_eq!(
            parse_partition_expr("truncate(4,name)").unwrap(),
            (Transform::Truncate(4), "name".into())
        );
        assert_eq!(
            parse_partition_expr(" day( event_at ) ").unwrap(),
            (Transform::Day, "event_at".into())
        );
        for bad in [
            "bucket(id)",      // missing N
            "bucket(0, id)",   // N must be ≥ 1
            "bucket(x, id)",   // non-integer N
            "day(a, b)",       // day takes one column
            "md5(id)",         // unknown transform
            "day(event_at) x", // trailing garbage
            "day(",            // unbalanced
            "day(event_at))",  // stray extra `)`
            "day((event_at))", // nested parens
        ] {
            assert!(parse_partition_expr(bad).is_err(), "should reject `{bad}`");
        }
    }

    #[test]
    fn resolve_validates_transform_type_applicability() {
        let fields = vec![
            ice("id", "long", 1),
            ice("amt", "double", 2),
            ice("event_at", "timestamp", 3),
            ice("day", "date", 4),
        ];
        // Applies: names carry the Java-style suffix; result types are the transform's.
        let p =
            resolve_partition_fields(&fields, &["day(event_at)".into(), "bucket(8, id)".into()])
                .unwrap();
        assert_eq!(p[0].name, "event_at_day");
        assert_eq!(p[0].iceberg_type, "int");
        assert_eq!(p[0].transform, Transform::Day);
        assert_eq!(p[1].name, "id_bucket");
        assert_eq!(p[1].iceberg_type, "int");
        // Rejected: bucket over double, hour over date, truncate over timestamp.
        assert!(resolve_partition_fields(&fields, &["bucket(8, amt)".into()]).is_err());
        assert!(resolve_partition_fields(&fields, &["hour(day)".into()]).is_err());
        assert!(resolve_partition_fields(&fields, &["truncate(2, event_at)".into()]).is_err());
        // Duplicate partition field names are rejected.
        assert!(resolve_partition_fields(
            &fields,
            &["day(event_at)".into(), "day(event_at)".into()]
        )
        .is_err());
    }

    #[test]
    fn temporal_path_segments_are_human_readable() {
        let fields = vec![ice("event_at", "timestamp", 1)];
        let day = &resolve_partition_fields(&fields, &["day(event_at)".into()]).unwrap()[0];
        assert_eq!(day.path_segment(&PartVal::Int(17486)), "2017-11-16");
        let hour = &resolve_partition_fields(&fields, &["hour(event_at)".into()]).unwrap()[0];
        assert_eq!(
            hour.path_segment(&PartVal::Int(17486 * 24 + 22)),
            "2017-11-16-22"
        );
        let month = &resolve_partition_fields(&fields, &["month(event_at)".into()]).unwrap()[0];
        assert_eq!(month.path_segment(&PartVal::Int(47 * 12 + 10)), "2017-11");
        let year = &resolve_partition_fields(&fields, &["year(event_at)".into()]).unwrap()[0];
        assert_eq!(year.path_segment(&PartVal::Int(47)), "2017");
    }

    #[test]
    fn part_val_hashes_and_compares_by_value() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(vec![PartVal::Str("us".into()), PartVal::Long(1)]);
        assert!(set.contains(&vec![PartVal::Str("us".into()), PartVal::Long(1)]));
        assert!(!set.contains(&vec![PartVal::Str("eu".into()), PartVal::Long(1)]));
        // NaN keys are stable (bit-equal), so rows with a NaN partition still group.
        assert_eq!(PartVal::Double(f64::NAN), PartVal::Double(f64::NAN));
    }
}
