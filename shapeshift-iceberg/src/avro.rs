//! A minimal, hand-rolled Avro Object Container File writer for exactly the two
//! Iceberg manifest shapes.
//!
//! Why not a general Avro crate? Iceberg readers map manifest columns by the
//! `field-id` attributes embedded in the file's Avro schema — and the general Rust
//! Avro writers drop those custom attributes when they serialize the schema into
//! the file header, so the resulting table is unreadable (DuckDB: *"No default
//! expression in FieldId Map"*). Emitting the container ourselves keeps the exact
//! `field-id`-carrying schema JSON in the header, and the binary encoding needed
//! here is small: zig-zag varint longs, length-prefixed strings, unions, and
//! (empty) records. This also drops a dependency, keeping the single-binary ethos.

use crate::partition::{PartVal, PartitionField};

/// Zig-zag + varint encode an Avro `int`/`long`.
fn enc_long(buf: &mut Vec<u8>, v: i64) {
    let mut n = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        if n & !0x7f == 0 {
            buf.push(n as u8);
            break;
        }
        buf.push(((n & 0x7f) | 0x80) as u8);
        n >>= 7;
    }
}

/// Length-prefixed bytes (also how Avro `string` is encoded).
fn enc_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    enc_long(buf, b.len() as i64);
    buf.extend_from_slice(b);
}

fn enc_str(buf: &mut Vec<u8>, s: &str) {
    enc_bytes(buf, s.as_bytes());
}

/// Assemble a single-block Avro OCF: magic, metadata map (schema + codec + any
/// extra Iceberg keys), sync marker, then one data block holding `record_count`
/// pre-encoded records.
fn build_ocf(
    schema_json: &str,
    extra_meta: &[(&str, &[u8])],
    record_count: usize,
    records: &[u8],
    sync: [u8; 16],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(records.len() + schema_json.len() + 128);
    out.extend_from_slice(b"Obj\x01");

    // Metadata map<string,bytes>: a positive block count, the entries, then a 0.
    let mut entries: Vec<(&str, &[u8])> = vec![
        ("avro.schema", schema_json.as_bytes()),
        ("avro.codec", b"null"),
    ];
    entries.extend_from_slice(extra_meta);
    enc_long(&mut out, entries.len() as i64);
    for (k, v) in &entries {
        enc_str(&mut out, k);
        enc_bytes(&mut out, v);
    }
    enc_long(&mut out, 0);
    out.extend_from_slice(&sync);

    // One data block: object count, byte size, the objects, the sync marker.
    enc_long(&mut out, record_count as i64);
    enc_long(&mut out, records.len() as i64);
    out.extend_from_slice(records);
    out.extend_from_slice(&sync);
    out
}

/// `manifest_entry` schema (Iceberg v2), `field-id`-carrying, with `__PARTITION__` a
/// placeholder for the `partition` record — its fields depend on the table's identity
/// partition spec, so the schema is built per table. The `data_file` record also carries
/// the optional per-column statistics (value / null counts, lower / upper bounds), each
/// an Iceberg `map<int, X>` as an Avro array of key-value records with the spec field-ids.
const MANIFEST_ENTRY_TEMPLATE: &str = r#"{"type":"record","name":"manifest_entry","fields":[{"name":"status","type":"int","field-id":0},{"name":"snapshot_id","type":["null","long"],"default":null,"field-id":1},{"name":"sequence_number","type":["null","long"],"default":null,"field-id":3},{"name":"file_sequence_number","type":["null","long"],"default":null,"field-id":4},{"name":"data_file","type":{"type":"record","name":"r2","fields":[{"name":"content","type":"int","field-id":134},{"name":"file_path","type":"string","field-id":100},{"name":"file_format","type":"string","field-id":101},{"name":"partition","type":__PARTITION__,"field-id":102},{"name":"record_count","type":"long","field-id":103},{"name":"file_size_in_bytes","type":"long","field-id":104},{"name":"value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k119_v120","fields":[{"name":"key","type":"int","field-id":119},{"name":"value","type":"long","field-id":120}]},"logicalType":"map"}],"default":null,"field-id":109},{"name":"null_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"k121_v122","fields":[{"name":"key","type":"int","field-id":121},{"name":"value","type":"long","field-id":122}]},"logicalType":"map"}],"default":null,"field-id":110},{"name":"lower_bounds","type":["null",{"type":"array","items":{"type":"record","name":"k126_v127","fields":[{"name":"key","type":"int","field-id":126},{"name":"value","type":"bytes","field-id":127}]},"logicalType":"map"}],"default":null,"field-id":125},{"name":"upper_bounds","type":["null",{"type":"array","items":{"type":"record","name":"k129_v130","fields":[{"name":"key","type":"int","field-id":129},{"name":"value","type":"bytes","field-id":130}]},"logicalType":"map"}],"default":null,"field-id":128}]},"field-id":2}]}"#;

/// The Avro type JSON for an identity partition field's value (matching the source type).
fn part_avro_type(iceberg_type: &str) -> &'static str {
    match iceberg_type {
        "boolean" => r#""boolean""#,
        "int" => r#""int""#, // bucket + temporal transform results
        "long" => r#""long""#,
        "double" => r#""double""#,
        "date" => r#"{"type":"int","logicalType":"date"}"#,
        "timestamp" => r#"{"type":"long","logicalType":"timestamp-micros"}"#,
        _ => r#""string""#, // string, and a safe fallback
    }
}

/// The `partition` record schema — one optional field per identity partition field.
fn partition_record_json(fields: &[PartitionField]) -> String {
    let cols: Vec<String> = fields
        .iter()
        .map(|f| {
            format!(
                r#"{{"name":{name:?},"type":["null",{ty}],"default":null,"field-id":{fid}}}"#,
                name = f.name,
                ty = part_avro_type(&f.iceberg_type),
                fid = f.field_id
            )
        })
        .collect();
    format!(
        r#"{{"type":"record","name":"r102","fields":[{}]}}"#,
        cols.join(",")
    )
}

/// The partition-spec fields JSON (identity transforms), embedded in the manifest header
/// and mirrored into the table metadata's `partition-specs`.
pub fn partition_spec_fields_json(fields: &[PartitionField]) -> String {
    let cols: Vec<String> = fields
        .iter()
        .map(|f| {
            format!(
                r#"{{"name":{name:?},"transform":{tf:?},"source-id":{sid},"field-id":{fid}}}"#,
                name = f.name,
                tf = f.transform.spec_str(),
                sid = f.source_id,
                fid = f.field_id
            )
        })
        .collect();
    format!("[{}]", cols.join(","))
}

/// Encode the `partition` record for one data file: each field is an optional value
/// (union tag, then the value in its Avro primitive encoding).
fn enc_partition(buf: &mut Vec<u8>, partition: &[PartVal]) {
    for v in partition {
        match v {
            PartVal::Null => enc_long(buf, 0), // union index 0 → null
            _ => {
                enc_long(buf, 1); // union index 1 → the value
                match v {
                    PartVal::Bool(b) => buf.push(*b as u8),
                    PartVal::Long(n) => enc_long(buf, *n),
                    PartVal::Int(n) => enc_long(buf, *n as i64),
                    PartVal::Double(d) => buf.extend_from_slice(&d.to_le_bytes()),
                    PartVal::Str(s) => enc_str(buf, s),
                    PartVal::Null => unreachable!(),
                }
            }
        }
    }
}

/// `manifest_file` schema (Iceberg v2), required fields only, `field-id`-carrying.
const MANIFEST_FILE_SCHEMA: &str = r#"{"type":"record","name":"manifest_file","fields":[{"name":"manifest_path","type":"string","field-id":500},{"name":"manifest_length","type":"long","field-id":501},{"name":"partition_spec_id","type":"int","field-id":502},{"name":"content","type":"int","field-id":517},{"name":"sequence_number","type":"long","field-id":515},{"name":"min_sequence_number","type":"long","field-id":516},{"name":"added_snapshot_id","type":"long","field-id":503},{"name":"added_files_count","type":"int","field-id":504},{"name":"existing_files_count","type":"int","field-id":505},{"name":"deleted_files_count","type":"int","field-id":506},{"name":"added_rows_count","type":"long","field-id":512},{"name":"existing_rows_count","type":"long","field-id":513},{"name":"deleted_rows_count","type":"long","field-id":514}]}"#;

/// One data file to record in the manifest, with its per-column statistics. Each stat
/// map is `(field_id, value)` pairs; an empty vec is encoded as a null (absent) field.
pub struct DataFileMeta {
    pub path: String,
    /// Identity partition values for this file (one per partition field, in spec order).
    /// Empty for an unpartitioned table.
    pub partition: Vec<PartVal>,
    pub record_count: i64,
    pub file_size: i64,
    pub value_counts: Vec<(i32, i64)>,
    pub null_counts: Vec<(i32, i64)>,
    pub lower_bounds: Vec<(i32, Vec<u8>)>,
    pub upper_bounds: Vec<(i32, Vec<u8>)>,
}

/// Encode an optional Iceberg `map<int, long>` (value / null counts): the union tag,
/// then a single Avro array block of key-value records, then the array terminator.
fn enc_opt_long_map(buf: &mut Vec<u8>, entries: &[(i32, i64)]) {
    if entries.is_empty() {
        enc_long(buf, 0); // union index 0 → null
        return;
    }
    enc_long(buf, 1); // union index 1 → the array
    enc_long(buf, entries.len() as i64);
    for (k, v) in entries {
        enc_long(buf, *k as i64);
        enc_long(buf, *v);
    }
    enc_long(buf, 0); // array end
}

/// Encode an optional Iceberg `map<int, bytes>` (lower / upper bounds).
fn enc_opt_bytes_map(buf: &mut Vec<u8>, entries: &[(i32, Vec<u8>)]) {
    if entries.is_empty() {
        enc_long(buf, 0);
        return;
    }
    enc_long(buf, 1);
    enc_long(buf, entries.len() as i64);
    for (k, v) in entries {
        enc_long(buf, *k as i64);
        enc_bytes(buf, v);
    }
    enc_long(buf, 0);
}

/// What a manifest's files hold: rows (`data`), or row-level deletes. Recorded both in
/// each entry's `data_file.content` and in the manifest header — and a manifest holds
/// only one kind, so a snapshot that deletes rows carries a separate delete manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestContent {
    /// Data files (`content = 0`).
    Data,
    /// Position-delete files (`content = 1`): `(file_path, pos)` rows naming the data
    /// rows a snapshot removes.
    PositionDeletes,
}

impl ManifestContent {
    /// The integer Iceberg records in `data_file.content` and the manifest list.
    pub fn id(self) -> i32 {
        match self {
            ManifestContent::Data => 0,
            ManifestContent::PositionDeletes => 1,
        }
    }

    fn header(self) -> &'static [u8] {
        match self {
            ManifestContent::Data => b"data",
            ManifestContent::PositionDeletes => b"deletes",
        }
    }
}

/// Write the manifest (Avro OCF) listing `data_files` as ADDED entries for
/// `snapshot_id`, over the identity `partition_fields` (empty = unpartitioned). The
/// embedded schema's `partition` record and each file's partition tuple are built from
/// `partition_fields`; `table_schema_json` and the partition spec are embedded as file
/// metadata, as Iceberg readers expect. `content` says whether the files are data or
/// position deletes.
pub fn write_manifest(
    data_files: &[DataFileMeta],
    partition_fields: &[PartitionField],
    snapshot_id: i64,
    table_schema_json: &str,
    content: ManifestContent,
    sync: [u8; 16],
) -> Vec<u8> {
    let mut body = Vec::new();
    for df in data_files {
        // manifest_entry
        enc_long(&mut body, 1); // status = ADDED
        enc_long(&mut body, 1); // snapshot_id union index 1 (long)
        enc_long(&mut body, snapshot_id);
        enc_long(&mut body, 0); // sequence_number union index 0 (null → inherited)
        enc_long(&mut body, 0); // file_sequence_number union index 0 (null → inherited)
                                // data_file record
        enc_long(&mut body, content.id() as i64); // content: 0 data, 1 position deletes
        enc_str(&mut body, &df.path); // file_path
        enc_str(&mut body, "PARQUET"); // file_format
        enc_partition(&mut body, &df.partition); // partition record (one value per field)
        enc_long(&mut body, df.record_count);
        enc_long(&mut body, df.file_size);
        // Optional per-column statistics (in schema order).
        enc_opt_long_map(&mut body, &df.value_counts);
        enc_opt_long_map(&mut body, &df.null_counts);
        enc_opt_bytes_map(&mut body, &df.lower_bounds);
        enc_opt_bytes_map(&mut body, &df.upper_bounds);
    }
    let schema =
        MANIFEST_ENTRY_TEMPLATE.replace("__PARTITION__", &partition_record_json(partition_fields));
    let spec_fields = partition_spec_fields_json(partition_fields);
    let extra: &[(&str, &[u8])] = &[
        ("schema", table_schema_json.as_bytes()),
        ("schema-id", b"0"),
        ("partition-spec", spec_fields.as_bytes()),
        ("partition-spec-id", b"0"),
        ("format-version", b"2"),
        ("content", content.header()),
    ];
    build_ocf(&schema, extra, data_files.len(), &body, sync)
}

/// One `manifest_file` record — a manifest referenced by a snapshot's manifest list.
/// All 13 v2 fields are carried, so a manifest from a prior snapshot can be re-emitted
/// **losslessly** when appending a new snapshot onto an existing table.
#[derive(Debug, Clone)]
pub struct ManifestFileEntry {
    pub path: String,
    pub length: i64,
    pub partition_spec_id: i32,
    pub content: i32,
    pub sequence_number: i64,
    pub min_sequence_number: i64,
    pub added_snapshot_id: i64,
    pub added_files: i32,
    pub existing_files: i32,
    pub deleted_files: i32,
    pub added_rows: i64,
    pub existing_rows: i64,
    pub deleted_rows: i64,
}

impl ManifestFileEntry {
    /// A manifest whose data files are all *added* in this snapshot (no existing /
    /// deleted files) — an unpartitioned data manifest, the only kind shapeshift writes.
    pub fn added(
        path: String,
        length: i64,
        added_snapshot_id: i64,
        added_files: i32,
        added_rows: i64,
        sequence_number: i64,
    ) -> Self {
        ManifestFileEntry {
            path,
            length,
            partition_spec_id: 0,
            content: 0,
            sequence_number,
            min_sequence_number: sequence_number,
            added_snapshot_id,
            added_files,
            existing_files: 0,
            deleted_files: 0,
            added_rows,
            existing_rows: 0,
            deleted_rows: 0,
        }
    }

    /// The same manifest record, marked as holding `content` — a delete manifest's
    /// "rows" are its position deletes and its "files" its delete files.
    pub fn with_content(mut self, content: ManifestContent) -> Self {
        self.content = content.id();
        self
    }

    /// Live (non-deleted) rows this manifest contributes to the table.
    pub fn live_rows(&self) -> i64 {
        self.added_rows + self.existing_rows
    }

    /// Live (non-deleted) data files this manifest contributes.
    pub fn live_files(&self) -> i64 {
        (self.added_files + self.existing_files) as i64
    }
}

/// Write the manifest list (Avro OCF) for a snapshot from full `manifest_file` records.
pub fn write_manifest_list(manifests: &[ManifestFileEntry], sync: [u8; 16]) -> Vec<u8> {
    let mut body = Vec::new();
    for m in manifests {
        enc_str(&mut body, &m.path); // manifest_path
        enc_long(&mut body, m.length); // manifest_length
        enc_long(&mut body, m.partition_spec_id as i64); // partition_spec_id
        enc_long(&mut body, m.content as i64); // content
        enc_long(&mut body, m.sequence_number); // sequence_number
        enc_long(&mut body, m.min_sequence_number); // min_sequence_number
        enc_long(&mut body, m.added_snapshot_id); // added_snapshot_id
        enc_long(&mut body, m.added_files as i64); // added_files_count
        enc_long(&mut body, m.existing_files as i64); // existing_files_count
        enc_long(&mut body, m.deleted_files as i64); // deleted_files_count
        enc_long(&mut body, m.added_rows); // added_rows_count
        enc_long(&mut body, m.existing_rows); // existing_rows_count
        enc_long(&mut body, m.deleted_rows); // deleted_rows_count
    }
    build_ocf(MANIFEST_FILE_SCHEMA, &[], manifests.len(), &body, sync)
}

// ---- Reading, for append-to-existing / multi-snapshot ----
//
// The single wrinkle the hand-rolled writer buys us: to append a snapshot we must read
// the prior snapshot's manifest list back and carry its `manifest_file` records forward.
// The subset we emit is small, so the reader is too — the exact inverse of `build_ocf`.

use shapeshift_core::{Result, ShapeError};

fn read_err(msg: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(format!("reading Iceberg manifest metadata: {msg}"))
}

/// A byte cursor over the Avro OCF subset shapeshift writes.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| read_err("length overflow"))?;
        let s = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| read_err("unexpected end of file"))?;
        self.pos = end;
        Ok(s)
    }

    /// Decode a zig-zag varint `long`/`int`.
    fn long(&mut self) -> Result<i64> {
        let mut shift = 0u32;
        let mut acc: u64 = 0;
        loop {
            let byte = *self
                .buf
                .get(self.pos)
                .ok_or_else(|| read_err("truncated varint"))?;
            self.pos += 1;
            acc |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift >= 64 {
                return Err(read_err("varint too long"));
            }
        }
        Ok(((acc >> 1) as i64) ^ -((acc & 1) as i64))
    }

    fn int(&mut self) -> Result<i32> {
        Ok(self.long()? as i32)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.long()?;
        if n < 0 {
            return Err(read_err("negative length"));
        }
        self.take(n as usize)
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(read_err)
    }
}

/// Skip an Avro `map<string,bytes>` block series (the OCF metadata block).
fn skip_map(r: &mut Reader) -> Result<()> {
    loop {
        let n = r.long()?;
        if n == 0 {
            break;
        }
        // A negative block count is followed by a byte size, then |n| entries.
        let count = if n < 0 {
            let _byte_size = r.long()?;
            n.checked_neg().ok_or_else(|| read_err("bad block count"))?
        } else {
            n
        };
        for _ in 0..count {
            let _k = r.bytes()?;
            let _v = r.bytes()?;
        }
    }
    Ok(())
}

/// Parse a manifest-list OCF back into its `manifest_file` records — the manifests a
/// snapshot references. Append-to-existing carries these forward into the new snapshot's
/// manifest list so the table's full history stays live.
pub fn read_manifest_list(data: &[u8]) -> Result<Vec<ManifestFileEntry>> {
    let mut r = Reader::new(data);
    if r.take(4)? != b"Obj\x01" {
        return Err(read_err("not an Avro object container file (bad magic)"));
    }
    skip_map(&mut r)?;
    r.take(16)?; // header sync marker
    let mut out = Vec::new();
    // Data blocks run to EOF; each is: object count, byte size, objects, sync marker.
    while r.remaining() > 0 {
        let count = r.long()?;
        let _block_bytes = r.long()?;
        for _ in 0..count {
            out.push(ManifestFileEntry {
                path: r.string()?,
                length: r.long()?,
                partition_spec_id: r.int()?,
                content: r.int()?,
                sequence_number: r.long()?,
                min_sequence_number: r.long()?,
                added_snapshot_id: r.long()?,
                added_files: r.int()?,
                existing_files: r.int()?,
                deleted_files: r.int()?,
                added_rows: r.long()?,
                existing_rows: r.long()?,
                deleted_rows: r.long()?,
            });
        }
        r.take(16)?; // block sync marker
    }
    Ok(out)
}

/// One data-file entry read back from a manifest OCF.
#[derive(Debug, Clone)]
pub struct ManifestDataFile {
    pub file_path: String,
    pub record_count: i64,
    /// Iceberg entry status: 0 EXISTING, 1 ADDED, 2 DELETED.
    pub status: i32,
    /// File content: 0 = data, 1 = position deletes, 2 = equality deletes.
    pub content: i32,
}

/// Parse a manifest OCF back into its data-file entries — the summary view of
/// [`read_manifest_entries`]. Partitioned and unpartitioned manifests both read: the
/// layout comes from the schema the file carries.
pub fn read_manifest_data_files(data: &[u8]) -> Result<Vec<ManifestDataFile>> {
    Ok(read_manifest_entries(data)?
        .into_iter()
        .map(|e| ManifestDataFile {
            file_path: e.file_path,
            record_count: e.record_count,
            status: e.status,
            content: e.content,
        })
        .collect())
}

// ---- Schema-driven decoding (manifests) ----
//
// A manifest's `data_file.partition` record has one field per partition field, so its
// byte layout depends on the table — and other engines add optional fields shapeshift
// never writes. Rather than assume a layout, the reader parses the Avro schema the file
// carries in its own header and decodes against it. That is what lets a merge recover
// the exact partition tuple of every existing data file: a delete written into the wrong
// partition is silently never applied.

/// The parsed shape of an Avro schema — just enough to walk the bytes. Logical types
/// (`date`, `timestamp-micros`, …) decode as their underlying encoding.
#[derive(Debug, Clone)]
enum AvroType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    Str,
    Record(Vec<(String, AvroType)>),
    Enum,
    Array(Box<AvroType>),
    Map(Box<AvroType>),
    Union(Vec<AvroType>),
    Fixed(usize),
}

fn primitive(name: &str) -> Option<AvroType> {
    Some(match name {
        "null" => AvroType::Null,
        "boolean" => AvroType::Boolean,
        "int" => AvroType::Int,
        "long" => AvroType::Long,
        "float" => AvroType::Float,
        "double" => AvroType::Double,
        "bytes" => AvroType::Bytes,
        "string" => AvroType::Str,
        _ => return None,
    })
}

/// Parse a schema JSON node. Named types (records, enums, fixed) are registered as they
/// are defined, so a later reference by name resolves to the same shape.
fn parse_type(
    v: &serde_json::Value,
    named: &mut std::collections::HashMap<String, AvroType>,
) -> Result<AvroType> {
    use serde_json::Value as J;
    match v {
        J::String(s) => match primitive(s) {
            Some(t) => Ok(t),
            None => named
                .get(s.as_str())
                .cloned()
                .ok_or_else(|| read_err(format!("unknown avro type `{s}`"))),
        },
        J::Array(branches) => Ok(AvroType::Union(
            branches
                .iter()
                .map(|b| parse_type(b, named))
                .collect::<Result<_>>()?,
        )),
        J::Object(o) => {
            let ty = o
                .get("type")
                .ok_or_else(|| read_err("avro schema object has no `type`"))?;
            let t = match ty.as_str() {
                Some("record") | Some("error") => {
                    let fields = o
                        .get("fields")
                        .and_then(|f| f.as_array())
                        .ok_or_else(|| read_err("avro record has no `fields`"))?;
                    let mut out = Vec::with_capacity(fields.len());
                    for f in fields {
                        let name = f["name"]
                            .as_str()
                            .ok_or_else(|| read_err("avro record field has no `name`"))?;
                        out.push((name.to_string(), parse_type(&f["type"], named)?));
                    }
                    AvroType::Record(out)
                }
                Some("enum") => AvroType::Enum,
                Some("array") => AvroType::Array(Box::new(parse_type(&o["items"], named)?)),
                Some("map") => AvroType::Map(Box::new(parse_type(&o["values"], named)?)),
                Some("fixed") => AvroType::Fixed(
                    o.get("size")
                        .and_then(|n| n.as_u64())
                        .ok_or_else(|| read_err("avro fixed has no `size`"))?
                        as usize,
                ),
                // `{"type":"int","logicalType":"date"}` and friends: a logical type rides
                // on its underlying encoding.
                _ => parse_type(ty, named)?,
            };
            if let Some(name) = o.get("name").and_then(|n| n.as_str()) {
                if let Some(ns) = o.get("namespace").and_then(|n| n.as_str()) {
                    named.insert(format!("{ns}.{name}"), t.clone());
                }
                named.insert(name.to_string(), t.clone());
            }
            Ok(t)
        }
        _ => Err(read_err("malformed avro schema")),
    }
}

/// A decoded Avro value. A union decodes straight to its chosen branch. Types no
/// manifest field shapeshift reads can hold are consumed and kept only as `Other`.
#[derive(Debug, Clone)]
enum AvroValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Double(f64),
    Bytes(Vec<u8>),
    Str(String),
    Record(Vec<(String, AvroValue)>),
    Array(Vec<AvroValue>),
    /// A float, enum, map, or fixed — named by its Avro type.
    Other(&'static str),
}

impl AvroValue {
    /// The Avro type a value decoded from, for error messages.
    fn kind(&self) -> &'static str {
        match self {
            AvroValue::Null => "null",
            AvroValue::Boolean(_) => "boolean",
            AvroValue::Int(_) => "int",
            AvroValue::Long(_) => "long",
            AvroValue::Double(_) => "double",
            AvroValue::Bytes(_) => "bytes",
            AvroValue::Str(_) => "string",
            AvroValue::Record(_) => "record",
            AvroValue::Array(_) => "array",
            AvroValue::Other(kind) => kind,
        }
    }

    /// A record's field by name (`None` for an absent field or a non-record).
    fn get(&self, name: &str) -> Option<&AvroValue> {
        match self {
            AvroValue::Record(fields) => fields.iter().find(|(n, _)| n == name).map(|(_, v)| v),
            _ => None,
        }
    }

    fn as_long(&self) -> Option<i64> {
        match self {
            AvroValue::Int(n) => Some(*n as i64),
            AvroValue::Long(n) => Some(*n),
            _ => None,
        }
    }
}

/// Walk an Avro block series (arrays, maps, the OCF header map): a count, then that many
/// items, until a zero count. A negative count is followed by the block's byte size.
fn read_blocks(r: &mut Reader, mut item: impl FnMut(&mut Reader) -> Result<()>) -> Result<()> {
    loop {
        let n = r.long()?;
        if n == 0 {
            return Ok(());
        }
        let count = if n < 0 {
            let _byte_size = r.long()?;
            n.checked_neg().ok_or_else(|| read_err("bad block count"))?
        } else {
            n
        };
        for _ in 0..count {
            item(r)?;
        }
    }
}

fn decode(r: &mut Reader, t: &AvroType) -> Result<AvroValue> {
    Ok(match t {
        AvroType::Null => AvroValue::Null,
        AvroType::Boolean => AvroValue::Boolean(r.take(1)?[0] != 0),
        AvroType::Int => AvroValue::Int(r.int()?),
        AvroType::Long => AvroValue::Long(r.long()?),
        AvroType::Float => {
            r.take(4)?;
            AvroValue::Other("float")
        }
        AvroType::Double => AvroValue::Double(f64::from_le_bytes(r.take(8)?.try_into().unwrap())),
        AvroType::Bytes => AvroValue::Bytes(r.bytes()?.to_vec()),
        AvroType::Str => AvroValue::Str(r.string()?),
        AvroType::Record(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for (name, ft) in fields {
                out.push((name.clone(), decode(r, ft)?));
            }
            AvroValue::Record(out)
        }
        AvroType::Enum => {
            r.int()?;
            AvroValue::Other("enum")
        }
        AvroType::Array(items) => {
            let mut out = Vec::new();
            read_blocks(r, |r| {
                out.push(decode(r, items)?);
                Ok(())
            })?;
            AvroValue::Array(out)
        }
        AvroType::Map(values) => {
            read_blocks(r, |r| {
                r.bytes()?; // key
                decode(r, values).map(drop)
            })?;
            AvroValue::Other("map")
        }
        AvroType::Union(branches) => {
            let i = r.long()?;
            let branch = usize::try_from(i)
                .ok()
                .and_then(|i| branches.get(i))
                .ok_or_else(|| read_err(format!("union branch {i} out of range")))?;
            decode(r, branch)?
        }
        AvroType::Fixed(n) => {
            r.take(*n)?;
            AvroValue::Other("fixed")
        }
    })
}

/// Decode a whole Avro Object Container File against the schema in its own header.
/// Only the `null` codec is read — the one shapeshift writes; a compressed file (other
/// engines often use `deflate`) is refused by name rather than misread.
fn read_ocf(data: &[u8]) -> Result<Vec<AvroValue>> {
    let mut r = Reader::new(data);
    if r.take(4)? != b"Obj\x01" {
        return Err(read_err("not an Avro object container file (bad magic)"));
    }
    let mut schema: Option<Vec<u8>> = None;
    let mut codec: Option<Vec<u8>> = None;
    read_blocks(&mut r, |r| {
        let k = r.string()?;
        let v = r.bytes()?;
        match k.as_str() {
            "avro.schema" => schema = Some(v.to_vec()),
            "avro.codec" => codec = Some(v.to_vec()),
            _ => {}
        }
        Ok(())
    })?;
    let sync = r.take(16)?;
    if let Some(c) = codec.filter(|c| c.as_slice() != b"null") {
        return Err(read_err(format!(
            "avro codec `{}` is not supported — shapeshift reads the uncompressed \
             manifests it writes",
            String::from_utf8_lossy(&c)
        )));
    }
    let schema = schema.ok_or_else(|| read_err("no avro.schema in the file header"))?;
    let schema: serde_json::Value = serde_json::from_slice(&schema).map_err(read_err)?;
    let ty = parse_type(&schema, &mut std::collections::HashMap::new())?;

    let mut records = Vec::new();
    // Data blocks run to EOF; each is: object count, byte size, objects, sync marker.
    while r.remaining() > 0 {
        let count = r.long()?;
        let _block_bytes = r.long()?;
        for _ in 0..count {
            records.push(decode(&mut r, &ty)?);
        }
        if r.take(16)? != sync {
            return Err(read_err("block sync marker does not match the header"));
        }
    }
    Ok(records)
}

/// One manifest entry, read back in full: the entry's status and sequence numbers, and
/// the data (or delete) file it tracks — including its partition tuple and the
/// per-column bounds a merge prunes files with.
#[derive(Debug, Clone)]
pub struct ManifestEntry {
    /// 0 EXISTING, 1 ADDED, 2 DELETED.
    pub status: i32,
    pub snapshot_id: Option<i64>,
    /// `None` = inherited from the manifest list (an entry ADDED in that snapshot).
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    /// 0 data, 1 position deletes, 2 equality deletes.
    pub content: i32,
    pub file_path: String,
    /// One value per partition field, in the partition spec's order (empty when
    /// unpartitioned).
    pub partition: Vec<PartVal>,
    pub record_count: i64,
    pub file_size: i64,
    pub lower_bounds: Vec<(i32, Vec<u8>)>,
    pub upper_bounds: Vec<(i32, Vec<u8>)>,
}

impl ManifestEntry {
    /// Whether the entry is live in the snapshot that lists it (not DELETED).
    pub fn is_live(&self) -> bool {
        self.status != 2
    }
}

fn opt_long(v: Option<&AvroValue>) -> Option<i64> {
    v.and_then(AvroValue::as_long)
}

/// An Iceberg `map<int, bytes>` (bounds), written as an array of key/value records.
fn bounds_map(v: Option<&AvroValue>) -> Vec<(i32, Vec<u8>)> {
    let Some(AvroValue::Array(items)) = v else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|kv| match (kv.get("key"), kv.get("value")) {
            (Some(k), Some(AvroValue::Bytes(b))) => Some((k.as_long()? as i32, b.clone())),
            _ => None,
        })
        .collect()
}

fn part_val(v: &AvroValue) -> Result<PartVal> {
    Ok(match v {
        AvroValue::Null => PartVal::Null,
        AvroValue::Boolean(b) => PartVal::Bool(*b),
        AvroValue::Int(n) => PartVal::Int(*n),
        AvroValue::Long(n) => PartVal::Long(*n),
        AvroValue::Double(d) => PartVal::Double(*d),
        AvroValue::Str(s) => PartVal::Str(s.clone()),
        other => {
            return Err(read_err(format!(
                "a partition value of Avro type `{}` is not one shapeshift partitions by",
                other.kind()
            )))
        }
    })
}

/// Parse a manifest OCF into its entries, decoding against the schema in the file's own
/// header — so the partition tuple reads correctly for any partition spec.
pub fn read_manifest_entries(data: &[u8]) -> Result<Vec<ManifestEntry>> {
    let mut out = Vec::new();
    for rec in read_ocf(data)? {
        let df = rec
            .get("data_file")
            .ok_or_else(|| read_err("manifest entry has no data_file"))?;
        let partition = match df.get("partition") {
            Some(AvroValue::Record(fields)) => fields
                .iter()
                .map(|(_, v)| part_val(v))
                .collect::<Result<Vec<_>>>()?,
            _ => Vec::new(),
        };
        out.push(ManifestEntry {
            status: rec
                .get("status")
                .and_then(AvroValue::as_long)
                .ok_or_else(|| read_err("manifest entry has no status"))?
                as i32,
            snapshot_id: opt_long(rec.get("snapshot_id")),
            sequence_number: opt_long(rec.get("sequence_number")),
            file_sequence_number: opt_long(rec.get("file_sequence_number")),
            // v1 manifests have no `content` — every file in them is data.
            content: opt_long(df.get("content")).unwrap_or(0) as i32,
            file_path: match df.get("file_path") {
                Some(AvroValue::Str(s)) => s.clone(),
                _ => return Err(read_err("data_file has no file_path")),
            },
            partition,
            record_count: opt_long(df.get("record_count"))
                .ok_or_else(|| read_err("data_file has no record_count"))?,
            file_size: opt_long(df.get("file_size_in_bytes")).unwrap_or(0),
            lower_bounds: bounds_map(df.get("lower_bounds")),
            upper_bounds: bounds_map(df.get("upper_bounds")),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_roundtrip_matches_reference() {
        // Known Avro zig-zag varint encodings.
        let mut b = Vec::new();
        enc_long(&mut b, 0);
        assert_eq!(b, vec![0x00]);
        b.clear();
        enc_long(&mut b, -1);
        assert_eq!(b, vec![0x01]);
        b.clear();
        enc_long(&mut b, 1);
        assert_eq!(b, vec![0x02]);
        b.clear();
        enc_long(&mut b, 2147483647);
        assert_eq!(b, vec![0xfe, 0xff, 0xff, 0xff, 0x0f]);
    }

    #[test]
    fn manifest_has_field_ids_and_magic() {
        let bytes = write_manifest(
            &[DataFileMeta {
                path: "/t/data/x.parquet".into(),
                partition: vec![],
                record_count: 3,
                file_size: 100,
                value_counts: vec![(1, 3)],
                null_counts: vec![(1, 0)],
                lower_bounds: vec![(1, 1i64.to_le_bytes().to_vec())],
                upper_bounds: vec![(1, 3i64.to_le_bytes().to_vec())],
            }],
            &[],
            42,
            "{\"type\":\"struct\"}",
            ManifestContent::Data,
            [7u8; 16],
        );
        assert_eq!(&bytes[..4], b"Obj\x01");
        let head = String::from_utf8_lossy(&bytes[..800]);
        assert!(
            head.contains("field-id"),
            "embedded schema must carry field-id"
        );
        assert!(head.contains("manifest_entry"));
    }

    #[test]
    fn manifest_data_files_write_read_roundtrip() {
        let bytes = write_manifest(
            &[
                DataFileMeta {
                    path: "/t/data/a.parquet".into(),
                    partition: vec![],
                    record_count: 3,
                    file_size: 100,
                    value_counts: vec![(1, 3)],
                    null_counts: vec![(1, 0)],
                    lower_bounds: vec![(1, 1i64.to_le_bytes().to_vec())],
                    upper_bounds: vec![(1, 3i64.to_le_bytes().to_vec())],
                },
                DataFileMeta {
                    path: "/t/data/b.parquet".into(),
                    partition: vec![],
                    record_count: 5,
                    file_size: 200,
                    value_counts: vec![],
                    null_counts: vec![],
                    lower_bounds: vec![],
                    upper_bounds: vec![],
                },
            ],
            &[],
            42,
            "{\"type\":\"struct\"}",
            ManifestContent::Data,
            [7u8; 16],
        );
        let files = read_manifest_data_files(&bytes).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].file_path, "/t/data/a.parquet");
        assert_eq!(files[0].record_count, 3);
        assert_eq!(files[0].status, 1); // ADDED
        assert_eq!(files[0].content, 0);
        assert_eq!(files[1].file_path, "/t/data/b.parquet");
        assert_eq!(files[1].record_count, 5);
    }

    #[test]
    fn partitioned_manifest_entries_roundtrip_through_the_header_schema() {
        use crate::partition::resolve_partition_fields;
        use crate::IceField;
        let ice = |id, name: &str, ty: &str| IceField {
            id,
            name: name.into(),
            iceberg_type: ty.into(),
            required: false,
        };
        let fields = vec![
            ice(1, "id", "long"),
            ice(2, "region", "string"),
            ice(3, "at", "timestamp"),
        ];
        let parts = resolve_partition_fields(
            &fields,
            &[
                "region".to_string(),
                "day(at)".to_string(),
                "bucket(4, id)".to_string(),
            ],
        )
        .unwrap();
        let file = |path: &str, region: Option<&str>, day: i32| DataFileMeta {
            path: path.into(),
            partition: vec![
                region.map_or(PartVal::Null, |r| PartVal::Str(r.into())),
                PartVal::Int(day),
                PartVal::Int(3),
            ],
            record_count: 7,
            file_size: 99,
            value_counts: vec![(1, 7)],
            null_counts: vec![(1, 0)],
            lower_bounds: vec![(1, 10i64.to_le_bytes().to_vec())],
            upper_bounds: vec![(1, 20i64.to_le_bytes().to_vec())],
        };
        let bytes = write_manifest(
            &[
                file("/t/a.parquet", Some("eu"), 20_000),
                file("/t/b.parquet", None, -1),
            ],
            &parts,
            42,
            "{}",
            ManifestContent::PositionDeletes,
            [3u8; 16],
        );
        let entries = read_manifest_entries(&bytes).unwrap();
        assert_eq!(entries.len(), 2);
        let a = &entries[0];
        assert_eq!(a.file_path, "/t/a.parquet");
        assert_eq!(a.content, 1, "a delete manifest's entries say so");
        assert_eq!(a.status, 1);
        assert_eq!(a.snapshot_id, Some(42));
        assert_eq!(a.sequence_number, None, "ADDED entries inherit");
        assert_eq!(
            a.partition,
            vec![
                PartVal::Str("eu".into()),
                PartVal::Int(20_000),
                PartVal::Int(3)
            ]
        );
        assert_eq!(a.record_count, 7);
        assert_eq!(a.file_size, 99);
        assert_eq!(a.lower_bounds, vec![(1, 10i64.to_le_bytes().to_vec())]);
        assert_eq!(a.upper_bounds, vec![(1, 20i64.to_le_bytes().to_vec())]);
        assert_eq!(entries[1].partition[0], PartVal::Null);
        assert_eq!(entries[1].partition[1], PartVal::Int(-1));
    }

    #[test]
    fn compressed_manifests_are_refused_by_name() {
        let mut bytes = write_manifest(&[], &[], 1, "{}", ManifestContent::Data, [1u8; 16]);
        // The codec's value follows its key in the header map; "null" also appears in
        // the schema JSON, so search from the key.
        let key = bytes.windows(10).position(|w| w == b"avro.codec").unwrap();
        let at = key + bytes[key..].windows(4).position(|w| w == b"null").unwrap();
        bytes[at..at + 4].copy_from_slice(b"zstd");
        let err = read_manifest_entries(&bytes).unwrap_err().to_string();
        assert!(err.contains("codec `zstd`"), "{err}");
    }

    #[test]
    fn manifest_list_write_read_roundtrip() {
        // Two manifests: one "added this snapshot", one carried forward with existing
        // files — exactly the shape append-to-existing produces.
        let carried = ManifestFileEntry {
            path: "s3://b/t/metadata/old-m0.avro".into(),
            length: 1234,
            partition_spec_id: 0,
            content: 0,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 111,
            added_files: 2,
            existing_files: 0,
            deleted_files: 0,
            added_rows: 20,
            existing_rows: 0,
            deleted_rows: 0,
        };
        let fresh =
            ManifestFileEntry::added("s3://b/t/metadata/new-m0.avro".into(), 567, 222, 1, 5, 2);
        let bytes = write_manifest_list(&[carried.clone(), fresh.clone()], [9u8; 16]);
        let back = read_manifest_list(&bytes).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].path, carried.path);
        assert_eq!(back[0].added_snapshot_id, 111);
        assert_eq!(back[0].added_rows, 20);
        assert_eq!(back[0].sequence_number, 1);
        assert_eq!(back[1].path, fresh.path);
        assert_eq!(back[1].added_snapshot_id, 222);
        assert_eq!(back[1].added_files, 1);
        assert_eq!(back[1].added_rows, 5);
        assert_eq!(back[1].sequence_number, 2);
        // Live-row accounting the append path relies on.
        assert_eq!(back.iter().map(|m| m.live_rows()).sum::<i64>(), 25);
    }

    #[test]
    fn read_manifest_list_rejects_bad_magic() {
        assert!(read_manifest_list(b"not-avro").is_err());
    }
}
