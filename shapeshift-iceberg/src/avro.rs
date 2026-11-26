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
const MANIFEST_FILE_SCHEMA: &str = r#"{"type":"record","name":"manifest_file","fields":[{"name":"manifest_path","type":"string","field-id":500},{"name":"manifest_length","type":"long","field-id":501},{"name":"partition_spec_id","type":"int","field-id":502},{"name":"content","type":"int","field-id":517},{"name":"sequence_number","type":"long","field-id":515},{"name":"min_sequence_number","type":"long","field-id":516},{"name":"added_snapshot_id","type":"long","field-id":503},{"name":"added_files_count","type":"int","field-id":504},{"name":"existing_files_count","type":"int","field-id":505},{"name":"deleted_files_count","type":"int","field-id":506},{"name":"added_rows_count","type":"long","field-id":512},{"name":"existing_rows_count","type":"long","field-id":513},{"name":"deleted_rows_count","type":"long","field-id":514},{"name":"partitions","type":["null",{"type":"array","items":{"type":"record","name":"r508","fields":[{"name":"contains_null","type":"boolean","field-id":509},{"name":"contains_nan","type":["null","boolean"],"default":null,"field-id":518},{"name":"lower_bound","type":["null","bytes"],"default":null,"field-id":510},{"name":"upper_bound","type":["null","bytes"],"default":null,"field-id":511}]},"element-id":508}],"default":null,"field-id":507}]}"#;

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

/// How a manifest entry stands in the snapshot whose manifest list names the manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// Live, added by an earlier snapshot.
    Existing,
    /// Live, added by this snapshot.
    Added,
    /// Removed by this snapshot. Readers skip it; it records what the snapshot removed.
    Deleted,
}

impl EntryStatus {
    fn id(self) -> i64 {
        match self {
            EntryStatus::Existing => 0,
            EntryStatus::Added => 1,
            EntryStatus::Deleted => 2,
        }
    }
}

/// One manifest entry to write: a file and how it stands in the snapshot.
pub struct EntryOut<'a> {
    pub status: EntryStatus,
    /// The snapshot that added the file (an EXISTING entry) or that adds or removes it
    /// (an ADDED or DELETED entry).
    pub snapshot_id: i64,
    /// The file's data sequence number. `None` inherits the manifest's, which the spec
    /// allows only for an entry ADDED by the manifest's own snapshot. A carried EXISTING
    /// or DELETED entry must keep the number it was first committed with, since a
    /// position delete applies only to data files at or below its own number.
    pub sequence_number: Option<i64>,
    pub file_sequence_number: Option<i64>,
    pub file: &'a DataFileMeta,
}

fn enc_opt_long(buf: &mut Vec<u8>, v: Option<i64>) {
    match v {
        None => enc_long(buf, 0), // union index 0 → null
        Some(n) => {
            enc_long(buf, 1); // union index 1 → long
            enc_long(buf, n);
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
    let entries: Vec<EntryOut> = data_files
        .iter()
        .map(|file| EntryOut {
            status: EntryStatus::Added,
            snapshot_id,
            sequence_number: None,
            file_sequence_number: None,
            file,
        })
        .collect();
    write_manifest_entries(&entries, partition_fields, table_schema_json, content, sync)
}

/// Write a manifest (Avro OCF) of `entries`, each with its own status, snapshot and
/// sequence numbers — what a snapshot that rewrites files needs: the files it adds, the
/// live files it carries, and the files it removes, in one manifest. Otherwise as
/// [`write_manifest`].
pub fn write_manifest_entries(
    entries: &[EntryOut],
    partition_fields: &[PartitionField],
    table_schema_json: &str,
    content: ManifestContent,
    sync: [u8; 16],
) -> Vec<u8> {
    let mut body = Vec::new();
    for e in entries {
        let df = e.file;
        // manifest_entry
        enc_long(&mut body, e.status.id());
        enc_opt_long(&mut body, Some(e.snapshot_id));
        enc_opt_long(&mut body, e.sequence_number);
        enc_opt_long(&mut body, e.file_sequence_number);
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
    build_ocf(&schema, extra, entries.len(), &body, sync)
}

/// One `manifest_file` record — a manifest referenced by a snapshot's manifest list.
/// Every v2 field a reader plans with is carried — the 13 counts and sequence numbers, and
/// the partition summaries other engines write — so a manifest from a prior snapshot is
/// re-emitted **losslessly** when a new snapshot is appended onto the table.
#[derive(Debug, Clone, PartialEq)]
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
    /// One summary per partition field (`partitions`, field 507), when the writer recorded
    /// them: a reader skips the manifest when no partition it holds can match. shapeshift
    /// writes none for its own manifests, and carries other engines' as they were.
    pub partitions: Option<Vec<FieldSummary>>,
}

/// A manifest's summary of one partition field (`field_summary`): whether any file in it
/// has a null (or NaN) there, and the bounds of the values, in the single-value binary form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSummary {
    pub contains_null: bool,
    pub contains_nan: Option<bool>,
    pub lower_bound: Option<Vec<u8>>,
    pub upper_bound: Option<Vec<u8>>,
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
            partitions: None,
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
        enc_manifest_file(&mut body, m);
    }
    build_ocf(MANIFEST_FILE_SCHEMA, &[], manifests.len(), &body, sync)
}

/// One `manifest_file` record, in [`MANIFEST_FILE_SCHEMA`]'s layout.
fn enc_manifest_file(body: &mut Vec<u8>, m: &ManifestFileEntry) {
    enc_str(body, &m.path); // manifest_path
    enc_long(body, m.length); // manifest_length
    enc_long(body, m.partition_spec_id as i64); // partition_spec_id
    enc_long(body, m.content as i64); // content
    enc_long(body, m.sequence_number); // sequence_number
    enc_long(body, m.min_sequence_number); // min_sequence_number
    enc_long(body, m.added_snapshot_id); // added_snapshot_id
    enc_long(body, m.added_files as i64); // added_files_count
    enc_long(body, m.existing_files as i64); // existing_files_count
    enc_long(body, m.deleted_files as i64); // deleted_files_count
    enc_long(body, m.added_rows); // added_rows_count
    enc_long(body, m.existing_rows); // existing_rows_count
    enc_long(body, m.deleted_rows); // deleted_rows_count
    enc_partitions(body, m.partitions.as_deref()); // partitions
}

/// `partitions`: `["null", array<field_summary>]`.
fn enc_partitions(buf: &mut Vec<u8>, summaries: Option<&[FieldSummary]>) {
    let Some(summaries) = summaries else {
        enc_long(buf, 0); // null
        return;
    };
    enc_long(buf, 1);
    if !summaries.is_empty() {
        enc_long(buf, summaries.len() as i64);
        for f in summaries {
            buf.push(u8::from(f.contains_null));
            match f.contains_nan {
                None => enc_long(buf, 0),
                Some(nan) => {
                    enc_long(buf, 1);
                    buf.push(u8::from(nan));
                }
            }
            for bound in [&f.lower_bound, &f.upper_bound] {
                match bound {
                    None => enc_long(buf, 0),
                    Some(b) => {
                        enc_long(buf, 1);
                        enc_bytes(buf, b);
                    }
                }
            }
        }
    }
    enc_long(buf, 0); // end of the array
}

// ---- Reading, for append-to-existing / multi-snapshot ----
//
// To append a snapshot the prior snapshot's manifest list is read back and its
// `manifest_file` records carried forward; a merge, a compaction and an expiry read the
// manifests too. The files may be shapeshift's or another engine's, so both are decoded
// against the schema each carries, through whatever codec compressed its blocks.

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

/// Parse a manifest-list OCF back into its `manifest_file` records — the manifests a
/// snapshot references. Append-to-existing carries these forward into the new snapshot's
/// manifest list so the table's full history stays live.
///
/// Decoded against the schema the file carries, like a manifest: other engines write more
/// fields than shapeshift does (`partitions`, `key_metadata`), compress the blocks, and in
/// a v1 table name the counts `added_data_files_count` and so on. A manifest with
/// `key_metadata` is encrypted, which shapeshift cannot read, and is refused.
pub fn read_manifest_list(data: &[u8]) -> Result<Vec<ManifestFileEntry>> {
    let mut out = Vec::new();
    for rec in read_ocf(data)? {
        let path = match rec.get("manifest_path") {
            Some(AvroValue::Str(s)) => s.clone(),
            _ => return Err(read_err("manifest_file has no manifest_path")),
        };
        if !matches!(rec.get("key_metadata"), None | Some(AvroValue::Null)) {
            return Err(read_err(format!(
                "manifest {path} is encrypted (it has key_metadata); shapeshift reads only \
                 unencrypted tables"
            )));
        }
        let long = |names: &[&str]| names.iter().find_map(|n| opt_long(rec.get(n)));
        let need = |name: &str| {
            opt_long(rec.get(name))
                .ok_or_else(|| read_err(format!("manifest_file {path} has no {name}")))
        };
        // An Iceberg `int`, which another writer may have declared wider: refused when it
        // does not fit, rather than truncated and written back wrong.
        let int = |name: &str, v: i64| {
            i32::try_from(v)
                .map_err(|_| read_err(format!("manifest_file {path} has {name} {v}, past an int")))
        };
        let count = |names: &[&str]| int(names[0], long(names).unwrap_or(0));
        out.push(ManifestFileEntry {
            length: need("manifest_length")?,
            partition_spec_id: int("partition_spec_id", need("partition_spec_id")?)?,
            // v1 lists have no content (every manifest holds data) and no sequence numbers.
            content: count(&["content"])?,
            sequence_number: long(&["sequence_number"]).unwrap_or(0),
            min_sequence_number: long(&["min_sequence_number"]).unwrap_or(0),
            added_snapshot_id: need("added_snapshot_id")?,
            added_files: count(&["added_files_count", "added_data_files_count"])?,
            existing_files: count(&["existing_files_count", "existing_data_files_count"])?,
            deleted_files: count(&["deleted_files_count", "deleted_data_files_count"])?,
            added_rows: long(&["added_rows_count"]).unwrap_or(0),
            existing_rows: long(&["existing_rows_count"]).unwrap_or(0),
            deleted_rows: long(&["deleted_rows_count"]).unwrap_or(0),
            partitions: field_summaries(rec.get("partitions"))?,
            path,
        });
    }
    Ok(out)
}

/// `partitions`: absent or null, or one `field_summary` record per partition field.
fn field_summaries(v: Option<&AvroValue>) -> Result<Option<Vec<FieldSummary>>> {
    let items = match v {
        None | Some(AvroValue::Null) => return Ok(None),
        Some(AvroValue::Array(items)) => items,
        Some(other) => {
            return Err(read_err(format!(
                "manifest_file partitions is a {}, not an array",
                other.kind()
            )))
        }
    };
    let bytes = |v: Option<&AvroValue>| match v {
        Some(AvroValue::Bytes(b)) => Some(b.clone()),
        _ => None,
    };
    items
        .iter()
        .map(|f| {
            Ok(FieldSummary {
                contains_null: match f.get("contains_null") {
                    Some(AvroValue::Boolean(b)) => *b,
                    _ => return Err(read_err("field_summary has no contains_null")),
                },
                contains_nan: match f.get("contains_nan") {
                    Some(AvroValue::Boolean(b)) => Some(*b),
                    _ => None,
                },
                lower_bound: bytes(f.get("lower_bound")),
                upper_bound: bytes(f.get("upper_bound")),
            })
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
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

/// How an OCF's data blocks are compressed (`avro.codec`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Codec {
    /// Uncompressed: what shapeshift writes.
    Null,
    /// Raw DEFLATE (RFC 1951, no zlib header): Java's and PyIceberg's default, which an
    /// Iceberg table calls `gzip` (`write.avro.compression-codec`).
    Deflate,
    /// Snappy, followed by the big-endian CRC-32 of the uncompressed block.
    Snappy,
}

/// A decompressed block larger than this is refused rather than allocated: a manifest's
/// blocks run to megabytes, and a corrupt or hostile file should fail, not exhaust memory.
const MAX_BLOCK_BYTES: usize = 256 << 20;

/// The same for a whole file, all its blocks together.
const MAX_FILE_BYTES: usize = 1 << 30;

impl Codec {
    fn named(name: Option<&[u8]>) -> Result<Codec> {
        match name {
            None | Some(b"null") => Ok(Codec::Null),
            Some(b"deflate") => Ok(Codec::Deflate),
            Some(b"snappy") => Ok(Codec::Snappy),
            Some(other) => Err(read_err(format!(
                "avro codec `{}` is not supported: shapeshift reads uncompressed, deflate \
                 (`gzip`) and snappy manifests. Set the table's \
                 `write.avro.compression-codec` to `gzip` or `snappy`, and rewrite its \
                 manifests with the engine that wrote them",
                String::from_utf8_lossy(other)
            ))),
        }
    }

    /// The block's bytes, uncompressed.
    fn decompress<'a>(self, block: &'a [u8]) -> Result<std::borrow::Cow<'a, [u8]>> {
        use std::borrow::Cow;
        match self {
            Codec::Null => Ok(Cow::Borrowed(block)),
            Codec::Deflate => {
                miniz_oxide::inflate::decompress_to_vec_with_limit(block, MAX_BLOCK_BYTES)
                    .map(Cow::Owned)
                    .map_err(|e| read_err(format!("a deflate block does not inflate: {e}")))
            }
            Codec::Snappy => {
                let split = block
                    .len()
                    .checked_sub(4)
                    .ok_or_else(|| read_err("a snappy block is shorter than its checksum"))?;
                let (compressed, crc) = block.split_at(split);
                let len = snap::raw::decompress_len(compressed)
                    .map_err(|e| read_err(format!("a snappy block: {e}")))?;
                if len > MAX_BLOCK_BYTES {
                    return Err(read_err(format!("a snappy block claims {len} bytes")));
                }
                let out = snap::raw::Decoder::new()
                    .decompress_vec(compressed)
                    .map_err(|e| read_err(format!("a snappy block: {e}")))?;
                if crc32(&out).to_be_bytes() != crc {
                    return Err(read_err("a snappy block fails its CRC-32 check"));
                }
                Ok(Cow::Owned(out))
            }
        }
    }
}

/// CRC-32 (IEEE 802.3, as zlib computes it): snappy blocks carry it. Bitwise, since the
/// blocks it checks are a manifest's.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

/// An Avro Object Container File's header: every metadata entry (the schema, the codec,
/// and any other key, such as the ones Iceberg writes), the parsed schema, and the sync
/// marker; and a cursor at its first data block.
struct Ocf<'a> {
    meta: Vec<(String, Vec<u8>)>,
    ty: AvroType,
    codec: Codec,
    sync: &'a [u8],
    body: Reader<'a>,
}

fn open_ocf(data: &[u8]) -> Result<Ocf<'_>> {
    let mut r = Reader::new(data);
    if r.take(4)? != b"Obj\x01" {
        return Err(read_err("not an Avro object container file (bad magic)"));
    }
    let mut meta: Vec<(String, Vec<u8>)> = Vec::new();
    read_blocks(&mut r, |r| {
        let k = r.string()?;
        let v = r.bytes()?;
        meta.push((k, v.to_vec()));
        Ok(())
    })?;
    let sync = r.take(16)?;
    let value = |key: &str| {
        meta.iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_slice())
    };
    let codec = Codec::named(value("avro.codec"))?;
    let schema =
        value("avro.schema").ok_or_else(|| read_err("no avro.schema in the file header"))?;
    let schema: serde_json::Value = serde_json::from_slice(schema).map_err(read_err)?;
    let ty = parse_type(&schema, &mut std::collections::HashMap::new())?;
    Ok(Ocf {
        meta,
        ty,
        codec,
        sync,
        body: r,
    })
}

impl Ocf<'_> {
    /// Decode every record with `item`, block by block, the blocks uncompressed by the
    /// codec the header names (see [`Codec`]).
    fn records<T>(
        mut self,
        mut item: impl FnMut(&mut Reader, &AvroType) -> Result<T>,
    ) -> Result<Vec<T>> {
        let r = &mut self.body;
        let mut records = Vec::new();
        let mut total = 0usize;
        // Data blocks run to EOF; each is: object count, byte size, the (compressed)
        // objects, sync marker. A block decodes to exactly its objects: bytes left over
        // mean the schema and the data disagree.
        while r.remaining() > 0 {
            let count = r.long()?;
            let size = usize::try_from(r.long()?).map_err(|_| read_err("bad block size"))?;
            let block = self.codec.decompress(r.take(size)?)?;
            total = total.saturating_add(block.len());
            if total > MAX_FILE_BYTES {
                return Err(read_err(format!(
                    "the file decompresses to more than {MAX_FILE_BYTES} bytes"
                )));
            }
            // Every manifest record holds a required field, so it takes at least one byte:
            // a count past the block's length is corrupt, and is refused before it is
            // looped on.
            if usize::try_from(count).map_or(true, |n| n > block.len()) {
                return Err(read_err(format!(
                    "a block of {} byte(s) claims {count} object(s)",
                    block.len()
                )));
            }
            let mut b = Reader::new(&block);
            for _ in 0..count {
                records.push(item(&mut b, &self.ty)?);
            }
            if b.remaining() != 0 {
                return Err(read_err(format!(
                    "a block holds {} byte(s) past its {count} object(s)",
                    b.remaining()
                )));
            }
            if r.take(16)? != self.sync {
                return Err(read_err("block sync marker does not match the header"));
            }
        }
        Ok(records)
    }
}

/// Decode a whole Avro Object Container File against the schema in its own header, its
/// blocks uncompressed by the codec the header names (see [`Codec`]).
fn read_ocf(data: &[u8]) -> Result<Vec<AvroValue>> {
    open_ocf(data)?.records(decode)
}

// ---- Transcoding ----
//
// Relocating a table rewrites the paths its manifests and manifest lists record, and must
// keep every other field exactly as the engine that wrote the file wrote it: column
// sizes, split offsets, sort order ids, the partition summaries, fields shapeshift has
// never heard of. So a file is decoded against its own schema into values that keep
// everything (floats and fixed values as their bytes, enums as their index, a union's
// branch), edited, and encoded back against the same schema.

/// A decoded Avro value that encodes back to the same value against the same schema.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Datum {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float([u8; 4]),
    Double([u8; 8]),
    Bytes(Vec<u8>),
    Str(String),
    Record(Vec<(String, Datum)>),
    /// The symbol's index.
    Enum(i32),
    Array(Vec<Datum>),
    Map(Vec<(String, Datum)>),
    /// The branch taken, and its value.
    Union(usize, Box<Datum>),
    Fixed(Vec<u8>),
}

impl Datum {
    /// The value inside a union's branch, or the value itself.
    pub(crate) fn inner(&self) -> &Datum {
        match self {
            Datum::Union(_, v) => v.inner(),
            v => v,
        }
    }

    pub(crate) fn inner_mut(&mut self) -> &mut Datum {
        match self {
            Datum::Union(_, v) => v.inner_mut(),
            v => v,
        }
    }

    /// A record's field by name, looking through unions on the way.
    pub(crate) fn field(&self, name: &str) -> Option<&Datum> {
        match self.inner() {
            Datum::Record(fields) => fields.iter().find(|(n, _)| n == name).map(|(_, v)| v),
            _ => None,
        }
    }

    pub(crate) fn field_mut(&mut self, name: &str) -> Option<&mut Datum> {
        match self.inner_mut() {
            Datum::Record(fields) => fields.iter_mut().find(|(n, _)| n == name).map(|(_, v)| v),
            _ => None,
        }
    }
}

fn decode_datum(r: &mut Reader, t: &AvroType) -> Result<Datum> {
    Ok(match t {
        AvroType::Null => Datum::Null,
        AvroType::Boolean => Datum::Boolean(r.take(1)?[0] != 0),
        AvroType::Int => Datum::Int(r.int()?),
        AvroType::Long => Datum::Long(r.long()?),
        AvroType::Float => Datum::Float(r.take(4)?.try_into().unwrap()),
        AvroType::Double => Datum::Double(r.take(8)?.try_into().unwrap()),
        AvroType::Bytes => Datum::Bytes(r.bytes()?.to_vec()),
        AvroType::Str => Datum::Str(r.string()?),
        AvroType::Record(fields) => {
            let mut out = Vec::with_capacity(fields.len());
            for (name, ft) in fields {
                out.push((name.clone(), decode_datum(r, ft)?));
            }
            Datum::Record(out)
        }
        AvroType::Enum => Datum::Enum(r.int()?),
        AvroType::Array(items) => {
            let mut out = Vec::new();
            read_blocks(r, |r| {
                out.push(decode_datum(r, items)?);
                Ok(())
            })?;
            Datum::Array(out)
        }
        AvroType::Map(values) => {
            let mut out = Vec::new();
            read_blocks(r, |r| {
                let k = r.string()?;
                out.push((k, decode_datum(r, values)?));
                Ok(())
            })?;
            Datum::Map(out)
        }
        AvroType::Union(branches) => {
            let i = r.long()?;
            let (i, branch) = usize::try_from(i)
                .ok()
                .and_then(|i| branches.get(i).map(|b| (i, b)))
                .ok_or_else(|| read_err(format!("union branch {i} out of range")))?;
            Datum::Union(i, Box::new(decode_datum(r, branch)?))
        }
        AvroType::Fixed(n) => Datum::Fixed(r.take(*n)?.to_vec()),
    })
}

fn encode_datum(buf: &mut Vec<u8>, d: &Datum, t: &AvroType) -> Result<()> {
    let mismatch = || {
        ShapeError::Sink(format!(
            "internal error: an Avro value does not match its schema ({d:?} as {t:?})"
        ))
    };
    match (t, d) {
        (AvroType::Null, Datum::Null) => {}
        (AvroType::Boolean, Datum::Boolean(b)) => buf.push(*b as u8),
        (AvroType::Int, Datum::Int(n)) => enc_long(buf, *n as i64),
        (AvroType::Long, Datum::Long(n)) => enc_long(buf, *n),
        (AvroType::Float, Datum::Float(b)) => buf.extend_from_slice(b),
        (AvroType::Double, Datum::Double(b)) => buf.extend_from_slice(b),
        (AvroType::Bytes, Datum::Bytes(b)) => enc_bytes(buf, b),
        (AvroType::Str, Datum::Str(s)) => enc_str(buf, s),
        (AvroType::Record(fields), Datum::Record(values)) => {
            if fields.len() != values.len() {
                return Err(mismatch());
            }
            for ((name, ft), (vname, v)) in fields.iter().zip(values) {
                if name != vname {
                    return Err(mismatch());
                }
                encode_datum(buf, v, ft)?;
            }
        }
        (AvroType::Enum, Datum::Enum(i)) => enc_long(buf, *i as i64),
        (AvroType::Array(items), Datum::Array(values)) => {
            if !values.is_empty() {
                enc_long(buf, values.len() as i64);
                for v in values {
                    encode_datum(buf, v, items)?;
                }
            }
            enc_long(buf, 0);
        }
        (AvroType::Map(vt), Datum::Map(entries)) => {
            if !entries.is_empty() {
                enc_long(buf, entries.len() as i64);
                for (k, v) in entries {
                    enc_str(buf, k);
                    encode_datum(buf, v, vt)?;
                }
            }
            enc_long(buf, 0);
        }
        (AvroType::Union(branches), Datum::Union(i, v)) => {
            let branch = branches.get(*i).ok_or_else(mismatch)?;
            enc_long(buf, *i as i64);
            encode_datum(buf, v, branch)?;
        }
        (AvroType::Fixed(n), Datum::Fixed(b)) if b.len() == *n => buf.extend_from_slice(b),
        _ => return Err(mismatch()),
    }
    Ok(())
}

/// An Avro object container file decoded without loss: its header's metadata entries (the
/// schema and every other key, Iceberg's among them, in their order) and its records.
pub(crate) struct Container {
    meta: Vec<(String, Vec<u8>)>,
    ty: AvroType,
    pub records: Vec<Datum>,
}

impl Container {
    /// Decode `data` against the schema in its own header, whatever its codec.
    pub(crate) fn read(data: &[u8]) -> Result<Container> {
        let ocf = open_ocf(data)?;
        let (meta, ty) = (ocf.meta.clone(), ocf.ty.clone());
        let records = ocf.records(decode_datum)?;
        Ok(Container { meta, ty, records })
    }

    /// Encode it back: the same header entries and schema, one uncompressed block (which
    /// every reader accepts), and a fresh sync marker.
    pub(crate) fn write(&self) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        for rec in &self.records {
            encode_datum(&mut body, rec, &self.ty)?;
        }
        let schema = self
            .meta
            .iter()
            .find(|(k, _)| k == "avro.schema")
            .map(|(_, v)| v.as_slice())
            .ok_or_else(|| read_err("no avro.schema in the file header"))?;
        let schema = std::str::from_utf8(schema).map_err(read_err)?;
        let extra: Vec<(&str, &[u8])> = self
            .meta
            .iter()
            .filter(|(k, _)| k != "avro.schema" && k != "avro.codec")
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        Ok(build_ocf(
            schema,
            &extra,
            self.records.len(),
            &body,
            *uuid::Uuid::new_v4().as_bytes(),
        ))
    }
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
    pub value_counts: Vec<(i32, i64)>,
    pub null_counts: Vec<(i32, i64)>,
    pub lower_bounds: Vec<(i32, Vec<u8>)>,
    pub upper_bounds: Vec<(i32, Vec<u8>)>,
}

impl ManifestEntry {
    /// Whether the entry is live in the snapshot that lists it (not DELETED).
    pub fn is_live(&self) -> bool {
        self.status != 2
    }

    /// The file as a manifest records it: path, partition, counts and column statistics.
    /// Carrying it into a new manifest keeps everything a reader prunes with.
    pub fn file_meta(&self) -> DataFileMeta {
        DataFileMeta {
            path: self.file_path.clone(),
            partition: self.partition.clone(),
            record_count: self.record_count,
            file_size: self.file_size,
            value_counts: self.value_counts.clone(),
            null_counts: self.null_counts.clone(),
            lower_bounds: self.lower_bounds.clone(),
            upper_bounds: self.upper_bounds.clone(),
        }
    }
}

fn opt_long(v: Option<&AvroValue>) -> Option<i64> {
    v.and_then(AvroValue::as_long)
}

/// An Iceberg `map<int, long>` (value or null counts), written as an array of key/value
/// records.
fn counts_map(v: Option<&AvroValue>) -> Vec<(i32, i64)> {
    let Some(AvroValue::Array(items)) = v else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|kv| {
            Some((
                kv.get("key")?.as_long()? as i32,
                kv.get("value")?.as_long()?,
            ))
        })
        .collect()
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
            value_counts: counts_map(df.get("value_counts")),
            null_counts: counts_map(df.get("null_value_counts")),
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
    fn carried_entries_keep_their_status_snapshot_sequence_and_stats() {
        let file = |path: &str| DataFileMeta {
            path: path.into(),
            partition: vec![],
            record_count: 7,
            file_size: 99,
            value_counts: vec![(1, 7), (2, 7)],
            null_counts: vec![(1, 0), (2, 3)],
            lower_bounds: vec![(1, 10i64.to_le_bytes().to_vec())],
            upper_bounds: vec![(1, 20i64.to_le_bytes().to_vec())],
        };
        let (added, kept, gone) = (
            file("/t/new.parquet"),
            file("/t/kept.parquet"),
            file("/t/old.parquet"),
        );
        let entry = |status, snapshot_id, seq, file| EntryOut {
            status,
            snapshot_id,
            sequence_number: seq,
            file_sequence_number: seq,
            file,
        };
        let bytes = write_manifest_entries(
            &[
                entry(EntryStatus::Added, 9, None, &added),
                entry(EntryStatus::Existing, 4, Some(2), &kept),
                entry(EntryStatus::Deleted, 9, Some(3), &gone),
            ],
            &[],
            "{}",
            ManifestContent::Data,
            [5u8; 16],
        );
        let entries = read_manifest_entries(&bytes).unwrap();
        let got: Vec<_> = entries
            .iter()
            .map(|e| {
                (
                    e.status,
                    e.snapshot_id,
                    e.sequence_number,
                    e.file_sequence_number,
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![
                (1, Some(9), None, None),
                (0, Some(4), Some(2), Some(2)),
                (2, Some(9), Some(3), Some(3)),
            ]
        );
        assert!(!entries[2].is_live());
        // A carried entry round-trips everything a reader prunes with.
        let back = entries[1].file_meta();
        assert_eq!(back.path, "/t/kept.parquet");
        assert_eq!(back.value_counts, kept.value_counts);
        assert_eq!(back.null_counts, kept.null_counts);
        assert_eq!(back.lower_bounds, kept.lower_bounds);
        assert_eq!(back.upper_bounds, kept.upper_bounds);
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
            partitions: Some(vec![FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(b"eu".to_vec()),
                upper_bound: Some(b"us".to_vec()),
            }]),
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
        // Every field round-trips, the partition summaries included.
        assert_eq!(back, [carried, fresh]);
    }

    #[test]
    fn read_manifest_list_rejects_bad_magic() {
        assert!(read_manifest_list(b"not-avro").is_err());
    }

    /// An OCF of `schema` whose blocks (`(object count, encoded objects)`) are compressed
    /// with `codec`, as another engine writes one.
    fn ocf_with(schema: &str, codec: &str, blocks: &[(i64, Vec<u8>)]) -> Vec<u8> {
        let sync = [7u8; 16];
        let mut out = b"Obj\x01".to_vec();
        enc_long(&mut out, 2);
        enc_str(&mut out, "avro.schema");
        enc_bytes(&mut out, schema.as_bytes());
        enc_str(&mut out, "avro.codec");
        enc_bytes(&mut out, codec.as_bytes());
        enc_long(&mut out, 0);
        out.extend_from_slice(&sync);
        for (count, raw) in blocks {
            let body = match codec {
                "null" => raw.clone(),
                "deflate" => miniz_oxide::deflate::compress_to_vec(raw, 6),
                "snappy" => {
                    let mut c = snap::raw::Encoder::new().compress_vec(raw).unwrap();
                    c.extend_from_slice(&crc32(raw).to_be_bytes());
                    c
                }
                other => panic!("no {other}"),
            };
            enc_long(&mut out, *count);
            enc_long(&mut out, body.len() as i64);
            out.extend_from_slice(&body);
            out.extend_from_slice(&sync);
        }
        out
    }

    fn some_manifests() -> Vec<ManifestFileEntry> {
        (0..3)
            .map(|i| ManifestFileEntry {
                partitions: (i == 1).then(|| {
                    vec![FieldSummary {
                        contains_null: true,
                        contains_nan: Some(false),
                        lower_bound: None,
                        upper_bound: Some(vec![i as u8; 3]),
                    }]
                }),
                ..ManifestFileEntry::added(
                    format!("s3://b/t/metadata/m{i}.avro"),
                    100 + i,
                    7,
                    2,
                    9,
                    1,
                )
            })
            .collect()
    }

    #[test]
    fn compressed_blocks_read_like_uncompressed_ones() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926, "zlib's CRC-32");
        let ms = some_manifests();
        let enc = |m: &[ManifestFileEntry]| {
            let mut b = Vec::new();
            m.iter().for_each(|m| enc_manifest_file(&mut b, m));
            b
        };
        // Several blocks, as a writer that flushes by size makes them.
        let blocks = [(1, enc(&ms[..1])), (2, enc(&ms[1..]))];
        for codec in ["null", "deflate", "snappy"] {
            let file = ocf_with(MANIFEST_FILE_SCHEMA, codec, &blocks);
            assert_eq!(read_manifest_list(&file).unwrap(), ms, "{codec}");
        }
        // A snappy block whose checksum does not match is refused, not trusted.
        let mut bad = ocf_with(MANIFEST_FILE_SCHEMA, "snappy", &blocks[..1]);
        let at = bad.len() - 17;
        bad[at] ^= 0xff;
        let err = read_manifest_list(&bad).unwrap_err().to_string();
        assert!(err.contains("CRC-32"), "{err}");
        // A block that holds more bytes than its objects decode from is refused.
        let mut long = enc(&ms[..1]);
        long.push(0);
        let err = read_manifest_list(&ocf_with(MANIFEST_FILE_SCHEMA, "deflate", &[(1, long)]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("past its 1 object(s)"), "{err}");
        // A count past the block's bytes is corrupt, and refused rather than looped on.
        let huge = ocf_with(MANIFEST_FILE_SCHEMA, "null", &[(1 << 40, enc(&ms[..1]))]);
        let err = read_manifest_list(&huge).unwrap_err().to_string();
        assert!(err.contains("claims 1099511627776 object(s)"), "{err}");
        // zstd stays out of the lean build: refused by name, with what to do instead.
        let err = Codec::named(Some(b"zstandard")).unwrap_err().to_string();
        assert!(
            err.contains("`zstandard`") && err.contains("write.avro.compression-codec"),
            "{err}"
        );
    }

    #[test]
    fn a_manifest_list_is_read_by_the_schema_it_carries() {
        // Another engine's list: its own field order, v1's count names (optional, as v1
        // has them), no content or sequence numbers, and a `key_metadata` field.
        let schema = r#"{"type":"record","name":"manifest_file","fields":[
            {"name":"manifest_path","type":"string"},
            {"name":"manifest_length","type":"long"},
            {"name":"partition_spec_id","type":"int"},
            {"name":"added_snapshot_id","type":["null","long"]},
            {"name":"added_data_files_count","type":["null","int"]},
            {"name":"existing_data_files_count","type":["null","int"]},
            {"name":"deleted_data_files_count","type":["null","int"]},
            {"name":"partitions","type":["null",{"type":"array","items":{"type":"record",
              "name":"r508","fields":[{"name":"contains_null","type":"boolean"},
              {"name":"lower_bound","type":["null","bytes"]},
              {"name":"upper_bound","type":["null","bytes"]}]}}]},
            {"name":"added_rows_count","type":["null","long"]},
            {"name":"key_metadata","type":["null","bytes"]}]}"#;
        let record = |key: Option<&[u8]>| {
            let mut b = Vec::new();
            enc_str(&mut b, "file:/t/metadata/a-m0.avro");
            enc_long(&mut b, 4321);
            enc_long(&mut b, 1);
            enc_long(&mut b, 1);
            enc_long(&mut b, 99); // added_snapshot_id
            enc_long(&mut b, 1);
            enc_long(&mut b, 3); // added
            enc_long(&mut b, 1);
            enc_long(&mut b, 2); // existing
            enc_long(&mut b, 0); // deleted: null
            enc_long(&mut b, 1); // partitions: one summary
            enc_long(&mut b, 1);
            b.push(0);
            enc_long(&mut b, 1);
            enc_bytes(&mut b, b"a");
            enc_long(&mut b, 0); // no upper bound
            enc_long(&mut b, 0);
            enc_long(&mut b, 1);
            enc_long(&mut b, 30); // added rows
            match key {
                None => enc_long(&mut b, 0),
                Some(k) => {
                    enc_long(&mut b, 1);
                    enc_bytes(&mut b, k);
                }
            }
            b
        };
        let back = read_manifest_list(&ocf_with(schema, "deflate", &[(1, record(None))])).unwrap();
        let want = ManifestFileEntry {
            path: "file:/t/metadata/a-m0.avro".into(),
            length: 4321,
            partition_spec_id: 1,
            content: 0,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 99,
            added_files: 3,
            existing_files: 2,
            deleted_files: 0,
            added_rows: 30,
            existing_rows: 0,
            deleted_rows: 0,
            partitions: Some(vec![FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: Some(b"a".to_vec()),
                upper_bound: None,
            }]),
        };
        assert_eq!(back, [want]);
        // A count another writer declared `long` and filled past an int is refused, not
        // truncated into the list shapeshift writes back.
        let wide = r#"{"type":"record","name":"manifest_file","fields":[
            {"name":"manifest_path","type":"string"},
            {"name":"manifest_length","type":"long"},
            {"name":"partition_spec_id","type":"int"},
            {"name":"added_snapshot_id","type":"long"},
            {"name":"added_files_count","type":"long"}]}"#;
        let mut rec = Vec::new();
        enc_str(&mut rec, "file:/t/metadata/a-m0.avro");
        enc_long(&mut rec, 4321);
        enc_long(&mut rec, 0);
        enc_long(&mut rec, 99);
        enc_long(&mut rec, 1 << 33);
        let err = read_manifest_list(&ocf_with(wide, "null", &[(1, rec)]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("added_files_count 8589934592, past an int"),
            "{err}"
        );
        // An encrypted manifest cannot be read, so the list is refused rather than half-read.
        let err = read_manifest_list(&ocf_with(schema, "null", &[(1, record(Some(b"k")))]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("encrypted"), "{err}");
    }

    #[test]
    fn a_transcoded_file_keeps_every_value_and_header_key() {
        // Every Avro type a manifest can hold, in several deflate blocks, with a header key
        // of the engine's own: decoded and written back, each value is the same, the key
        // survives, and the blocks are uncompressed.
        let schema = r#"{"type":"record","name":"r","fields":[
            {"name":"s","type":"string"},
            {"name":"f","type":"float"},
            {"name":"d","type":"double"},
            {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]}},
            {"name":"x","type":{"type":"fixed","name":"F","size":3}},
            {"name":"m","type":{"type":"map","values":"long"}},
            {"name":"a","type":["null",{"type":"array","items":{"type":"record","name":"kv",
              "fields":[{"name":"key","type":"int"},{"name":"value","type":"bytes"}]}}]},
            {"name":"b","type":"boolean"},
            {"name":"again","type":["null","F"]}]}"#;
        let record = |i: u8| {
            let mut b = Vec::new();
            enc_str(&mut b, &format!("/w/t/data/{i}.parquet"));
            b.extend_from_slice(&(1.5f32 + i as f32).to_le_bytes());
            b.extend_from_slice(&f64::NAN.to_le_bytes());
            enc_long(&mut b, (i % 3) as i64);
            b.extend_from_slice(&[i, i + 1, i + 2]);
            enc_long(&mut b, 2); // map: two entries
            enc_str(&mut b, "k1");
            enc_long(&mut b, -5);
            enc_str(&mut b, "k2");
            enc_long(&mut b, i as i64);
            enc_long(&mut b, 0);
            enc_long(&mut b, 1); // array branch
            enc_long(&mut b, -1); // a negative count carries the block's byte size
            let mut item = Vec::new();
            enc_long(&mut item, 2_147_483_546);
            enc_bytes(&mut item, b"/w/t/data/x");
            enc_long(&mut b, item.len() as i64);
            b.extend_from_slice(&item);
            enc_long(&mut b, 0);
            b.push(i % 2);
            enc_long(&mut b, 1);
            b.extend_from_slice(&[9, 9, i]);
            b
        };
        let mut file = ocf_with(
            schema,
            "deflate",
            &[(1, record(0)), (2, [record(1), record(2)].concat())],
        );
        // The engine's own header key, as Iceberg writes `partition-spec` and the like.
        let at = 4;
        let mut head = Vec::new();
        enc_long(&mut head, 3);
        file.splice(at..at + head.len(), head);
        let tail_at = file
            .windows(b"deflate".len())
            .position(|w| w == b"deflate")
            .unwrap()
            + b"deflate".len();
        let mut key = Vec::new();
        enc_str(&mut key, "partition-spec-id");
        enc_bytes(&mut key, b"7");
        file.splice(tail_at..tail_at, key);

        let c = Container::read(&file).unwrap();
        assert_eq!(c.records.len(), 3);
        let out = c.write().unwrap();
        let back = Container::read(&out).unwrap();
        assert_eq!(back.records, c.records);
        let ocf = open_ocf(&out).unwrap();
        assert_eq!(ocf.codec, Codec::Null);
        let keys: Vec<&str> = ocf.meta.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["avro.schema", "avro.codec", "partition-spec-id"]);
        assert_eq!(ocf.meta[2].1, b"7");
        // The values are the ones written: the float's and the NaN's bits, the enum's
        // index, the fixed bytes, the map, the bounds array.
        let r = &back.records[1];
        assert_eq!(r.field("f"), Some(&Datum::Float(2.5f32.to_le_bytes())));
        assert_eq!(r.field("d"), Some(&Datum::Double(f64::NAN.to_le_bytes())));
        assert_eq!(r.field("e"), Some(&Datum::Enum(1)));
        assert_eq!(r.field("x"), Some(&Datum::Fixed(vec![1, 2, 3])));
        assert_eq!(
            r.field("m"),
            Some(&Datum::Map(vec![
                ("k1".into(), Datum::Long(-5)),
                ("k2".into(), Datum::Long(1))
            ]))
        );
        let bounds = r.field("a").unwrap().inner();
        assert_eq!(
            bounds,
            &Datum::Array(vec![Datum::Record(vec![
                ("key".into(), Datum::Int(2_147_483_546)),
                ("value".into(), Datum::Bytes(b"/w/t/data/x".to_vec())),
            ])])
        );
        assert_eq!(
            r.field("again").unwrap().inner(),
            &Datum::Fixed(vec![9, 9, 1])
        );

        // A file shapeshift writes comes back byte for byte, but for its sync marker.
        let ms = some_manifests();
        let list = write_manifest_list(&ms, [3; 16]);
        let again = Container::read(&list).unwrap().write().unwrap();
        let without_sync = |f: &[u8]| {
            let sync = open_ocf(f).unwrap().sync.to_vec();
            let mut out = f.to_vec();
            let mut i = 0;
            while i + 16 <= out.len() {
                if out[i..i + 16] == sync[..] {
                    out[i..i + 16].fill(0);
                    i += 16;
                } else {
                    i += 1;
                }
            }
            out
        };
        assert_eq!(without_sync(&list), without_sync(&again));
        assert_eq!(read_manifest_list(&again).unwrap(), ms);
        // A value that does not match the schema is refused, not written.
        let mut bad = Container::read(&list).unwrap();
        *bad.records[0].field_mut("manifest_length").unwrap() = Datum::Str("x".into());
        assert!(bad.write().is_err());
    }
}
