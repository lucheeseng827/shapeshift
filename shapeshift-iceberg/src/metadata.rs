//! Iceberg v2 table metadata (`metadata.json`) and the schema/partition-spec JSON
//! that also rides inside the manifest. v0.1 emits a single append snapshot over an
//! unpartitioned spec.

use serde_json::{json, Value};

use shapeshift_core::{Result, ShapeError};

use crate::partition::PartitionField;

/// One column in the Iceberg table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IceField {
    pub id: i32,
    pub name: String,
    pub iceberg_type: String,
    pub required: bool,
}

/// Join a table's base location with a table-relative key into the fully-qualified
/// path Iceberg embeds in its manifests and metadata. The base can be a local
/// absolute path (`/warehouse/db/table`) or an object-store URI
/// (`s3://bucket/db/table`); either way the result is `<base>/<key>` with exactly one
/// separator. This is what makes a table land, and read back, at its real location —
/// local or in a bucket — instead of a `canonicalize`d local path.
pub fn join_location(base: &str, key: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        key.trim_start_matches('/')
    )
}

/// The Iceberg schema JSON (`{"type":"struct","schema-id":0,"fields":[...]}`).
pub fn schema_json(fields: &[IceField]) -> Value {
    let cols: Vec<Value> = fields
        .iter()
        .map(|f| {
            json!({
                "id": f.id,
                "name": f.name,
                "required": f.required,
                "type": f.iceberg_type,
            })
        })
        .collect();
    json!({
        "type": "struct",
        "schema-id": 0,
        "fields": cols,
    })
}

/// The partition spec JSON — identity transforms over `fields` (empty = unpartitioned).
pub fn partition_spec_json(fields: &[PartitionField]) -> Value {
    let cols: Vec<Value> = fields
        .iter()
        .map(|f| {
            json!({
                "name": f.name,
                "transform": f.transform.spec_str(),
                "source-id": f.source_id,
                "field-id": f.field_id,
            })
        })
        .collect();
    json!({ "spec-id": 0, "fields": cols })
}

/// Iceberg's `last-partition-id`: partition field ids start at 1000, so an unpartitioned
/// spec's "last assigned" is 999, and each partition field consumes the next id.
pub fn last_partition_id(fields: &[PartitionField]) -> i32 {
    fields.iter().map(|f| f.field_id).max().unwrap_or(999)
}

/// Inputs for building the table metadata document.
pub struct MetadataInput<'a> {
    pub table_uuid: &'a str,
    pub location: &'a str,
    pub fields: &'a [IceField],
    pub partition_fields: &'a [PartitionField],
    pub last_column_id: i32,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list_path: &'a str,
    pub total_records: i64,
    pub total_files: i64,
}

/// Build the full Iceberg v2 `metadata.json` document.
pub fn table_metadata(inp: &MetadataInput) -> Result<Value> {
    if inp.fields.is_empty() {
        return Err(ShapeError::Sink("iceberg table has no columns".into()));
    }
    let schema = schema_json(inp.fields);
    let summary = json!({
        "operation": "append",
        "added-data-files": inp.total_files.to_string(),
        "added-records": inp.total_records.to_string(),
        "total-data-files": inp.total_files.to_string(),
        "total-records": inp.total_records.to_string(),
        "total-delete-files": "0",
        "total-position-deletes": "0",
        "total-equality-deletes": "0",
    });
    let snapshot = json!({
        "sequence-number": inp.sequence_number,
        "snapshot-id": inp.snapshot_id,
        "timestamp-ms": inp.timestamp_ms,
        "summary": summary,
        "manifest-list": inp.manifest_list_path,
        "schema-id": 0,
    });
    Ok(json!({
        "format-version": 2,
        "table-uuid": inp.table_uuid,
        "location": inp.location,
        "last-sequence-number": inp.sequence_number,
        "last-updated-ms": inp.timestamp_ms,
        "last-column-id": inp.last_column_id,
        "current-schema-id": 0,
        "schemas": [schema],
        "default-spec-id": 0,
        "partition-specs": [partition_spec_json(inp.partition_fields)],
        "last-partition-id": last_partition_id(inp.partition_fields),
        "default-sort-order-id": 0,
        "sort-orders": [ { "order-id": 0, "fields": [] } ],
        "properties": {
            "write.format.default": "parquet",
            "created-by": "shapeshift",
        },
        "current-snapshot-id": inp.snapshot_id,
        "refs": { "main": { "snapshot-id": inp.snapshot_id, "type": "branch" } },
        "snapshots": [snapshot],
        "snapshot-log": [ { "timestamp-ms": inp.timestamp_ms, "snapshot-id": inp.snapshot_id } ],
        "metadata-log": [],
    }))
}

/// Inputs for [`append_snapshot`]: one new append snapshot over an existing table.
pub struct AppendInput<'a> {
    /// The table's current `metadata.json`, parsed. Cloned and extended — every field
    /// not touched here (table-uuid, schema, spec, sort order, properties) is preserved.
    pub prior: &'a Value,
    pub base_location: &'a str,
    /// The current version number, so the prior metadata file can be recorded in the
    /// `metadata-log`.
    pub prior_version: u64,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    pub manifest_list_path: &'a str,
    pub added_records: i64,
    pub added_files: i64,
    pub total_records: i64,
    pub total_files: i64,
    /// The (already evolved) Iceberg fields this write's data files were stamped with.
    /// When they differ from the prior current schema — an additive evolution added
    /// columns — a new schema object is appended and `current-schema-id` /
    /// `last-column-id` advance; otherwise the prior schema is carried forward.
    pub schema_fields: &'a [IceField],
}

/// Append a new snapshot onto the prior table metadata: add the snapshot, advance the
/// table pointers (`current-snapshot-id`, `last-sequence-number`, `last-updated-ms`,
/// `refs.main`), extend the snapshot log, and record the prior metadata file in the
/// metadata log. On an **additive schema evolution** (the caller's evolved fields carry
/// columns the prior current schema lacks) a new schema object is appended — prior
/// schemas stay in `schemas`, so old snapshots remain readable — and the partition spec
/// is carried forward unchanged (the caller checks compatibility first).
///
/// Roadmap — retention: `snapshots`, `snapshot-log`, and `metadata-log` only ever grow
/// here (every append clones the whole prior `metadata.json` and re-parses it), so a table
/// appended to indefinitely accumulates unbounded history. Snapshot / metadata-log
/// expiration (the retention that real catalogs perform) is future catalog-side work.
pub fn append_snapshot(inp: &AppendInput) -> Result<Value> {
    let mut m = inp.prior.clone();
    if !m.is_object() {
        return Err(ShapeError::Sink(
            "prior iceberg metadata is not a JSON object".into(),
        ));
    }
    let parent = m["current-snapshot-id"].as_i64();
    let schema_id = advance_schema(&mut m, inp.schema_fields)?;

    let summary = json!({
        "operation": "append",
        "added-data-files": inp.added_files.to_string(),
        "added-records": inp.added_records.to_string(),
        "total-data-files": inp.total_files.to_string(),
        "total-records": inp.total_records.to_string(),
        "total-delete-files": "0",
        "total-position-deletes": "0",
        "total-equality-deletes": "0",
    });
    let mut snapshot = json!({
        "sequence-number": inp.sequence_number,
        "snapshot-id": inp.snapshot_id,
        "timestamp-ms": inp.timestamp_ms,
        "summary": summary,
        "manifest-list": inp.manifest_list_path,
        "schema-id": schema_id,
    });
    if let Some(pid) = parent {
        snapshot["parent-snapshot-id"] = json!(pid);
    }

    m["last-sequence-number"] = json!(inp.sequence_number);
    m["last-updated-ms"] = json!(inp.timestamp_ms);
    m["current-snapshot-id"] = json!(inp.snapshot_id);
    m["refs"] = json!({ "main": { "snapshot-id": inp.snapshot_id, "type": "branch" } });

    push_array(&mut m, "snapshots", snapshot);
    push_array(
        &mut m,
        "snapshot-log",
        json!({ "timestamp-ms": inp.timestamp_ms, "snapshot-id": inp.snapshot_id }),
    );
    // Record the metadata file we are superseding.
    let prior_ts = inp.prior["last-updated-ms"]
        .as_i64()
        .unwrap_or(inp.timestamp_ms);
    let prior_meta_file = join_location(
        inp.base_location,
        &format!("metadata/v{}.metadata.json", inp.prior_version),
    );
    push_array(
        &mut m,
        "metadata-log",
        json!({ "timestamp-ms": prior_ts, "metadata-file": prior_meta_file }),
    );

    Ok(m)
}

/// Append `item` to `m[key]`, creating the array if it is absent or not an array.
fn push_array(m: &mut Value, key: &str, item: Value) {
    match m.get_mut(key).and_then(|v| v.as_array_mut()) {
        Some(arr) => arr.push(item),
        None => m[key] = json!([item]),
    }
}

/// If `fields` differs from the metadata's current schema (compared as a **set** of
/// `(id, name, type, required)` — field order is not identity), append a new schema
/// object with the next `schema-id`, point `current-schema-id` at it, and advance
/// `last-column-id`. Returns the (possibly new) current schema id for the snapshot.
fn advance_schema(m: &mut Value, fields: &[IceField]) -> Result<i64> {
    let current_id = m["current-schema-id"].as_i64().unwrap_or(0);
    let sig = |fs: &Value| -> Vec<(i64, String, String, bool)> {
        let mut v: Vec<_> = fs["fields"]
            .as_array()
            .map(|a| {
                a.iter()
                    .map(|f| {
                        (
                            f["id"].as_i64().unwrap_or(-1),
                            f["name"].as_str().unwrap_or("").to_string(),
                            f["type"].as_str().unwrap_or("").to_string(),
                            f["required"].as_bool().unwrap_or(false),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    };
    let new_schema = schema_json(fields);
    let empty = Vec::new();
    let schemas = m["schemas"].as_array().unwrap_or(&empty);
    let prior_current = schemas
        .iter()
        .find(|s| s["schema-id"].as_i64() == Some(current_id))
        .or_else(|| schemas.first());
    if let Some(pc) = prior_current {
        if sig(pc) == sig(&new_schema) {
            return Ok(current_id); // unchanged — carry the prior schema forward
        }
    }
    let next_id = schemas
        .iter()
        .filter_map(|s| s["schema-id"].as_i64())
        .max()
        .unwrap_or(current_id)
        + 1;
    let mut new_schema = new_schema;
    new_schema["schema-id"] = json!(next_id);
    push_array(m, "schemas", new_schema);
    m["current-schema-id"] = json!(next_id);
    let max_field_id = fields.iter().map(|f| f.id as i64).max().unwrap_or(0);
    let last = m["last-column-id"].as_i64().unwrap_or(0).max(max_field_id);
    m["last-column-id"] = json!(last);
    Ok(next_id)
}
