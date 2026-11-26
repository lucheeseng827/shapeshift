//! Iceberg v2 table metadata (`metadata.json`) and the schema/partition-spec JSON
//! that also rides inside the manifest. Each write adds one snapshot: an `append`, or an
//! `overwrite` when a merge removed rows with position deletes.

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
    pub counts: SnapshotCounts,
    /// The metadata file of an earlier table at this location that this fresh table
    /// replaces (a write without `append` onto an existing table), recorded as its
    /// `metadata-log` predecessor. The next version continues from the current one, so a
    /// reader looking past the version hint can tell a successor from a leftover.
    pub replaces_metadata_file: Option<&'a str>,
    pub tags: &'a SnapshotTags,
}

/// The row and file counts one snapshot's summary records. Data and delete files are
/// counted apart, as the Iceberg spec's summary keys do: `total-records` counts every
/// row in a live data file, and `total-position-deletes` the rows deleted out of them,
/// so a reader's live row count is the difference.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotCounts {
    pub added_data_files: i64,
    pub added_records: i64,
    pub added_delete_files: i64,
    pub added_position_deletes: i64,
    /// Data files the snapshot removes, and every row they held (deleted ones included).
    pub removed_data_files: i64,
    pub removed_records: i64,
    /// Delete files the snapshot removes, and the positions they deleted.
    pub removed_delete_files: i64,
    pub removed_position_deletes: i64,
    pub total_data_files: i64,
    pub total_records: i64,
    pub total_delete_files: i64,
    pub total_position_deletes: i64,
    /// The snapshot rewrites files without changing the table's rows (a compaction).
    pub replace: bool,
}

/// The snapshot summary key naming the run that wrote a snapshot (see [`SnapshotTags`]).
pub const RUN_ID_KEY: &str = "shapeshift.run-id";
/// The snapshot summary key naming the shapeshift version that wrote a snapshot.
pub const VERSION_KEY: &str = "shapeshift.version";
/// The snapshot summary key counting how many times a catalog append was built again onto
/// a newer head before this snapshot landed. Absent when it landed as first built.
pub const REBASES_KEY: &str = "shapeshift.rebases";

/// What a snapshot records about the write itself, beside its counts: the run it belongs to
/// when the caller names one, and how many times it was rebuilt onto a newer head. Every
/// snapshot also records the shapeshift version that wrote it. All three are ordinary
/// summary properties, which any Iceberg reader shows and ignores.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotTags {
    /// The caller's id for this run (`shape --run-id`). An orchestrator gives the same id
    /// to every attempt of one run, so an attempt can tell that an earlier one already
    /// committed (see [`snapshot_of_run`]).
    pub run_id: Option<String>,
    pub rebases: u32,
}

/// The longest run id accepted.
pub const RUN_ID_MAX: usize = 128;

/// Check a run id: 1 to [`RUN_ID_MAX`] characters, each an ASCII letter, a digit, or one of
/// `.` `_` `:` `-`, so it reads the same in a summary, a log line and a URL.
pub fn validate_run_id(id: &str) -> Result<()> {
    let ok = !id.is_empty()
        && id.len() <= RUN_ID_MAX
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(ShapeError::Spec(format!(
            "run id `{id}` is not usable: use 1 to {RUN_ID_MAX} characters, each a letter, a \
             digit, `.`, `_`, `:` or `-`"
        )))
    }
}

/// A snapshot that a run committed, found by its run id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunSnapshot {
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub timestamp_ms: i64,
    /// Its `shapeshift.rebases`, 0 when absent.
    pub rebases: u32,
}

/// The snapshot in this table's history that run `run_id` committed, if any. An attempt
/// that finds one must not commit again: the table already took this run's commit.
///
/// Any snapshot still in the history counts, on `main` or not. A rollback that took `main`
/// back past the run is a decision to take its rows off `main`, and a retry of the same run
/// must not undo it: that retry finds the snapshot and commits nothing, and the rows stay
/// off `main` (a run started again with a new run id writes them again). An expiry that
/// removed the snapshot also removes the record (retention is days; a retry comes within
/// minutes).
pub fn snapshot_of_run(meta: &Value, run_id: &str) -> Option<RunSnapshot> {
    meta["snapshots"].as_array()?.iter().find_map(|s| {
        if s["summary"][RUN_ID_KEY].as_str() != Some(run_id) {
            return None;
        }
        Some(RunSnapshot {
            snapshot_id: s["snapshot-id"].as_i64()?,
            sequence_number: s["sequence-number"].as_i64().unwrap_or(0),
            timestamp_ms: s["timestamp-ms"].as_i64().unwrap_or(0),
            rebases: s["summary"][REBASES_KEY]
                .as_str()
                .and_then(|r| r.parse().ok())
                .unwrap_or(0),
        })
    })
}

/// The snapshot `summary`. A compaction is a `replace`: the rows stay the same, only the
/// files holding them change. A snapshot that deletes rows is an `overwrite` — the
/// operation Iceberg names for one that both adds and removes — otherwise an `append`.
fn summary_json(c: &SnapshotCounts, tags: &SnapshotTags) -> Value {
    let operation = if c.replace {
        "replace"
    } else if c.added_position_deletes > 0 {
        "overwrite"
    } else {
        "append"
    };
    let mut summary = json!({
        "operation": operation,
        "added-data-files": c.added_data_files.to_string(),
        "added-records": c.added_records.to_string(),
        "total-data-files": c.total_data_files.to_string(),
        "total-records": c.total_records.to_string(),
        "total-delete-files": c.total_delete_files.to_string(),
        "total-position-deletes": c.total_position_deletes.to_string(),
        "total-equality-deletes": "0",
    });
    if c.added_delete_files > 0 {
        summary["added-delete-files"] = json!(c.added_delete_files.to_string());
        summary["added-position-delete-files"] = json!(c.added_delete_files.to_string());
        summary["added-position-deletes"] = json!(c.added_position_deletes.to_string());
    }
    if c.removed_data_files > 0 {
        summary["deleted-data-files"] = json!(c.removed_data_files.to_string());
        summary["deleted-records"] = json!(c.removed_records.to_string());
    }
    if c.removed_delete_files > 0 {
        summary["removed-delete-files"] = json!(c.removed_delete_files.to_string());
        summary["removed-position-delete-files"] = json!(c.removed_delete_files.to_string());
        summary["removed-position-deletes"] = json!(c.removed_position_deletes.to_string());
    }
    summary[VERSION_KEY] = json!(env!("CARGO_PKG_VERSION"));
    if let Some(id) = &tags.run_id {
        summary[RUN_ID_KEY] = json!(id);
    }
    if tags.rebases > 0 {
        summary[REBASES_KEY] = json!(tags.rebases.to_string());
    }
    summary
}

/// Build the full Iceberg v2 `metadata.json` document.
pub fn table_metadata(inp: &MetadataInput) -> Result<Value> {
    if inp.fields.is_empty() {
        return Err(ShapeError::Sink("iceberg table has no columns".into()));
    }
    let schema = schema_json(inp.fields);
    let summary = summary_json(&inp.counts, inp.tags);
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
        "metadata-log": match inp.replaces_metadata_file {
            Some(file) => json!([{ "timestamp-ms": inp.timestamp_ms, "metadata-file": file }]),
            None => json!([]),
        },
    }))
}

/// Inputs for [`append_snapshot`]: one new snapshot over an existing table.
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
    pub counts: SnapshotCounts,
    /// The (already evolved) Iceberg fields this write's data files were stamped with.
    /// When they differ from the prior current schema — an additive evolution added
    /// columns — a new schema object is appended and `current-schema-id` /
    /// `last-column-id` advance; otherwise the prior schema is carried forward.
    pub schema_fields: &'a [IceField],
    pub tags: &'a SnapshotTags,
}

/// Append a new snapshot onto the prior table metadata: add the snapshot, advance the
/// table pointers (`current-snapshot-id`, `last-sequence-number`, `last-updated-ms`,
/// `refs.main`), extend the snapshot log, and record the prior metadata file in the
/// metadata log. On an **additive schema evolution** (the caller's evolved fields carry
/// columns the prior current schema lacks) a new schema object is appended — prior
/// schemas stay in `schemas`, so old snapshots remain readable — and the partition spec
/// is carried forward unchanged (the caller checks compatibility first).
///
/// Retention: `snapshots`, `snapshot-log`, and `metadata-log` only ever grow here (every
/// append clones the whole prior `metadata.json`); [`remove_snapshots`] is how an expiry
/// shortens them.
pub fn append_snapshot(inp: &AppendInput) -> Result<Value> {
    let mut m = inp.prior.clone();
    if !m.is_object() {
        return Err(ShapeError::Sink(
            "prior iceberg metadata is not a JSON object".into(),
        ));
    }
    // A table a catalog created empty has no snapshot: the Java catalog writes `-1`.
    let parent = m["current-snapshot-id"].as_i64().filter(|&id| id != -1);
    let schema_id = advance_schema(&mut m, inp.schema_fields)?;

    let summary = summary_json(&inp.counts, inp.tags);
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

/// Inputs for [`remove_snapshots`]: the same table with fewer snapshots.
pub struct RemoveInput<'a> {
    /// The table's current `metadata.json`, parsed.
    pub prior: &'a Value,
    pub base_location: &'a str,
    pub prior_version: u64,
    /// The snapshots to keep. Must include the current one.
    pub keep: &'a std::collections::HashSet<i64>,
    pub timestamp_ms: i64,
}

/// The table's metadata without the snapshots not in `keep`. The current snapshot, the
/// sequence numbers and the schemas are unchanged. `refs` keep only the references whose
/// snapshot survives; `snapshot-log` keeps its newest entries up to the first one whose
/// snapshot is gone (the log is the current snapshot's unbroken history, as Iceberg's own
/// expiry leaves it); the superseded metadata file is recorded in the `metadata-log`.
pub fn remove_snapshots(inp: &RemoveInput) -> Result<Value> {
    let mut m = inp.prior.clone();
    if !m.is_object() {
        return Err(ShapeError::Sink(
            "prior iceberg metadata is not a JSON object".into(),
        ));
    }
    // An entry without an id is kept: it names nothing the expiry removed.
    let kept =
        |v: &Value, key: &str| !matches!(v[key].as_i64(), Some(id) if !inp.keep.contains(&id));
    if let Some(current) = m["current-snapshot-id"].as_i64() {
        if !inp.keep.contains(&current) {
            return Err(ShapeError::Sink(
                "internal error: an expiry would remove the current snapshot".into(),
            ));
        }
    }
    let snapshots: Vec<Value> = m["snapshots"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|s| kept(s, "snapshot-id"))
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    m["snapshots"] = Value::Array(snapshots);

    let log: Vec<Value> = m["snapshot-log"].as_array().cloned().unwrap_or_default();
    let mut suffix: Vec<Value> = log
        .into_iter()
        .rev()
        .take_while(|e| kept(e, "snapshot-id"))
        .collect();
    suffix.reverse();
    m["snapshot-log"] = Value::Array(suffix);

    if let Some(refs) = m["refs"].as_object() {
        let refs: serde_json::Map<String, Value> = refs
            .iter()
            .filter(|(_, r)| kept(r, "snapshot-id"))
            .map(|(k, r)| (k.clone(), r.clone()))
            .collect();
        m["refs"] = Value::Object(refs);
    }

    let prior_ts = inp.prior["last-updated-ms"]
        .as_i64()
        .unwrap_or(inp.timestamp_ms);
    m["last-updated-ms"] = json!(inp.timestamp_ms);
    push_array(
        &mut m,
        "metadata-log",
        json!({
            "timestamp-ms": prior_ts,
            "metadata-file": join_location(
                inp.base_location,
                &format!("metadata/v{}.metadata.json", inp.prior_version)
            ),
        }),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_is_found_in_the_history_even_after_main_was_rolled_back_past_it() {
        let snap = |id: i64, parent: Option<i64>, run: Option<&str>| {
            let mut summary = json!({ "operation": "append" });
            if let Some(run) = run {
                summary[RUN_ID_KEY] = json!(run);
                summary[REBASES_KEY] = json!("2");
            }
            json!({ "snapshot-id": id, "parent-snapshot-id": parent, "sequence-number": id,
                    "timestamp-ms": id * 10, "summary": summary })
        };
        let mut meta = json!({
            "snapshots": [snap(1, None, None), snap(2, Some(1), Some("r-7"))],
            "refs": { "main": { "snapshot-id": 2, "type": "branch" } },
        });
        let found = snapshot_of_run(&meta, "r-7").unwrap();
        assert_eq!((found.snapshot_id, found.rebases), (2, 2));
        // Rolled back to 1: the run's snapshot is off main but retained, and a retry of the
        // run still finds it rather than committing the rows again.
        meta["refs"]["main"]["snapshot-id"] = json!(1);
        assert_eq!(
            snapshot_of_run(&meta, "r-7").map(|f| f.snapshot_id),
            Some(2)
        );
        assert_eq!(snapshot_of_run(&meta, "r-8"), None);
        // Expired: no record.
        meta["snapshots"] = json!([snap(1, None, None)]);
        assert_eq!(snapshot_of_run(&meta, "r-7"), None);
    }
}
