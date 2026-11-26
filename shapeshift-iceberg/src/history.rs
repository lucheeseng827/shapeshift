//! A table's history as a list of events, read from its metadata alone.
//!
//! Iceberg metadata is already a durable, append-only record of everything that happened to a
//! table: every snapshot says when it was made, which operation it was, the rows and files it
//! added and removed, its parent, the schema it was written with, and (for a snapshot shapeshift
//! wrote) the run and engine version behind it. [`table_events`] turns that into events a person
//! can read as a timeline, with no state of its own and no agent: an `append`, an `overwrite` (a
//! merge, with its deletes), a `replace` (a compaction), a schema change riding on a snapshot,
//! and an `expire`.
//!
//! An expiry adds no snapshot. It shows as snapshots leaving the history between two metadata
//! versions, so finding one needs the earlier versions too; the caller passes whichever it has
//! (a local table keeps every `vN.metadata.json`).

use std::collections::{HashMap, HashSet};

use serde_json::{json, Value};

use crate::metadata::{REBASES_KEY, RUN_ID_KEY, VERSION_KEY};

/// What happened to a table, at one point in its history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEvent {
    /// The snapshot's operation (`append`, `overwrite`, `replace`, `delete`), or `expire` for
    /// snapshots leaving the history, or `recreate` for a table written afresh in its place.
    pub kind: String,
    /// The snapshot this event is (`None` for an expiry or a recreation).
    pub snapshot_id: Option<i64>,
    pub sequence_number: Option<i64>,
    pub parent_id: Option<i64>,
    pub timestamp_ms: i64,
    /// The snapshot is `main` or one of its ancestors (for an expired snapshot: was, in the
    /// last version that held it).
    pub on_main: bool,
    /// The snapshot has since left the history (an expiry removed it); it is known from an
    /// earlier metadata version.
    pub expired: bool,
    pub counts: EventCounts,
    /// Who wrote it, as far as the snapshot says.
    pub writer: Writer,
    /// The schema changed with this snapshot.
    pub schema: Option<SchemaChange>,
    /// For an expiry or a recreation: the snapshots that left the history.
    pub removed: Vec<RemovedSnapshot>,
}

/// The counts a snapshot's summary records (0 where it records none).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventCounts {
    pub added_records: i64,
    pub deleted_records: i64,
    pub added_data_files: i64,
    pub removed_data_files: i64,
    pub added_delete_files: i64,
    pub removed_delete_files: i64,
    pub added_position_deletes: i64,
    pub total_records: i64,
    pub total_data_files: i64,
    pub total_delete_files: i64,
    pub total_position_deletes: i64,
    pub total_equality_deletes: i64,
}

impl EventCounts {
    /// The records less the position deletes: the rows a reader sees after this snapshot when
    /// it has no equality deletes (shapeshift writes position deletes only). See
    /// [`EventCounts::known_live_records`] for a count that says when it cannot know.
    pub fn live_records(&self) -> i64 {
        self.total_records - self.total_position_deletes
    }

    /// The rows a reader sees after this snapshot, when the summary can say. Equality deletes
    /// (another engine's) match rows by value, so with any of them the count is unknown.
    pub fn known_live_records(&self) -> Option<i64> {
        (self.total_equality_deletes == 0).then(|| self.live_records())
    }
}

/// What a snapshot says about its writer. A snapshot with no engine version came from a writer
/// other than shapeshift.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Writer {
    pub run_id: Option<String>,
    pub engine_version: Option<String>,
    pub rebases: u32,
}

/// A schema change between a snapshot and the one before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaChange {
    pub from: i64,
    pub to: i64,
    /// Columns in the new schema and not the old (nested ones as `parent.child`).
    pub added: Vec<String>,
    pub removed: Vec<String>,
}

/// A snapshot that left the history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemovedSnapshot {
    pub snapshot_id: i64,
    pub sequence_number: Option<i64>,
}

impl TableEvent {
    /// The event as JSON. Snapshot ids are strings: they are 64-bit, past what a JSON number
    /// holds exactly in JavaScript.
    pub fn to_json(&self) -> Value {
        let c = &self.counts;
        json!({
            "kind": self.kind,
            "snapshot_id": self.snapshot_id.map(|id| id.to_string()),
            "sequence_number": self.sequence_number,
            "parent_id": self.parent_id.map(|id| id.to_string()),
            "timestamp_ms": self.timestamp_ms,
            "on_main": self.on_main,
            "expired": self.expired,
            "counts": {
                "added_records": c.added_records,
                "deleted_records": c.deleted_records,
                "added_data_files": c.added_data_files,
                "removed_data_files": c.removed_data_files,
                "added_delete_files": c.added_delete_files,
                "removed_delete_files": c.removed_delete_files,
                "added_position_deletes": c.added_position_deletes,
                "total_records": c.total_records,
                "total_data_files": c.total_data_files,
                "total_delete_files": c.total_delete_files,
                "total_position_deletes": c.total_position_deletes,
                "total_equality_deletes": c.total_equality_deletes,
                "live_records": c.known_live_records(),
            },
            "writer": {
                "run_id": self.writer.run_id,
                "engine_version": self.writer.engine_version,
                "rebases": self.writer.rebases,
            },
            "schema": self.schema.as_ref().map(|s| json!({
                "from": s.from, "to": s.to, "added": s.added, "removed": s.removed,
            })),
            "removed": self.removed.iter().map(|r| json!({
                "snapshot_id": r.snapshot_id.to_string(),
                "sequence_number": r.sequence_number,
            })).collect::<Vec<_>>(),
        })
    }
}

/// The events in a table's history, oldest first: one per snapshot, plus an `expire` (or
/// `recreate`) wherever snapshots left the history between two metadata versions among
/// `earlier` and `current`. Snapshots an expiry removed are still events (marked `expired`),
/// known from the earlier versions of the same table that held them. `earlier` may be in any
/// order and may be empty (then only `current`'s snapshots show, and no expiry is found).
pub fn table_events(current: &Value, earlier: &[Value]) -> Vec<TableEvent> {
    let ids_now: HashSet<i64> = current["snapshots"]
        .as_array()
        .map(|a| a.iter().filter_map(|s| s["snapshot-id"].as_i64()).collect())
        .unwrap_or_default();
    // Every snapshot this table has had, newest version first so its latest record wins. A
    // table written afresh in place (another uuid) has a history of its own, left out.
    let mut versions: Vec<&Value> = std::iter::once(current)
        .chain(
            earlier
                .iter()
                .filter(|m| m["table-uuid"] == current["table-uuid"]),
        )
        .collect();
    versions.sort_by_key(|m| std::cmp::Reverse(m["last-updated-ms"].as_i64().unwrap_or(0)));
    // Parent links from every version: a snapshot an expiry removed still links its retained
    // ancestors (one a tag keeps) to `main`.
    let mut parents: HashMap<i64, Option<i64>> = HashMap::new();
    for m in &versions {
        for s in m["snapshots"].as_array().into_iter().flatten() {
            if let Some(id) = s["snapshot-id"].as_i64() {
                parents
                    .entry(id)
                    .or_insert(s["parent-snapshot-id"].as_i64());
            }
        }
    }
    let mut seen: HashMap<i64, (&Value, bool)> = HashMap::new();
    for m in &versions {
        let lineage = main_lineage(m, &parents);
        for s in m["snapshots"].as_array().into_iter().flatten() {
            if let Some(id) = s["snapshot-id"].as_i64() {
                seen.entry(id).or_insert((s, lineage.contains(&id)));
            }
        }
    }
    let mut snapshots: Vec<(&Value, bool)> = seen.into_values().collect();
    snapshots.sort_by_key(|(s, _)| (s["sequence-number"].as_i64(), s["timestamp-ms"].as_i64()));
    let schemas = schema_columns(current);
    // A schema change is against the snapshot's parent (another branch's commits in between
    // are not its history); a snapshot whose parent is unknown shows none.
    let schema_of: HashMap<i64, i64> = snapshots
        .iter()
        .filter_map(|(s, _)| Some((s["snapshot-id"].as_i64()?, s["schema-id"].as_i64()?)))
        .collect();

    let mut events = Vec::new();
    for (s, on_main) in &snapshots {
        let id = s["snapshot-id"].as_i64();
        let summary = &s["summary"];
        let num = |k: &str| -> i64 {
            summary[k]
                .as_str()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0)
        };
        let parent_schema = s["parent-snapshot-id"]
            .as_i64()
            .and_then(|p| schema_of.get(&p).copied());
        let schema = match (parent_schema, s["schema-id"].as_i64()) {
            (Some(from), Some(to)) if from != to => Some(schema_change(&schemas, from, to)),
            _ => None,
        };
        events.push(TableEvent {
            kind: summary["operation"]
                .as_str()
                .unwrap_or("append")
                .to_string(),
            snapshot_id: id,
            sequence_number: s["sequence-number"].as_i64(),
            parent_id: s["parent-snapshot-id"].as_i64(),
            timestamp_ms: s["timestamp-ms"].as_i64().unwrap_or(0),
            on_main: *on_main,
            expired: id.is_some_and(|id| !ids_now.contains(&id)),
            counts: EventCounts {
                added_records: num("added-records"),
                deleted_records: num("deleted-records"),
                added_data_files: num("added-data-files"),
                removed_data_files: num("deleted-data-files"),
                added_delete_files: num("added-delete-files"),
                removed_delete_files: num("removed-delete-files"),
                added_position_deletes: num("added-position-deletes"),
                total_records: num("total-records"),
                total_data_files: num("total-data-files"),
                total_delete_files: num("total-delete-files"),
                total_position_deletes: num("total-position-deletes"),
                total_equality_deletes: num("total-equality-deletes"),
            },
            writer: Writer {
                run_id: summary[RUN_ID_KEY].as_str().map(str::to_string),
                engine_version: summary[VERSION_KEY].as_str().map(str::to_string),
                rebases: summary[REBASES_KEY]
                    .as_str()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0),
            },
            schema,
            removed: Vec::new(),
        });
    }

    events.extend(removals(current, earlier));
    // Oldest first; an expiry sorts after the snapshots made before it.
    events.sort_by_key(|e| (e.timestamp_ms, e.sequence_number.unwrap_or(i64::MAX)));
    events
}

/// `meta`'s `main` and every ancestor of it, by the parent links known from every version.
fn main_lineage(meta: &Value, parents: &HashMap<i64, Option<i64>>) -> HashSet<i64> {
    let mut out = HashSet::new();
    let mut at = crate::catalog::main_snapshot(meta);
    while let Some(id) = at {
        if !out.insert(id) {
            break; // a cycle would be malformed metadata; stop rather than loop
        }
        at = parents.get(&id).copied().flatten();
    }
    out
}

/// Every schema's column names, by schema id: nested struct fields as `parent.child`, and
/// fields inside a list or a map as Iceberg names them (`items.element.sku`, `m.value.x`).
fn schema_columns(meta: &Value) -> HashMap<i64, Vec<String>> {
    fn walk(fields: &Value, prefix: &str, out: &mut Vec<String>) {
        for f in fields.as_array().into_iter().flatten() {
            let Some(name) = f["name"].as_str() else {
                continue;
            };
            let full = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}.{name}")
            };
            out.push(full.clone());
            nested(&f["type"], &full, out);
        }
    }
    fn nested(ty: &Value, path: &str, out: &mut Vec<String>) {
        match ty["type"].as_str() {
            Some("struct") => walk(&ty["fields"], path, out),
            Some("list") => nested(&ty["element"], &format!("{path}.element"), out),
            Some("map") => {
                nested(&ty["key"], &format!("{path}.key"), out);
                nested(&ty["value"], &format!("{path}.value"), out);
            }
            _ => {}
        }
    }
    meta["schemas"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| {
                    let mut cols = Vec::new();
                    walk(&s["fields"], "", &mut cols);
                    Some((s["schema-id"].as_i64()?, cols))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn schema_change(schemas: &HashMap<i64, Vec<String>>, from: i64, to: i64) -> SchemaChange {
    let (a, b) = (
        schemas.get(&from).cloned().unwrap_or_default(),
        schemas.get(&to).cloned().unwrap_or_default(),
    );
    SchemaChange {
        from,
        to,
        added: b.iter().filter(|c| !a.contains(c)).cloned().collect(),
        removed: a.iter().filter(|c| !b.contains(c)).cloned().collect(),
    }
}

/// Snapshots that left the history between consecutive metadata versions: an `expire` when
/// the table stayed the same table, a `recreate` when another table (a new uuid) took its place.
fn removals(current: &Value, earlier: &[Value]) -> Vec<TableEvent> {
    let mut docs: Vec<&Value> = earlier.iter().chain(std::iter::once(current)).collect();
    // Versions in the order they were written. Two in the same millisecond with the same last
    // sequence number differ only by what left (an expiry adds no snapshot): the one holding
    // more snapshots came first, whatever order the caller passed them in.
    let count = |m: &Value| m["snapshots"].as_array().map_or(0, Vec::len);
    docs.sort_by_key(|m| {
        (
            m["last-updated-ms"].as_i64().unwrap_or(0),
            m["last-sequence-number"].as_i64().unwrap_or(0),
            std::cmp::Reverse(count(m)),
        )
    });
    docs.dedup_by(|a, b| a["last-updated-ms"] == b["last-updated-ms"] && a == b);
    let ids = |m: &Value| -> Vec<(i64, Option<i64>)> {
        m["snapshots"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| {
                        Some((s["snapshot-id"].as_i64()?, s["sequence-number"].as_i64()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let mut out = Vec::new();
    for pair in docs.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        // Another table in its place (a new uuid) replaced all of the old one's snapshots, even
        // none, and even ids the new one happens to reuse.
        let same_table = a["table-uuid"] == b["table-uuid"];
        let kept: HashSet<i64> = if same_table {
            ids(b).into_iter().map(|(id, _)| id).collect()
        } else {
            HashSet::new()
        };
        let removed: Vec<RemovedSnapshot> = ids(a)
            .into_iter()
            .filter(|(id, _)| !kept.contains(id))
            .map(|(snapshot_id, sequence_number)| RemovedSnapshot {
                snapshot_id,
                sequence_number,
            })
            .collect();
        if removed.is_empty() && same_table {
            continue;
        }
        out.push(TableEvent {
            kind: if same_table { "expire" } else { "recreate" }.to_string(),
            snapshot_id: None,
            sequence_number: None,
            parent_id: None,
            timestamp_ms: b["last-updated-ms"].as_i64().unwrap_or(0),
            on_main: false,
            expired: false,
            counts: EventCounts::default(),
            writer: Writer::default(),
            schema: None,
            removed,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(id: i64, parent: Option<i64>, seq: i64, op: &str, schema: i64, extra: Value) -> Value {
        let mut summary = json!({
            "operation": op, "added-records": "10", "total-records": (seq * 10).to_string(),
            "total-data-files": seq.to_string(), "total-position-deletes": "0",
        });
        for (k, v) in extra.as_object().unwrap() {
            summary[k] = v.clone();
        }
        let mut s = json!({ "snapshot-id": id, "sequence-number": seq, "timestamp-ms": seq * 1000,
                            "schema-id": schema, "summary": summary });
        if let Some(p) = parent {
            s["parent-snapshot-id"] = json!(p);
        }
        s
    }

    fn meta(uuid: &str, snaps: Vec<Value>, updated: i64) -> Value {
        let main = snaps
            .last()
            .map(|s| s["snapshot-id"].clone())
            .unwrap_or(json!(-1));
        json!({
            "table-uuid": uuid, "last-updated-ms": updated,
            "last-sequence-number": snaps.len(),
            "current-snapshot-id": main, "refs": { "main": { "snapshot-id": main, "type": "branch" } },
            "schemas": [
                { "schema-id": 0, "fields": [ { "id": 1, "name": "id", "type": "long" } ] },
                { "schema-id": 1, "fields": [ { "id": 1, "name": "id", "type": "long" },
                    { "id": 2, "name": "device", "type": { "type": "struct", "fields": [
                        { "id": 3, "name": "os", "type": "string" } ] } } ] },
            ],
            "snapshots": snaps,
        })
    }

    #[test]
    fn snapshots_become_events_with_their_writer_and_schema_changes() {
        let s1 = snap(
            11,
            None,
            1,
            "append",
            0,
            json!({ VERSION_KEY: "0.5.0", RUN_ID_KEY: "r-1" }),
        );
        let s2 = snap(12, Some(11), 2, "append", 0, json!({}));
        let s3 = snap(
            13,
            Some(12),
            3,
            "overwrite",
            1,
            json!({ VERSION_KEY: "0.5.0", RUN_ID_KEY: "r-3", REBASES_KEY: "2",
                    "added-position-deletes": "4", "total-position-deletes": "4" }),
        );
        let m = meta("u", vec![s1, s2, s3], 3000);
        let ev = table_events(&m, &[]);
        assert_eq!(ev.len(), 3);
        assert_eq!(
            ev.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>(),
            ["append", "append", "overwrite"]
        );
        assert_eq!(ev[0].writer.run_id.as_deref(), Some("r-1"));
        assert_eq!(
            ev[1].writer,
            Writer::default(),
            "another writer names no run or version"
        );
        assert_eq!(ev[2].writer.rebases, 2);
        assert_eq!(ev[2].counts.live_records(), 26);
        assert!(ev.iter().all(|e| e.on_main));
        assert_eq!(ev[1].schema, None);
        let change = ev[2].schema.as_ref().unwrap();
        assert_eq!((change.from, change.to), (0, 1));
        assert_eq!(change.added, ["device", "device.os"]);
        assert!(change.removed.is_empty());
        let j = ev[2].to_json();
        assert_eq!(j["snapshot_id"], "13");
        assert_eq!(j["writer"]["run_id"], "r-3");
    }

    #[test]
    fn snapshots_leaving_the_history_are_an_expiry_and_a_new_table_is_a_recreation() {
        let all = vec![
            snap(11, None, 1, "append", 0, json!({})),
            snap(12, Some(11), 2, "append", 0, json!({})),
            snap(13, Some(12), 3, "append", 0, json!({})),
        ];
        let v3 = meta("u", all.clone(), 3000);
        let v4 = meta("u", all[2..].to_vec(), 4000); // 11 and 12 expired
        let ev = table_events(&v4, std::slice::from_ref(&v3));
        assert_eq!(ev.len(), 4, "the expired snapshots still show, from v3");
        assert_eq!(
            ev.iter().map(|e| e.expired).collect::<Vec<_>>(),
            [true, true, false, false]
        );
        let expiry = ev.iter().find(|e| e.kind == "expire").unwrap();
        assert_eq!(expiry.timestamp_ms, 4000);
        assert_eq!(
            expiry
                .removed
                .iter()
                .map(|r| r.snapshot_id)
                .collect::<Vec<_>>(),
            [11, 12]
        );
        assert_eq!(
            ev.last().unwrap().kind,
            "expire",
            "it comes after the snapshots before it"
        );
        // No earlier version given: no expiry can be seen.
        assert!(table_events(&v4, &[]).iter().all(|e| e.kind != "expire"));
        // A table written afresh in place (a new uuid) is a recreation, not an expiry.
        let fresh = meta("w", vec![snap(21, None, 1, "append", 0, json!({}))], 5000);
        let ev = table_events(&fresh, &[v3, v4]);
        assert_eq!(
            ev.iter().filter(|e| e.snapshot_id.is_some()).count(),
            1,
            "the replaced table's snapshots are not this table's"
        );
        assert_eq!(ev.iter().filter(|e| e.kind == "recreate").count(), 1);
        assert_eq!(ev.iter().filter(|e| e.kind == "expire").count(), 1);
    }

    #[test]
    fn only_main_and_its_ancestors_are_on_main() {
        let s1 = snap(11, None, 1, "append", 0, json!({}));
        let s2 = snap(12, Some(11), 2, "append", 0, json!({}));
        let stray = snap(99, Some(11), 3, "append", 0, json!({}));
        let mut m = meta("u", vec![s1, stray, s2], 3000);
        m["refs"]["main"]["snapshot-id"] = json!(12);
        m["current-snapshot-id"] = json!(12);
        let ev = table_events(&m, &[]);
        let on: Vec<(i64, bool)> = ev
            .iter()
            .map(|e| (e.snapshot_id.unwrap(), e.on_main))
            .collect();
        assert!(on.contains(&(12, true)) && on.contains(&(11, true)) && on.contains(&(99, false)));
    }
}

#[cfg(test)]
mod review_tests {
    use super::*;

    fn meta(
        uuid: &str,
        updated: i64,
        last_seq: i64,
        snaps: Value,
        main: i64,
        schemas: Value,
    ) -> Value {
        json!({ "table-uuid": uuid, "last-updated-ms": updated, "last-sequence-number": last_seq,
                "current-snapshot-id": main, "refs": { "main": { "snapshot-id": main, "type": "branch" } },
                "schemas": schemas, "snapshots": snaps })
    }

    fn s(id: i64, parent: Option<i64>, seq: i64, schema: i64) -> Value {
        let mut v = json!({ "snapshot-id": id, "sequence-number": seq, "timestamp-ms": seq * 1000,
                            "schema-id": schema, "summary": { "operation": "append" } });
        if let Some(p) = parent {
            v["parent-snapshot-id"] = json!(p);
        }
        v
    }

    fn plain() -> Value {
        json!([{ "schema-id": 0, "fields": [ { "id": 1, "name": "id", "type": "long" } ] }])
    }

    #[test]
    fn a_field_added_inside_a_list_or_map_is_a_named_change() {
        let schemas = json!([
            { "schema-id": 0, "fields": [ { "id": 1, "name": "items", "type": { "type": "list",
                "element-id": 2, "element": { "type": "struct", "fields": [
                    { "id": 3, "name": "qty", "type": "int" } ] } } } ] },
            { "schema-id": 1, "fields": [ { "id": 1, "name": "items", "type": { "type": "list",
                "element-id": 2, "element": { "type": "struct", "fields": [
                    { "id": 3, "name": "qty", "type": "int" },
                    { "id": 4, "name": "sku", "type": "string" } ] } } },
                { "id": 5, "name": "tags", "type": { "type": "map", "key-id": 6, "key": "string",
                    "value-id": 7, "value": { "type": "struct", "fields": [
                        { "id": 8, "name": "w", "type": "int" } ] } } } ] },
        ]);
        let m = meta(
            "u",
            2000,
            2,
            json!([s(1, None, 1, 0), s(2, Some(1), 2, 1)]),
            2,
            schemas,
        );
        let ev = table_events(&m, &[]);
        let change = ev[1].schema.as_ref().unwrap();
        assert_eq!(change.added, ["items.element.sku", "tags", "tags.value.w"]);
    }

    #[test]
    fn a_schema_change_is_against_the_parent_not_another_branch() {
        let schemas = json!([
            { "schema-id": 0, "fields": [ { "id": 1, "name": "id", "type": "long" } ] },
            { "schema-id": 1, "fields": [ { "id": 1, "name": "id", "type": "long" },
                                          { "id": 2, "name": "x", "type": "long" } ] },
        ]);
        // 1 → 2 on main; 3 on a branch off 1 adds `x`; 4 on main (parent 2) keeps schema 0.
        let snaps = json!([
            s(1, None, 1, 0),
            s(2, Some(1), 2, 0),
            s(3, Some(1), 3, 1),
            s(4, Some(2), 4, 0)
        ]);
        let ev = table_events(&meta("u", 4000, 4, snaps, 4, schemas), &[]);
        let by_id = |id: i64| ev.iter().find(|e| e.snapshot_id == Some(id)).unwrap();
        assert_eq!(by_id(3).schema.as_ref().unwrap().added, ["x"]);
        assert_eq!(by_id(4).schema, None, "main's next commit removed nothing");
    }

    #[test]
    fn equality_deletes_make_the_live_count_unknown() {
        let c = EventCounts {
            total_records: 10,
            total_equality_deletes: 3,
            ..Default::default()
        };
        assert_eq!(c.known_live_records(), None);
        let c = EventCounts {
            total_records: 10,
            total_position_deletes: 3,
            ..Default::default()
        };
        assert_eq!(c.known_live_records(), Some(7));
    }

    #[test]
    fn two_expiries_in_one_millisecond_are_two_events_whatever_the_order_given() {
        let all = json!([
            s(11, None, 1, 0),
            s(12, Some(11), 2, 0),
            s(13, Some(12), 3, 0)
        ]);
        let v1 = meta("u", 3000, 3, all.clone(), 13, plain());
        let v2 = meta("u", 4000, 3, json!([all[1], all[2]]), 13, plain());
        let v3 = meta("u", 4000, 3, json!([all[2]]), 13, plain());
        // Newest first, as a local table's history is read.
        let ev = table_events(&v3, &[v2, v1]);
        let removed: Vec<Vec<i64>> = ev
            .iter()
            .filter(|e| e.kind == "expire")
            .map(|e| e.removed.iter().map(|r| r.snapshot_id).collect())
            .collect();
        assert_eq!(removed, [vec![11], vec![12]]);
    }

    #[test]
    fn a_new_table_in_place_is_a_recreation_even_with_no_snapshots_or_reused_ids() {
        let empty = meta("old", 1000, 0, json!([]), -1, plain());
        let fresh = meta("new", 2000, 1, json!([s(11, None, 1, 0)]), 11, plain());
        let ev = table_events(&fresh, &[empty]);
        assert_eq!(ev.iter().filter(|e| e.kind == "recreate").count(), 1);
        let old = meta("old", 1000, 1, json!([s(11, None, 1, 0)]), 11, plain());
        let ev = table_events(&fresh, &[old]);
        let r = ev
            .iter()
            .find(|e| e.kind == "recreate")
            .expect("a recreation");
        assert_eq!(
            r.removed.iter().map(|r| r.snapshot_id).collect::<Vec<_>>(),
            [11]
        );
    }

    #[test]
    fn a_retained_ancestor_stays_on_main_after_its_child_expires() {
        let all = json!([
            s(11, None, 1, 0),
            s(12, Some(11), 2, 0),
            s(13, Some(12), 3, 0)
        ]);
        let before = meta("u", 3000, 3, all.clone(), 13, plain());
        // 12 expires; a tag keeps 11.
        let after = meta("u", 4000, 3, json!([all[0], all[2]]), 13, plain());
        let ev = table_events(&after, &[before]);
        let on = |id: i64| {
            ev.iter()
                .find(|e| e.snapshot_id == Some(id))
                .unwrap()
                .on_main
        };
        assert!(on(11) && on(12) && on(13));
    }
}
