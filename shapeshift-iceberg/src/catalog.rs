//! The REST catalog protocol's commit, as data: what a write asks a catalog to check
//! (requirements) and to apply (updates), the request that creates an empty table, and how
//! a table loaded again judges a commit whose response was lost. No HTTP here: the client
//! lives in `shapeshift-objstore`, beside the stores it writes the files to.
//!
//! A catalog commit is optimistic. The write loads the table, writes its files, and asks
//! the catalog to move `main` from the snapshot it loaded to the one it adds. The catalog
//! applies that only if the table is still the one loaded (`assert-table-uuid`) and `main`
//! has not moved (`assert-ref-snapshot-id`), so two writers that loaded the same head cannot
//! both commit onto it. An expiry adds no snapshot: it asks the catalog to remove the ones
//! it planned to, on the same conditions, with every other ref held where it was too.

use serde_json::{json, Value};
use shapeshift_core::{Result, ShapeError};

use crate::metadata::{partition_spec_json, schema_json};
use crate::{IceField, MetadataArtifacts, PartitionField};

/// A table a catalog names: `namespace.name`, where the namespace may have several levels
/// (`analytics.raw.events` is table `events` in namespace `analytics.raw`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIdent {
    pub namespace: Vec<String>,
    pub name: String,
}

impl TableIdent {
    /// Parse `namespace.name`. A table needs a namespace, and no level may be empty.
    pub fn parse(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.trim().split('.').collect();
        if parts.len() < 2 || parts.iter().any(|p| p.is_empty()) {
            return Err(ShapeError::Spec(format!(
                "catalog table `{s}` must be `<namespace>.<name>` (e.g. `analytics.events`)"
            )));
        }
        let (name, namespace) = parts.split_last().expect("two or more parts");
        Ok(TableIdent {
            namespace: namespace.iter().map(|p| p.to_string()).collect(),
            name: name.to_string(),
        })
    }
}

impl std::fmt::Display for TableIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace.join("."), self.name)
    }
}

/// The snapshot `main` points at, or `None` for a table with no snapshot yet. Read from
/// `refs.main`, falling back to `current-snapshot-id` for metadata written without refs;
/// `-1` is how the Java catalog writes "none".
pub fn main_snapshot(meta: &Value) -> Option<i64> {
    let id = match meta["refs"]["main"]["snapshot-id"].as_i64() {
        Some(id) => Some(id),
        None if meta["refs"].get("main").is_some() => None,
        None => meta["current-snapshot-id"].as_i64(),
    };
    id.filter(|&id| id != -1)
}

/// The body of `POST …/namespaces/{namespace}/tables`: an empty table with this write's
/// schema and partition spec. The catalog chooses the location and may renumber the
/// field ids, so the write reconciles its fields against the table it gets back.
pub fn create_table_request(
    name: &str,
    fields: &[IceField],
    partition_fields: &[PartitionField],
) -> Value {
    json!({
        "name": name,
        "schema": schema_json(fields),
        "partition-spec": partition_spec_json(partition_fields),
        "stage-create": false,
    })
}

/// The body of `POST …/tables/{table}` for one write: requirements and updates.
#[derive(Debug, Clone, PartialEq)]
pub struct CatalogCommit {
    pub body: Value,
    /// What the commit does to the table, which is what the table loaded again is searched
    /// for when its answer was lost.
    pub effect: CommitEffect,
    /// The snapshot `main` pointed at when the table was loaded (`None` = no snapshot).
    pub base: Option<i64>,
}

/// What a catalog commit does to the table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitEffect {
    /// Adds this snapshot and moves `main` to it: an append, a merge, a compaction.
    AddsSnapshot(i64),
    /// Removes these snapshots, and moves no ref: an expiry.
    RemovesSnapshots(Vec<i64>),
}

impl std::fmt::Display for CommitEffect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitEffect::AddsSnapshot(id) => write!(f, "snapshot {id}"),
            CommitEffect::RemovesSnapshots(ids) => {
                let ids: Vec<String> = ids.iter().map(i64::to_string).collect();
                write!(f, "the expiry of snapshot(s) {}", ids.join(", "))
            }
        }
    }
}

impl CatalogCommit {
    /// The snapshot this commit adds, if it adds one.
    pub fn snapshot_id(&self) -> Option<i64> {
        match self.effect {
            CommitEffect::AddsSnapshot(id) => Some(id),
            CommitEffect::RemovesSnapshots(_) => None,
        }
    }

    /// Judge this commit from the table's metadata as loaded after its answer was lost. A
    /// table that is not the one the commit asserted (dropped and created again under the
    /// same name) can never take it, whatever it holds: none of an expiry's snapshots is
    /// in a new table, and that says nothing about the old one.
    pub fn judge(&self, meta: &Value) -> Reloaded {
        let asserted = self.body["requirements"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|r| r["type"] == "assert-table-uuid")
            .map(|r| &r["uuid"]);
        if asserted.is_some_and(|uuid| *uuid != meta["table-uuid"]) {
            return Reloaded::Refused;
        }
        match &self.effect {
            CommitEffect::AddsSnapshot(id) => judge_reload(meta, *id, self.base),
            CommitEffect::RemovesSnapshots(ids) => judge_removal(meta, ids, &self.asserted_refs()),
        }
    }

    /// Every ref the commit asserts, and the snapshot it asserts the ref points at.
    fn asserted_refs(&self) -> Vec<(String, Option<i64>)> {
        self.body["requirements"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|r| r["type"] == "assert-ref-snapshot-id")
            .filter_map(|r| Some((r["ref"].as_str()?.to_string(), r["snapshot-id"].as_i64())))
            .collect()
    }
}

/// The snapshot the ref `name` points at (`None` = no such ref). `main` falls back to
/// `current-snapshot-id`, as in [`main_snapshot`].
fn ref_snapshot(meta: &Value, name: &str) -> Option<i64> {
    if name == "main" {
        main_snapshot(meta)
    } else {
        meta["refs"][name]["snapshot-id"]
            .as_i64()
            .filter(|&id| id != -1)
    }
}

/// The commit that publishes `art` onto `loaded`, the table metadata the write started
/// from. `art` was built as an append onto `loaded` (its metadata file is never written:
/// the catalog writes its own), so the new snapshot, and the schema it needs when the
/// write added columns, are read out of it.
///
/// Requirements: the table is still `loaded` (its uuid) and `main` is still where it was.
/// A new schema also asserts the current schema and the last assigned field id, so a
/// concurrent schema change refuses this commit instead of being overwritten.
pub fn catalog_commit(art: &MetadataArtifacts, loaded: &Value) -> Result<CatalogCommit> {
    let built: Value = serde_json::from_slice(&art.metadata_bytes)
        .map_err(|e| ShapeError::Sink(format!("re-reading the built metadata: {e}")))?;
    let snapshot = built["snapshots"]
        .as_array()
        .and_then(|s| s.iter().find(|s| s["snapshot-id"] == art.snapshot_id))
        .cloned()
        .ok_or_else(|| ShapeError::Sink("internal error: the built snapshot is missing".into()))?;
    let uuid = loaded["table-uuid"]
        .as_str()
        .ok_or_else(|| ShapeError::Sink("the catalog's table has no table-uuid".into()))?;
    let base = main_snapshot(loaded);

    let mut requirements = vec![
        json!({ "type": "assert-table-uuid", "uuid": uuid }),
        json!({ "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": base }),
    ];
    let mut updates = Vec::new();
    let schema_id = snapshot["schema-id"].as_i64();
    if schema_id.is_some() && schema_id != loaded["current-schema-id"].as_i64() {
        let schema = built["schemas"]
            .as_array()
            .and_then(|s| s.iter().find(|s| s["schema-id"].as_i64() == schema_id))
            .cloned()
            .ok_or_else(|| {
                ShapeError::Sink("internal error: the snapshot's schema is missing".into())
            })?;
        requirements.push(json!({
            "type": "assert-current-schema-id",
            "current-schema-id": loaded["current-schema-id"],
        }));
        requirements.push(json!({
            "type": "assert-last-assigned-field-id",
            "last-assigned-field-id": loaded["last-column-id"],
        }));
        updates.push(json!({
            "action": "add-schema",
            "schema": schema,
            "last-column-id": built["last-column-id"],
        }));
        // -1: the schema added by this same commit.
        updates.push(json!({ "action": "set-current-schema", "schema-id": -1 }));
    }
    updates.push(json!({ "action": "add-snapshot", "snapshot": snapshot }));
    // The statistics files this snapshot names (its tombstones), so the catalog's copy of
    // the table keeps them reachable as a table directory's does.
    for entry in built["statistics"].as_array().into_iter().flatten() {
        if entry["snapshot-id"] == art.snapshot_id {
            updates.push(json!({
                "action": "set-statistics",
                "snapshot-id": art.snapshot_id,
                "statistics": entry,
            }));
        }
    }
    updates.push(json!({
        "action": "set-snapshot-ref",
        "ref-name": "main",
        "type": "branch",
        "snapshot-id": art.snapshot_id,
    }));
    Ok(CatalogCommit {
        body: json!({ "requirements": requirements, "updates": updates }),
        effect: CommitEffect::AddsSnapshot(art.snapshot_id),
        base,
    })
}

/// The commit that expires `snapshots` from `loaded`, the table metadata the expiry was
/// planned against: `remove-snapshots`, asserting the table is still `loaded` and that
/// `main` and every other ref still point where they did.
///
/// The plan kept every snapshot a ref pointed at, and deletes the files only the expired
/// ones read once the commit lands. So a ref moved since (a branch reset, a tag moved onto
/// an old snapshot) must refuse the commit, or it would be left naming a snapshot whose
/// files are gone; another writer's commit to `main` refuses it too, as for every commit.
/// A ref created after the table was loaded is not covered: the protocol has no
/// requirement that says "no other ref", and a catalog drops a ref left pointing at a
/// removed snapshot.
pub fn expiry_commit(loaded: &Value, snapshots: &[i64]) -> Result<CatalogCommit> {
    if snapshots.is_empty() {
        return Err(ShapeError::Sink(
            "internal error: an expiry commit expires no snapshot".into(),
        ));
    }
    let uuid = loaded["table-uuid"]
        .as_str()
        .ok_or_else(|| ShapeError::Sink("the catalog's table has no table-uuid".into()))?;
    let base = main_snapshot(loaded);
    let mut requirements = vec![
        json!({ "type": "assert-table-uuid", "uuid": uuid }),
        json!({ "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": base }),
    ];
    if let Some(refs) = loaded["refs"].as_object() {
        let mut others: Vec<(&String, &Value)> =
            refs.iter().filter(|(name, _)| *name != "main").collect();
        others.sort_by(|a, b| a.0.cmp(b.0));
        for (name, r) in others {
            requirements.push(json!({
                "type": "assert-ref-snapshot-id",
                "ref": name,
                "snapshot-id": r["snapshot-id"],
            }));
        }
    }
    // A statistics entry names one snapshot: those of the expired snapshots go with them,
    // as a table directory's expiry drops them.
    let mut updates = vec![json!({ "action": "remove-snapshots", "snapshot-ids": snapshots })];
    for (list, action) in [
        ("statistics", "remove-statistics"),
        ("partition-statistics", "remove-partition-statistics"),
    ] {
        for entry in loaded[list].as_array().into_iter().flatten() {
            if let Some(id) = entry["snapshot-id"]
                .as_i64()
                .filter(|id| snapshots.contains(id))
            {
                updates.push(json!({ "action": action, "snapshot-id": id }));
            }
        }
    }
    Ok(CatalogCommit {
        body: json!({
            "requirements": requirements,
            "updates": updates,
        }),
        effect: CommitEffect::RemovesSnapshots(snapshots.to_vec()),
        base,
    })
}

/// Why an append built on `loaded` cannot be rebuilt onto `now`, the same table loaded again
/// after another writer committed to it; `None` when it can.
///
/// An append's data files stand on their own: another writer's snapshot does not change
/// what they hold, so after a refusal only the manifest list (and the snapshot pointing at
/// it) need to be built again, on the new head. That holds while the files still fit the
/// table: the same table, at the same location, under the schema and partition spec they
/// were written for. A change to any of those, and the files themselves would be wrong.
pub fn rebase_blocked(loaded: &Value, now: &Value) -> Option<String> {
    let same = |key: &str| loaded[key] == now[key];
    if !same("table-uuid") {
        Some("the table was dropped and created again".into())
    } else if !same("location") {
        Some("the table moved to another location".into())
    } else if !same("current-schema-id") || !same("last-column-id") {
        Some("its schema changed".into())
    } else if !same("default-spec-id") || !same("partition-specs") {
        Some("its partitioning changed".into())
    } else {
        None
    }
}

/// What the table, loaded again, says about a commit whose response never came back (a
/// 5xx, a timeout, a dropped connection: the catalog may or may not have applied it).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reloaded {
    /// The snapshot is in the table, or `main` descends from it: the commit landed (perhaps
    /// with others after it).
    Committed,
    /// `main` moved, and its history reaches the old head without passing this snapshot.
    /// The commit asserted the old head, so it was refused, and can no longer be applied.
    Refused,
    /// Nothing can be concluded, or deleted: `main` is still where it was and the snapshot
    /// is not in the table (the request may still be on its way through the catalog), or
    /// the history between `main` and the old head is no longer all there.
    Unknown,
}

/// Judge a commit of `ours` onto `base` from the table's metadata as loaded after it.
///
/// A snapshot missing from `snapshots` is not proof that its commit failed: an expiry may
/// have removed it since, while later snapshots still read its files. So when `main` has
/// moved, its history is followed back (parent by parent, through the snapshots still
/// there): meeting `ours` first means the commit landed under later ones; meeting `base`
/// first means `main` moved past the old head on another line, so the commit was refused.
/// A link that is gone (expired) before either is met, or a history that ends without
/// meeting the old head (a rollback below it), decides nothing.
pub fn judge_reload(meta: &Value, ours: i64, base: Option<i64>) -> Reloaded {
    let snapshots: std::collections::HashMap<i64, &Value> = meta["snapshots"]
        .as_array()
        .map(|s| {
            s.iter()
                .filter_map(|s| Some((s["snapshot-id"].as_i64()?, s)))
                .collect()
        })
        .unwrap_or_default();
    if snapshots.contains_key(&ours) {
        return Reloaded::Committed;
    }
    let head = main_snapshot(meta);
    if head == base {
        return Reloaded::Unknown;
    }
    let mut at = head;
    // Each step goes one snapshot further back; more steps than snapshots is a loop.
    for _ in 0..=snapshots.len() {
        let Some(id) = at else {
            // The start of the table's history, without meeting the old head: there was
            // none (the commit asserted an empty table), or `main` was rolled back below it.
            return if base.is_none() {
                Reloaded::Refused
            } else {
                Reloaded::Unknown
            };
        };
        if id == ours {
            return Reloaded::Committed;
        }
        if Some(id) == base {
            return Reloaded::Refused;
        }
        let Some(snapshot) = snapshots.get(&id) else {
            return Reloaded::Unknown;
        };
        at = snapshot["parent-snapshot-id"].as_i64();
    }
    Reloaded::Unknown
}

/// Judge an expiry of `removed` from the table's metadata as loaded after its answer was
/// lost. A snapshot id is never reused, so one still in the table means the expiry has not
/// been applied: if a ref it asserted (`asserted`) has moved since, it never can be
/// (refused); if none has, it may still be (unknown). None left means it was applied, or
/// another expiry removed the same snapshots: either way the files only they read are
/// read by nothing now.
pub fn judge_removal(
    meta: &Value,
    removed: &[i64],
    asserted: &[(String, Option<i64>)],
) -> Reloaded {
    let present: std::collections::HashSet<i64> = meta["snapshots"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s["snapshot-id"].as_i64())
        .collect();
    if !removed.iter().any(|id| present.contains(id)) {
        return Reloaded::Committed;
    }
    if asserted
        .iter()
        .any(|(name, id)| ref_snapshot(meta, name) != *id)
    {
        Reloaded::Refused
    } else {
        Reloaded::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_needs_a_namespace_and_every_level_named() {
        let t = TableIdent::parse("analytics.raw.events").unwrap();
        assert_eq!(t.namespace, vec!["analytics", "raw"]);
        assert_eq!(t.name, "events");
        assert_eq!(t.to_string(), "analytics.raw.events");
        for bad in ["events", "", "a..b", ".events", "analytics."] {
            assert!(TableIdent::parse(bad).is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn main_is_read_from_refs_and_minus_one_is_none() {
        let with_refs = json!({ "refs": { "main": { "snapshot-id": 7, "type": "branch" } },
                                "current-snapshot-id": 7 });
        assert_eq!(main_snapshot(&with_refs), Some(7));
        assert_eq!(
            main_snapshot(&json!({ "refs": {}, "current-snapshot-id": -1 })),
            None
        );
        assert_eq!(main_snapshot(&json!({ "current-snapshot-id": 9 })), Some(9));
        assert_eq!(main_snapshot(&json!({ "current-snapshot-id": null })), None);
    }

    #[test]
    fn an_append_rebases_onto_another_writers_snapshot_but_not_onto_a_changed_table() {
        let loaded = json!({
            "table-uuid": "u", "location": "s3://b/t", "current-schema-id": 0,
            "last-column-id": 2, "default-spec-id": 0,
            "partition-specs": [{ "spec-id": 0, "fields": [] }],
            "refs": { "main": { "snapshot-id": 1 } }, "snapshots": [{ "snapshot-id": 1 }],
        });
        // Another writer's snapshot on main, and anything else that does not touch the
        // files' fit (properties, history): rebase.
        let mut now = loaded.clone();
        now["refs"]["main"]["snapshot-id"] = json!(2);
        now["snapshots"] = json!([{ "snapshot-id": 1 }, { "snapshot-id": 2 }]);
        now["properties"] = json!({ "owner": "someone" });
        assert_eq!(rebase_blocked(&loaded, &now), None);
        for (key, value, why) in [
            ("table-uuid", json!("v"), "dropped and created again"),
            ("location", json!("s3://b/u"), "another location"),
            ("current-schema-id", json!(1), "schema changed"),
            ("last-column-id", json!(3), "schema changed"),
            ("default-spec-id", json!(1), "partitioning changed"),
            ("partition-specs", json!([]), "partitioning changed"),
        ] {
            let mut changed = now.clone();
            changed[key] = value;
            let reason = rebase_blocked(&loaded, &changed).unwrap_or_default();
            assert!(reason.contains(why), "{key}: {reason:?}");
        }
    }

    #[test]
    fn a_lost_response_is_judged_by_what_the_table_holds() {
        // `(snapshot, parent)` pairs: the snapshots still in the table.
        let table = |main: Option<i64>, snaps: &[(i64, Option<i64>)]| {
            json!({
                "refs": match main { Some(id) => json!({ "main": { "snapshot-id": id } }),
                                     None => json!({}) },
                "snapshots": snaps.iter()
                    .map(|(id, parent)| match parent {
                        Some(p) => json!({ "snapshot-id": id, "parent-snapshot-id": p }),
                        None => json!({ "snapshot-id": id }),
                    })
                    .collect::<Vec<_>>(),
            })
        };
        let judge =
            |main, snaps: &[(i64, Option<i64>)], base| judge_reload(&table(main, snaps), 2, base);
        // Ours is the head, or under a head another writer added after it: committed.
        assert_eq!(
            judge(Some(2), &[(1, None), (2, Some(1))], Some(1)),
            Reloaded::Committed
        );
        assert_eq!(
            judge(Some(3), &[(1, None), (2, Some(1)), (3, Some(2))], Some(1)),
            Reloaded::Committed
        );
        // Ours was expired since, but a later snapshot still on main descends from it: its
        // rows are committed (and its files read), however the table looks now.
        assert_eq!(
            judge(Some(3), &[(3, Some(2))], Some(1)),
            Reloaded::Committed
        );
        // Another writer moved main from the old head, and ours is nowhere: refused, and
        // can never apply.
        assert_eq!(
            judge(Some(3), &[(1, None), (3, Some(1))], Some(1)),
            Reloaded::Refused
        );
        assert_eq!(
            judge(Some(4), &[(3, Some(1)), (4, Some(3))], Some(1)),
            Reloaded::Refused,
            "the old head itself may have been expired: its id still links"
        );
        assert_eq!(judge(Some(3), &[(3, None)], None), Reloaded::Refused);
        // Main has not moved and ours is not there: it may still land.
        assert_eq!(judge(Some(1), &[(1, None)], Some(1)), Reloaded::Unknown);
        assert_eq!(judge(None, &[], None), Reloaded::Unknown);
        // The history between main and the old head is gone (ours may be in the gap), or
        // main was rolled back below the old head: nothing can be told.
        assert_eq!(judge(Some(4), &[(4, Some(3))], Some(1)), Reloaded::Unknown);
        assert_eq!(
            judge(Some(0), &[(0, None), (1, Some(0))], Some(1)),
            Reloaded::Unknown
        );
        // A history that loops (a malformed table) ends undecided rather than spinning.
        assert_eq!(
            judge(Some(5), &[(5, Some(6)), (6, Some(5))], Some(1)),
            Reloaded::Unknown
        );
    }

    #[test]
    fn an_expiry_removes_its_snapshots_and_holds_every_ref_where_it_was() {
        let loaded = json!({
            "table-uuid": "u",
            "refs": { "main": { "snapshot-id": 5, "type": "branch" },
                      "v1": { "snapshot-id": 2, "type": "tag" },
                      "audit": { "snapshot-id": 4, "type": "branch" } },
            "snapshots": [{ "snapshot-id": 1 }, { "snapshot-id": 2 }, { "snapshot-id": 3 },
                          { "snapshot-id": 4 }, { "snapshot-id": 5 }],
        });
        let change = expiry_commit(&loaded, &[1, 3]).unwrap();
        assert_eq!(
            change.body,
            json!({
                "requirements": [
                    { "type": "assert-table-uuid", "uuid": "u" },
                    { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 5 },
                    { "type": "assert-ref-snapshot-id", "ref": "audit", "snapshot-id": 4 },
                    { "type": "assert-ref-snapshot-id", "ref": "v1", "snapshot-id": 2 },
                ],
                "updates": [{ "action": "remove-snapshots", "snapshot-ids": [1, 3] }],
            })
        );
        assert_eq!(change.base, Some(5));
        assert_eq!(change.snapshot_id(), None);
        assert_eq!(change.effect.to_string(), "the expiry of snapshot(s) 1, 3");
        assert!(expiry_commit(&loaded, &[]).is_err());

        // Judged by what the table holds: none of them left is committed, whatever moved.
        let mut now = loaded.clone();
        now["snapshots"] = json!([{ "snapshot-id": 2 }, { "snapshot-id": 4 },
                                  { "snapshot-id": 5 }, { "snapshot-id": 6 }]);
        now["refs"]["main"]["snapshot-id"] = json!(6);
        assert_eq!(change.judge(&now), Reloaded::Committed);
        // Still there, nothing moved: it may yet land.
        assert_eq!(change.judge(&loaded), Reloaded::Unknown);
        // Still there, and `main` or another ref it asserted moved: it never can.
        let mut moved = loaded.clone();
        moved["refs"]["main"]["snapshot-id"] = json!(6);
        assert_eq!(change.judge(&moved), Reloaded::Refused);
        let mut reset = loaded.clone();
        reset["refs"]["audit"]["snapshot-id"] = json!(1);
        assert_eq!(change.judge(&reset), Reloaded::Refused);
        let mut dropped = loaded.clone();
        dropped["refs"].as_object_mut().unwrap().remove("v1");
        assert_eq!(change.judge(&dropped), Reloaded::Refused);
        // A ref created since is not one it asserted.
        let mut tagged = loaded.clone();
        tagged["refs"]["new"] = json!({ "snapshot-id": 5, "type": "tag" });
        assert_eq!(change.judge(&tagged), Reloaded::Unknown);
        // Only some of them gone: another expiry, not this one.
        let mut partly = moved.clone();
        partly["snapshots"].as_array_mut().unwrap().remove(0);
        assert_eq!(change.judge(&partly), Reloaded::Refused);
        // The table dropped and created again: it holds none of them, and that does not
        // mean they were expired.
        let recreated = json!({ "table-uuid": "v", "refs": loaded["refs"], "snapshots": [] });
        assert_eq!(change.judge(&recreated), Reloaded::Refused);
    }

    #[test]
    fn a_snapshot_commit_is_judged_as_before() {
        let change = CatalogCommit {
            body: json!({}),
            effect: CommitEffect::AddsSnapshot(2),
            base: Some(1),
        };
        assert_eq!(change.snapshot_id(), Some(2));
        assert_eq!(change.effect.to_string(), "snapshot 2");
        let landed = json!({ "refs": { "main": { "snapshot-id": 2 } },
                             "snapshots": [{ "snapshot-id": 1 },
                                           { "snapshot-id": 2, "parent-snapshot-id": 1 }] });
        assert_eq!(change.judge(&landed), Reloaded::Committed);
        // A commit built against a table that has since been dropped and created again is
        // refused by the uuid it asserts, even if the new table's history happens to match.
        let asserting = CatalogCommit {
            body: json!({ "requirements": [{ "type": "assert-table-uuid", "uuid": "u" }] }),
            ..change
        };
        let mut other = landed.clone();
        other["table-uuid"] = json!("v");
        assert_eq!(asserting.judge(&other), Reloaded::Refused);
        other["table-uuid"] = json!("u");
        assert_eq!(asserting.judge(&other), Reloaded::Committed);
    }
}
