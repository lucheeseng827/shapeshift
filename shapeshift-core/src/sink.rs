//! The sink boundary. A sink consumes Arrow RecordBatches (one per row group) and
//! finalizes into one or more files. Parquet and Iceberg live in sibling crates and
//! are driven exclusively through this trait, so the shaping engine never depends on
//! a physical format.

use std::path::PathBuf;

use arrow_array::RecordBatch;

use crate::error::Result;

/// What a finished sink wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkSummary {
    /// Files produced (data files, and for Iceberg the metadata/manifest files).
    pub files: Vec<PathBuf>,
    /// Total rows written.
    pub rows: u64,
    /// Total on-disk bytes of the data files.
    pub bytes: u64,
    /// What a merge (`merge_on`) did, for a sink that merged; `None` for a plain write.
    pub merge: Option<MergeSummary>,
    /// For a table sink: the table version this write committed (`v{N}.metadata.json`).
    pub table_version: Option<u64>,
    /// For a table sink: the snapshot this write committed.
    pub snapshot_id: Option<i64>,
    /// For a table sink that commits through a catalog: how many times the write was built
    /// again onto a newer snapshot another writer committed first (0 = committed as built).
    pub rebases: u32,
    /// For a table sink given a run id: an earlier attempt of the same run had already
    /// committed `snapshot_id`, so this one committed nothing and removed what it wrote.
    pub earlier_attempt: bool,
}

/// What one merge (upsert on a key) did to the table. Every count is exact: the rows a
/// merge replaces are found by reading the existing data, never estimated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeSummary {
    /// The key columns rows were matched on.
    pub key_columns: Vec<String>,
    /// Distinct non-null keys this write carried.
    pub distinct_keys: u64,
    /// Distinct keys that already had a live row in the table (an update).
    pub keys_updated: u64,
    /// Distinct keys the table had not seen (an insert).
    pub keys_inserted: u64,
    /// Live rows already in the table that this write replaced (deleted). Exceeds
    /// `keys_updated` when the table held duplicates of a key — all are replaced.
    pub prior_rows_replaced: u64,
    /// Earlier occurrences of a key this same write carried again — the last occurrence
    /// wins and the earlier ones are deleted.
    pub duplicates_in_input: u64,
    /// Rows with a null in any key column. Appended, but they match nothing — a null key
    /// equals no other key, as in SQL `MERGE`.
    pub null_key_rows: u64,
    /// Existing data files read to find matching keys.
    pub data_files_scanned: u64,
    /// Existing data files skipped because their key bounds could not hold a match.
    pub data_files_pruned: u64,
    /// Position-delete files written.
    pub delete_files: u64,
    /// Rows deleted: `prior_rows_replaced + rows_deleted + duplicates_in_input`, and the
    /// rows of stale changes this write had written.
    pub position_deletes: u64,
    /// A change feed's keys whose latest change was a delete that applied.
    pub keys_deleted: u64,
    /// Live rows already in the table that a delete removed.
    pub rows_deleted: u64,
    /// Keys whose latest change was not newer than the key's live row (`changes.order_by`),
    /// so the table kept its row: as new or newer, including a change replayed.
    pub changes_stale: u64,
    /// The columns changes were ordered by (`changes.order_by`); empty: arrival order.
    pub order_by: Vec<String>,
    /// Tombstones (with `order_by`): deletes this write remembered; tombstones it retired
    /// because a newer change re-created the key; those it trimmed past the retention; and
    /// how many the table remembers after it.
    pub tombstones_written: u64,
    pub tombstones_retired: u64,
    pub tombstones_trimmed: u64,
    pub tombstones_live: u64,
}

/// A columnar output target.
pub trait Sink {
    /// Write one RecordBatch (a row group's worth of rows). The batch schema must
    /// match the schema the sink was constructed with.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    /// Flush footers / write table metadata and return what was produced. Must be
    /// called exactly once; writing after finish is a logic error.
    fn finish(&mut self) -> Result<SinkSummary>;

    /// Record a pulled source's position in this write's commit, as the snapshot summary
    /// key `shapeshift.source.<kind>`: the data and the position land together, or neither
    /// does. Called before [`Sink::finish`]. Only a table records one; a file cannot.
    fn set_source_position(&mut self, kind: &str, _position: &serde_json::Value) -> Result<()> {
        Err(crate::ShapeError::Spec(format!(
            "an incremental `{kind}` source records its position in the table it writes: \
             it needs `output.format: iceberg`"
        )))
    }
}
