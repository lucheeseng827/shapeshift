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
}

/// A columnar output target.
pub trait Sink {
    /// Write one RecordBatch (a row group's worth of rows). The batch schema must
    /// match the schema the sink was constructed with.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    /// Flush footers / write table metadata and return what was produced. Must be
    /// called exactly once; writing after finish is a logic error.
    fn finish(&mut self) -> Result<SinkSummary>;
}
