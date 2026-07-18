//! Logical column types and their mapping onto Arrow (and, downstream, Parquet /
//! Iceberg) physical types.

use serde::{Deserialize, Serialize};

use arrow_schema::{DataType, TimeUnit};

/// The closed set of logical types a column can take. Kept deliberately small —
/// the scalar spine plus two temporal types and a JSON escape hatch — because that
/// is what a JSON→columnar shaper actually needs, and every one maps cleanly to
/// both Parquet and the Iceberg type system.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum ColumnType {
    Bool,
    Int64,
    Float64,
    String,
    /// Days since the Unix epoch (Arrow Date32 → Parquet DATE → Iceberg `date`).
    Date,
    /// Microseconds since the Unix epoch, no zone (Arrow Timestamp(µs) → Parquet
    /// TIMESTAMP → Iceberg `timestamp`).
    Timestamp,
    /// Any nested value, preserved as a JSON-encoded UTF-8 string. The lossless
    /// escape hatch for arrays / heterogeneous objects.
    Json,
}

impl ColumnType {
    /// The Arrow physical type this logical type is written as.
    pub fn arrow_type(self) -> DataType {
        match self {
            ColumnType::Bool => DataType::Boolean,
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::String | ColumnType::Json => DataType::Utf8,
            ColumnType::Date => DataType::Date32,
            ColumnType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }

    /// The Iceberg primitive-type name for the table metadata schema.
    pub fn iceberg_type(self) -> &'static str {
        match self {
            ColumnType::Bool => "boolean",
            ColumnType::Int64 => "long",
            ColumnType::Float64 => "double",
            ColumnType::String | ColumnType::Json => "string",
            ColumnType::Date => "date",
            ColumnType::Timestamp => "timestamp",
        }
    }
}
