//! Per-column statistics for a data file — min/max bounds and null counts, computed
//! while the data is written and encoded into the Iceberg manifest so a reader can skip
//! whole files by predicate (`WHERE id > 100` never opens a file whose max id is 50).
//!
//! **Correctness bar:** a lower bound must be ≤ the true minimum and an upper bound ≥ the
//! true maximum, or a reader would wrongly prune a file and drop rows. Bounds are the
//! exact values (no truncation), serialized in Iceberg's little-endian single-value form.

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};

/// Running min/max for one column. `Empty` means "no non-null value seen yet", so no
/// bound is emitted (a reader then can't prune on it — the safe default).
enum Bounds {
    Empty,
    Bool(bool, bool),
    // Int64 and timestamp(µs) share the 8-byte little-endian `long` encoding.
    Long(i64, i64),
    Double(f64, f64),
    Date(i32, i32),
    Str(String, String),
}

impl Bounds {
    fn merge_bool(&mut self, v: bool) {
        match self {
            Bounds::Empty => *self = Bounds::Bool(v, v),
            Bounds::Bool(lo, hi) => {
                *lo = *lo && v;
                *hi = *hi || v;
            }
            _ => {}
        }
    }
    fn merge_long(&mut self, v: i64) {
        match self {
            Bounds::Empty => *self = Bounds::Long(v, v),
            Bounds::Long(lo, hi) => {
                if v < *lo {
                    *lo = v;
                }
                if v > *hi {
                    *hi = v;
                }
            }
            _ => {}
        }
    }
    fn merge_double(&mut self, v: f64) {
        match self {
            Bounds::Empty => *self = Bounds::Double(v, v),
            Bounds::Double(lo, hi) => {
                if v < *lo {
                    *lo = v;
                }
                if v > *hi {
                    *hi = v;
                }
            }
            _ => {}
        }
    }
    fn merge_date(&mut self, v: i32) {
        match self {
            Bounds::Empty => *self = Bounds::Date(v, v),
            Bounds::Date(lo, hi) => {
                if v < *lo {
                    *lo = v;
                }
                if v > *hi {
                    *hi = v;
                }
            }
            _ => {}
        }
    }
    fn merge_str(&mut self, v: &str) {
        match self {
            Bounds::Empty => *self = Bounds::Str(v.to_string(), v.to_string()),
            Bounds::Str(lo, hi) => {
                if v < lo.as_str() {
                    *lo = v.to_string();
                }
                if v > hi.as_str() {
                    *hi = v.to_string();
                }
            }
            _ => {}
        }
    }

    /// `(lower, upper)` as Iceberg single-value binary, or `None` if all-null.
    fn to_bytes(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Bounds::Empty => None,
            Bounds::Bool(lo, hi) => Some((vec![*lo as u8], vec![*hi as u8])),
            Bounds::Long(lo, hi) => Some((lo.to_le_bytes().to_vec(), hi.to_le_bytes().to_vec())),
            Bounds::Double(lo, hi) => Some((lo.to_le_bytes().to_vec(), hi.to_le_bytes().to_vec())),
            Bounds::Date(lo, hi) => Some((lo.to_le_bytes().to_vec(), hi.to_le_bytes().to_vec())),
            Bounds::Str(lo, hi) => Some((lo.as_bytes().to_vec(), hi.as_bytes().to_vec())),
        }
    }
}

struct ColumnStats {
    field_id: i32,
    value_count: i64,
    null_count: i64,
    bounds: Bounds,
}

/// Accumulates per-column stats over all of a data file's batches.
pub struct StatsCollector {
    cols: Vec<ColumnStats>,
}

impl StatsCollector {
    /// One accumulator per column, keyed by the columns' Iceberg field-ids (`1..=N`).
    pub fn new(field_ids: &[i32]) -> Self {
        StatsCollector {
            cols: field_ids
                .iter()
                .map(|&field_id| ColumnStats {
                    field_id,
                    value_count: 0,
                    null_count: 0,
                    bounds: Bounds::Empty,
                })
                .collect(),
        }
    }

    /// Fold one batch into the running stats.
    pub fn update(&mut self, batch: &RecordBatch) {
        for (i, col) in self.cols.iter_mut().enumerate() {
            let arr = batch.column(i);
            col.value_count += arr.len() as i64;
            col.null_count += arr.null_count() as i64;
            update_bounds(&mut col.bounds, arr.as_ref());
        }
    }

    /// The stats for the manifest's `data_file` record.
    pub fn finish(&self) -> Stats {
        let mut stats = Stats::default();
        for c in &self.cols {
            stats.value_counts.push((c.field_id, c.value_count));
            stats.null_counts.push((c.field_id, c.null_count));
            if let Some((lo, hi)) = c.bounds.to_bytes() {
                stats.lower_bounds.push((c.field_id, lo));
                stats.upper_bounds.push((c.field_id, hi));
            }
        }
        stats
    }
}

/// Per-column stats for one data file, ready to encode into the manifest.
#[derive(Default)]
pub struct Stats {
    pub value_counts: Vec<(i32, i64)>,
    pub null_counts: Vec<(i32, i64)>,
    pub lower_bounds: Vec<(i32, Vec<u8>)>,
    pub upper_bounds: Vec<(i32, Vec<u8>)>,
}

fn update_bounds(b: &mut Bounds, arr: &dyn Array) {
    macro_rules! fold {
        ($ty:ty, $merge:ident) => {{
            let a = arr.as_any().downcast_ref::<$ty>().unwrap();
            for i in 0..a.len() {
                if a.is_valid(i) {
                    b.$merge(a.value(i));
                }
            }
        }};
    }
    match arr.data_type() {
        DataType::Boolean => fold!(BooleanArray, merge_bool),
        DataType::Int64 => fold!(Int64Array, merge_long),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            fold!(TimestampMicrosecondArray, merge_long)
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..a.len() {
                // NaN has no meaningful ordering — leave it out of the bounds (Iceberg
                // tracks it separately via nan_value_counts, which we don't emit).
                if a.is_valid(i) && !a.value(i).is_nan() {
                    b.merge_double(a.value(i));
                }
            }
        }
        DataType::Date32 => fold!(Date32Array, merge_date),
        DataType::Utf8 => fold!(StringArray, merge_str),
        _ => {} // unknown type → no bounds (a reader simply can't prune on it)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn collects_bounds_and_null_counts() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amt", DataType::Float64, true),
            Field::new("tag", DataType::Utf8, true),
        ]));
        // id out of order (min/max must not assume sorted); one null in amt and in tag.
        let id = Int64Array::from(vec![Some(3), Some(1), Some(2)]);
        let amt = Float64Array::from(vec![Some(1.5), None, Some(3.5)]);
        let tag = StringArray::from(vec![Some("b"), Some("a"), None]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(id), Arc::new(amt), Arc::new(tag)]).unwrap();

        let mut c = StatsCollector::new(&[1, 2, 3]);
        c.update(&batch);
        let s = c.finish();

        assert_eq!(s.value_counts, vec![(1, 3), (2, 3), (3, 3)]);
        assert_eq!(s.null_counts, vec![(1, 0), (2, 1), (3, 1)]);

        let lb: HashMap<_, _> = s.lower_bounds.into_iter().collect();
        let ub: HashMap<_, _> = s.upper_bounds.into_iter().collect();
        // id: [1, 3] as little-endian i64.
        assert_eq!(lb[&1], 1i64.to_le_bytes().to_vec());
        assert_eq!(ub[&1], 3i64.to_le_bytes().to_vec());
        // amt: [1.5, 3.5] as little-endian f64 (the null is ignored).
        assert_eq!(lb[&2], 1.5f64.to_le_bytes().to_vec());
        assert_eq!(ub[&2], 3.5f64.to_le_bytes().to_vec());
        // tag: ["a", "b"] as UTF-8 (lexicographic).
        assert_eq!(lb[&3], b"a".to_vec());
        assert_eq!(ub[&3], b"b".to_vec());
    }

    #[test]
    fn all_null_column_emits_no_bound() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let x = Int64Array::from(vec![Option::<i64>::None, None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(x)]).unwrap();
        let mut c = StatsCollector::new(&[1]);
        c.update(&batch);
        let s = c.finish();
        assert_eq!(s.null_counts, vec![(1, 2)]);
        assert!(s.lower_bounds.is_empty(), "no min for an all-null column");
        assert!(s.upper_bounds.is_empty());
    }

    #[test]
    fn bounds_accumulate_across_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut c = StatsCollector::new(&[1]);
        for chunk in [vec![5, 8], vec![2, 9, 3]] {
            let arr = Int64Array::from(chunk.iter().map(|&v| Some(v)).collect::<Vec<_>>());
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
            c.update(&batch);
        }
        let s = c.finish();
        assert_eq!(s.value_counts, vec![(1, 5)]);
        assert_eq!(s.lower_bounds[0].1, 2i64.to_le_bytes().to_vec());
        assert_eq!(s.upper_bounds[0].1, 9i64.to_le_bytes().to_vec());
    }
}
