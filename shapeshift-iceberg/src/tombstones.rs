//! Tombstones: the keys a change feed deleted, and the version that deleted each.
//!
//! A delete removes a key's row, and with it the version a late change would be compared
//! with. With `changes.order_by`, each applied delete is also remembered here, so a change
//! for that key that is not newer than its tombstone is stale, not a resurrection. A newer
//! change re-creates the row and retires the tombstone; one older than the retention is
//! trimmed (a change that arrives later than that for a deleted key brings it back: a
//! stated limit).
//!
//! **Where they live.** A [Puffin](https://iceberg.apache.org/puffin-spec/) file under
//! `metadata/`, holding one blob of type [`BLOB_TYPE`] whose bytes are a small Parquet file
//! (`key`, `version`, `deleted_at_ms`). Each snapshot names it twice: in its summary
//! ([`crate::metadata::TOMBSTONES_KEY`], a table-relative key, so a moved table needs no
//! rewrite) and in the table's `statistics` list, which is what keeps the file reachable
//! for other engines' orphan cleanup and for `relocate`. A reader that does not know the
//! blob type skips it, as the Puffin spec requires.
//!
//! The key and version are stored in this crate's own byte encodings (the merge key's, and
//! [`encode_version`]'s), with the key columns and the order recorded beside them: a merge
//! keyed or ordered differently refuses the file rather than misread it.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value};

use shapeshift_core::{Result, ShapeError};

/// The Puffin blob type a tombstone file holds.
pub const BLOB_TYPE: &str = "shapeshift-tombstones-v1";

const MAGIC: &[u8; 4] = b"PFA1";

fn err(e: impl std::fmt::Display) -> ShapeError {
    ShapeError::Sink(e.to_string())
}

/// One remembered delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    /// The version (`changes.order_by`) of the delete, in [`encode_version`]'s bytes.
    pub version: Vec<u8>,
    /// When the delete was applied, for the retention.
    pub deleted_at_ms: i64,
}

/// The tombstones of a table, by key (the merge key's encoding).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TombstoneSet {
    /// The merge key's columns, and the order, the set was written for.
    pub key_columns: Vec<String>,
    pub order_by: Vec<String>,
    pub by_key: HashMap<Vec<u8>, Tombstone>,
}

/// A version's parts, as bytes that sort the way the version compares, so two encoded
/// versions compare as plain byte strings: each part is a tag (`0` an integer, `1` bytes)
/// then the value (an integer as 8 big-endian bytes with the sign bit flipped; bytes with
/// each `0x00` written `0x00 0xFF`, then `0x00 0x00`, so a shorter string that is a prefix
/// sorts first and `b` sorts after `aa`).
pub fn encode_version(parts: &[VersionValue<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        match p {
            VersionValue::Int(i) => {
                out.push(0);
                out.extend_from_slice(&((*i as u64) ^ (1 << 63)).to_be_bytes());
            }
            VersionValue::Bytes(b) => {
                out.push(1);
                for &c in *b {
                    out.push(c);
                    if c == 0 {
                        out.push(0xFF);
                    }
                }
                out.extend_from_slice(&[0, 0]);
            }
        }
    }
    out
}

/// The inverse of [`encode_version`] (tests read versions back with it).
#[cfg(test)]
pub fn decode_version(mut b: &[u8]) -> Option<Vec<VersionOwned>> {
    let mut out = Vec::new();
    while let Some((&tag, rest)) = b.split_first() {
        match tag {
            0 => {
                let v: [u8; 8] = rest.get(..8)?.try_into().ok()?;
                out.push(VersionOwned::Int(
                    (u64::from_be_bytes(v) ^ (1 << 63)) as i64,
                ));
                b = &rest[8..];
            }
            1 => {
                let mut v = Vec::new();
                let mut i = 0;
                loop {
                    match (rest.get(i)?, rest.get(i + 1)) {
                        (0, Some(0)) => break,
                        (0, Some(0xFF)) => v.push(0),
                        (0, _) => return None,
                        (&c, _) => {
                            v.push(c);
                            i += 1;
                            continue;
                        }
                    }
                    i += 2;
                }
                out.push(VersionOwned::Bytes(v));
                b = &rest[i + 2..];
            }
            _ => return None,
        }
    }
    Some(out)
}

/// One part of a version, borrowed, for [`encode_version`].
#[derive(Debug, Clone, Copy)]
pub enum VersionValue<'a> {
    Int(i64),
    Bytes(&'a [u8]),
}

/// One part of a version, owned, from [`decode_version`].
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum VersionOwned {
    Int(i64),
    Bytes(Vec<u8>),
}

/// A tombstone file as written: its bytes, and what a snapshot records about it.
pub struct TombstoneFile {
    pub bytes: Vec<u8>,
    pub footer_size: u64,
    pub count: u64,
}

impl TombstoneSet {
    /// Write the set as a Puffin file (one blob, a Parquet file of the tombstones).
    pub fn to_puffin(&self, snapshot_id: i64, sequence_number: i64) -> Result<TombstoneFile> {
        let mut keys: Vec<(&Vec<u8>, &Tombstone)> = self.by_key.iter().collect();
        keys.sort_by(|a, b| a.0.cmp(b.0));
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("version", DataType::Binary, false),
            Field::new("deleted_at_ms", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BinaryArray::from_iter_values(
                    keys.iter().map(|(k, _)| k.as_slice()),
                )),
                Arc::new(BinaryArray::from_iter_values(
                    keys.iter().map(|(_, t)| t.version.as_slice()),
                )),
                Arc::new(Int64Array::from_iter_values(
                    keys.iter().map(|(_, t)| t.deleted_at_ms),
                )),
            ],
        )
        .map_err(err)?;
        let mut blob = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut blob, schema, None).map_err(err)?;
            w.write(&batch).map_err(err)?;
            w.close().map_err(err)?;
        }
        let payload = json!({
            "blobs": [{
                "type": BLOB_TYPE,
                "fields": [],
                "snapshot-id": snapshot_id,
                "sequence-number": sequence_number,
                "offset": MAGIC.len(),
                "length": blob.len(),
                "properties": {
                    "key-columns": json!(self.key_columns).to_string(),
                    "order-by": json!(self.order_by).to_string(),
                    "count": self.by_key.len().to_string(),
                },
            }],
            "properties": { "created-by": concat!("shapeshift ", env!("CARGO_PKG_VERSION")) },
        })
        .to_string()
        .into_bytes();
        let mut out = Vec::with_capacity(blob.len() + payload.len() + 24);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&blob);
        let footer_start = out.len();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&payload);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // flags: nothing compressed
        out.extend_from_slice(MAGIC);
        let footer_size = (out.len() - footer_start) as u64;
        Ok(TombstoneFile {
            bytes: out,
            footer_size,
            count: self.by_key.len() as u64,
        })
    }

    /// Read a Puffin tombstone file. `path` names it in errors. Anything this build cannot
    /// read with certainty (not Puffin, a compressed footer, no [`BLOB_TYPE`] blob) is an
    /// error: a merge must not go on without the tombstones it was told exist.
    pub fn from_puffin(path: &str, bytes: &[u8]) -> Result<Self> {
        let bad = |why: &str| err(format!("the tombstone file {path} cannot be read: {why}"));
        let n = bytes.len();
        if n < 4 + 16 || &bytes[..4] != MAGIC || &bytes[n - 4..] != MAGIC {
            return Err(bad("it is not a Puffin file"));
        }
        let flags = u32::from_le_bytes(bytes[n - 8..n - 4].try_into().unwrap());
        if flags & 1 != 0 {
            return Err(bad(
                "its footer is compressed, which this build does not read",
            ));
        }
        let size = u32::from_le_bytes(bytes[n - 12..n - 8].try_into().unwrap()) as usize;
        let start = n
            .checked_sub(12 + size + 4)
            .filter(|&s| s >= 4)
            .ok_or_else(|| bad("its footer is larger than the file"))?;
        if &bytes[start..start + 4] != MAGIC {
            return Err(bad("its footer does not start where its size says"));
        }
        let payload: Value = serde_json::from_slice(&bytes[start + 4..start + 4 + size])
            .map_err(|e| bad(&format!("its footer is not JSON ({e})")))?;
        let blob = payload["blobs"]
            .as_array()
            .and_then(|b| b.iter().find(|b| b["type"] == BLOB_TYPE))
            .ok_or_else(|| bad(&format!("it holds no `{BLOB_TYPE}` blob")))?;
        if blob["compression-codec"]
            .as_str()
            .is_some_and(|c| !c.is_empty())
        {
            return Err(bad(
                "its blob is compressed, which this build does not read",
            ));
        }
        let (off, len) = (
            blob["offset"]
                .as_u64()
                .ok_or_else(|| bad("the blob has no offset"))? as usize,
            blob["length"]
                .as_u64()
                .ok_or_else(|| bad("the blob has no length"))? as usize,
        );
        let data = bytes
            .get(
                off..off
                    .checked_add(len)
                    .ok_or_else(|| bad("the blob overflows"))?,
            )
            .ok_or_else(|| bad("the blob lies outside the file"))?;
        let list = |key: &str| -> Result<Vec<String>> {
            let s = blob["properties"][key]
                .as_str()
                .ok_or_else(|| bad(&format!("the blob does not say its `{key}`")))?;
            serde_json::from_str(s).map_err(|_| bad(&format!("its `{key}` is not a list")))
        };
        let mut set = TombstoneSet {
            key_columns: list("key-columns")?,
            order_by: list("order-by")?,
            by_key: HashMap::new(),
        };
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(data))
            .map_err(|e| bad(&e.to_string()))?
            .build()
            .map_err(|e| bad(&e.to_string()))?;
        for batch in reader {
            let batch = batch.map_err(|e| bad(&e.to_string()))?;
            if batch.num_columns() != 3 {
                return Err(bad(&format!(
                    "its blob has {} columns, not the 3 a tombstone set has",
                    batch.num_columns()
                )));
            }
            let col = |i: usize| batch.column(i).clone();
            let (k, v, d) = (col(0), col(1), col(2));
            let keys = k
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| bad("its `key` is not binary"))?;
            let versions = v
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| bad("its `version` is not binary"))?;
            let at = d
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| bad("its `deleted_at_ms` is not a long"))?;
            for i in 0..batch.num_rows() {
                if keys.is_null(i) || versions.is_null(i) || at.is_null(i) {
                    return Err(bad("a tombstone has a null field"));
                }
                set.by_key.insert(
                    keys.value(i).to_vec(),
                    Tombstone {
                        version: versions.value(i).to_vec(),
                        deleted_at_ms: at.value(i),
                    },
                );
            }
        }
        Ok(set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_encode_in_their_order() {
        let enc = |p: &[VersionValue]| encode_version(p);
        let mut vs = vec![
            enc(&[VersionValue::Int(-5)]),
            enc(&[VersionValue::Int(i64::MIN)]),
            enc(&[VersionValue::Int(7), VersionValue::Bytes(b"b")]),
            enc(&[VersionValue::Int(7), VersionValue::Bytes(b"a")]),
            enc(&[VersionValue::Int(7), VersionValue::Bytes(b"aa")]),
            enc(&[VersionValue::Int(7), VersionValue::Bytes(b"a\0z")]),
            enc(&[VersionValue::Int(7), VersionValue::Bytes(b"")]),
            enc(&[VersionValue::Int(i64::MAX)]),
            enc(&[VersionValue::Int(0)]),
        ];
        let decoded: Vec<_> = vs.iter().map(|v| decode_version(v).unwrap()).collect();
        vs.sort();
        let mut by_value = decoded.clone();
        by_value.sort();
        let resorted: Vec<_> = vs.iter().map(|v| decode_version(v).unwrap()).collect();
        assert_eq!(resorted, by_value, "byte order is value order");
        assert_eq!(decode_version(&[9]), None);
    }

    #[test]
    fn a_set_round_trips_through_puffin_and_refuses_what_it_cannot_read() {
        let mut set = TombstoneSet {
            key_columns: vec!["id".into()],
            order_by: vec!["ts".into()],
            ..Default::default()
        };
        for i in 0..100u8 {
            set.by_key.insert(
                vec![i],
                Tombstone {
                    version: encode_version(&[VersionValue::Int(i as i64)]),
                    deleted_at_ms: 1000 + i as i64,
                },
            );
        }
        let f = set.to_puffin(42, 7).unwrap();
        assert_eq!(f.count, 100);
        assert_eq!(&f.bytes[..4], b"PFA1");
        let back = TombstoneSet::from_puffin("t.puffin", &f.bytes).unwrap();
        assert_eq!(back, set);
        // The footer's size is the bytes from the footer's magic to the end.
        let n = f.bytes.len();
        assert_eq!(&f.bytes[n - f.footer_size as usize..][..4], b"PFA1");
        for broken in [&f.bytes[..n - 1], &b"PFA1PFA1"[..], &f.bytes[1..]] {
            assert!(TombstoneSet::from_puffin("t.puffin", broken).is_err());
        }
        // An empty set is a valid file too (every tombstone retired).
        let empty = TombstoneSet {
            key_columns: vec!["id".into()],
            order_by: vec!["ts".into()],
            ..Default::default()
        };
        let f = empty.to_puffin(1, 1).unwrap();
        assert_eq!(TombstoneSet::from_puffin("e", &f.bytes).unwrap(), empty);
    }
}
