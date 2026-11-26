//! Where a table's recorded paths point once the table has moved.
//!
//! A table records absolute paths: its `location`, every manifest list, manifest, data file
//! and delete file. When the table is moved or copied, those paths still name the old place.
//! A write after the move records its new files under the new place, so one table can hold
//! paths written against several directories. Each directory a table has lived at is an
//! **anchor**. A recorded path under an anchor is one of the table's own files, found at the
//! same key under wherever the table is now. Every reader resolves paths by this one rule
//! ([`Anchors::key`]), so a table moved twice reads the same as one moved once.

use serde_json::Value;

use shapeshift_core::{Result, ShapeError};

/// The directories a table's recorded paths were written against, longest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Anchors(Vec<String>);

impl Anchors {
    /// The anchors `metadata` names: its `location`, and the directory above `metadata/` of
    /// every snapshot's manifest list and of every `metadata-log` entry. A write into a
    /// moved table puts its manifest list, and its record of the metadata file it
    /// superseded, under the table's directory at the time, so these name every place the
    /// table has been written at. A file under the table's own `write.metadata.path` is not
    /// one: that directory is where an engine was told to keep metadata, wherever the table
    /// is, so its files are read where they are recorded.
    pub fn of(metadata: &Value) -> Self {
        let configured = metadata["properties"]["write.metadata.path"]
            .as_str()
            .map(|p| p.trim_end_matches('/'))
            .filter(|p| !p.is_empty());
        let placed = |file: &&str| {
            !configured.is_some_and(|dir| {
                file.strip_prefix(dir)
                    .is_some_and(|rest| rest.starts_with('/'))
            })
        };
        let lists = metadata["snapshots"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| s["manifest-list"].as_str());
        let logged = metadata["metadata-log"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|e| e["metadata-file"].as_str());
        let roots = metadata["location"]
            .as_str()
            .into_iter()
            .chain(lists.chain(logged).filter(placed).filter_map(anchor_of));
        Self::from_roots(roots)
    }

    /// `location` alone: for a catalog's table, whose recorded paths are where its files
    /// are, so a path is read under `location` or at what it says.
    pub fn at(location: &str) -> Self {
        Self::from_roots([location])
    }

    fn from_roots<'a>(roots: impl IntoIterator<Item = &'a str>) -> Self {
        let mut roots: Vec<String> = roots
            .into_iter()
            .map(|r| r.trim_end_matches('/'))
            .filter(|r| !r.is_empty())
            .map(str::to_string)
            .collect();
        // Longest first, so a path is cut at the deepest anchor it is under.
        roots.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        roots.dedup();
        Anchors(roots)
    }

    /// The directories, longest first.
    pub fn roots(&self) -> &[String] {
        &self.0
    }

    /// The key within the table of the recorded `path` (`data/k=x/f.parquet`): the path with
    /// the longest anchor it is under cut off. An anchor counts only on a path-component
    /// boundary, so the anchor `/w/orders` holds `/w/orders/data/f` but not
    /// `/w/orders-archive/data/f`. `None` for a path under no anchor: a file outside the
    /// table, read where it says. A path under an anchor whose key has a `.` or `..`
    /// segment is refused, since it could lead out of the table.
    pub fn key<'p>(&self, path: &'p str) -> Result<Option<&'p str>> {
        for root in &self.0 {
            let Some(key) = path
                .strip_prefix(root.as_str())
                .and_then(|rest| rest.strip_prefix('/'))
            else {
                continue;
            };
            let key = key.trim_start_matches('/');
            if key.is_empty() || key.split('/').any(|seg| seg == "." || seg == "..") {
                return Err(ShapeError::Sink(format!(
                    "the table records the path {path}, which does not name a file inside the \
                     table (under {root})"
                )));
            }
            return Ok(Some(key));
        }
        Ok(None)
    }

    /// Whether `path` names a file inside the table: under an anchor, with a key
    /// [`key`](Self::key) accepts.
    pub fn holds(&self, path: &str) -> bool {
        matches!(self.key(path), Ok(Some(_)))
    }
}

/// The directory a metadata file was written under: `<dir>` for `<dir>/metadata/<file>`.
/// `None` for a file not directly in a `metadata/` directory (an engine's own
/// `write.metadata.path`), which names no anchor.
fn anchor_of(file: &str) -> Option<&str> {
    let (dir, _) = file.rsplit_once('/')?;
    let root = dir.strip_suffix("/metadata")?;
    (!root.is_empty() && !root.ends_with('/') && !root.ends_with(':')).then_some(root)
}

/// Whether a table found at `here` is somewhere other than the `location` its metadata
/// records: it was moved or copied. A local `file:` URI and a plain path name the same
/// place, a trailing `/` is ignored, and two local paths that resolve to the same directory
/// (through a symlink, such as macOS's `/tmp`) are the same place. An empty `location` is
/// taken to be here.
pub fn is_elsewhere(location: &str, here: &str) -> bool {
    fn plain(s: &str) -> String {
        let s = same_store(s.trim_end_matches('/'));
        let s = s
            .strip_prefix("file://")
            .or_else(|| s.strip_prefix("file:"))
            .unwrap_or(&s);
        s.to_string()
    }
    let (there, here) = (plain(location), plain(here));
    if location.is_empty() || there == here {
        return false;
    }
    let resolved = |p: &str| {
        p.starts_with('/')
            .then(|| std::fs::canonicalize(p).ok())
            .flatten()
    };
    !matches!((resolved(&there), resolved(&here)), (Some(a), Some(b)) if a == b)
}

/// A URI with Hadoop's names for S3 (`s3a://`, `s3n://`, which Spark writes) spelled
/// `s3://`: the same objects, through another client.
pub fn same_store(uri: &str) -> std::borrow::Cow<'_, str> {
    match uri
        .strip_prefix("s3a://")
        .or_else(|| uri.strip_prefix("s3n://"))
    {
        Some(rest) => std::borrow::Cow::Owned(format!("s3://{rest}")),
        None => std::borrow::Cow::Borrowed(uri),
    }
}

/// Warn that a table found at `here` is not where its metadata says. shapeshift reads and
/// writes it there regardless; other engines' default readers look for its files at the
/// recorded `location`. (A write into a moved table warns rather than being refused:
/// RELOCATE-PLAN decision 1.)
pub fn warn_if_elsewhere(metadata: &Value, here: &str) {
    let location = metadata["location"].as_str().unwrap_or("");
    if is_elsewhere(location, here) {
        tracing::warn!(
            %location,
            %here,
            "the table is not where its metadata says (moved or copied): shapeshift reads and \
             writes it where it is, but other engines' default readers look for its files at \
             the recorded location"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A table written at `/w/a`, moved to `/w/b` and written to there.
    fn moved_once() -> Value {
        json!({
            "location": "/w/a",
            "snapshots": [
                { "snapshot-id": 1, "manifest-list": "/w/a/metadata/snap-1.avro" },
                { "snapshot-id": 2, "manifest-list": "/w/b/metadata/snap-2.avro" },
            ],
            "metadata-log": [{ "metadata-file": "/w/b/metadata/v1.metadata.json" }],
        })
    }

    #[test]
    fn every_place_the_table_was_written_at_is_an_anchor() {
        assert_eq!(Anchors::of(&moved_once()).roots(), ["/w/a", "/w/b"]);
        // A manifest list outside a `metadata/` directory names no anchor.
        let m = json!({
            "location": "s3://lake/t/",
            "snapshots": [{ "manifest-list": "s3://meta/elsewhere/snap-1.avro" }],
        });
        assert_eq!(Anchors::of(&m).roots(), ["s3://lake/t"]);
        assert_eq!(Anchors::of(&json!({})).roots(), [] as [String; 0]);
    }

    #[test]
    fn a_configured_metadata_path_is_not_a_place_the_table_was_written_at() {
        // An engine told to keep metadata in /meta/t/metadata: its lists there are read where
        // they are recorded, not taken for a place the table lived at.
        let m = json!({
            "location": "/w/t",
            "properties": { "write.metadata.path": "/meta/t/metadata/" },
            "snapshots": [
                { "manifest-list": "/meta/t/metadata/snap-1.avro" },
                { "manifest-list": "/w/old/metadata/snap-2.avro" },
            ],
            "metadata-log": [{ "metadata-file": "/meta/t/metadata/v1.metadata.json" }],
        });
        let a = Anchors::of(&m);
        assert_eq!(a.roots(), ["/w/old", "/w/t"]);
        assert_eq!(a.key("/meta/t/metadata/snap-1.avro").unwrap(), None);
        // A sibling of the configured directory is no part of it.
        let m = json!({
            "location": "/w/t",
            "properties": { "write.metadata.path": "/meta/t/meta" },
            "snapshots": [{ "manifest-list": "/meta/t/metadata/snap-1.avro" }],
        });
        assert_eq!(Anchors::of(&m).roots(), ["/meta/t", "/w/t"]);
    }

    #[test]
    fn a_path_is_cut_at_the_longest_anchor_on_a_component_boundary() {
        let a = Anchors::of(&moved_once());
        assert_eq!(
            a.key("/w/a/data/f.parquet").unwrap(),
            Some("data/f.parquet")
        );
        assert_eq!(
            a.key("/w/b/metadata/m0.avro").unwrap(),
            Some("metadata/m0.avro")
        );
        // A sibling sharing the anchor's name as a prefix is not under it.
        assert_eq!(a.key("/w/a-archive/data/f.parquet").unwrap(), None);
        assert_eq!(a.key("/w/b2/data/f.parquet").unwrap(), None);
        // Outside every anchor: read where it says.
        assert_eq!(a.key("/elsewhere/f.parquet").unwrap(), None);
        // The deepest anchor wins.
        let nested = Anchors::at("/w");
        let both = Anchors::from_roots(["/w", "/w/t"]);
        assert_eq!(nested.key("/w/t/data/f").unwrap(), Some("t/data/f"));
        assert_eq!(both.key("/w/t/data/f").unwrap(), Some("data/f"));
        // An extra slash after the anchor is tolerated.
        assert_eq!(a.key("/w/a//data/f").unwrap(), Some("data/f"));
    }

    #[test]
    fn a_key_that_could_leave_the_table_is_refused() {
        let a = Anchors::at("s3://lake/t");
        for bad in [
            "s3://lake/t/data/../../other/x.parquet",
            "s3://lake/t/./data/a.parquet",
            "s3://lake/t/data/..",
            "s3://lake/t/",
        ] {
            assert!(a.key(bad).is_err(), "{bad}");
            assert!(!a.holds(bad), "{bad}");
        }
        assert!(a.holds("s3://lake/t/data/a.parquet"));
        assert!(!a.holds("s3://lake/t2/data/a.parquet"));
        assert!(!Anchors::default().holds("anything"));
    }

    #[test]
    fn a_table_is_elsewhere_only_when_its_location_names_another_place() {
        assert!(!is_elsewhere("/w/t", "/w/t"));
        assert!(!is_elsewhere("/w/t/", "/w/t"));
        assert!(!is_elsewhere("file:///w/t", "/w/t"));
        assert!(!is_elsewhere("file:/w/t", "/w/t"));
        assert!(!is_elsewhere("s3://lake/t", "s3://lake/t/"));
        assert!(!is_elsewhere("", "/w/t"));
        assert!(is_elsewhere("/w/a", "/w/b"));
        assert!(is_elsewhere("s3://old/t", "s3://new/t"));
        // Spark's names for S3 are S3.
        assert!(!is_elsewhere("s3a://lake/t", "s3://lake/t"));
        assert!(!is_elsewhere("s3n://lake/t/", "s3://lake/t"));
        assert!(is_elsewhere("s3a://lake/t", "s3://other/t"));
        assert_eq!(same_store("s3a://b/k"), "s3://b/k");
        assert_eq!(same_store("gs://b/k"), "gs://b/k");
        // A path through a symlink names the directory it resolves to.
        let dir = std::env::temp_dir().join(format!("ss-anchor-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("real")).unwrap();
        let real = dir.join("real").canonicalize().unwrap();
        let other = dir.join("other");
        std::fs::create_dir_all(&other).unwrap();
        #[cfg(unix)]
        {
            let link = dir.join("link");
            std::os::unix::fs::symlink(&real, &link).unwrap();
            let (link, real) = (link.to_str().unwrap(), real.to_str().unwrap());
            assert!(!is_elsewhere(link, real));
            assert!(!is_elsewhere(&format!("file://{link}"), real));
        }
        // A copy whose original is still there is elsewhere.
        assert!(is_elsewhere(
            other.to_str().unwrap(),
            real.to_str().unwrap()
        ));
        std::fs::remove_dir_all(&dir).ok();
    }
}
