//! Relocation (`relocate_table`): a moved or copied table, relocated, records only where it
//! is, reads the same rows as a table that never moved, and keeps being written to. A copy
//! is refused until it is named one, and gets its own identity; an incomplete copy is
//! refused before anything is written. DuckDB reading relocated tables with no flag, at
//! every snapshot, is checked in `examples/merge/verify_duckdb.py`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow_array::{Array, Int64Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{json, Value};

use shapeshift_core::{ColumnSpec, ColumnType, Compression, ShapeError, Shaper, Sink};
use shapeshift_iceberg::{
    current_scan_files, inspect, read_manifest_entries, read_manifest_list, relocate_table,
    IcebergSink, Identity, RelocateOptions,
};

fn tmp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ss-relocate-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d.canonicalize().unwrap()
}

fn row(id: i64, region: &str, v: i64) -> Value {
    json!({ "id": id, "region": region, "v": v })
}

/// Merge `rows` on `id` into the table at `dir`, partitioned by `region`.
fn merge(dir: &Path, rows: &[Value]) {
    let col = |name: &str, ty| ColumnSpec {
        name: name.into(),
        from: None,
        ty,
        transform: None,
        required: false,
    };
    let mut sh = Shaper::new(
        vec![
            col("id", ColumnType::Int64),
            col("region", ColumnType::String),
            col("v", ColumnType::Int64),
        ],
        false,
    )
    .unwrap();
    for r in rows {
        sh.push(r).unwrap();
    }
    let mut sink = IcebergSink::create_merge(
        dir,
        sh.schema(),
        Compression::Snappy,
        &["region".to_string()],
        &["id".to_string()],
    )
    .unwrap();
    sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
    sink.finish().unwrap();
}

fn rounds() -> Vec<Vec<Value>> {
    vec![
        vec![row(1, "eu", 1), row(2, "us", 2), row(3, "eu", 3)],
        // id 1 moves partition; id 2 is updated.
        vec![row(1, "us", 10), row(2, "us", 20), row(4, "ap", 40)],
        vec![row(2, "eu", 22), row(3, "eu", 33), row(5, "us", 50)],
        vec![row(4, "ap", 44), row(6, "eu", 6)],
        vec![row(5, "us", 55), row(7, "ap", 7)],
    ]
}

/// The table's live rows as `(id, region, v)`, sorted: data files minus their deletes.
fn live(dir: &Path) -> Vec<(i64, String, i64)> {
    let mut out = Vec::new();
    for f in current_scan_files(dir).unwrap() {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&f.path).unwrap())
                .unwrap()
                .build()
                .unwrap();
        let mut pos = 0u64;
        for b in reader {
            let b = b.unwrap();
            let get = |name: &str| b.column(b.schema().index_of(name).unwrap()).clone();
            let (id, region, v) = (get("id"), get("region"), get("v"));
            let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
            let region = region.as_any().downcast_ref::<StringArray>().unwrap();
            let v = v.as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                if f.deleted_positions.binary_search(&pos).is_err() {
                    out.push((id.value(i), region.value(i).to_string(), v.value(i)));
                }
                pos += 1;
            }
        }
    }
    out.sort();
    out
}

fn metadata(dir: &Path) -> Value {
    let hint = std::fs::read_to_string(dir.join("metadata/version-hint.text")).unwrap();
    let text =
        std::fs::read_to_string(dir.join(format!("metadata/v{}.metadata.json", hint.trim())))
            .unwrap();
    serde_json::from_str(&text).unwrap()
}

/// Every path the table's current metadata records, down to the rows of its position delete
/// files, read where they are recorded.
fn recorded(dir: &Path) -> Vec<String> {
    let meta = metadata(dir);
    let mut out = vec![meta["location"].as_str().unwrap().to_string()];
    for e in meta["metadata-log"].as_array().unwrap() {
        out.push(e["metadata-file"].as_str().unwrap().to_string());
    }
    for s in meta["snapshots"].as_array().unwrap() {
        let list = s["manifest-list"].as_str().unwrap();
        out.push(list.to_string());
        for m in read_manifest_list(&std::fs::read(list).unwrap()).unwrap() {
            out.push(m.path.clone());
            for e in read_manifest_entries(&std::fs::read(&m.path).unwrap()).unwrap() {
                out.push(e.file_path.clone());
                if e.content == 1 && e.is_live() {
                    let file = std::fs::File::open(&e.file_path).unwrap();
                    for b in ParquetRecordBatchReaderBuilder::try_new(file)
                        .unwrap()
                        .build()
                        .unwrap()
                    {
                        let b = b.unwrap();
                        let paths = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                        out.extend((0..b.num_rows()).map(|i| paths.value(i).to_string()));
                    }
                }
            }
        }
    }
    out
}

/// Every file under `dir`, with its bytes.
fn on_disk(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(at: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for e in std::fs::read_dir(at).unwrap().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else {
                out.insert(p.clone(), std::fs::read(&p).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, &mut out);
    out
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let (p, dest) = (e.path(), to.join(e.file_name()));
        if p.is_dir() {
            copy_dir(&p, &dest);
        } else {
            std::fs::copy(&p, &dest).unwrap();
        }
    }
}

fn options(identity: Option<Identity>, dry_run: bool) -> RelocateOptions {
    RelocateOptions { identity, dry_run }
}

/// A table moved, merged into and moved again holds paths written at two places, neither
/// of them where it is. Relocated, every path it records (the metadata's, every snapshot's
/// manifest list and manifests, every file, every position delete's row) names where it is;
/// it reads the rows the same merges leave in a table that never moved, keeps its identity,
/// takes more merges, and a second relocation does nothing.
#[test]
fn a_moved_table_relocated_records_only_where_it_is() {
    let root = tmp_dir("moved");
    let (a, b, c, still) = (
        root.join("a"),
        root.join("b"),
        root.join("c"),
        root.join("still"),
    );
    let rounds = rounds();
    for r in &rounds[..3] {
        merge(&a, r);
        merge(&still, r);
    }
    std::fs::rename(&a, &b).unwrap();
    merge(&b, &rounds[3]);
    merge(&still, &rounds[3]);
    std::fs::rename(&b, &c).unwrap();
    let uuid = metadata(&c)["table-uuid"].clone();

    // A dry run counts what a real run does, and changes nothing.
    let before = on_disk(&c);
    let dry = relocate_table(&c, &options(None, true)).unwrap();
    assert!(!dry.committed && dry.version.is_none());
    assert_eq!(on_disk(&c), before, "a dry run changes nothing");

    let s = relocate_table(&c, &options(None, false)).unwrap();
    assert!(s.committed, "{s:?}");
    assert!(!s.new_uuid && s.undecided.is_none(), "{s:?}");
    assert_eq!(s.to, c.to_str().unwrap());
    assert!(s.from.ends_with("/a"), "{s:?}");
    assert_eq!(s.snapshots, 4);
    assert!(
        s.delete_files_rewritten > 0 && s.manifests_rewritten > 0,
        "{s:?}"
    );
    assert_eq!(s.manifest_lists_rewritten, 4, "{s:?}");
    assert_eq!(s.files_outside_table, 0);
    assert_eq!(s.superseded_deleted, s.superseded);
    assert_eq!(s.superseded_not_deleted, 0);
    for counted in [
        dry.manifest_lists_rewritten,
        dry.manifests_rewritten,
        dry.delete_files_rewritten,
        dry.superseded,
    ]
    .into_iter()
    .zip([
        s.manifest_lists_rewritten,
        s.manifests_rewritten,
        s.delete_files_rewritten,
        s.superseded,
    ]) {
        assert_eq!(counted.0, counted.1, "the dry run counted what the run did");
    }

    let here = format!("{}/", c.display());
    let paths = recorded(&c);
    let stray: Vec<&String> = paths
        .iter()
        .skip(1)
        .filter(|p| !p.starts_with(&here))
        .collect();
    assert!(stray.is_empty(), "paths not under {here}: {stray:?}");
    assert_eq!(paths[0], c.to_str().unwrap());
    assert_eq!(live(&c), live(&still));
    assert_eq!(
        metadata(&c)["table-uuid"],
        uuid,
        "a move keeps its identity"
    );
    let info = inspect(&c).unwrap();
    assert!(!info.moved, "{info:?}");

    // Running it again does nothing.
    let version = metadata(&c)["last-updated-ms"].clone();
    let again = relocate_table(&c, &options(None, false)).unwrap();
    assert!(again.already_anchored && !again.committed, "{again:?}");
    assert_eq!(metadata(&c)["last-updated-ms"], version);

    // And it takes more merges.
    merge(&c, &rounds[4]);
    merge(&still, &rounds[4]);
    assert_eq!(live(&c), live(&still));
    std::fs::remove_dir_all(&root).ok();
}

/// A copy whose original still answers is refused until it is named a copy or a move. As a
/// copy it gets a new `table-uuid`; as a move it keeps its own. The original is never
/// touched, and both read the same rows.
#[test]
fn a_copy_is_refused_until_named_and_then_gets_its_own_identity() {
    let root = tmp_dir("copy");
    let original = root.join("original");
    for r in &rounds()[..3] {
        merge(&original, r);
    }
    let before = on_disk(&original);
    let uuid = metadata(&original)["table-uuid"].clone();

    let copy = root.join("copy");
    copy_dir(&original, &copy);
    let copied = on_disk(&copy);
    let e = relocate_table(&copy, &options(None, false))
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("still answers at") && e.contains("--moved") && e.contains("--copy"),
        "{e}"
    );
    assert_eq!(
        on_disk(&copy),
        copied,
        "a refused relocation writes nothing"
    );
    // A dry run goes ahead, and says what a real run needs.
    let dry = relocate_table(&copy, &options(None, true)).unwrap();
    assert!(dry.undecided.as_deref().unwrap().contains("still answers"));

    let s = relocate_table(&copy, &options(Some(Identity::Copy), false)).unwrap();
    assert!(s.committed && s.new_uuid, "{s:?}");
    assert_ne!(metadata(&copy)["table-uuid"], uuid);
    assert_eq!(
        s.table_uuid,
        metadata(&copy)["table-uuid"].as_str().unwrap()
    );
    let here = format!("{}/", copy.display());
    assert!(recorded(&copy).iter().skip(1).all(|p| p.starts_with(&here)));
    assert_eq!(live(&copy), live(&original));

    // Asked again, a relocated copy has nothing to do, and keeps the identity it was given.
    let given = metadata(&copy)["table-uuid"].clone();
    let again = relocate_table(&copy, &options(Some(Identity::Copy), false)).unwrap();
    assert!(again.already_anchored && !again.committed, "{again:?}");
    assert!(
        !again.new_uuid && again.table_uuid == given.as_str().unwrap(),
        "{again:?}"
    );
    assert_eq!(metadata(&copy)["table-uuid"], given);

    let moved = root.join("moved");
    copy_dir(&original, &moved);
    let s = relocate_table(&moved, &options(Some(Identity::Moved), false)).unwrap();
    assert!(s.committed && !s.new_uuid, "{s:?}");
    assert_eq!(metadata(&moved)["table-uuid"], uuid);

    assert_eq!(on_disk(&original), before, "the original is untouched");
    std::fs::remove_dir_all(&root).ok();
}

/// A copy missing a file a snapshot reads is refused, naming the file, before anything is
/// written.
#[test]
fn an_incomplete_copy_is_refused_before_anything_is_written() {
    let root = tmp_dir("incomplete");
    let (a, b) = (root.join("a"), root.join("b"));
    for r in &rounds()[..2] {
        merge(&a, r);
    }
    std::fs::rename(&a, &b).unwrap();
    let data = std::fs::read_dir(b.join("data/region=eu"))
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| !p.to_string_lossy().contains("delete"))
        .unwrap();
    std::fs::remove_file(&data).unwrap();
    let before = on_disk(&b);
    let e = relocate_table(&b, &options(None, false))
        .unwrap_err()
        .to_string();
    let name = data.file_name().unwrap().to_string_lossy();
    assert!(
        e.contains("is incomplete")
            && e.contains(name.as_ref())
            && e.contains("nothing was written"),
        "{e}"
    );
    assert_eq!(on_disk(&b), before);
    std::fs::remove_dir_all(&root).ok();
}

/// The listing a relocation checks files against follows a symlinked directory (a table's
/// data on another disk), and ends on a link loop instead of following it forever.
#[cfg(unix)]
#[test]
fn a_symlinked_directory_is_listed_and_a_link_loop_ends() {
    let root = tmp_dir("symlinks");
    let (a, b, elsewhere) = (root.join("a"), root.join("b"), root.join("disk2"));
    for r in &rounds()[..2] {
        merge(&a, r);
    }
    std::fs::rename(&a, &b).unwrap();
    // The data on another disk, linked in; and two links back up the tree, which a walk
    // that followed them would branch on at every level (2^40 paths before the kernel's
    // own link limit stopped it).
    std::fs::rename(b.join("data"), &elsewhere).unwrap();
    std::os::unix::fs::symlink(&elsewhere, b.join("data")).unwrap();
    std::os::unix::fs::symlink(&b, b.join("metadata/loop")).unwrap();
    std::os::unix::fs::symlink(&b, b.join("loop")).unwrap();
    let s = relocate_table(&b, &options(None, false)).unwrap();
    assert!(s.committed, "{s:?}");
    let reloc = metadata(&b)["location"].as_str().unwrap().to_string();
    assert_eq!(reloc, b.to_str().unwrap());
    assert_eq!(live(&b).len(), 4);
    std::fs::remove_dir_all(&root).ok();
}

/// A write that loaded the table before a relocation committed loses the commit guard to it
/// and fails as a conflict (exit 75), leaving nothing behind: a table at a path has no
/// rebase, so a writer never republishes the manifests and delete files the relocation
/// superseded. Run again, the write goes into the relocated table. Both an append and a
/// merge (which also reads the superseded delete files) are checked.
#[test]
fn a_write_that_loaded_the_table_before_a_relocation_is_a_conflict() {
    let root = tmp_dir("race");
    for merging in [false, true] {
        let (a, b) = (
            root.join(format!("a{merging}")),
            root.join(format!("b{merging}")),
        );
        for r in &rounds()[..2] {
            merge(&a, r);
        }
        std::fs::rename(&a, &b).unwrap();
        let col = |name: &str, ty| ColumnSpec {
            name: name.into(),
            from: None,
            ty,
            transform: None,
            required: false,
        };
        let open = || {
            let mut sh = Shaper::new(
                vec![
                    col("id", ColumnType::Int64),
                    col("region", ColumnType::String),
                    col("v", ColumnType::Int64),
                ],
                false,
            )
            .unwrap();
            for r in &rounds()[2] {
                sh.push(r).unwrap();
            }
            let parts = ["region".to_string()];
            let mut sink = if merging {
                IcebergSink::create_merge(
                    &b,
                    sh.schema(),
                    Compression::Snappy,
                    &parts,
                    &["id".to_string()],
                )
            } else {
                IcebergSink::create(&b, sh.schema(), Compression::Snappy, true, &parts)
            }
            .unwrap();
            sink.write_batch(&sh.flush().unwrap().unwrap()).unwrap();
            sink
        };
        // The writer loads the moved table and stages its files; the relocation commits.
        let mut sink = open();
        let s = relocate_table(&b, &options(None, false)).unwrap();
        assert!(s.committed && s.superseded_deleted > 0, "{s:?}");
        let relocated = metadata(&b);
        let rows = live(&b);
        let e = sink.finish().unwrap_err();
        assert!(
            matches!(e, ShapeError::Conflict(_)),
            "merging {merging}: {e}"
        );
        assert_eq!(metadata(&b), relocated, "the relocation's version stands");
        let here = format!("{}/", b.display());
        let stray: Vec<String> = recorded(&b)
            .into_iter()
            .skip(1)
            .filter(|p| !p.starts_with(&here) || !Path::new(p).exists())
            .collect();
        assert!(
            stray.is_empty(),
            "named but not where the table is: {stray:?}"
        );
        assert_eq!(live(&b), rows);
        // Nothing of the refused write is left: every file under the table is one the
        // relocated table names, or an older metadata version.
        let named: std::collections::BTreeSet<PathBuf> =
            recorded(&b).iter().skip(1).map(PathBuf::from).collect();
        let left: Vec<PathBuf> = on_disk(&b)
            .into_keys()
            .filter(|p| {
                !named.contains(p)
                    && !p.to_string_lossy().ends_with(".metadata.json")
                    && !p.ends_with("metadata/version-hint.text")
            })
            .collect();
        assert!(left.is_empty(), "merging {merging}: left behind {left:?}");
        // Run again, it lands.
        open().finish().unwrap();
        assert!(live(&b).len() > rows.len() - 1);
        assert_eq!(live(&b).iter().filter(|r| r.0 == 5).count(), 1);
    }
    std::fs::remove_dir_all(&root).ok();
}
