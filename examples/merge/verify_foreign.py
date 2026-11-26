#!/usr/bin/env python3
"""Check shapeshift writing into an Iceberg table another engine created (PyIceberg).

PyIceberg writes tables the way Iceberg's Java writers do: manifests and manifest lists
compressed with Avro `deflate`, manifest lists carrying fields shapeshift never writes
(`partitions` summaries, `key_metadata`), and zstd-compressed Parquet data files. This
script makes such a table, then has shapeshift append to it, merge into it, compact it
and expire it, and has PyIceberg write to it again in between, so the table's history
interleaves the two writers. After every step DuckDB and PyIceberg must both read exactly
what a plain-Python model of the rows says, and every summary must name the snapshot the
table now has.

It also checks that the partition summaries PyIceberg wrote for its manifests are still
there after shapeshift's commits (a reader prunes manifests by them), and that a format
version 1 table is refused rather than written with version 2 snapshots.

A table both engines wrote is then moved and relocated (`shapeshift relocate`): DuckDB and
PyIceberg must read it from its new metadata with no flag, PyIceberg's manifests must keep
their partition summaries through the rewrite, and both engines must write to it afterwards.

The table is exposed to shapeshift in the Hadoop layout (`metadata/v<N>.metadata.json`
and `version-hint.text`, as Spark's Hadoop catalog writes it), each time from the metadata
file PyIceberg wrote. Every file in it is PyIceberg's or shapeshift's own.

Usage (from the workspace root; merging and compacting read PyIceberg's zstd data files,
so the binary needs the `zstd` feature, and `file://` tables need `object_store`):

    cargo build -p shapeshift-cli --features object_store,zstd
    python3 -m venv .venv && .venv/bin/pip install -r examples/merge/requirements.txt
    .venv/bin/python examples/merge/verify_foreign.py --bin target/debug/shapeshift

Each scenario runs twice: on a local table path, and through a `file://` URL (the
object-store sink, which reads the table through ranged GETs). Exit status 0 means
every step matched.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import clear_manifest_cache, read_manifest_list
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "region", StringType(), required=False),
    NestedField(3, "v", LongType(), required=False),
)
BY_REGION = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="region")
)

SPEC = """\
dataset: t
schema: strict
output: {{ format: iceberg, path: {path} }}
columns:
  - {{ name: id, type: int64 }}
  - {{ name: region, type: string }}
  - {{ name: v, type: int64 }}
"""


class Mismatch(Exception):
    """A reader disagrees with the model, or a summary with the table."""


def arrow(rows):
    """`{id: (region, v)}` as the Arrow table PyIceberg appends."""
    ids = sorted(rows)
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "region": pa.array([rows[i][0] for i in ids], pa.string()),
        "v": pa.array([rows[i][1] for i in ids], pa.int64()),
    })


class Table:
    """One PyIceberg-made table, its model, and the commands that write it."""

    def __init__(self, binary, work, url, format_version=2):
        self.binary, self.work = binary, work
        self.catalog = SqlCatalog(
            "foreign", uri=f"sqlite:///{work}/catalog.db", warehouse=f"file://{work}/wh"
        )
        self.catalog.create_namespace("lake")
        self.table = self.catalog.create_table(
            "lake.t", schema=SCHEMA, partition_spec=BY_REGION,
            properties={"format-version": str(format_version)},
        )
        self.dir = f"{work}/wh/lake/t"
        self.target = f"file://{self.dir}" if url else self.dir
        self.model = {}
        self.version = 0
        self.spec = os.path.join(work, "t.spec.yaml")
        with open(self.spec, "w") as f:
            f.write(SPEC.format(path=self.target))

    # -- the two writers ------------------------------------------------------------

    def pyiceberg_append(self, rows):
        """PyIceberg appends `rows` onto the table as it now stands, then the table is
        exposed to shapeshift at its next version."""
        if self.version:
            # Take up where shapeshift left off: its newest metadata, registered anew.
            name = f"lake.t{self.version}"
            self.table = self.catalog.register_table(name, f"file://{self.metadata()}")
        self.table.append(arrow(rows))
        self.model.update(rows)
        self.expose(self.table.metadata_location.removeprefix("file://"))

    def expose(self, metadata_file):
        """Publish `metadata_file` as the next version in the Hadoop layout."""
        self.version += 1
        shutil.copy(metadata_file, self.metadata())
        with open(f"{self.dir}/metadata/version-hint.text", "w") as f:
            f.write(str(self.version))

    def metadata(self, version=None):
        return f"{self.dir}/metadata/v{version or self.version}.metadata.json"

    def move_to(self, new_dir):
        """Move the table's directory, as any tool would, and write to it there from now on."""
        os.makedirs(os.path.dirname(new_dir), exist_ok=True)
        os.rename(self.dir, new_dir)
        url = self.target.startswith("file://")
        self.dir = new_dir
        self.target = f"file://{new_dir}" if url else new_dir
        with open(self.spec, "w") as f:
            f.write(SPEC.format(path=self.target))

    def shapeshift(self, *args, ok=True):
        out = subprocess.run([self.binary, *args], capture_output=True, text=True)
        if ok and out.returncode != 0:
            sys.exit(f"shapeshift {' '.join(args)} failed:\n{out.stdout}{out.stderr}")
        if not ok:
            return out
        # shapeshift moved the table on: its hint names the version it committed.
        with open(f"{self.dir}/metadata/version-hint.text") as f:
            self.version = int(f.read().strip())
        doc = json.loads(out.stdout) if "--summary-json" in args else None
        return doc

    def shape(self, rows, *flags):
        path = os.path.join(self.work, "in.jsonl")
        with open(path, "w") as f:
            f.writelines(
                json.dumps({"id": i, "region": r, "v": v}) + "\n" for i, (r, v) in rows.items()
            )
        doc = self.shapeshift("shape", "--spec", self.spec, "-i", path,
                              "--partition-by", "region", *flags, "--summary-json")
        self.model.update(rows)
        return doc

    # -- the readers ----------------------------------------------------------------

    def check(self, name, when, snapshot=None):
        want = sorted((i, r, v) for i, (r, v) in self.model.items())
        con = duckdb.connect()
        con.sql("INSTALL iceberg; LOAD iceberg;")
        meta = self.metadata()
        by_duckdb = sorted(con.sql(f"select id, region, v from iceberg_scan('{meta}')").fetchall())
        # PyIceberg keeps the manifest records of every list it read, by manifest path, for
        # the whole process: without this it would read shapeshift's lists through what it
        # saw in its own, and miss anything shapeshift wrote wrong in them.
        clear_manifest_cache()
        table = StaticTable.from_metadata(f"file://{meta}")
        got = table.scan().to_arrow()
        by_pyiceberg = sorted(zip(got["id"].to_pylist(), got["region"].to_pylist(),
                                  got["v"].to_pylist()))
        for reader, rows in (("DuckDB", by_duckdb), ("PyIceberg", by_pyiceberg)):
            if rows != want:
                raise Mismatch(
                    f"[{name}] {when}: {reader} read {len(rows)} rows, the model has {len(want)}"
                    f"\n  missing: {sorted(set(want) - set(rows))[:5]}"
                    f"\n  extra:   {sorted(set(rows) - set(want))[:5]}"
                )
        current = table.metadata.current_snapshot_id
        if snapshot is not None and int(snapshot) != current:
            raise Mismatch(f"[{name}] {when}: the summary says snapshot {snapshot}, the table's "
                           f"is {current}")
        return table

    def pyiceberg_summaries_kept(self, name, when, table):
        """Every live manifest PyIceberg added keeps the partition summaries it wrote for
        it (a reader prunes manifests by them). PyIceberg's snapshots are the ones without
        `shapeshift.version` in their summary. Returns how many manifests were checked."""
        theirs = {s.snapshot_id for s in table.metadata.snapshots
                  if s.summary is None or s.summary.get("shapeshift.version") is None}
        # Read the list itself, past PyIceberg's cache of the records it wrote.
        listed = read_manifest_list(table.io.new_input(table.current_snapshot().manifest_list))
        manifests = [m for m in listed if m.added_snapshot_id in theirs]
        lost = [m.manifest_path for m in manifests if not m.partitions]
        if lost:
            raise Mismatch(f"[{name}] {when}: PyIceberg's partition summaries were dropped from "
                           f"{len(lost)} manifest(s), e.g. {lost[0]}")
        return len(manifests)


def interleaved(binary, root, url):
    name = f"interleaved writers ({'file:// URL' if url else 'local path'})"
    work = os.path.join(root, "url" if url else "path")
    os.makedirs(work)
    t = Table(binary, work, url)
    t.pyiceberg_append({1: ("eu", 1), 2: ("us", 2), 3: ("eu", 3)})
    t.pyiceberg_append({4: ("us", 4)})
    t.check(name, "as PyIceberg wrote it")

    doc = t.shape({5: ("eu", 5), 6: ("ap", 6)}, "--append")
    t.check(name, "shapeshift appended", doc["table"]["snapshot_id"])
    # Id 1 moves eu -> us, 4 is updated, 7 is new: the merge reads PyIceberg's zstd files.
    doc = t.shape({1: ("us", 10), 4: ("us", 40), 7: ("eu", 7)}, "--merge-on", "id")
    if doc["merge"]["keys_updated"] != 2:
        raise Mismatch(f"[{name}] the merge says {doc['merge']['keys_updated']} keys updated, not 2")
    table = t.check(name, "shapeshift merged", doc["table"]["snapshot_id"])
    kept = t.pyiceberg_summaries_kept(name, "after shapeshift's merge", table)
    if kept == 0:
        raise Mismatch(f"[{name}] no manifest of PyIceberg's is left to check")

    # PyIceberg writes on top of shapeshift's snapshots, and shapeshift merges after it.
    t.pyiceberg_append({8: ("ap", 8), 9: ("us", 9)})
    t.check(name, "PyIceberg appended onto shapeshift's snapshots")
    doc = t.shape({8: ("eu", 80), 2: ("us", 20)}, "--merge-on", "id")
    table = t.check(name, "shapeshift merged after PyIceberg", doc["table"]["snapshot_id"])
    t.pyiceberg_summaries_kept(name, "after the second merge", table)

    doc = t.shapeshift("compact", t.target, "--summary-json")
    if not doc["committed"]:
        raise Mismatch(f"[{name}] compaction found nothing to do")
    table = t.check(name, "shapeshift compacted", doc["snapshot_id"])
    live_deletes = [f for m in table.current_snapshot().manifests(table.io)
                    for f in m.fetch_manifest_entry(table.io) if f.data_file.content != 0]
    if live_deletes:
        raise Mismatch(f"[{name}] compaction left {len(live_deletes)} delete file(s) live")

    doc = t.shapeshift("expire", t.target, "--older-than", "0s", "--summary-json")
    if not doc["committed"] or doc["deleted"]["data_files"] == 0:
        raise Mismatch(f"[{name}] the expiry deleted no data file: {doc['deleted']}")
    t.check(name, "shapeshift expired")

    # And PyIceberg once more, onto the compacted, expired table.
    t.pyiceberg_append({10: ("eu", 10)})
    t.check(name, "PyIceberg appended after the maintenance")
    print(f"ok  {name}: {len(t.model)} rows, v{t.version}; DuckDB and PyIceberg read the model "
          f"after every step; {kept} PyIceberg manifest(s) kept their partition summaries")


def relocated(binary, root, url):
    """A table both engines wrote, moved and relocated: DuckDB and PyIceberg read it from its
    new metadata with no flag, PyIceberg's manifests keep the partition summaries it wrote
    (relocation transcodes them), and PyIceberg and shapeshift both write to it afterwards."""
    name = f"relocated ({'file:// URL' if url else 'local path'})"
    work = os.path.join(root, "relocated-url" if url else "relocated-path")
    os.makedirs(work)
    t = Table(binary, work, url)
    t.pyiceberg_append({1: ("eu", 1), 2: ("us", 2), 3: ("eu", 3)})
    t.pyiceberg_append({4: ("us", 4)})
    # A merge, so position delete files (shapeshift's) name PyIceberg's data files.
    t.shape({1: ("us", 10), 4: ("us", 40), 5: ("ap", 5)}, "--merge-on", "id")
    t.pyiceberg_append({6: ("eu", 6)})
    t.check(name, "before the move")

    t.move_to(os.path.join(work, "moved", "t"))
    doc = t.shapeshift("relocate", t.target, "--summary-json")
    if not doc["committed"] or doc["new_uuid"] or doc["rewritten"]["delete_files"] == 0:
        raise Mismatch(f"[{name}] relocation: {doc}")
    with open(t.metadata()) as f:
        meta = json.load(f)
    if not meta["location"].endswith("/moved/t") or not meta["location"].startswith("file://"):
        raise Mismatch(f"[{name}] the relocated location is {meta['location']}")
    table = t.check(name, "relocated")
    kept = t.pyiceberg_summaries_kept(name, "relocated", table)
    if kept == 0:
        raise Mismatch(f"[{name}] no manifest of PyIceberg's is left to check")

    t.pyiceberg_append({7: ("ap", 7)})
    t.check(name, "PyIceberg appended after the relocation")
    doc = t.shape({2: ("eu", 20), 8: ("us", 8)}, "--merge-on", "id")
    table = t.check(name, "shapeshift merged after the relocation", doc["table"]["snapshot_id"])
    t.pyiceberg_summaries_kept(name, "the last merge", table)
    print(f"ok  {name}: {len(t.model)} rows, v{t.version}; DuckDB and PyIceberg read it with no "
          f"flag, {kept} PyIceberg manifest(s) kept their partition summaries, both engines "
          f"wrote to it afterwards")


def version_one_refused(binary, root):
    name = "a format version 1 table"
    work = os.path.join(root, "v1")
    os.makedirs(work)
    t = Table(binary, work, url=False, format_version=1)
    t.pyiceberg_append({1: ("eu", 1)})
    before = sorted(os.listdir(f"{t.dir}/metadata"))
    path = os.path.join(work, "in.jsonl")
    with open(path, "w") as f:
        f.write(json.dumps({"id": 2, "region": "us", "v": 2}) + "\n")
    out = t.shapeshift("shape", "--spec", t.spec, "-i", path, "--partition-by", "region",
                       "--append", ok=False)
    if out.returncode != 1 or "format version 1" not in out.stderr:
        raise Mismatch(f"[{name}] was not refused as one:\n{out.stdout}{out.stderr}")
    if sorted(os.listdir(f"{t.dir}/metadata")) != before:
        raise Mismatch(f"[{name}] the refused write left files behind")
    print(f"ok  {name}: refused before anything was written")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", default="target/debug/shapeshift", help="the shapeshift binary")
    ap.add_argument("--keep", action="store_true", help="keep the scratch tables")
    a = ap.parse_args()
    binary = os.path.abspath(a.bin)
    root = tempfile.mkdtemp(prefix="shapeshift-foreign-")
    try:
        interleaved(binary, root, url=False)
        interleaved(binary, root, url=True)
        relocated(binary, root, url=False)
        relocated(binary, root, url=True)
        version_one_refused(binary, root)
    except Mismatch as e:
        sys.exit(str(e))
    finally:
        if a.keep:
            print(f"tables kept under {root}")
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
