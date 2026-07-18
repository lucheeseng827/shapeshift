#!/usr/bin/env python3
"""commit_parallel_add_files.py — parallel shapeshift writers, one atomic catalog commit.

The share-nothing model taken one step further: instead of N *tables* unioned at read
(see parallel_write.sh), produce N data files **in parallel** with N shapeshift instances,
then register all of them into ONE Iceberg table with a **single atomic commit** through
the catalog (PyIceberg ``add_files``). This is exactly how Spark/Flink write a lake: many
tasks emit data files concurrently; the catalog commits them once, with optimistic
concurrency. The expensive part (shaping the bytes) is parallel; the commit is a fast
metadata operation the catalog serializes safely.

Why this is safe when concurrent ``--append`` is not: shapeshift's file-system append has
no compare-and-swap on ``version-hint.text`` (last writer wins → lost snapshots). A catalog
commit *does* CAS the table pointer, so a single ``add_files`` — or several, each retried on
conflict — can never silently drop data.

Usage:
  CATALOG_TYPE=rest CATALOG_URI=http://localhost:19120/iceberg/ CATALOG_WAREHOUSE=warehouse \
  SHAPESHIFT=./target/release/shapeshift \
    python commit_parallel_add_files.py --input events.jsonl --shards 4 --identifier analytics.events

Requires: pip install "pyiceberg[s3fs]" pyarrow ; shapeshift on PATH or $SHAPESHIFT.
"""
import argparse
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError


def split_input(input_path: str, shards: int, tmp: str) -> list[str]:
    """Round-robin the JSONL into `shards` files (line-oriented keeps JSON valid)."""
    os.makedirs(tmp, exist_ok=True)
    paths = [os.path.join(tmp, f"shard{i}.jsonl") for i in range(shards)]
    files = [open(p, "w") for p in paths]
    try:
        with open(input_path) as src:
            for n, line in enumerate(src):
                files[n % shards].write(line)
    finally:
        for f in files:
            f.close()
    return paths


def infer_shared_spec(input_path: str, spec_path: str) -> str:
    """Infer ONE spec from the whole input up front, so every parallel shard is shaped to
    the *same* schema. Otherwise each shard would infer independently and a sparse/mixed
    JSONL shard could produce a divergent column set — nondeterministic, and a set add_files
    can reject."""
    ss = os.environ.get("SHAPESHIFT", "shapeshift")
    subprocess.run([ss, "infer", "-i", input_path, "-o", spec_path],
                   check=True, stdout=subprocess.DEVNULL)
    return spec_path


def shape_one(shard_jsonl: str, out_parquet: str, spec_path: str) -> str:
    """Run one shapeshift instance under the SHARED spec → a plain Parquet data file
    (fast, no table metadata). -i/-o override the spec's paths (see the CLI override order),
    so every shard uses the identical schema but its own output file."""
    ss = os.environ.get("SHAPESHIFT", "shapeshift")
    subprocess.run(
        [ss, "shape", "-s", spec_path, "-i", shard_jsonl, "-o", out_parquet, "--to", "parquet"],
        check=True, stdout=subprocess.DEVNULL,
    )
    return out_parquet


def build_catalog():
    """Build a PyIceberg catalog (REST/Glue/…) from the CATALOG_*/S3/AWS env vars."""
    props = {"type": os.environ.get("CATALOG_TYPE", "rest")}
    for env, key in (("CATALOG_URI", "uri"), ("CATALOG_WAREHOUSE", "warehouse"),
                     ("CATALOG_TOKEN", "token"), ("CATALOG_CREDENTIAL", "credential"),
                     ("S3_ENDPOINT", "s3.endpoint"), ("AWS_REGION", "s3.region")):
        if val := os.environ.get(env):
            props[key] = val
    return load_catalog(os.environ.get("CATALOG_NAME", "shapeshift"), **props)


def main() -> int:
    """Shard the input, shape each shard to Parquet in parallel under one shared spec, then
    commit every resulting file into a single Iceberg table with one atomic add_files."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True, help="source JSONL to fan out")
    ap.add_argument("--shards", type=int, default=os.cpu_count() or 4)
    ap.add_argument("--identifier", required=True, help="namespace.table to commit into")
    ap.add_argument("--tmp", default="./_parallel_parquet", help="scratch dir for the shard Parquet")
    args = ap.parse_args()

    if args.shards < 1:
        ap.error("--shards must be at least 1")

    ns, _, name = args.identifier.rpartition(".")
    if not ns:
        print("identifier must be namespace.table", file=sys.stderr)
        return 2

    # add_files needs full (absolute) paths — a relative --tmp would be resolved against
    # each reader's cwd and break portability, so anchor everything at an absolute dir.
    tmp = os.path.abspath(args.tmp)

    # 1. Infer one SHARED spec, then shape every shard to Parquet IN PARALLEL under it
    #    (the expensive, scalable step). Results kept in shard order so the schema the
    #    table is created from is deterministic, not "whichever worker finished first".
    spec_path = infer_shared_spec(args.input, os.path.join(tmp, "shared.spec.yaml"))
    shard_jsonl = split_input(args.input, args.shards, tmp)
    parquet_paths: list[str | None] = [None] * args.shards
    with ThreadPoolExecutor(max_workers=args.shards) as ex:
        futs = {ex.submit(shape_one, sj, os.path.join(tmp, f"part{i}.parquet"), spec_path): i
                for i, sj in enumerate(shard_jsonl)}
        for fut in as_completed(futs):
            parquet_paths[futs[fut]] = fut.result()
    parquet_paths = [p for p in parquet_paths if p]
    print(f"shaped {len(parquet_paths)} Parquet files across {args.shards} parallel instances")

    # 2. Ensure the table exists (schema from the deterministic first shard; all shards
    #    share the inferred spec, so any shard's schema is identical).
    cat = build_catalog()
    cat.create_namespace_if_not_exists(ns)
    ident = (ns, name)
    try:
        table = cat.load_table(ident)
    except NoSuchTableError:
        schema = pq.read_schema(parquet_paths[0])   # pyarrow schema → pyiceberg maps it
        table = cat.create_table(ident, schema=schema)

    # 3. ONE atomic commit registers every data file. No data is rewritten or copied;
    #    add_files applies the table's name-mapping to Parquet without field-ids.
    table.add_files(file_paths=sorted(parquet_paths))

    # Report the committed row count (accessor differs across pyiceberg versions → be defensive).
    total = "?"
    try:
        snap = table.current_snapshot()
        summary = dict(snap.summary) if snap and snap.summary else {}
        total = summary.get("total-records", "?")
    except (AttributeError, KeyError, TypeError, ValueError):
        pass
    print(f"committed {len(parquet_paths)} files into {args.identifier} in one snapshot; "
          f"total-records={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
