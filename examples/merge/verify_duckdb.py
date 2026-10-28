#!/usr/bin/env python3
"""Model-check `shapeshift shape --merge-on` against DuckDB's Iceberg reader.

Each scenario runs a sequence of random merges into one Iceberg table and, after
every merge, compares what DuckDB's `iceberg_scan` returns with a plain-Python model
of upsert semantics:

  * a row whose key is already in the table replaces it;
  * a key repeated within one input keeps its last occurrence;
  * a row with a null key is appended and matches nothing.

At the end it time-travels to every snapshot the run produced and checks each one
against the model as it stood then, so a delete that leaked into an older snapshot
fails as loudly as one that is missing from the newest. It also checks that
`shapeshift inspect` reports the same live row count DuckDB sees.

Usage (from the workspace root, after `cargo build -p shapeshift-cli`):

    python3 -m venv .venv && .venv/bin/pip install duckdb
    .venv/bin/python examples/merge/verify_duckdb.py --bin target/debug/shapeshift

Object stores: build with `--features object_store` and pass `--tables` a URL prefix to
put the tables there instead. `file:///abs/dir` runs the object-store sink on local
disk. `s3://bucket/prefix` takes its credentials and endpoint from the usual `AWS_*`
variables (`AWS_ENDPOINT` + `AWS_ALLOW_HTTP=true` for an S3-compatible server such as
MinIO or moto), and DuckDB reads the tables back through `httpfs` with the same
credentials. Tables under an `s3://` prefix are left in place.

Exit status 0 means every scenario matched at every snapshot.

`--negative-control` writes each delta with `--append` instead of `--merge-on`. An append
keeps every version of a key, so the model check must fail; the run then exits 0 only
if it failed *as a model mismatch*. A checker that could not tell a merge from an append
(exit 1), or a run that broke some other way (exit 1, with its own error), fails it.
"""

import argparse
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile

import duckdb

SPEC = """\
dataset: t
schema: strict
output: {{ format: iceberg, path: {path} }}
columns:
  - {{ name: id, type: int64 }}
  - {{ name: region, type: string }}
  - {{ name: day, type: date }}
  - {{ name: v, type: int64 }}
  - {{ name: round, type: int64 }}
options: {{ row_group_rows: {rg} }}
"""

COLUMNS = ["id", "region", "day", "v", "round"]

# name, partition_by, merge key, rounds, rows per round, key space, row-group rows,
# whether keys climb round over round (so key-bound pruning has something to prune)
SCENARIOS = [
    ("unpartitioned", [], ["id"], 6, 300, 500, 50_000, False),
    ("identity-partition, keys move between partitions", ["region"], ["id"], 6, 300, 400, 50_000, False),
    ("hidden partitions + composite key", ["day(day)", "bucket(4, id)"], ["region", "id"], 5, 400, 300, 50_000, False),
    ("many row groups per file", [], ["id"], 4, 1000, 800, 64, False),
    ("climbing keys, pruned by bounds", [], ["id"], 5, 200, 0, 50_000, True),
    # Files well past the 64 KiB footer prefetch, so reads come from per-chunk ranges:
    # the composite key's columns are adjacent chunks, and each round's delete file
    # (tens of thousands of rows, two adjacent columns) is read back by the next merge.
    ("large files, adjacent key chunks", [], ["id", "region"], 3, 40_000, 15_000, 50_000, False),
]


class Mismatch(Exception):
    """The table DuckDB reads disagrees with the model."""


def shape(binary, args):
    out = subprocess.run([binary, *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"shapeshift {' '.join(args)} failed:\n{out.stdout}{out.stderr}")
    return out.stdout


def current_snapshot(con, table):
    # Every shapeshift write adds one snapshot, so the newest is the current one.
    return con.sql(
        f"select snapshot_id from iceberg_snapshots('{table}') "
        f"order by sequence_number desc limit 1"
    ).fetchall()[0][0]


def as_tuple(row):
    return tuple(row.get(c) for c in COLUMNS)


def read_back(con, table, snapshot=None):
    at = f", snapshot_from_id={snapshot}" if snapshot is not None else ""
    rows = con.sql(
        f"select id, region, strftime(day, '%Y-%m-%d'), v, round "
        f"from iceberg_scan('{table}'{at})"
    ).fetchall()
    return sorted(rows, key=repr)


def run_scenario(binary, con, root, tables, seed, scenario, write):
    name, partition_by, key, rounds, per_round, key_space, rg, climbing = scenario
    rng = random.Random(seed)
    slug = re.sub(r"[^a-z0-9]+", "-", name)
    work = os.path.join(root, slug)
    os.makedirs(work)
    table = f"{tables}/{slug}/t" if tables else os.path.join(work, "t")
    spec = os.path.join(work, "t.spec.yaml")
    with open(spec, "w") as f:
        f.write(SPEC.format(path=table, rg=rg))

    model, nulls, history, pruned = {}, [], [], 0
    for r in range(rounds):
        delta = []
        for i in range(per_round):
            row = {
                "id": (r * per_round + i) if climbing else rng.randrange(key_space),
                "region": rng.choice(["eu", "us", "ap"]),
                "day": f"2026-01-{rng.randrange(1, 4):02d}",
                "v": rng.randrange(1_000_000),
                "round": r,
            }
            if rng.random() < 0.03:
                del row["id"]  # a null key
            delta.append(row)
        path = os.path.join(work, f"delta-{r}.jsonl")
        with open(path, "w") as f:
            f.writelines(json.dumps(row) + "\n" for row in delta)

        args = ["shape", "--spec", spec, "-i", path, *write(key)]
        for p in partition_by:
            args += ["--partition-by", p]
        out = shape(binary, args)
        m = re.search(r"pruned (\d+) by key bounds", out)
        pruned += int(m.group(1)) if m else 0

        for row in delta:
            k = tuple(row.get(c) for c in key)
            if any(v is None for v in k):
                nulls.append(row)
            else:
                model[k] = row
        expected = sorted((as_tuple(row) for row in [*model.values(), *nulls]), key=repr)
        got = read_back(con, table)
        if got != expected:
            missing = set(expected) - set(got)
            extra = set(got) - set(expected)
            raise Mismatch(
                f"[{name}] round {r}: DuckDB read {len(got)} rows, model has {len(expected)}"
                f"\n  missing (first 5): {sorted(missing, key=repr)[:5]}"
                f"\n  extra   (first 5): {sorted(extra, key=repr)[:5]}"
            )
        live = re.search(r"live-records: (\d+)|total-records: (\d+)", shape(binary, ["inspect", table]))
        reported = int(live.group(1) or live.group(2))
        if reported != len(expected):
            raise Mismatch(
                f"[{name}] round {r}: inspect reports {reported} live rows, DuckDB {len(expected)}"
            )
        history.append((current_snapshot(con, table), expected))

    for i, (snapshot, expected) in enumerate(history):
        if read_back(con, table, snapshot) != expected:
            raise Mismatch(f"[{name}] time travel to snapshot {i + 1} ({snapshot}) does not match")

    if climbing and pruned == 0:
        sys.exit(f"[{name}] expected disjoint key ranges to prune files, none were")
    print(
        f"ok  {name}: {rounds} merges, {len(model)} keys + {len(nulls)} null-key rows live, "
        f"{len(history)} snapshots time-travelled, {pruned} file(s) pruned"
    )


def configure_s3(con):
    """Point DuckDB's httpfs at the same S3 the binary writes to (the `AWS_*` env)."""
    con.sql("INSTALL httpfs; LOAD httpfs;")
    opts = {
        "KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "SECRET": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "REGION": os.environ.get("AWS_REGION", "us-east-1"),
    }
    endpoint = os.environ.get("AWS_ENDPOINT")
    if endpoint:
        scheme, _, host = endpoint.partition("://")
        opts["ENDPOINT"] = host.rstrip("/")
        opts["USE_SSL"] = "true" if scheme == "https" else "false"
        opts["URL_STYLE"] = "path"
    body = ", ".join(
        f"{k} {v}" if v in ("true", "false") else f"{k} '{v}'" for k, v in opts.items()
    )
    con.sql(f"CREATE SECRET verify_s3 (TYPE s3, {body})")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", default="target/debug/shapeshift", help="the shapeshift binary")
    ap.add_argument("--seed", type=int, default=74)
    ap.add_argument("--keep", action="store_true", help="keep the scratch tables")
    ap.add_argument(
        "--tables",
        help="URL prefix to write the tables under (file:///dir or s3://bucket/prefix) "
        "instead of local paths; needs a binary built with --features object_store",
    )
    ap.add_argument(
        "--negative-control",
        action="store_true",
        help="append instead of merging; succeed only if the model check catches it",
    )
    a = ap.parse_args()
    if a.negative_control:
        write = lambda key: ["--append"]  # noqa: E731
    else:
        write = lambda key: ["--merge-on", ",".join(key)]  # noqa: E731

    binary = os.path.abspath(a.bin)
    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg;")
    root = tempfile.mkdtemp(prefix="shapeshift-merge-")
    tables = None
    if a.tables:
        # A fresh prefix per run, so a rerun never merges into the last run's tables.
        tables = f"{a.tables.rstrip('/')}/{os.path.basename(root)}"
        if tables.startswith("s3://"):
            configure_s3(con)
    mode = " (negative control: appending)" if a.negative_control else ""
    print(f"duckdb {duckdb.__version__}, seed {a.seed}, tables under {tables or root}{mode}")
    try:
        for i, scenario in enumerate(SCENARIOS):
            run_scenario(binary, con, root, tables, a.seed + i, scenario, write)
    except Mismatch as e:
        if a.negative_control:
            print(f"ok  negative control: the check caught the append — {str(e).splitlines()[0]}")
            return
        sys.exit(str(e))
    else:
        if a.negative_control:
            sys.exit("negative control FAILED: every scenario passed with --append, so the "
                     "check cannot tell a merge from an append")
    finally:
        if a.keep:
            print(f"tables kept under {root}")
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
