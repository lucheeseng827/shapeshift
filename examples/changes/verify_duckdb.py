#!/usr/bin/env python3
"""Model-check `changes:` (change feeds) against DuckDB's Iceberg reader.

Each scenario writes a random feed of inserts, updates and deletes into one Iceberg table,
split into several writes and delivered **out of order**, and after every write compares
what DuckDB's `iceberg_scan` returns with a plain-Python model of the rules:

  * per write, each key's winning change is its greatest `order_by` version (the later
    arrival on a tie), or simply its last change without `order_by`;
  * a winning change applies only if the key has no live row, or (with `order_by`) the
    live row's version is lower: equal is stale, so a replayed write changes nothing;
  * an applied delete removes the key's row; an applied insert or update replaces it.

Every write's `--summary-json` is checked too: the changes it counted, the rows a delete
removed, and the stale changes must be the model's. The feeds come flat (the record is the
row) and as Debezium envelopes (`row: after`, a delete's key in `before`, the version read
from the envelope with a `$.source.ts_ms` path). A changelog scenario (`apply: changelog`)
must keep every change, with its `_op`.

Usage (from the workspace root, after `cargo build -p shapeshift-cli`):

    .venv/bin/python examples/changes/verify_duckdb.py --bin target/debug/shapeshift

`--tables file:///dir` or `--tables s3://bucket/prefix` put the tables in an object store
(a binary built with `--features object_store`), as for `examples/merge/verify_duckdb.py`,
whose S3 setup and helpers this reuses.
"""

import argparse
import json
import os
import random
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "merge"))
from verify_duckdb import Mismatch, connect, current_snapshot, summary  # noqa: E402

import duckdb  # noqa: E402

FLAT = """\
dataset: c
schema: strict
changes: {{ op: op, insert: [c], update: [u], delete: [d]{order} }}
output: {{ format: iceberg, path: {path}, merge_on: [id] }}
columns:
  - {{ name: id, type: int64 }}
  - {{ name: v, type: string }}
  - {{ name: ver, type: int64 }}
"""

DEBEZIUM = """\
dataset: c
schema: strict
changes:
  op: op
  insert: [c, r]
  update: [u]
  delete: [d]
  row: after
  key_from: before
{order}output: {{ format: iceberg, path: {path}, merge_on: [id] }}
columns:
  - {{ name: id, type: int64 }}
  - {{ name: v, type: string }}
  - {{ name: ver, type: int64, from: $.source.ts_ms }}
"""

CHANGELOG = """\
dataset: c
schema: strict
changes: {{ op: op, insert: [c], update: [u], delete: [d] }}
output: {{ format: iceberg, path: {path}, apply: changelog }}
columns:
  - {{ name: id, type: int64 }}
  - {{ name: v, type: string }}
  - {{ name: ver, type: int64 }}
"""

# name, envelope, ordered, writes, changes per write, key space
SCENARIOS = [
    ("flat, arrival order", "flat", False, 6, 60, 25),
    ("flat, ordered, out of order", "flat", True, 6, 60, 25),
    ("flat, ordered, few keys (many ties and replays)", "flat", True, 8, 40, 4),
    ("debezium envelope, ordered", "debezium", True, 6, 60, 25),
    ("debezium envelope, arrival order", "debezium", False, 5, 50, 20),
]


def change(rng, i, keys, ver):
    return {"op": rng.choice("cud"), "id": rng.randrange(keys), "v": f"s{i}", "ver": ver}


def as_record(ch, envelope):
    if envelope == "flat":
        return ch
    row = {"id": ch["id"], "v": ch["v"]}
    if ch["op"] == "d":
        return {"op": "d", "before": {"id": ch["id"]}, "after": None,
                "source": {"ts_ms": ch["ver"]}}
    return {"op": ch["op"], "before": None, "after": row, "source": {"ts_ms": ch["ver"]}}


def apply_model(state, tombs, write, ordered):
    """Apply one write to `state` ({id: (v, ver)}) and, with `order_by`, to `tombs` ({id: the
    version that deleted it}); return (deletes applied that removed a row, stale winners)."""
    winners = {}
    for ch in write:
        prev = winners.get(ch["id"])
        if prev is None or not ordered or ch["ver"] >= prev["ver"]:
            winners[ch["id"]] = ch
    removed = stale = 0
    for key, ch in winners.items():
        live = state.get(key)
        if ordered and (
            (live is not None and ch["ver"] <= live[1])
            or (key in tombs and ch["ver"] <= tombs[key])
        ):
            stale += 1
            continue
        if ch["op"] == "d":
            if state.pop(key, None) is not None:
                removed += 1
            if ordered:
                tombs[key] = ch["ver"]
        else:
            state[key] = (ch["v"], ch["ver"])
            tombs.pop(key, None)
    return removed, stale


def read_back(con, table):
    return sorted(con.sql(f"select id, v, ver from iceberg_scan('{table}')").fetchall())


def run_scenario(binary, con, root, tables, seed, scenario):
    name, envelope, ordered, writes, per_write, keys = scenario
    rng = random.Random(seed)
    slug = "".join(ch if ch.isalnum() else "-" for ch in name)
    table = f"{tables}/{slug}" if tables else os.path.join(root, slug)
    order = ""
    if ordered:
        order = ", order_by: [ver]" if envelope == "flat" else "  order_by: [ver]\n"
    spec_path = os.path.join(root, f"{slug}.yaml")
    with open(spec_path, "w") as f:
        f.write((FLAT if envelope == "flat" else DEBEZIUM).format(order=order, path=table))

    # Versions come from one clock, then the feed is shuffled: changes arrive out of order,
    # and some versions repeat (a replay), so stale and equal both happen.
    total = writes * per_write
    feed = [change(rng, i, keys, rng.randrange(total // 2 + 1)) for i in range(total)]
    rng.shuffle(feed)
    state, tombs = {}, {}
    for w in range(writes):
        write = feed[w * per_write:(w + 1) * per_write]
        if w == writes - 1 and w > 0:
            write = write + feed[:per_write // 3]  # replay part of the first write
        path = os.path.join(root, f"{slug}-{w}.jsonl")
        with open(path, "w") as f:
            for ch in write:
                f.write(json.dumps(as_record(ch, envelope)) + "\n")
        doc = summary(binary, ["shape", "--spec", spec_path, "-i", path])
        removed, stale = apply_model(state, tombs, write, ordered)
        expected = sorted((k, v, ver) for k, (v, ver) in state.items())
        got = read_back(con, table)
        when = f"[{name}] after write {w + 1}/{writes}"
        if got != expected:
            raise Mismatch(
                f"{when}: DuckDB read {len(got)} rows, the model has {len(expected)}"
                f"\n  missing: {sorted(set(expected) - set(got))[:5]}"
                f"\n  extra:   {sorted(set(got) - set(expected))[:5]}"
            )
        counts = doc["changes"]
        want = {k: sum(ch["op"] == o for ch in write) for k, o in
                [("inserts", "c"), ("updates", "u"), ("deletes", "d")]}
        if {k: counts[k] for k in want} != want:
            raise Mismatch(f"{when}: the summary counts {counts}, the feed has {want}")
        rows = doc["rows"]
        if (rows["deleted"], rows["stale"]) != (removed, stale):
            raise Mismatch(
                f"{when}: the summary says deleted={rows['deleted']} stale={rows['stale']}, "
                f"the model {removed} and {stale}"
            )
        if int(doc["table"]["snapshot_id"]) != current_snapshot(con, table):
            raise Mismatch(f"{when}: the summary's snapshot is not DuckDB's newest")
    print(f"ok  {name}: {writes} writes, {len(state)} live keys")


def run_changelog(binary, con, root, tables, seed):
    rng = random.Random(seed)
    table = f"{tables}/changelog" if tables else os.path.join(root, "changelog")
    spec_path = os.path.join(root, "changelog.yaml")
    with open(spec_path, "w") as f:
        f.write(CHANGELOG.format(path=table))
    kept = []
    for w in range(3):
        write = [change(rng, f"{w}-{i}", 10, rng.randrange(50)) for i in range(30)]
        path = os.path.join(root, f"changelog-{w}.jsonl")
        with open(path, "w") as f:
            for ch in write:
                f.write(json.dumps(ch) + "\n")
        summary(binary, ["shape", "--spec", spec_path, "-i", path])
        word = {"c": "insert", "u": "update", "d": "delete"}
        kept += [(ch["id"], ch["v"], ch["ver"], word[ch["op"]]) for ch in write]
        got = sorted(con.sql(f"select id, v, ver, _op from iceberg_scan('{table}')").fetchall())
        if got != sorted(kept):
            raise Mismatch(f"[changelog] after write {w + 1}: {len(got)} rows, {len(kept)} changes")
    print(f"ok  changelog: {len(kept)} changes kept, each with its _op")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", default="target/debug/shapeshift", help="the shapeshift binary")
    ap.add_argument("--seed", type=int, default=606)
    ap.add_argument("--keep", action="store_true", help="keep the scratch tables")
    ap.add_argument("--tables", help="URL prefix for the tables (file:///dir or s3://bucket/prefix)")
    a = ap.parse_args()
    binary = os.path.abspath(a.bin)
    root = tempfile.mkdtemp(prefix="shapeshift-changes-")
    tables = f"{a.tables.rstrip('/')}/{os.path.basename(root)}" if a.tables else None
    con = connect(tables)
    print(f"duckdb {duckdb.__version__}, seed {a.seed}, tables under {tables or root}")
    try:
        for i, scenario in enumerate(SCENARIOS):
            run_scenario(binary, con, root, tables, a.seed + i, scenario)
        run_changelog(binary, con, root, tables, a.seed + len(SCENARIOS))
    except Mismatch as e:
        sys.exit(str(e))
    finally:
        if a.keep:
            print(f"tables kept under {tables or root}")
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
