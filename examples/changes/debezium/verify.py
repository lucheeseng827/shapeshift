#!/usr/bin/env python3
"""Apply a drained Debezium feed with shapeshift, out of order, and compare with the source.

Called by run.sh. The feed is shuffled and split into `--runs` writes (so later changes to a
key often land before earlier ones), applied with `shapeshift shape --spec`, and DuckDB's
`iceberg_scan` of the table must equal the database's final rows. Then the whole feed is
applied once more, in one write: a replay, which must change nothing.
"""

import argparse
import json
import os
import random
import subprocess
import sys

import duckdb


def shape(binary, spec, path):
    out = subprocess.run([binary, "shape", "--spec", spec, "-i", path, "--summary-json"],
                         capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"shapeshift failed on {path}:\n{out.stdout}{out.stderr}")
    return json.loads(out.stdout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--spec", required=True)
    ap.add_argument("--feed", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--runs", type=int, default=4)
    ap.add_argument("--expected", required=True, help="the database's final rows: id,status,amount")
    a = ap.parse_args()

    table = os.path.join(a.work, "orders")
    spec = os.path.join(a.work, "spec.yaml")
    with open(a.spec) as f, open(spec, "w") as out:
        out.write(f.read().replace("PATH", table))
    with open(a.feed) as f:
        feed = [line for line in f if line.strip()]
    rng = random.Random(7)
    shuffled = feed[:]
    rng.shuffle(shuffled)
    n = max(1, len(shuffled) // a.runs)
    counts = {"deleted": 0, "stale": 0}
    for i in range(a.runs):
        part = shuffled[i * n:] if i == a.runs - 1 else shuffled[i * n:(i + 1) * n]
        path = os.path.join(a.work, f"part-{i}.jsonl")
        with open(path, "w") as f:
            f.writelines(part)
        doc = shape(a.bin, spec, path)
        for k in counts:
            counts[k] += doc["rows"][k]

    with open(a.expected) as f:
        want = sorted(tuple(int(x) if j != 1 else x for j, x in enumerate(line.strip().split(",")))
                      for line in f if line.strip())
    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg;")

    def got():
        return sorted(con.sql(f"select id, status, amount from iceberg_scan('{table}')").fetchall())

    if got() != want:
        g = got()
        sys.exit(f"MISMATCH after {a.runs} out-of-order writes: {len(g)} rows, the database has "
                 f"{len(want)}\n  missing: {sorted(set(want) - set(g))[:5]}"
                 f"\n  extra:   {sorted(set(g) - set(want))[:5]}")
    print(f"ok  {len(feed)} changes in {a.runs} shuffled writes: {len(want)} rows, as the database "
          f"has them ({counts['deleted']} deleted, {counts['stale']} stale on the way)")

    replay = feed[:]
    rng.shuffle(replay)
    path = os.path.join(a.work, "replay.jsonl")
    with open(path, "w") as f:
        f.writelines(replay)
    doc = shape(a.bin, spec, path)
    merge = doc["merge"]
    applied = (merge["keys_inserted"], merge["keys_updated"], merge["keys_deleted"], doc["rows"]["deleted"])
    if applied != (0, 0, 0, 0):
        sys.exit(f"MISMATCH: the replay applied changes (inserted, updated, deleted keys, rows "
                 f"deleted = {applied})")
    if got() != want:
        sys.exit("MISMATCH: replaying the feed changed the table")
    print(f"ok  replay, shuffled: nothing applied ({merge['changes_stale']} keys stale), the table "
          f"unchanged")


if __name__ == "__main__":
    main()
