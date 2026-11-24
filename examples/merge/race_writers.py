#!/usr/bin/env python3
"""Concurrent writers against one Iceberg table: nothing lost, nothing doubled.

Several writer processes merge into the same table at once while a janitor runs
`shapeshift compact` and `shapeshift expire` in a loop. Each writer owns its own keys and
merges a round of updates and inserts at a time; when shapeshift exits 75 (another writer
committed first, nothing of this run was published) it runs the same round again, as an
orchestrator would. When everyone is done, DuckDB must read exactly the rows the writers'
last rounds say, and `shapeshift inspect` the same count.

Every run reads its `--summary-json` document: a refused run must say `conflict`, and the
table versions the successful runs report must be 1..N with each claimed exactly once — two
runs that both believe they committed version N is the lost update itself.

Without the commit guard, two writers that built on the same version both "succeed" and
the later one silently overwrites the earlier one's snapshot. `--negative-control` runs a
binary expected to have no guard (one built before it) and succeeds only if rows were lost;
such a binary predates `--summary-json` too, so that run reads the text report instead.

Usage (from the workspace root, after `cargo build -p shapeshift-cli`):

    .venv/bin/python examples/merge/race_writers.py --bin target/debug/shapeshift
    .venv/bin/python examples/merge/race_writers.py --bin target/debug/shapeshift --tables s3://lake/race

`--tables` takes a URL prefix as `verify_duckdb.py` does (a build with `--features object_store`;
`AWS_*` settings for S3).
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
import threading
import time

import duckdb

from verify_duckdb import configure_s3

SPEC = """\
dataset: t
schema: strict
output: {{ format: iceberg, path: {path} }}
columns:
  - {{ name: id, type: int64 }}
  - {{ name: writer, type: int64 }}
  - {{ name: round, type: int64 }}
  - {{ name: v, type: int64 }}
"""

CONFLICT = 75


def run(binary, args, summary=False):
    """Run shapeshift: (exit status, stdout + stderr, the `--summary-json` document or None)."""
    p = subprocess.run([binary, *args, *(["--summary-json"] if summary else [])],
                       capture_output=True, text=True)
    doc = None
    if summary:
        try:
            doc = json.loads(p.stdout)
        except ValueError:
            raise RuntimeError(f"{' '.join(args)}: exit {p.returncode}, stdout is not one JSON "
                               f"document:\n{p.stdout}{p.stderr}") from None
        if p.returncode == CONFLICT and doc.get("error", {}).get("kind") != "conflict":
            raise RuntimeError(f"{' '.join(args)}: exit {CONFLICT} but the summary says {doc}")
    return p.returncode, p.stdout + p.stderr, doc


class Writer(threading.Thread):
    def __init__(self, binary, spec, work, w, rounds, per_round, seed, retry):
        super().__init__()
        self.binary, self.spec, self.work, self.w = binary, spec, work, w
        self.rounds, self.per_round, self.retry = rounds, per_round, retry
        self.rng = random.Random(seed)
        self.model, self.commits, self.conflicts, self.error = {}, 0, 0, None
        self.versions = []  # the table version each commit reports
        self.failed = []  # negative control only: runs that failed outright

    def run(self):
        try:
            for r in range(self.rounds):
                rows = {}
                for _ in range(self.per_round):
                    k = self.w * 1_000_000 + self.rng.randrange(self.per_round * 3)
                    rows[k] = {"id": k, "writer": self.w, "round": r, "v": self.rng.randrange(10**6)}
                path = os.path.join(self.work, f"w{self.w}-r{r}.jsonl")
                with open(path, "w") as f:
                    f.writelines(json.dumps(row) + "\n" for row in rows.values())
                for attempt in range(200):
                    code, out, doc = run(self.binary, ["shape", "--spec", self.spec, "-i", path, "--merge-on", "id"],
                                         summary=self.retry)
                    if code == 0:
                        self.commits += 1
                        self.model.update(rows)
                        if doc:
                            self.versions.append(doc["table"]["version"])
                        break
                    if code == CONFLICT and self.retry:
                        self.conflicts += 1
                        time.sleep(self.rng.random() * 0.05)
                        continue
                    if not self.retry:  # negative control: note it and move on
                        self.failed.append(out.strip().splitlines()[0] if out.strip() else f"exit {code}")
                        break
                    raise RuntimeError(f"writer {self.w} round {r}: exit {code}\n{out}")
                else:
                    raise RuntimeError(f"writer {self.w} round {r}: still conflicting after 200 tries")
        except Exception as e:  # noqa: BLE001 — reported by the main thread
            self.error = e


class Janitor(threading.Thread):
    """Compacts and expires the table until told to stop."""

    def __init__(self, binary, table, stop, tolerate):
        super().__init__()
        self.binary, self.table, self.stop, self.tolerate = binary, table, stop, tolerate
        self.done = {"compact": 0, "expire": 0}
        self.conflicts, self.error, self.failed = 0, None, 0
        self.versions = []

    def run(self):
        try:
            while not self.stop.is_set():
                for cmd in (["compact", self.table], ["expire", self.table, "--older-than", "0s", "--retain-last", "2"]):
                    code, out, doc = run(self.binary, cmd, summary=not self.tolerate)
                    if code == 0:
                        if doc["committed"] if doc else not out.startswith("nothing to"):
                            self.done[cmd[0]] += 1
                            if doc:
                                self.versions.append(doc["version"])
                    elif code == CONFLICT:
                        self.conflicts += 1
                    elif self.tolerate:
                        self.failed += 1
                    else:
                        raise RuntimeError(f"{' '.join(cmd)}: exit {code}\n{out}")
        except Exception as e:  # noqa: BLE001
            self.error = e


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", default="target/debug/shapeshift")
    ap.add_argument("--tables", help="URL prefix for the table (file:///dir or s3://bucket/prefix)")
    ap.add_argument("--writers", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--per-round", type=int, default=50)
    ap.add_argument("--seed", type=int, default=74)
    ap.add_argument("--no-janitor", action="store_true", help="writers only")
    ap.add_argument("--negative-control", action="store_true",
                    help="the binary has no commit guard: succeed only if writes were lost")
    a = ap.parse_args()

    binary = os.path.abspath(a.bin)
    work = tempfile.mkdtemp(prefix="shapeshift-race-")
    table = f"{a.tables.rstrip('/')}/{os.path.basename(work)}/t" if a.tables else os.path.join(work, "t")
    spec = os.path.join(work, "t.spec.yaml")
    with open(spec, "w") as f:
        f.write(SPEC.format(path=table))
    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg;")
    if table.startswith("s3://"):
        configure_s3(con)
    mode = " (negative control)" if a.negative_control else ""
    print(f"{a.writers} writers x {a.rounds} rounds into {table}{mode}")

    # The table exists before the race starts.
    seed_rows = os.path.join(work, "seed.jsonl")
    with open(seed_rows, "w") as f:
        f.write(json.dumps({"id": -1, "writer": -1, "round": -1, "v": 0}) + "\n")
    code, out, doc = run(binary, ["shape", "--spec", spec, "-i", seed_rows, "--merge-on", "id"],
                         summary=not a.negative_control)
    if code != 0:
        sys.exit(f"seeding the table failed: {out}")
    versions = [doc["table"]["version"]] if doc else []

    writers = [Writer(binary, spec, work, w, a.rounds, a.per_round, a.seed + w, not a.negative_control)
               for w in range(a.writers)]
    stop = threading.Event()
    janitor = None if a.no_janitor else Janitor(binary, table, stop, a.negative_control)
    t0 = time.perf_counter()
    if janitor:
        janitor.start()
    for w in writers:
        w.start()
    for w in writers:
        w.join()
    stop.set()
    if janitor:
        janitor.join()
    elapsed = time.perf_counter() - t0
    try:
        for t in [*writers, *([janitor] if janitor else [])]:
            if t.error:
                sys.exit(str(t.error))

        expected = {-1: (-1, -1, -1, 0)}
        for w in writers:
            for k, row in w.model.items():
                expected[k] = (row["id"], row["writer"], row["round"], row["v"])
        got = con.sql(f"select id, writer, round, v from iceberg_scan('{table}')").fetchall()
        lost = set(expected.values()) - set(got)
        extra = set(got) - set(expected.values())
        _, out, _ = run(binary, ["inspect", table])
        live = re.search(r"live-records: (\d+)|total-records: (\d+)", out)
        reported = int(live.group(1) or live.group(2))
        commits = sum(w.commits for w in writers)
        retries = sum(w.conflicts for w in writers)
        summary = (f"{commits} writer commits, {retries} retried after a conflict"
                   + (f", {janitor.done['compact']} compactions, {janitor.done['expire']} expiries, "
                      f"{janitor.conflicts} janitor conflicts" if janitor else "")
                   + f", {elapsed:.1f} s")
        if a.negative_control:
            failed = [f for w in writers for f in w.failed]
            if lost or extra:
                print(f"ok  negative control: without the guard {len(lost)} row(s) that "
                      f"writers were told were committed are not in the table ({summary}); "
                      f"{len(failed)} write(s) failed outright"
                      + (f", e.g. `{failed[0]}`" if failed else ""))
                return
            sys.exit(f"negative control FAILED: no write was lost ({summary}); the race did "
                     "not reach the commit — try more writers or rounds")
        if lost or extra or len(got) != len(expected) or reported != len(expected):
            sys.exit(f"FAILED: {len(lost)} row(s) lost, {len(extra)} unexpected, DuckDB read "
                     f"{len(got)}, inspect {reported}, expected {len(expected)} ({summary})"
                     f"\n  lost (first 5): {sorted(lost)[:5]}\n  extra (first 5): {sorted(extra)[:5]}")
        if retries == 0:
            sys.exit(f"no writer ever hit a conflict ({summary}): the race was not exercised")
        for t in [*writers, *([janitor] if janitor else [])]:
            versions += t.versions
        if sorted(versions) != list(range(1, len(versions) + 1)):
            claimed = {v: versions.count(v) for v in set(versions) if versions.count(v) > 1}
            sys.exit(f"FAILED: the runs that succeeded report versions {sorted(versions)[:20]}…, "
                     f"not 1..{len(versions)} once each (claimed twice or more: {claimed})")
        summary += f"; versions 1..{len(versions)} each claimed by exactly one run"
        print(f"ok  {len(expected)} rows exactly as the writers left them: {summary}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
