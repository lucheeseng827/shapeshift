#!/usr/bin/env python3
"""Check the HTTP source (`source.http`) against DuckDB's Iceberg reader.

A stand-in API serves orders `since` an `updated_at` (inclusive), oldest first, 100 a page,
behind a bearer token. Many orders share one `updated_at`. Bounded runs
(`--max-records`) are repeated until one reads nothing new, while the API changes between
them: orders are updated (a newer `updated_at`), new ones appear, and one turns up late with
an `updated_at` inside the lookback. One run is killed part-way (a page fails): it must land
nothing, and the next resumes from the last commit. After each drain DuckDB's count, distinct
ids and sums must be the API's own, and the token must appear in no output.

Usage (from the workspace root, after `cargo build -p shapeshift-cli --features http`):

    .venv/bin/python examples/http/verify_duckdb.py --bin target/debug/shapeshift
"""

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import duckdb

TOKEN = "verify-tok-91c0-never-printed"
T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)


def ts(minutes):
    return (T0 + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


class Api:
    """The source of truth: id -> (updated_at minutes, amount)."""

    def __init__(self):
        self.rows = {}
        self.fail_page = None  # a page that fails once (the killed run)
        self.lock = threading.Lock()

    def page(self, since, page, size):
        with self.lock:
            if self.fail_page == page:
                self.fail_page = None
                return None
            rows = sorted(
                (m, i, amt) for i, (m, amt) in self.rows.items() if since is None or ts(m) >= since
            )
        chunk = rows[(page - 1) * size : page * size]
        return [{"id": i, "updated_at": ts(m), "amount": amt} for m, i, amt in chunk]


def serve(api):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                self.send_response(401)
                self.end_headers()
                return
            q = parse_qs(urlparse(self.path).query)
            since = q.get("since", [None])[0]
            items = api.page(since, int(q["page"][0]), int(q["size"][0]))
            if items is None:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"killed")
                return
            body = json.dumps({"data": items}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def shape(binary, spec, bound):
    env = dict(os.environ, SS_VERIFY_TOKEN=TOKEN)
    p = subprocess.run(
        [binary, "shape", "--spec", spec, "--max-records", str(bound), "--summary-json"],
        capture_output=True,
        text=True,
        env=env,
    )
    if TOKEN in p.stdout or TOKEN in p.stderr:
        sys.exit("MISMATCH: the token appeared in shapeshift's output")
    return p


def drain(binary, spec, bound):
    news = []
    for _ in range(200):
        p = shape(binary, spec, bound)
        if p.returncode != 0:
            sys.exit(f"a run failed:\n{p.stdout}{p.stderr}")
        doc = json.loads(p.stdout)
        news.append(doc["source"]["new"])
        if doc["source"]["new"] == 0:
            return news, doc["source"]["position"]
    sys.exit(f"MISMATCH: the runs never ran dry: {news}")


def check(con, table, api, what):
    got = con.sql(
        f"select count(*), count(distinct id), sum(amount), max(updated_at) "
        f"from iceberg_scan('{table}')"
    ).fetchall()[0]
    rows = api.rows.values()
    want = (len(api.rows), len(api.rows), sum(a for _, a in rows), max(m for m, _ in rows))
    got_max = int((datetime.fromisoformat(str(got[3]).replace("Z", "+00:00")).replace(
        tzinfo=timezone.utc) - T0).total_seconds() // 60) if got[3] is not None else None
    if (got[0], got[1], got[2], got_max) != want:
        sys.exit(f"MISMATCH ({what}): duckdb {got[:3]} max {got_max}, api {want}")
    print(f"ok  {what}: {got[0]} rows, {got[1]} ids, sum {got[2]}, as the API has them")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", default="target/debug/shapeshift")
    ap.add_argument("--seed", type=int, default=74)
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    rng = random.Random(a.seed)

    api = Api()
    # 2,000 orders over 400 minutes: about five to a minute, so most values are shared.
    for i in range(2000):
        api.rows[i] = (rng.randrange(400), rng.randrange(1, 10_000))
    srv = serve(api)
    root = tempfile.mkdtemp(prefix="ss-http-verify-")
    table = os.path.join(root, "orders")
    spec = os.path.join(root, "orders.yaml")
    with open(spec, "w") as f:
        f.write(
            f"""dataset: orders
source:
  http:
    url: http://127.0.0.1:{srv.server_port}/orders
    auth: {{ bearer_env: SS_VERIFY_TOKEN }}
    records: data
    paging: {{ page: page, size: size, per_page: 100 }}
    incremental: {{ param: since, from: updated_at, order: asc, lookback: 10m }}
schema: strict
columns:
  - {{ name: id, type: int64, required: true }}
  - {{ name: updated_at, type: timestamp }}
  - {{ name: amount, type: int64 }}
output: {{ format: iceberg, path: {table}, merge_on: [id] }}
"""
        )
    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg;")
    print(f"duckdb {duckdb.__version__}, seed {a.seed}, table {table}")

    news, pos = drain(a.bin, spec, 300)
    print(f"    initial load: runs of {news}, position {pos}")
    if sum(news) != 2000:
        sys.exit(f"MISMATCH: {sum(news)} new records over the runs, not 2000")
    check(con, table, api, "initial load")

    for round_no in range(3):
        top = max(m for m, _ in api.rows.values())
        # Updates (moved to a newer minute), new orders, and one late order 4 minutes behind
        # the newest value read: inside the 10-minute lookback.
        for i in rng.sample(sorted(api.rows), 150):
            api.rows[i] = (top + rng.randrange(1, 60), rng.randrange(1, 10_000))
        for i in range(len(api.rows), len(api.rows) + 200):
            api.rows[i] = (top + rng.randrange(1, 60), rng.randrange(1, 10_000))
        # A killed run: a page fails, the run lands nothing.
        before = con.sql(f"select count(*), sum(amount) from iceberg_scan('{table}')").fetchall()
        api.fail_page = 2
        p = shape(a.bin, spec, 300)
        if p.returncode == 0:
            sys.exit("MISMATCH: the killed run succeeded")
        after = con.sql(f"select count(*), sum(amount) from iceberg_scan('{table}')").fetchall()
        if before != after:
            sys.exit(f"MISMATCH: the killed run changed the table: {before} -> {after}")
        news, pos = drain(a.bin, spec, 300)
        newest = max(m for m, _ in api.rows.values())
        late = len(api.rows)
        api.rows[late] = (newest - 4, 4242)
        more, pos = drain(a.bin, spec, 300)
        print(f"    round {round_no + 1}: killed run landed nothing; runs of {news}, then {more} "
              f"with the late order; position {pos}")
        check(con, table, api, f"round {round_no + 1}")

    srv.shutdown()
    if a.keep:
        print(f"kept {root}")
    else:
        shutil.rmtree(root, ignore_errors=True)
    print("all checks passed")


if __name__ == "__main__":
    main()
