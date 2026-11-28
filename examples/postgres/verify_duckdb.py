#!/usr/bin/env python3
"""Check the Postgres source (`source.postgres`) against DuckDB's Iceberg reader.

Against a Postgres server (the password in SHAPESHIFT_POSTGRES_PASSWORD), a table is pulled
with bounded incremental runs (`--max-records`), repeated until one reads nothing new, while
rows are updated (stamped now, as a trigger or the application does) and inserted between
rounds; a run is killed part-way (`kill -9`) each round. DuckDB's count, distinct ids and sums
must be Postgres's own. Soft deletes go through `changes:` (`op: is_deleted`).

Usage (from the workspace root, after `cargo build -p shapeshift-cli --features postgres`):

    .venv/bin/python examples/postgres/verify_duckdb.py --bin target/debug/shapeshift \\
        --url postgres://postgres@127.0.0.1:5432/ssdb
"""

import argparse
import atexit
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import duckdb
import psycopg


def shape(binary, args):
    p = subprocess.run([binary, "shape", *args, "--summary-json"], capture_output=True, text=True)
    if os.environ["SHAPESHIFT_POSTGRES_PASSWORD"] in p.stdout + p.stderr:
        sys.exit("MISMATCH: the password appeared in shapeshift's output")
    return p


def drain(binary, args, bound):
    news = []
    for _ in range(300):
        p = shape(binary, [*args, "--max-records", str(bound)])
        if p.returncode != 0:
            sys.exit(f"a run failed:\n{p.stdout}{p.stderr}")
        n = json.loads(p.stdout)["source"]["new"]
        news.append(n)
        if n == 0:
            return news
    sys.exit(f"MISMATCH: the runs never ran dry: {news}")


def compare(duck, pg, table, where, what):
    got = duck.sql(
        f"select count(*), count(distinct id), coalesce(sum(amount), 0) from iceberg_scan('{table}')"
    ).fetchall()[0]
    want = pg.execute(
        f"select count(*), count(distinct id), coalesce(sum(amount), 0) from orders {where}"
    ).fetchone()
    if tuple(int(x) for x in got) != tuple(int(x) for x in want):
        sys.exit(f"MISMATCH ({what}): duckdb {got}, postgres {want}")
    print(f"ok  {what}: {want[0]} rows, {want[1]} ids, sum {want[2]}, as Postgres has them")


def position_of(table, kind):
    """The source position the table's current snapshot records, if any."""
    meta_dir = os.path.join(table, "metadata")
    files = sorted(
        (f for f in os.listdir(meta_dir) if f.endswith(".metadata.json")),
        key=lambda f: int(f.split(".")[0].lstrip("v") or 0),
    )
    with open(os.path.join(meta_dir, files[-1])) as f:
        meta = json.load(f)
    head = meta["current-snapshot-id"]
    snap = next(s for s in meta["snapshots"] if s["snapshot-id"] == head)
    raw = snap["summary"].get(f"shapeshift.source.{kind}")
    return json.loads(raw) if raw else None


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", default="target/debug/shapeshift")
    ap.add_argument("--url", default="postgres://postgres@127.0.0.1:5432/ssdb")
    a = ap.parse_args()
    pg = psycopg.connect(a.url, password=os.environ["SHAPESHIFT_POSTGRES_PASSWORD"], autocommit=True)
    # A schema of this run's own: nothing of the database's is touched or dropped.
    schema = f"ss_verify_{os.getpid()}"
    pg.execute(f"create schema {schema}")

    def drop_scratch():
        # On success or failure alike; a failure here never hides the run's own.
        try:
            with psycopg.connect(a.url, password=os.environ["SHAPESHIFT_POSTGRES_PASSWORD"],
                                 autocommit=True) as c:
                c.execute(f"drop schema if exists {schema} cascade")
        except Exception as e:  # noqa: BLE001
            print(f"note: could not drop schema {schema}: {e}", file=sys.stderr)

    atexit.register(drop_scratch)
    pg.execute(f"set search_path to {schema}")
    duck = duckdb.connect()
    duck.sql("INSTALL iceberg; LOAD iceberg;")
    root = tempfile.mkdtemp(prefix="ss-pg-verify-")
    print(f"duckdb {duckdb.__version__}, tables under {root}")

    pg.execute("create table orders (id bigint primary key, updated_at timestamptz not null, "
               "amount int not null, is_deleted boolean not null default false)")
    pg.execute("create index on orders (updated_at)")
    # 20,000 rows over 600 minutes, about 33 to a minute.
    pg.execute("insert into orders select i, timestamptz '2026-11-01 00:00:00+00' + "
               "(i * 7919 % 600) * interval '1 minute', i * 31 % 10000, false "
               "from generate_series(0, 19999) as i")

    t1 = os.path.join(root, "by_table")
    args = ["--from", a.url, "--source-table", f"{schema}.orders", "--watermark", "updated_at",
            "--merge-on", "id", "--to", "iceberg", "-o", t1]
    print(f"    initial load: runs of {drain(a.bin, args, 3000)}")
    compare(duck, pg, t1, "", "table, initial load")
    killed_mid_read = 0
    for r in range(3):
        # Updates are stamped now; new rows after them.
        pg.execute("update orders set amount = amount + 1, updated_at = (select max(updated_at) "
                   f"from orders) + (id % 40 + 1) * interval '1 minute' where id % 13 = {r}")
        pg.execute("insert into orders select i, (select max(updated_at) from orders) + "
                   "(i % 300 + 1) * interval '1 minute', i % 10000, false "
                   "from generate_series((select max(id) + 1 from orders), "
                   "(select max(id) + 1500 from orders)) as i")
        before = duck.sql(f"select count(*), sum(amount) from iceberg_scan('{t1}')").fetchall()
        pos_before = position_of(t1, "postgres")
        # Where a run that reads everything commits: the newest stamp.
        want_end = pg.execute("select to_char(max(updated_at) at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') from orders").fetchone()[0]
        # The run to kill names its own session, and is killed once its read is under way.
        u = urlsplit(a.url)
        query = [(k, v) for k, v in parse_qsl(u.query) if k != "application_name"]
        query.append(("application_name", "ss_verify_kill"))
        kill_url = urlunsplit(u._replace(query=urlencode(query)))
        kill_args = [kill_url if x == a.url else x for x in args]
        proc = subprocess.Popen([a.bin, "shape", *kill_args, "--max-records", "100000"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        started = False
        for _ in range(4000):
            if proc.poll() is not None:
                break
            if pg.execute("select count(*) from pg_stat_activity where application_name = 'ss_verify_kill' "
                          "and state in ('active', 'idle in transaction') "
                          "and query like 'SELECT * FROM %orders% WHERE %'").fetchone()[0]:
                started = True
                break
            time.sleep(0.005)
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
        _, err = proc.communicate()
        err = err.decode(errors="replace").replace(os.environ["SHAPESHIFT_POSTGRES_PASSWORD"], "***")
        # SIGKILL (-9), or it finished first (0). Anything else is the run failing by itself.
        if proc.returncode not in (0, -9):
            sys.exit(f"MISMATCH: the run to be killed failed on its own:\n{err}")
        if started and proc.returncode == -9:
            killed_mid_read += 1
        after = duck.sql(f"select count(*), sum(amount) from iceberg_scan('{t1}')").fetchall()
        pos_after = position_of(t1, "postgres")
        # A commit is all or nothing: the rows and the position move together, or neither does.
        if before == after and pos_after != pos_before:
            sys.exit(f"MISMATCH: the killed run moved the position ({pos_before} -> {pos_after}) "
                     f"and not the rows")
        if before != after:
            if not pos_after or pos_after.get("at") != want_end:
                sys.exit(f"MISMATCH: the run's rows landed with the position {pos_after}, "
                         f"not at {want_end}")
            compare(duck, pg, t1, "", f"round {r + 1}, the run killed after its commit")
        print(f"    round {r + 1}: runs of {drain(a.bin, args, 3000)}")
        compare(duck, pg, t1, "", f"table, round {r + 1}")
    if killed_mid_read == 0:
        sys.exit("MISMATCH: no run was killed while it read: the kill was never tested")
    print(f"    {killed_mid_read} of 3 runs killed while they read")

    t2 = os.path.join(root, "soft")
    spec = os.path.join(root, "soft.yaml")
    with open(spec, "w") as f:
        f.write(f"""dataset: soft
source:
  postgres:
    url: {a.url}
    query: >-
      select id, updated_at, amount, is_deleted from {schema}.orders
      where updated_at >= :cursor order by updated_at, id
    cursor: updated_at
    tiebreak: id
    start: '2026-01-01'
changes:
  op: is_deleted
  update: [false]
  delete: [true]
  order_by: [updated_at]
schema: strict
columns:
  - {{ name: id, type: int64, required: true }}
  - {{ name: updated_at, type: timestamp }}
  - {{ name: amount, type: int64 }}
output: {{ format: iceberg, path: {t2}, merge_on: [id] }}
""")
    drain(a.bin, ["--spec", spec], 4000)
    compare(duck, pg, t2, "where not is_deleted", "soft deletes, initial load")
    pg.execute("update orders set is_deleted = true, updated_at = (select max(updated_at) from "
               "orders) + (id % 40 + 1) * interval '1 minute' where id % 7 = 0")
    print(f"    deletes: runs of {drain(a.bin, ['--spec', spec], 4000)}")
    compare(duck, pg, t2, "where not is_deleted", "soft deletes")
    shutil.rmtree(root, ignore_errors=True)
    print("all checks passed")


if __name__ == "__main__":
    main()
