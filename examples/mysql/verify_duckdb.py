#!/usr/bin/env python3
"""Check the MySQL source (`source.mysql`) against DuckDB's Iceberg reader.

Against a MySQL 8 server (the password in SHAPESHIFT_MYSQL_PASSWORD), each check pulls a
table with bounded incremental runs (`--max-records`), repeated until one reads nothing new,
and compares DuckDB's count, distinct ids and sums with MySQL's own:

  * a table read by `--source-table --watermark`, its rows updated and new ones inserted
    between rounds, many sharing one `updated_at`;
  * a run killed part-way (`kill -9`): it lands nothing, and the next resumes;
  * soft deletes through `changes:` (`op: is_deleted`, `delete: [true]`), read by a query;
  * a million rows at flat memory: the peak RSS of a 1,000,000-row load is compared with a
    100,000-row one.

Usage (from the workspace root, after `cargo build -p shapeshift-cli --features mysql`):

    .venv/bin/python examples/mysql/verify_duckdb.py --bin target/debug/shapeshift \\
        --url mysql://root@127.0.0.1:3306/ssdb
"""

import argparse
import atexit
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlparse

import duckdb
import pymysql


def db(url):
    u = urlparse(url)
    return pymysql.connect(
        host=u.hostname,
        port=u.port or 3306,
        user=u.username,
        password=os.environ["SHAPESHIFT_MYSQL_PASSWORD"],
        database=u.path.lstrip("/"),
        autocommit=True,
    )


def sql(con, *stmts):
    with con.cursor() as c:
        for s in stmts:
            c.execute(s)
        return c.fetchall()


def shape(binary, args, timeout=600):
    p = subprocess.run([binary, "shape", *args, "--summary-json"], capture_output=True, text=True,
                       timeout=timeout)
    if os.environ["SHAPESHIFT_MYSQL_PASSWORD"] in p.stdout + p.stderr:
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


def compare(duck, mysql, table, where, what):
    got = duck.sql(
        f"select count(*), count(distinct id), coalesce(sum(amount), 0) from iceberg_scan('{table}')"
    ).fetchall()[0]
    (want,) = sql(mysql, f"select count(*), count(distinct id), coalesce(sum(amount), 0) from orders {where}")
    want = tuple(int(x) for x in want)
    if tuple(int(x) for x in got) != want:
        sys.exit(f"MISMATCH ({what}): duckdb {got}, mysql {want}")
    print(f"ok  {what}: {want[0]} rows, {want[1]} ids, sum {want[2]}, as MySQL has them")


def fill(mysql, start, n, rng, minutes=600, after=0):
    """Rows stamped `after` to `after + minutes` minutes into the day: new rows come after the
    newest one (a row stamped before the position is late, and missed by design)."""
    rows = []
    for i in range(start, start + n):
        rows.append(f"({i}, TIMESTAMP('2026-11-01') + INTERVAL {after + rng.randrange(minutes)} MINUTE, "
                    f"{rng.randrange(1, 10000)}, 0)")
        if len(rows) == 5000:
            sql(mysql, "insert into orders values " + ",".join(rows))
            rows = []
    if rows:
        sql(mysql, "insert into orders values " + ",".join(rows))


def peak_rss_kib(binary, args):
    """This one run's peak resident memory, in KiB (wait4 reports the child's own)."""
    p = subprocess.Popen([binary, "shape", *args], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    err = p.stderr.read().decode()
    _, status, usage = os.wait4(p.pid, 0)
    if os.waitstatus_to_exitcode(status) != 0:
        sys.exit(f"the load failed:\n{err}")
    return usage.ru_maxrss


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
    ap.add_argument("--url", default="mysql://root@127.0.0.1:3306/ssdb")
    ap.add_argument("--seed", type=int, default=74)
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    # A database of this run's own: nothing of the server's is touched or dropped.
    scratch = f"ss_verify_{os.getpid()}"
    sql(db(a.url), f"create database {scratch}")
    server_url = a.url

    def drop_scratch():
        # On success or failure alike; a failure here never hides the run's own.
        try:
            sql(db(server_url), f"drop database if exists {scratch}")
        except Exception as e:  # noqa: BLE001
            print(f"note: could not drop database {scratch}: {e}", file=sys.stderr)

    if not a.keep:
        atexit.register(drop_scratch)
    a.url = a.url.rsplit("/", 1)[0] + "/" + scratch
    mysql = db(a.url)
    duck = duckdb.connect()
    duck.sql("INSTALL iceberg; LOAD iceberg;")
    root = tempfile.mkdtemp(prefix="ss-mysql-verify-")
    print(f"duckdb {duckdb.__version__}, seed {a.seed}, tables under {root}")

    sql(mysql, "drop table if exists orders",
        "create table orders (id bigint primary key, updated_at datetime(6) not null, "
        "amount int not null, is_deleted tinyint(1) not null, key (updated_at))")
    fill(mysql, 0, 20_000, rng)

    # A table, by its watermark and primary key.
    t1 = os.path.join(root, "by_table")
    args = ["--from", a.url, "--source-table", "orders", "--watermark", "updated_at",
            "--merge-on", "id", "--to", "iceberg", "-o", t1]
    news = drain(a.bin, args, 3000)
    print(f"    initial load: runs of {news}")
    compare(duck, mysql, t1, "", "table, initial load")
    killed_mid_read = 0
    for r in range(3):
        # An update stamps its row now, after every row already there (as
        # `ON UPDATE CURRENT_TIMESTAMP` does); new rows come after that.
        (top, newest) = sql(mysql, "select max(id), "
                            "timestampdiff(minute, '2026-11-01', max(updated_at)) from orders")[0]
        sql(mysql,
            "update orders set amount = amount + 1, updated_at = timestamp('2026-11-01') + "
            f"interval ({int(newest) + 1} + id % 40) minute where id % 13 = {r}")
        fill(mysql, top + 1, 1500, rng, minutes=300, after=int(newest) + 41)
        # A killed run lands nothing.
        before = duck.sql(f"select count(*), sum(amount) from iceberg_scan('{t1}')").fetchall()
        pos_before = position_of(t1, "mysql")
        # Where a run that reads everything commits: the newest stamp.
        want_end = sql(mysql, "select date_format(max(updated_at), '%Y-%m-%dT%H:%i:%sZ') from orders")[0][0]
        # The run to kill names its own session, and is killed once its read is under way.
        kill_url = a.url
        kill_args = [kill_url if x == a.url else x for x in args]
        proc = subprocess.Popen([a.bin, "shape", *kill_args, "--max-records", "100000"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        started = False
        for _ in range(4000):
            if proc.poll() is not None:
                break
            if sql(mysql, "select count(*) from information_schema.PROCESSLIST where COMMAND in ('Query', 'Execute') and INFO like 'SELECT * FROM `orders` WHERE %'")[0][0]:
                started = True
                break
            time.sleep(0.005)
        if proc.poll() is None:
            proc.send_signal(signal.SIGKILL)
        _, err = proc.communicate()
        err = err.decode(errors="replace").replace(os.environ["SHAPESHIFT_MYSQL_PASSWORD"], "***")
        # SIGKILL (-9), or it finished first (0). Anything else is the run failing by itself.
        if proc.returncode not in (0, -9):
            sys.exit(f"MISMATCH: the run to be killed failed on its own:\n{err}")
        if started and proc.returncode == -9:
            killed_mid_read += 1
        after = duck.sql(f"select count(*), sum(amount) from iceberg_scan('{t1}')").fetchall()
        pos_after = position_of(t1, "mysql")
        # A commit is all or nothing: the rows and the position move together, or neither does.
        if before == after and pos_after != pos_before:
            sys.exit(f"MISMATCH: the killed run moved the position ({pos_before} -> {pos_after}) "
                     f"and not the rows")
        if before != after:
            if not pos_after or pos_after.get("at") != want_end:
                sys.exit(f"MISMATCH: the run's rows landed with the position {pos_after}, "
                         f"not at {want_end}")
            compare(duck, mysql, t1, "", f"round {r + 1}, the run killed after its commit")
        news = drain(a.bin, args, 3000)
        print(f"    round {r + 1}: runs of {news}")
        compare(duck, mysql, t1, "", f"table, round {r + 1}")
    if killed_mid_read == 0:
        sys.exit("MISMATCH: no run was killed while it read: the kill was never tested")
    print(f"    {killed_mid_read} of 3 runs killed while they read")

    # Soft deletes, read by a query, applied through `changes:`.
    t2 = os.path.join(root, "soft")
    spec = os.path.join(root, "soft.yaml")
    with open(spec, "w") as f:
        f.write(f"""dataset: soft
source:
  mysql:
    url: {a.url}
    query: >-
      select id, updated_at, amount, is_deleted from orders
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
    compare(duck, mysql, t2, "where is_deleted = 0", "soft deletes, initial load")
    (newest,) = sql(mysql, "select timestampdiff(minute, '2026-11-01', max(updated_at)) from orders")[0]
    sql(mysql, "update orders set is_deleted = 1, updated_at = timestamp('2026-11-01') + "
               f"interval ({int(newest) + 1} + id % 40) minute where id % 7 = 0")
    news = drain(a.bin, ["--spec", spec], 4000)
    print(f"    deletes: runs of {news}")
    compare(duck, mysql, t2, "where is_deleted = 0", "soft deletes")

    # A million rows at flat memory.
    sql(mysql, "drop table if exists wide",
        "create table wide (id bigint primary key, updated_at datetime not null, "
        "amount int, note varchar(40))",
        "set session cte_max_recursion_depth = 2000000",
        "insert into wide with recursive n(i) as (select 1 union all select i + 1 from n where i < 1000000) "
        "select i, timestamp('2026-01-01') + interval (i div 50) second, i % 977, "
        "concat('note-', i) from n")
    rss = {}
    for n in (100_000, 1_000_000):
        out = os.path.join(root, f"wide_{n}")
        q = f"select * from wide where id <= {n}"
        rss[n] = peak_rss_kib(a.bin, ["--from", a.url, "--query", q, "--to", "iceberg", "-o", out])
        (count,) = duck.sql(f"select count(*) from iceberg_scan('{out}')").fetchall()[0]
        if count != n:
            sys.exit(f"MISMATCH: {count} rows landed, not {n}")
    # Ten times the rows must not need much more memory.
    print(f"ok  1,000,000 rows landed; peak RSS {rss[1_000_000] // 1024} MiB "
          f"(100,000 rows: {rss[100_000] // 1024} MiB)")
    if rss[1_000_000] > rss[100_000] * 1.5 + 64 * 1024:
        sys.exit("MISMATCH: memory grew with the result's size")

    if a.keep:
        print(f"kept {root} and database {scratch}")
    else:
        shutil.rmtree(root, ignore_errors=True)
    print("all checks passed")


if __name__ == "__main__":
    main()
