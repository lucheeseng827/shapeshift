#!/usr/bin/env python3
"""shapeshift against a real Iceberg REST catalog: the 0.5 M4 model-check and the M5 race.

The catalog is Apache Iceberg's own REST fixture (`apache/iceberg-rest-fixture`, the server
Iceberg's clients are tested against). DuckDB reads every table back *through the catalog*
(`ATTACH … (TYPE iceberg)`), attached afresh for every look, so it sees what the catalog
says is current and nothing cached.

`model` (M4): one table, every command that commits through a catalog, checked against a
plain-Python model after each step:
  append (creating the table) · append with an added column · `--merge-on` (delete files and
  a delete manifest in one commit) · `compact` (no delete file left) · `expire` (old
  snapshots gone from the catalog, and the files only they read gone from the store) · a
  run retried under the same `--run-id` (reported as the earlier attempt, nothing added).

`race` (M5): writers append, a merger upserts and a compactor compacts one table at once,
all through `fault_proxy` — a proxy in front of the catalog that refuses some answers and
loses others: it answers 500 without passing a request on, passes it on and answers 500,
passes it on and drops the connection, or drops it before passing it on. Commits, creates
and loads are all fair game. Every write has a run id and is retried, as an orchestrator
would, on exit 75 (refused) and exit 1 (an outcome nobody could tell) until it succeeds.
Then, reading through the catalog itself (not the proxy):
  * DuckDB reads exactly the rows of every run, once;
  * each run id is on exactly one snapshot, and it is the one the run (or its last attempt,
    when it found an earlier attempt's) reported;
  * every snapshot that is not a compaction names a run;
  * every fault kind was met at least once (or the check proves nothing).
Expiry is left out of the race on purpose: a run id is found through the snapshots a table
keeps, so an orchestrator must not retry a run once its snapshot may have been expired. It
runs at the end instead, and the rows are checked again.

`register` (relocation's R3): a table written at a path (a merge, so it has position delete
files), moved, then `shapeshift relocate --catalog … --table …`: the catalog must name it at
the relocated metadata file, and DuckDB read it through the catalog. Relocating again reports
it already registered; an append through the catalog lands where the table is; another table
registered under the name is refused, and the catalog keeps the first. `--tables` is where
the path tables go: somewhere the catalog reads at the same path (its warehouse), as a local
path, a `file://` URL or an `s3://` prefix.

Usage (the catalog must be reachable, and its warehouse readable at the same path here):

    python examples/catalog/verify_catalog.py model --bin target/debug/shapeshift \
        --catalog http://127.0.0.1:8181
    python examples/catalog/verify_catalog.py race --bin target/debug/shapeshift \
        --catalog http://127.0.0.1:8181

The binary needs `--features object_store`. For tables in S3, set the `AWS_*` env as for
`examples/merge/verify_duckdb.py`; DuckDB is pointed at the same endpoint.
"""

import argparse
import http.client
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "merge"))
from verify_duckdb import configure_s3, move_table  # noqa: E402  (beside this harness, not a package)

CONFLICT = 75
COLUMNS = [("id", "int64"), ("writer", "int64"), ("seq", "int64"), ("v", "int64")]


# ---- the catalog, read directly --------------------------------------------------------------


def rest(catalog, method, path, body=None):
    req = urllib.request.Request(
        f"{catalog.rstrip('/')}/v1/{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            text = r.read()
            return r.status, (json.loads(text) if text else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="replace")


def prefix(catalog):
    _, cfg = rest(catalog, "GET", "config")
    # An override wins even when it is empty (it clears a default).
    over, dflt = cfg.get("overrides") or {}, cfg.get("defaults") or {}
    p = (over["prefix"] if "prefix" in over else dflt.get("prefix") or "").strip("/")
    return f"{p}/" if p else ""


def ensure_namespace(catalog, ns):
    code, body = rest(catalog, "POST", f"{prefix(catalog)}namespaces", {"namespace": [ns]})
    if code not in (200, 409):
        sys.exit(f"could not create namespace {ns}: {code} {body}")


def load(catalog, ns, name):
    code, body = rest(catalog, "GET", f"{prefix(catalog)}namespaces/{ns}/tables/{name}")
    if code != 200:
        raise RuntimeError(f"loading {ns}.{name}: {code} {body}")
    return body["metadata"]


def duck(catalog):
    """A DuckDB connection with the catalog attached as `cat` (and S3 configured if set)."""
    con = duckdb.connect()
    con.sql("INSTALL iceberg; LOAD iceberg;")
    if os.environ.get("AWS_ENDPOINT") or os.environ.get("AWS_ACCESS_KEY_ID"):
        configure_s3(con)
    con.sql(f"ATTACH '' AS cat (TYPE iceberg, ENDPOINT '{catalog.rstrip('/')}', "
            "AUTHORIZATION_TYPE 'none')")
    return con


def read_rows(catalog, ns, name, cols="id, writer, seq, v"):
    con = duck(catalog)
    try:
        return sorted(con.sql(f"SELECT {cols} FROM cat.{ns}.{name}").fetchall())
    finally:
        con.close()


# ---- running the binary ----------------------------------------------------------------------


def spec_file(work, name, catalog, table, extra_columns=()):
    cols = "".join(f"  - {{ name: {c}, type: {t} }}\n" for c, t in [*COLUMNS, *extra_columns])
    path = os.path.join(work, f"{name}.spec.yaml")
    with open(path, "w") as f:
        f.write(f"dataset: {name}\nschema: strict\n"
                f"output: {{ format: iceberg, catalog: {{ uri: '{catalog}', table: {table} }} }}\n"
                f"columns:\n{cols}")
    return path


def write_rows(work, rows):
    path = os.path.join(work, f"in-{uuid.uuid4().hex}.jsonl")
    with open(path, "w") as f:
        f.writelines(json.dumps(r) + "\n" for r in rows)
    return path


def run(binary, args):
    """(exit status, the `--summary-json` document, stdout + stderr)."""
    p = subprocess.run([binary, *args, "--summary-json"], capture_output=True, text=True)
    try:
        doc = json.loads(p.stdout)
    except ValueError:
        doc = None
    return p.returncode, doc, p.stdout + p.stderr


def must(binary, args, what):
    code, doc, out = run(binary, args)
    if code != 0:
        sys.exit(f"FAILED {what}: exit {code}\n{out}")
    return doc


# ---- M4: the model-check ---------------------------------------------------------------------


def check(catalog, ns, name, model, step, cols=("id", "writer", "seq", "v")):
    want = sorted(tuple(r.get(c) for c in cols) for r in model.values())
    got = read_rows(catalog, ns, name, ", ".join(cols))
    if got != want:
        lost = sorted(set(want) - set(got))[:5]
        extra = sorted(set(got) - set(want))[:5]
        sys.exit(f"FAILED after {step}: DuckDB read {len(got)} rows through the catalog, the "
                 f"model has {len(want)}\n  missing: {lost}\n  unexpected: {extra}")
    print(f"ok  {step}: {len(got)} rows, exactly the model")


def plain(path):
    """A file path or URL as both DuckDB's glob and Iceberg metadata can be compared by."""
    if path.startswith("file:"):
        return "/" + path[len("file:"):].lstrip("/")
    return path


def stray_data_files(catalog, ns, name, location):
    """Data and delete files under the table's `data/` that no snapshot the catalog keeps reads."""
    con = duck(catalog)
    try:
        on_disk = {plain(p) for (p,) in con.sql(
            f"SELECT file FROM glob('{plain(location).rstrip('/')}/data/**')").fetchall()}
        named = {plain(p) for (p,) in con.sql(
            f"SELECT file_path FROM iceberg_metadata(cat.{ns}.{name})").fetchall()}
    finally:
        con.close()
    return sorted(on_disk - named), len(on_disk)


def cmd_model(a):
    binary = os.path.abspath(a.bin)
    ns = f"m4_{uuid.uuid4().hex[:8]}"
    ensure_namespace(a.catalog, ns)
    table = f"{ns}.t"
    work = tempfile.mkdtemp(prefix="ss-catalog-model-")
    try:
        rng = random.Random(a.seed)
        model = {}
        spec = spec_file(work, "t", a.catalog, table)

        def row(i, seq):
            return {"id": i, "writer": i % 3, "seq": seq, "v": rng.randrange(10**6)}

        # 1. The table does not exist: created empty, then the first snapshot committed onto it.
        rows = [row(i, 1) for i in range(200)]
        doc = must(binary, ["shape", "--spec", spec, "-i", write_rows(work, rows)], "the first append")
        if doc["table"]["version"] is not None:
            sys.exit(f"FAILED: a catalog commit reported a table version of its own: {doc['table']}")
        model.update({r["id"]: r for r in rows})
        check(a.catalog, ns, "t", model, "append creating the table")

        # 2. An append that adds a column: add-schema in the same commit as the snapshot.
        spec2 = spec_file(work, "t2", a.catalog, table, extra_columns=[("channel", "string")])
        rows = [{**row(i, 2), "channel": "web"} for i in range(200, 300)]
        must(binary, ["shape", "--spec", spec2, "-i", write_rows(work, rows), "--append"],
             "an append adding a column")
        for r in model.values():
            r.setdefault("channel", None)
        model.update({r["id"]: r for r in rows})
        check(a.catalog, ns, "t", model, "append adding a column", cols=("id", "writer", "seq", "v", "channel"))
        meta = load(a.catalog, ns, "t")
        if len(meta["schemas"]) < 2:
            sys.exit(f"FAILED: the catalog's table has {len(meta['schemas'])} schema(s), expected the added one")

        # 3. A merge: replaces 100 keys (some on each side of the old/new data) and inserts 50.
        keys = rng.sample(range(300), 100) + list(range(300, 350))
        rows = [{**row(k, 3), "channel": "app"} for k in keys]
        doc = must(binary, ["shape", "--spec", spec2, "-i", write_rows(work, rows), "--merge-on", "id"],
                   "a merge")
        model.update({r["id"]: r for r in rows})
        check(a.catalog, ns, "t", model, "merge through the catalog", cols=("id", "writer", "seq", "v", "channel"))
        meta = load(a.catalog, ns, "t")
        head = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
        if head["summary"].get("operation") != "overwrite" or int(head["summary"].get("added-delete-files", 0)) < 1:
            sys.exit(f"FAILED: the merge's snapshot is not an overwrite with delete files: {head['summary']}")

        # 4. A compaction: one replace snapshot, no delete file left, the same rows.
        doc = must(binary, ["compact", table, "--catalog", a.catalog], "a compaction")
        if not doc["committed"]:
            sys.exit(f"FAILED: the compaction committed nothing: {doc}")
        check(a.catalog, ns, "t", model, "compaction through the catalog", cols=("id", "writer", "seq", "v", "channel"))
        meta = load(a.catalog, ns, "t")
        head = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
        if head["summary"].get("operation") != "replace" or int(head["summary"].get("total-delete-files", 0)) != 0:
            sys.exit(f"FAILED: after compacting, the head is not a replace without delete files: {head['summary']}")

        # 5. An expiry: every snapshot but the head leaves the catalog, and the files only they
        # read leave the store (listed with DuckDB's glob, so local and S3 alike).
        before = {s["snapshot-id"] for s in meta["snapshots"]}
        dry = must(binary, ["expire", table, "--catalog", a.catalog, "--older-than", "0s", "--dry-run"],
                   "a dry-run expiry")
        doc = must(binary, ["expire", table, "--catalog", a.catalog, "--older-than", "0s"], "an expiry")
        if not doc["committed"] or doc["deleted"] != dry["deleted"]:
            sys.exit(f"FAILED: the expiry deleted {doc['deleted']}, its dry run named {dry['deleted']}")
        meta = load(a.catalog, ns, "t")
        after = {s["snapshot-id"] for s in meta["snapshots"]}
        if after != {meta["current-snapshot-id"]} or not after < before:
            sys.exit(f"FAILED: after expiring, the catalog keeps snapshots {after} (head {meta['current-snapshot-id']})")
        check(a.catalog, ns, "t", model, "expiry through the catalog", cols=("id", "writer", "seq", "v", "channel"))
        stray, kept = stray_data_files(a.catalog, ns, "t", meta["location"])
        if stray:
            sys.exit(f"FAILED: {len(stray)} data file(s) left that no snapshot reads, e.g. {stray[:3]}")
        deleted = sum(doc["deleted"][k] for k in ("data_files", "delete_files", "manifests", "manifest_lists"))
        print(f"ok  expiry: {len(before) - 1} snapshot(s) gone, {deleted} file(s) deleted, "
              f"{kept} data file(s) left, every one read by the head")

        # 6. A run retried under its run id commits nothing the second time.
        rows = [{**row(i, 6), "channel": "api"} for i in range(400, 450)]
        path = write_rows(work, rows)
        first = must(binary, ["shape", "--spec", spec2, "-i", path, "--append", "--run-id", "m4-retry"], "a run")
        again = must(binary, ["shape", "--spec", spec2, "-i", path, "--append", "--run-id", "m4-retry"], "its retry")
        if not again["table"]["earlier_attempt"] or again["table"]["snapshot_id"] != first["table"]["snapshot_id"]:
            sys.exit(f"FAILED: the retry did not report the earlier attempt: {again['table']} vs {first['table']}")
        model.update({r["id"]: r for r in rows})
        check(a.catalog, ns, "t", model, "a run retried under its run id", cols=("id", "writer", "seq", "v", "channel"))
        print(f"ok  M4 model-check against {a.catalog}: append, add-column, merge, compact, expire, run-id retry")
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---- R3: relocate and register ---------------------------------------------------------------


def path_spec(work, name, path):
    cols = "".join(f"  - {{ name: {c}, type: {t} }}\n" for c, t in COLUMNS)
    spec = os.path.join(work, f"{name}.spec.yaml")
    with open(spec, "w") as f:
        f.write(f"dataset: {name}\nschema: strict\n"
                f"output: {{ format: iceberg, path: {json.dumps(path)} }}\ncolumns:\n{cols}")
    return spec


def cmd_register(a):
    binary = os.path.abspath(a.bin)
    ns = f"r3_{uuid.uuid4().hex[:8]}"
    ensure_namespace(a.catalog, ns)
    table = f"{ns}.t"
    base = f"{a.tables.rstrip('/')}/{ns}"
    if "://" not in base:
        os.makedirs(base)
    work = tempfile.mkdtemp(prefix="ss-catalog-register-")
    try:
        rng = random.Random(a.seed)
        model = {}

        def row(i, seq):
            return {"id": i, "writer": i % 3, "seq": seq, "v": rng.randrange(10**6)}

        # A table at a path, with a merge (so position delete files name its data files).
        spec = path_spec(work, "t", f"{base}/a")
        rows = [row(i, 1) for i in range(120)]
        must(binary, ["shape", "--spec", spec, "-i", write_rows(work, rows)], "the table at a path")
        model.update({r["id"]: r for r in rows})
        rows = [row(i, 2) for i in rng.sample(range(120), 40) + list(range(120, 140))]
        must(binary, ["shape", "--spec", spec, "-i", write_rows(work, rows), "--merge-on", "id"],
             "a merge into it")
        model.update({r["id"]: r for r in rows})

        # Moved, then relocated and registered in one run.
        move_table(f"{base}/a", f"{base}/b")
        moved = f"{base}/b"
        register = ["relocate", moved, "--catalog", a.catalog, "--table", table]
        doc = must(binary, register, "relocating and registering")
        if (not doc["committed"] or doc["catalog"]["registered"] is not True
                or doc["rewritten"]["delete_files"] < 1):
            sys.exit(f"FAILED: the relocation did not rewrite and register the table: {doc}")
        meta = load(a.catalog, ns, "t")
        if plain(meta["location"]).rstrip("/") != plain(doc["to"]).rstrip("/"):
            sys.exit(f"FAILED: the catalog's table is at {meta['location']}, relocated to {doc['to']}")
        check(a.catalog, ns, "t", model, "relocated and registered, read through the catalog")

        # Again: already anchored, and already registered at that file.
        again = must(binary, register, "relocating and registering again")
        if not again["already_anchored"] or again["catalog"]["registered"] is not False:
            sys.exit(f"FAILED: a second run was not a no-op: {again}")
        print("ok  a second relocate --catalog: already anchored and registered")

        # Written through the catalog: the snapshot lands where the table is.
        spec2 = spec_file(work, "t2", a.catalog, table)
        rows = [row(i, 3) for i in range(140, 180)]
        must(binary, ["shape", "--spec", spec2, "-i", write_rows(work, rows), "--append"],
             "an append through the catalog")
        model.update({r["id"]: r for r in rows})
        check(a.catalog, ns, "t", model, "an append through the catalog onto the registered table")
        meta = load(a.catalog, ns, "t")
        here = plain(doc["to"]).rstrip("/") + "/"
        stray = [s["manifest-list"] for s in meta["snapshots"] if not plain(s["manifest-list"]).startswith(here)]
        if stray:
            sys.exit(f"FAILED: manifest lists not where the table is: {stray}")

        # The same command once more, after that write (a retried pipeline step): the catalog
        # holds a later version of the file it registered, whose metadata log names it.
        later = must(binary, register, "relocating and registering after a write through the catalog")
        if later["catalog"]["registered"] is not False:
            sys.exit(f"FAILED: a rerun after a write through the catalog: {later}")
        check(a.catalog, ns, "t", model, "the same relocate --catalog after that write: still registered")

        # Another table under the name: refused, and the catalog keeps the first.
        other = path_spec(work, "o", f"{base}/other")
        must(binary, ["shape", "--spec", other, "-i", write_rows(work, [row(1, 9)])], "another table")
        code, _, out = run(binary, ["relocate", f"{base}/other", "--catalog", a.catalog, "--table", table])
        if code != 1 or "already has" not in out:
            sys.exit(f"FAILED: registering another table under {table}: exit {code}\n{out}")
        if load(a.catalog, ns, "t")["table-uuid"] != meta["table-uuid"]:
            sys.exit("FAILED: the catalog's table was replaced")
        check(a.catalog, ns, "t", model, "another table refused the name")
        print(f"ok  R3 against {a.catalog}: moved, relocated, registered, read and written through "
              f"the catalog, tables under {a.tables}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ---- M5: the fault proxy ---------------------------------------------------------------------

FAULTS = ("500-before", "500-after", "drop-after", "drop-before")


class FaultProxy:
    """A proxy in front of the catalog that loses or refuses some answers.

    `rate` of commits and creates (POSTs to …/tables[/name]) meet a fault, and `load_rate` of
    loads (GETs of a table). A load that fails right after a commit whose answer was lost is
    what leaves a writer unable to tell whether it committed (exit 1).
    """

    def __init__(self, upstream, rate, load_rate, seed):
        u = urllib.parse.urlsplit(upstream)
        if u.scheme != "http" or not u.hostname:
            raise SystemExit(f"the fault proxy forwards to an http:// catalog, not {upstream!r}")
        self.host, self.port = u.hostname, u.port or 80
        # A catalog served under a path (`http://host:8181/iceberg`) gets it in front of every
        # request the proxy passes on.
        self.base = u.path.rstrip("/")
        self.rate, self.load_rate = rate, load_rate
        self.rng, self.lock = random.Random(seed), threading.Lock()
        # Counted where a fault is injected. An "after" fault on a commit (a POST to a table)
        # counts only when the catalog took it (2xx): an applied commit whose answer was lost.
        # The same on a create (a POST to `…/tables`), one the catalog refused anyway, and an
        # upstream that failed by itself are counted apart.
        self.counts = {f: 0 for f in FAULTS}
        self.counts.update(passed=0, after_refused=0, upstream_error=0,
                           **{f"{f} (create)": 0 for f in ("500-after", "drop-after")})
        self.on = True
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                proxy.handle(self, "GET")

            def do_POST(self):
                proxy.handle(self, "POST")

            def do_HEAD(self):
                proxy.handle(self, "HEAD")

            def do_DELETE(self):
                proxy.handle(self, "DELETE")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def pick(self, method, path):
        writes = method == "POST" and "/tables" in path
        loads = method == "GET" and "/tables/" in path
        rate = self.rate if writes else self.load_rate if loads else 0
        with self.lock:
            if not self.on or self.rng.random() >= rate:
                return None
            # A load has nothing to apply: only its answer can be lost.
            return self.rng.choice(FAULTS if writes else ("500-before", "drop-before"))

    def count(self, what):
        with self.lock:
            self.counts[what] += 1

    def handle(self, h, method):
        length = int(h.headers.get("Content-Length") or 0)
        body = h.rfile.read(length) if length else None
        fault = self.pick(method, h.path)
        if fault == "drop-before":
            self.count(fault)
            h.close_connection = True
            h.connection.shutdown(socket.SHUT_RDWR)
            return
        if fault == "500-before":
            self.count(fault)
            return self.reply(h, 500, b'{"error":{"message":"injected","type":"ServiceFailureException","code":500}}', {})
        conn = http.client.HTTPConnection(self.host, self.port, timeout=60)
        headers = {k: v for k, v in h.headers.items() if k.lower() not in ("host", "connection")}
        try:
            conn.request(method, self.base + h.path, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
        except (OSError, http.client.HTTPException) as e:
            self.count("upstream_error")
            return self.reply(h, 502, json.dumps({"error": {"message": f"upstream: {e}", "type": "BadGateway", "code": 502}}).encode(), {})
        finally:
            conn.close()
        if fault in ("drop-after", "500-after"):
            create = h.path.rstrip("/").endswith("/tables")
            if not 200 <= resp.status < 300:
                self.count("after_refused")
            else:
                self.count(f"{fault} (create)" if create else fault)
        elif fault is None:
            self.count("passed")
        if fault == "drop-after":
            h.close_connection = True
            h.connection.shutdown(socket.SHUT_RDWR)
            return
        if fault == "500-after":
            return self.reply(h, 500, b'{"error":{"message":"injected after applying","type":"ServiceFailureException","code":500}}', {})
        keep = {k: v for k, v in resp.getheaders() if k.lower() in ("content-type", "location", "etag")}
        self.reply(h, resp.status, data, keep)

    @staticmethod
    def reply(h, status, data, headers):
        h.send_response(status)
        for k, v in headers.items():
            h.send_header(k, v)
        h.send_header("Content-Length", str(len(data)))
        h.end_headers()
        h.wfile.write(data)


class Inconclusive(Exception):
    """The race passed, but did not exercise everything it is meant to: run it again."""


# ---- M5: the race ----------------------------------------------------------------------------


class Worker(threading.Thread):
    """Runs its writes one after another, each under its own run id, retrying each until it
    succeeds (exit 75 or 1) the way an orchestrator retries a task."""

    def __init__(self, binary, spec, work, plan, name, run_ids=True, max_tries=60):
        super().__init__(name=name)
        self.binary, self.spec, self.work, self.plan = binary, spec, work, plan
        self.run_ids = run_ids
        self.max_tries = max_tries
        self.done = []  # (run id, snapshot id, earlier_attempt, rows)
        self.rebases = 0  # appends rebuilt onto a newer head inside shapeshift (M3)
        self.retries = {CONFLICT: 0, 1: 0}
        self.error = None

    def run(self):
        try:
            for run_id, rows, merge in self.plan:
                path = write_rows(self.work, rows)
                args = ["shape", "--spec", self.spec, "-i", path]
                args += ["--run-id", run_id] if self.run_ids else []
                args += ["--merge-on", "id"] if merge else ["--append"]
                out = ""
                for _ in range(self.max_tries):
                    code, doc, out = run(self.binary, args)
                    if code == 0:
                        t = doc["table"]
                        self.done.append((run_id, int(t["snapshot_id"]), bool(t.get("earlier_attempt")), rows))
                        self.rebases += int(t.get("rebases") or 0)
                        break
                    if code in (CONFLICT, 1):
                        self.retries[code] += 1
                        time.sleep(random.random() * 0.05)
                        continue
                    raise RuntimeError(f"{run_id}: exit {code}\n{out}")
                else:
                    raise RuntimeError(f"{run_id}: no success after {self.max_tries} attempts; the last "
                                       f"said:\n{out}")
        except Exception as e:  # noqa: BLE001 — reported by the main thread
            self.error = e


class Compactor(threading.Thread):
    def __init__(self, binary, table, catalog, stop):
        super().__init__(name="compactor")
        self.binary, self.table, self.catalog, self.stop = binary, table, catalog, stop
        self.committed, self.refused, self.unknown, self.error = 0, 0, 0, None

    def run(self):
        try:
            while not self.stop.is_set():
                code, doc, out = run(self.binary, ["compact", self.table, "--catalog", self.catalog,
                                                   "--target-file-size", "1MiB"])
                if code == 0:
                    self.committed += bool(doc and doc["committed"])
                elif code == CONFLICT:
                    self.refused += 1
                elif code == 1:
                    self.unknown += 1
                else:
                    raise RuntimeError(f"compact: exit {code}\n{out}")
                time.sleep(0.05)
        except Exception as e:  # noqa: BLE001
            self.error = e


def cmd_race(a):
    binary = os.path.abspath(a.bin)
    ns = f"m5_{uuid.uuid4().hex[:8]}"
    ensure_namespace(a.catalog, ns)
    table = f"{ns}.race"
    work = tempfile.mkdtemp(prefix="ss-catalog-race-")
    proxy = None
    try:
        proxy = FaultProxy(a.catalog, a.fault_rate, a.load_fault_rate, a.seed)
        spec = spec_file(work, "race", proxy.url, table)
        rng = random.Random(a.seed)
        # Appenders own disjoint keys; the merger upserts its own range with values that depend
        # only on the key and the round, so the rows the table must end with do not depend on
        # the order the commits landed in, only on every run landing once.
        # Key ranges are derived from the race's size, so no two runs share a key at any size.
        plans, expected = [], {}
        per_writer = a.rounds * a.per_round
        merge_base = a.writers * per_writer
        for w in range(a.writers):
            plan = []
            for r in range(a.rounds):
                rows = [{"id": w * per_writer + r * a.per_round + i, "writer": w, "seq": r, "v": rng.randrange(10**6)}
                        for i in range(a.per_round)]
                plan.append((f"w{w}-r{r}", rows, False))
                expected.update({x["id"]: x for x in rows})
            plans.append((f"writer{w}", plan))
        merge_plan = []
        for r in range(a.rounds):
            rows = [{"id": merge_base + k, "writer": -1, "seq": r, "v": k * 7 + r} for k in range(r * 10, r * 10 + 30)]
            merge_plan.append((f"merge-r{r}", rows, True))
        plans.append(("merger", merge_plan))
        # The merges run in order on one thread, so each key ends at its last round's row.
        for _, rows, _ in merge_plan:
            expected.update({x["id"]: x for x in rows})

        print(f"{a.writers} appenders x {a.rounds} runs, a merger, a compactor; faults on "
              f"{a.fault_rate:.0%} of writes and {a.load_fault_rate:.0%} of loads through "
              f"{proxy.url} -> {a.catalog}" + (" (negative control)" if a.negative_control else ""))
        workers = [Worker(binary, spec, work, plan, name, run_ids=not a.negative_control)
                   for name, plan in plans]
        stop = threading.Event()
        compactor = Compactor(binary, table, proxy.url, stop)
        t0 = time.perf_counter()
        # The table is created by whichever first run gets there (a create may meet a fault too).
        # The compactor starts once it exists: compacting a table that is not there yet is no
        # part of the race.
        for t in workers:
            t.start()
        while not any(t.done for t in workers):
            if all(not t.is_alive() for t in workers):
                break
            time.sleep(0.05)
        compactor.start()
        for t in workers:
            t.join()
        stop.set()
        compactor.join()
        elapsed = time.perf_counter() - t0
        proxy.on = False
        for t in [*workers, compactor]:
            if t.error:
                sys.exit(f"FAILED: {t.name}: {t.error}")

        # What the catalog itself (no proxy) holds.
        got = read_rows(a.catalog, ns, "race")
        want = sorted((x["id"], x["writer"], x["seq"], x["v"]) for x in expected.values())
        meta = load(a.catalog, ns, "race")
        snaps = meta["snapshots"]
        by_run = {}
        for s in snaps:
            rid = s.get("summary", {}).get("shapeshift.run-id")
            if rid is not None:
                by_run.setdefault(rid, []).append(s["snapshot-id"])
            elif s["summary"].get("operation") != "replace" and not a.negative_control:
                sys.exit(f"FAILED: snapshot {s['snapshot-id']} ({s['summary'].get('operation')}) names no run")
        reported = {run_id: (sid, earlier) for t in workers for run_id, sid, earlier, _ in t.done}
        twice = {k: v for k, v in by_run.items() if len(v) > 1}
        # Both ways: a snapshot naming a run nobody reported is as wrong as a run with none.
        unreported = sorted(set(by_run) - set(reported))
        # A compaction that landed during the writes: a replace with a writer's snapshot after it
        # (one still in flight when the writers stopped would not show that it raced them).
        seq = lambda s: s.get("sequence-number", 0)  # noqa: E731
        last_write = max((seq(s) for s in snaps if s["summary"].get("shapeshift.run-id")), default=0)
        compactions = sum(1 for s in snaps
                          if s["summary"].get("operation") == "replace" and seq(s) < last_write)
        missing = sorted(set(reported) - set(by_run))
        wrong = {k: (by_run.get(k), v[0]) for k, v in reported.items() if by_run.get(k) != [v[0]]}
        retried = {c: sum(t.retries[c] for t in workers) for c in (CONFLICT, 1)}
        earlier = sum(1 for _, e in reported.values() if e)
        rebases = sum(t.rebases for t in workers)
        stats = (f"{len(reported)} runs, {rebases} rebased past another writer, {retried[CONFLICT]} retried after exit 75, {retried[1]} after an "
                 f"unknown outcome (exit 1), {earlier} found their earlier attempt; compactor "
                 f"{compactor.committed} committed / {compactor.refused} refused / {compactor.unknown} unknown; "
                 f"faults {dict((k, v) for k, v in proxy.counts.items())}; {elapsed:.1f} s")
        if a.negative_control:
            # The control must show the check catching what run ids prevent: a run's rows landed
            # twice. Counted by id; anything else wrong (a row lost or changed) is a failure.
            ids = [r[0] for r in got]
            dup = len(ids) - len(set(ids))
            if dup > 0 and set(got) == set(want):
                print(f"ok  negative control: retried without run ids, {dup} row(s) landed twice "
                      f"({len(got)} read, {len(want)} written): the check catches it\n    {stats}")
                return
            if set(got) != set(want):
                lost = sorted(set(want) - set(got))[:5]
                extra = sorted(set(got) - set(want))[:5]
                sys.exit(f"FAILED: the negative control lost or changed rows, not only doubled them\n"
                         f"  missing: {lost}\n  unexpected: {extra}\n  {stats}")
            raise Inconclusive(f"negative control: retries without run ids still read exactly right "
                     f"({stats}); no fault landed a commit whose answer was lost — raise --fault-rate")
        if got != want:
            lost = sorted(set(want) - set(got))[:5]
            extra = sorted(set(got) - set(want))[:5]
            dup = len(got) - len(set(got))
            sys.exit(f"FAILED: DuckDB read {len(got)} rows ({dup} duplicated), expected {len(want)}\n"
                     f"  missing: {lost}\n  unexpected: {extra}\n  {stats}")
        if twice or missing or wrong or unreported:
            sys.exit(f"FAILED: run ids on more than one snapshot {twice}; runs with no snapshot "
                     f"{missing[:5]}; runs whose snapshot is not the one they reported "
                     f"{dict(list(wrong.items())[:5])}; snapshots of runs no worker reported "
                     f"{unreported[:5]}\n  {stats}")
        unmet = [f for f in FAULTS if proxy.counts[f] == 0]
        if earlier == 0:
            unmet.append("a lost commit retried (no run found its earlier attempt)")
        if compactions == 0:
            unmet.append("a compaction landing during the writes")
        if retried[CONFLICT] == 0 and rebases == 0:
            unmet.append("contention (no run was refused or rebased: writers never raced)")
        if unmet:
            raise Inconclusive(f"correct, but not exercised: {unmet} ({stats})")
        print(f"ok  M5 race: {len(got)} rows exactly once; every run id on exactly the snapshot it reported\n    {stats}")

        # Expiry, once the writers are done (see the module docstring), through the catalog.
        doc = must(binary, ["expire", table, "--catalog", a.catalog, "--older-than", "0s", "--retain-last", "1"],
                   "the expiry after the race")
        got = read_rows(a.catalog, ns, "race")
        if got != want:
            sys.exit(f"FAILED: after the expiry DuckDB reads {len(got)} rows, expected {len(want)}")
        print(f"ok  expiry after the race: {len(doc['snapshots']['expired'])} snapshot(s) expired, rows unchanged")
    finally:
        if proxy:
            proxy.server.shutdown()
        shutil.rmtree(work, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("model", "race", "register"):
        p = sub.add_parser(name)
        p.add_argument("--bin", default="target/debug/shapeshift")
        p.add_argument("--catalog", default="http://127.0.0.1:8181")
        p.add_argument("--seed", type=int, default=74)
        if name == "register":
            p.add_argument("--tables", required=True,
                           help="where the path tables go, read by the catalog at the same path")
        if name == "race":
            p.add_argument("--writers", type=int, default=4)
            p.add_argument("--rounds", type=int, default=8)
            p.add_argument("--per-round", type=int, default=40)
            p.add_argument("--fault-rate", type=float, default=0.25)
            p.add_argument("--load-fault-rate", type=float, default=0.4)
            p.add_argument("--negative-control", action="store_true",
                           help="retry without run ids: succeed only if the check catches rows landing twice")
    a = ap.parse_args()
    # shapeshift writes its sidecars (rejects, drift) to the working directory: run it in a
    # scratch one, so the checkout may be read-only.
    a.bin = os.path.abspath(a.bin)
    # A relative --tables is the caller's, not the scratch directory's.
    if a.cmd == "register" and "://" not in a.tables:
        a.tables = os.path.abspath(a.tables)
    cwd = tempfile.mkdtemp(prefix="ss-catalog-cwd-")
    os.chdir(cwd)
    try:
        if a.cmd == "model":
            return cmd_model(a)
        if a.cmd == "register":
            return cmd_register(a)
        # A race is random: one that was correct but did not meet every fault proves less, and is
        # run again with another seed (a wrong one fails at once).
        for _ in range(3):
            try:
                return cmd_race(a)
            except Inconclusive as e:
                print(f"..  inconclusive (seed {a.seed}): {e}; again with another seed")
                a.seed += 1000
        sys.exit("FAILED: three races in a row did not exercise every fault; raise the fault rates")
    finally:
        os.chdir("/")
        shutil.rmtree(cwd, ignore_errors=True)


if __name__ == "__main__":
    main()
