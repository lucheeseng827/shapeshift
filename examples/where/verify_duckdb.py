#!/usr/bin/env python3
"""Check `shape --where` against DuckDB, running the very same expression as SQL.

The row filter's grammar is a subset of SQL's WHERE, so DuckDB is an oracle for it. This
generates records (with nulls and missing fields on purpose) and random expressions over
them, then for each expression requires that the rows shapeshift lands are exactly the rows
`SELECT … FROM read_json(…) WHERE <the same text>` returns, and that every other record was
counted as filtered.

Comparisons always put a field against a value of its own kind: DuckDB casts across kinds
where shapeshift calls the comparison unknown (and reports it), and that difference is
covered by the unit tests instead.

A control runs alongside: each expression is also evaluated with unknown counted as true
(`(<expr>) is not false`). Enough of them must come out differently, or the generator is
not exercising SQL's three-valued logic and the check would pass a filter that got it wrong.

Usage (from the workspace root, after `cargo build -p shapeshift-cli`):

    python3 -m venv .venv && .venv/bin/pip install duckdb
    .venv/bin/python examples/where/verify_duckdb.py --bin target/debug/shapeshift

Exit status 0 means every expression matched.
"""

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile

import duckdb

KINDS = ["gate", "queue", "turnover", "it's", "Gate", "é"]
PLANS = ["pro", "free", "team"]

COLUMNS = {
    "id": "BIGINT",
    "kind": "VARCHAR",
    "status": "BIGINT",
    "score": "DOUBLE",
    "flag": "BOOLEAN",
    "account": "STRUCT(plan VARCHAR)",
}

SPEC = """\
dataset: w
schema: strict
output: {{ format: parquet, path: {out} }}
columns:
  - {{ name: id, type: int64, required: true }}
  - {{ name: kind, type: string }}
  - {{ name: status, type: int64 }}
  - {{ name: score, type: float64 }}
  - {{ name: flag, type: bool }}
  - {{ name: plan, from: account.plan, type: string }}
"""


def record(rng, i):
    """One record; each field is present, null or missing, with fixed kinds."""
    r = {"id": i}

    def maybe(name, value):
        roll = rng.random()
        if roll < 0.15:
            return  # missing
        r[name] = None if roll < 0.25 else value

    maybe("kind", rng.choice(KINDS))
    maybe("status", rng.choice([200, 201, 404, 500, 503, -1]))
    maybe("score", rng.choice([0.0, 0.5, 2.5, 99.75, -3.25, 1e3]))
    maybe("flag", rng.choice([True, False]))
    if rng.random() < 0.8:
        r["account"] = {} if rng.random() < 0.2 else {"plan": rng.choice(PLANS)}
    return r


def sql_str(s):
    return "'" + s.replace("'", "''") + "'"


def kw(rng, word):
    """Keywords in any case: both readers treat them alike."""
    return rng.choice([word, word.upper(), word.capitalize()])


def test(rng):
    """One test, with a field compared only against values of its own kind."""
    op = rng.choice(["=", "!=", "<", "<=", ">", ">="])
    choice = rng.randrange(9)
    if choice == 0:
        return f"kind {op} {sql_str(rng.choice(KINDS + ['h', 'a']))}"
    if choice == 1:
        return f"status {op} {rng.choice([200, 404, 500, 0, -1, 404.5])}"
    if choice == 2:
        return f"score {op} {rng.choice([0, 0.5, 2.5, 100, -3.25, 1e3])}"
    if choice == 3:
        return f"flag {rng.choice(['=', '!='])} {kw(rng, rng.choice(['true', 'false']))}"
    if choice == 4:
        return f"account.plan {op} {sql_str(rng.choice(PLANS))}"
    if choice == 5:
        values = ", ".join(sql_str(k) for k in rng.sample(KINDS, rng.randrange(1, 4)))
        neg = kw(rng, "not") + " " if rng.random() < 0.4 else ""
        return f"kind {neg}{kw(rng, 'in')} ({values})"
    if choice == 6:
        values = ", ".join(str(s) for s in rng.sample([200, 404, 500, 503], rng.randrange(1, 4)))
        neg = kw(rng, "not") + " " if rng.random() < 0.4 else ""
        return f"status {neg}{kw(rng, 'in')} ({values})"
    if choice == 7:
        field = rng.choice(["kind", "status", "score", "flag", "account", "account.plan"])
        neg = kw(rng, "not") + " " if rng.random() < 0.5 else ""
        return f"{field} {kw(rng, 'is')} {neg}{kw(rng, 'null')}"
    # A literal on the left reads the same.
    return f"{rng.choice([200, 500])} {op} status"


def expr(rng, depth=0):
    roll = rng.random()
    if depth >= 3 or roll < 0.35:
        return test(rng)
    if roll < 0.5:
        return f"{kw(rng, 'not')} ({expr(rng, depth + 1)})"
    joiner = kw(rng, rng.choice(["and", "or"]))
    parts = [expr(rng, depth + 1) for _ in range(rng.randrange(2, 4))]
    text = f" {joiner} ".join(parts)
    # Parenthesized or not: precedence must agree too.
    return f"({text})" if rng.random() < 0.5 else text


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--bin", default="target/debug/shapeshift", help="the shapeshift binary")
    ap.add_argument("--seed", type=int, default=74)
    ap.add_argument("--records", type=int, default=400)
    ap.add_argument("--exprs", type=int, default=300)
    a = ap.parse_args()
    binary = os.path.abspath(a.bin)
    rng = random.Random(a.seed)
    root = tempfile.mkdtemp(prefix="shapeshift-where-")
    try:
        data = os.path.join(root, "in.jsonl")
        with open(data, "w") as f:
            for i in range(a.records):
                f.write(json.dumps(record(rng, i)) + "\n")
        out = os.path.join(root, "out.parquet")
        spec = os.path.join(root, "w.yaml")
        with open(spec, "w") as f:
            f.write(SPEC.format(out=out))

        con = duckdb.connect()
        cols = "{" + ", ".join(f"'{k}': '{v}'" for k, v in COLUMNS.items()) + "}"
        con.sql(
            f"create table t as select * from read_json('{data}', "
            f"format='newline_delimited', columns={cols})"
        )
        unknown_mattered = 0
        kept_total = 0
        for n in range(a.exprs):
            text = expr(rng)
            want = sorted(r[0] for r in con.sql(f"select id from t where {text}").fetchall())
            lenient = sorted(
                r[0] for r in con.sql(f"select id from t where ({text}) is not false").fetchall()
            )
            unknown_mattered += want != lenient
            # A fresh output each time, so a run that wrote nothing cannot be read as one
            # that wrote the last expression's rows.
            if os.path.exists(out):
                os.remove(out)
            run = subprocess.run(
                [binary, "shape", "-s", spec, "-i", data, "--where", text, "--summary-json"],
                capture_output=True,
                text=True,
            )
            if run.returncode != 0:
                sys.exit(f"[{n}] shapeshift refused `{text}`:\n{run.stdout}{run.stderr}")
            doc = json.loads(run.stdout)
            got = sorted(r[0] for r in con.sql(f"select id from '{out}'").fetchall())
            if got != want:
                sys.exit(
                    f"[{n}] `{text}`: shapeshift kept {len(got)} record(s), DuckDB {len(want)}"
                    f"\n  only shapeshift: {sorted(set(got) - set(want))[:10]}"
                    f"\n  only DuckDB:     {sorted(set(want) - set(got))[:10]}"
                )
            rows = doc["rows"]
            if (rows["in"], rows["out"], rows["filtered"], rows["rejected"]) != (
                a.records, len(want), a.records - len(want), 0
            ):
                sys.exit(f"[{n}] `{text}`: the summary's counts are {rows}")
            if doc["where"]["type_mismatches"]:
                sys.exit(f"[{n}] `{text}`: a same-kind comparison was reported as a mismatch")
            kept_total += len(want)
        # Most expressions should have to get unknown right to pass.
        if unknown_mattered < a.exprs // 4:
            sys.exit(
                f"control failed: only {unknown_mattered} of {a.exprs} expressions depend on how "
                "unknown is treated, so the check cannot tell SQL's logic from a wrong one"
            )
        print(
            f"ok  {a.exprs} expressions over {a.records} records (seed {a.seed}) kept exactly "
            f"DuckDB's rows ({kept_total} in all); {unknown_mattered} of them depend on SQL's "
            "unknown"
        )
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
