#!/usr/bin/env python3
"""register_catalog.py — publish a shapeshift Iceberg table into a data catalog.

shapeshift writes a *catalog-less* Iceberg v2 table: data Parquet + Avro manifests +
``metadata/vN.metadata.json`` + ``metadata/version-hint.text`` (a Hadoop-style layout,
paths anchored at the table location). A catalog just needs a *pointer* to the current
``metadata.json`` — no data is copied or rewritten. PyIceberg's ``register_table`` does
exactly that against a REST catalog (Nessie / Polaris / any Iceberg REST), AWS Glue,
Hive, or a SQL catalog.

This is the honest division of labour: shapeshift is the writer; the catalog is the
name→metadata index; the engine (Trino/Spark/Athena/DuckDB) reads through the catalog.

Usage
-----
  # REST catalog (Nessie / Polaris / generic):
  CATALOG_TYPE=rest \
  CATALOG_URI=http://localhost:19120/iceberg/ \
  CATALOG_WAREHOUSE=warehouse \
  python register_catalog.py --table s3://lake/db/events --identifier analytics.events

  # AWS Glue:
  CATALOG_TYPE=glue AWS_REGION=us-east-1 \
  python register_catalog.py --table s3://lake/db/orders --identifier analytics.orders

  # Local table dir (against a local REST/SQL catalog):
  CATALOG_TYPE=sql CATALOG_URI=sqlite:////tmp/cat.db CATALOG_WAREHOUSE=file:///tmp/wh \
  python register_catalog.py --table ./events_tbl --identifier default.events

Re-register after a `shape --append` with `--replace` to move the catalog pointer to the
new current metadata (the table keeps every snapshot).
"""
import argparse
import os
import sys

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError


def _s3_storage_options() -> dict:
    """fsspec/s3fs options for reading the metadata hint from an S3-compatible store.

    This read happens BEFORE the catalog is built, so the catalog's own ``s3.endpoint``
    prop can't help here — thread the endpoint/creds (MinIO, etc.) in from the env so the
    hint read hits the same store the catalog will use."""
    opts: dict = {}
    if ep := os.environ.get("S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT"):
        opts["endpoint_url"] = ep
        opts["config_kwargs"] = {"s3": {"addressing_style": "path"}}  # MinIO = path-style
    if key := os.environ.get("AWS_ACCESS_KEY_ID"):
        opts["key"] = key
    if secret := os.environ.get("AWS_SECRET_ACCESS_KEY"):
        opts["secret"] = secret
    return opts


def _read_text(path: str) -> str:
    """Read a small text file from a local path or an object-store URL."""
    if "://" in path and not path.startswith("file://"):
        import fsspec  # pulled in by pyiceberg[s3fs]/[gcsfs]/[adlfs]

        with fsspec.open(path, "r", **_s3_storage_options()) as f:
            return f.read().strip()
    path = path[len("file://"):] if path.startswith("file://") else path
    with open(path, "r") as f:
        return f.read().strip()


def current_metadata_location(table_loc: str) -> str:
    """Resolve a shapeshift table dir → its *current* metadata.json URI.

    ``metadata/version-hint.text`` holds the current version integer N; the file is
    ``metadata/vN.metadata.json`` (Hadoop convention, what shapeshift writes)."""
    table_loc = table_loc.rstrip("/")
    hint = _read_text(f"{table_loc}/metadata/version-hint.text")
    try:
        n = int(hint.strip())
        return f"{table_loc}/metadata/v{n}.metadata.json"
    except ValueError:
        # Some writers store a filename/relative path in the hint — honour it verbatim.
        return f"{table_loc}/metadata/{hint}"


def build_catalog():
    """Build a PyIceberg catalog (REST/Glue/Hive/SQL) from the CATALOG_*/S3/AWS env vars."""
    ctype = os.environ.get("CATALOG_TYPE", "rest")
    props = {"type": ctype}
    if uri := os.environ.get("CATALOG_URI"):
        props["uri"] = uri
    if wh := os.environ.get("CATALOG_WAREHOUSE"):
        props["warehouse"] = wh
    # REST auth (Polaris/Nessie): a bearer token or OAuth2 client credentials.
    if tok := os.environ.get("CATALOG_TOKEN"):
        props["token"] = tok
    if cred := os.environ.get("CATALOG_CREDENTIAL"):
        props["credential"] = cred
    # S3 data access (MinIO/localstack override the endpoint; Glue/real S3 use env creds).
    if ep := os.environ.get("S3_ENDPOINT"):
        props["s3.endpoint"] = ep
    if region := os.environ.get("AWS_REGION"):
        props["s3.region"] = region
    return load_catalog(os.environ.get("CATALOG_NAME", "shapeshift"), **props)


def main() -> int:
    """Resolve a shapeshift table's current metadata pointer and register (or, with
    --replace, re-register) it under the given identifier in the configured catalog."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", required=True,
                    help="shapeshift Iceberg table location (path or s3://…/gs://…/az://…)")
    ap.add_argument("--identifier", required=True,
                    help="catalog identifier namespace.table (e.g. analytics.events)")
    ap.add_argument("--replace", action="store_true",
                    help="drop an existing entry first (moves the pointer to current metadata)")
    args = ap.parse_args()

    ns, _, name = args.identifier.rpartition(".")
    if not ns:
        print("identifier must be namespace.table (e.g. analytics.events)", file=sys.stderr)
        return 2

    meta = current_metadata_location(args.table)
    cat = build_catalog()
    cat.create_namespace_if_not_exists(ns)

    ident = (ns, name)
    if args.replace:
        # Move the pointer to the current metadata. drop_table removes only the catalog
        # entry (no data files are deleted); catch ONLY "table absent" so auth/network
        # errors still surface instead of being swallowed. Note this leaves a brief window
        # where the table is unregistered — a catalog that supports an atomic
        # register-with-overwrite is preferable in production.
        try:
            cat.drop_table(ident)
        except NoSuchTableError:
            pass  # nothing to replace yet — the register below creates it

    cat.register_table(ident, metadata_location=meta)
    print(f"registered {args.identifier} → {meta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
