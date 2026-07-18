# Demo 3 — shapeshift output → database sink

**Goal:** use a shapeshift-shaped table as the **source** for a **database sink** —
Snowflake, Postgres, ClickHouse, or DuckDB. This is the other half of the loop: shapeshift
lands governed Parquet/Iceberg once; every warehouse reads the *same* files.

```text
shapeshift ──▶ Iceberg / Parquet on S3  ──┬──▶ Snowflake  (Iceberg table / external table)
                (+ catalog entry)          ├──▶ Trino / Athena / Spark  (catalog)
                                           ├──▶ ClickHouse  (iceberg() / s3() table function)
                                           └──▶ DuckDB → Postgres  (iceberg_scan → COPY/ATTACH)
```

**Why this is cheap.** shapeshift already wrote open **Parquet** data files and standard
**Iceberg v2** metadata. A "database sink" is therefore either (a) a *zero-copy* attach —
the DB reads the files in place through the catalog — or (b) a one-time bulk load. Either
way there is no per-row connector bill and no proprietary format in the middle.

Assumes you have a table from [Demo 1](./01-streaming-to-iceberg-catalog.md) or
[Demo 2](./02-database-to-iceberg-catalog.md) at `s3://lake/db/orders`, registered as
`analytics.orders`.

## A. DuckDB — the fast local sink (zero-copy read, verified)

DuckDB reads both output formats with no JVM or server — the quickest way to validate a
shape and to pump rows onward:

```sql
INSTALL iceberg; LOAD iceberg;
-- catalog-less: point straight at the table location
SELECT status, count(*), sum(amount_cents)/100.0 AS dollars
FROM iceberg_scan('s3://lake/db/orders')     -- or a local path: 'out/orders_tbl'
GROUP BY status;
```

Then land it into a real database from DuckDB (Postgres shown; the `postgres` extension
does a bulk insert):

```sql
INSTALL postgres; LOAD postgres;
ATTACH 'host=localhost dbname=warehouse user=postgres' AS pg (TYPE postgres);
CREATE TABLE pg.public.orders AS SELECT * FROM iceberg_scan('s3://lake/db/orders');
```

See [`scripts/read_back.sql`](./scripts/read_back.sql) for a ready-to-edit version
(local Parquet, local Iceberg, S3/MinIO, latest-per-key dedup, Postgres load).

## B. Snowflake — Iceberg table (zero-copy) or external table

**Iceberg table** (Snowflake reads the shapeshift metadata directly; no data copied):

```sql
-- one-time: tell Snowflake where the bucket and metadata live
CREATE OR REPLACE EXTERNAL VOLUME lake_vol
  STORAGE_LOCATIONS = ((NAME='lake' STORAGE_PROVIDER='S3'
    STORAGE_BASE_URL='s3://lake/' STORAGE_AWS_ROLE_ARN='arn:aws:iam::…:role/…'));
CREATE OR REPLACE CATALOG INTEGRATION lake_cat
  CATALOG_SOURCE=OBJECT_STORE TABLE_FORMAT=ICEBERG ENABLED=TRUE;

CREATE OR REPLACE ICEBERG TABLE orders
  EXTERNAL_VOLUME='lake_vol' CATALOG='lake_cat'
  METADATA_FILE_PATH='db/orders/metadata/v3.metadata.json';   -- current metadata.json

SELECT status, count(*) FROM orders GROUP BY status;
```

After a `shape --append`, point Snowflake at the new metadata:

```sql
ALTER ICEBERG TABLE orders REFRESH 'db/orders/metadata/v4.metadata.json';
```

**External table over the Parquet** (if you shaped to Parquet instead of Iceberg):

```sql
CREATE OR REPLACE EXTERNAL TABLE orders_ext
  LOCATION=@lake_stage/db/ FILE_FORMAT=(TYPE=PARQUET) AUTO_REFRESH=TRUE
  PATTERN='.*orders.*[.]parquet';
```

> If you registered the table in a **REST catalog** (Nessie/Polaris) in Demos 1–2,
> Snowflake can instead attach it via a `CATALOG INTEGRATION` of type `ICEBERG_REST` and
> skip the explicit metadata path — the catalog tracks the current snapshot for you.

## C. ClickHouse — read Iceberg/Parquet in place

```sql
-- whole Iceberg table via the catalog-less table function
SELECT status, count() FROM iceberg('s3://lake/db/orders',
       'minio', 'minio123') GROUP BY status;

-- or persist a MergeTree copy for hot queries
CREATE TABLE orders ENGINE = MergeTree ORDER BY id AS
SELECT * FROM iceberg('s3://lake/db/orders', 'minio', 'minio123');
```

(Raw Parquet works too: `SELECT * FROM s3('s3://lake/db/orders.parquet', 'Parquet')`.)

## D. Postgres — bulk load without DuckDB

If you'd rather not go through DuckDB, `COPY` the Parquet in with the `parquet_fdw` /
`pg_parquet` extension, or convert to CSV and `COPY`:

```sql
-- with pg_parquet:
COPY orders FROM 's3://lake/db/orders.parquet' WITH (format 'parquet');
```

## Choosing zero-copy vs. bulk load

| | Zero-copy (attach via catalog / table function) | Bulk load (`CREATE TABLE AS` / `COPY`) |
|---|---|---|
| Data movement | none — reads files in place | copies into the DB's own storage |
| Freshness | always current (re-point at newest metadata) | snapshot at load time; re-run to refresh |
| Best for | Snowflake/Trino/ClickHouse/DuckDB over a lake | a DB that must own the rows (hot OLTP/serving) |

For continuously appended tables (Demos 1 & 4), prefer **zero-copy** and let the engine see
each new snapshot; run a bulk load only when a system genuinely needs a private copy.

---

Next: [Demo 4 — the full CDC round-trip](./04-cdc-roundtrip-stream-and-db.md) ties the
streaming source, the database source, the catalog, and the database sink into one pipeline.
