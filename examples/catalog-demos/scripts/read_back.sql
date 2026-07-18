-- read_back.sql — verify a shapeshift table from DuckDB (the "database as sink" edge).
--
-- Run:  duckdb < read_back.sql        (edit the paths/URLs below first)
--
-- shapeshift's Parquet and Iceberg outputs carry identical data; DuckDB reads both with
-- no JVM and no catalog server. This is the fastest way to confirm a shape landed
-- correctly and to feed the rows into a database sink.

INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;          -- only needed for s3://, gs://, az:// tables

-- ── Local Parquet file ────────────────────────────────────────────────────────────
SELECT count(*) AS rows, round(sum(amount), 2) AS total
FROM read_parquet('out/events.parquet');

-- ── Local Iceberg table (catalog-less: point at the directory) ─────────────────────
SELECT count(*) AS rows, round(sum(amount), 2) AS total
FROM iceberg_scan('out/events_iceberg');

-- ── Iceberg table on S3 / MinIO ────────────────────────────────────────────────────
-- SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false; SET s3_url_style='path';
-- SET s3_access_key_id='minio'; SET s3_secret_access_key='minio123';
-- SELECT count(*) FROM iceberg_scan('s3://lake/db/events');

-- ── Streaming append: dedup latest-per-key (append-only tables never mutate a row) ──
-- With many --append snapshots you may have multiple versions of a key. Resolve the
-- current row at read time by keeping the latest event per id. NOTE: order by event_at
-- PLUS a monotonic tiebreaker (a producer sequence, Kafka offset, or ULID) — event_at
-- alone is not unique, so equal timestamps would resolve arbitrarily:
-- SELECT * EXCLUDE (rn, seq) FROM (
--   SELECT *, row_number() OVER (PARTITION BY id ORDER BY event_at DESC, seq DESC) AS rn
--   FROM iceberg_scan('s3://lake/db/events')
-- ) WHERE rn = 1;

-- ── Load into a database sink (DuckDB → Postgres example) ───────────────────────────
-- INSTALL postgres; LOAD postgres;
-- ATTACH 'host=localhost dbname=warehouse user=postgres' AS pg (TYPE postgres);
-- CREATE TABLE pg.public.events AS
--   SELECT * FROM iceberg_scan('s3://lake/db/events');
