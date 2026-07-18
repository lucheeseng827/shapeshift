# shapeshift examples — from ragged JSONL to Parquet & Iceberg in one pass

This folder ships two fixtures and a script's worth of copy-paste commands that
exercise the whole engine end to end: infer a spec, shape to Parquet, shape to a
self-contained Iceberg v2 table, run a hand-written **strict** spec with
from-paths and transforms, inspect every output, and price the run against a
managed vendor's MAR (Monthly Active Rows) rate.

Everything here is local filesystem only, no network, no telemetry, no row cap —
this is the OSS engine (Apache-2.0), the same code whether you self-host it free
or run it under the commercial control plane.

> **Wiring shapeshift into a lakehouse?** See
> [`catalog-demos/`](./catalog-demos/README.md) for end-to-end recipes that put a
> **data catalog** (Iceberg REST / AWS Glue / Nessie / Polaris) in the middle, with
> **streaming systems** (Kafka/Redpanda) and **databases** (Postgres) as source *and*
> sink — plus a local `docker compose` stack to run them on your laptop.

## The fixtures

### `events.jsonl` — 5 lines, one deliberately broken

```jsonl
{"id": 1, "user": {"name": "Ada", "plan": "pro"}, "amount": 12.50, "event_at": "2026-07-13T09:15:00Z", "day": "2026-07-13", "tags": ["a","b"], "active": true}
{"id": 2, "user": {"name": "Grace", "plan": "free"}, "amount": 3, "event_at": "2026-07-13T10:00:00Z", "day": "2026-07-13", "tags": [], "active": false}
{"id": 3, "user": {"name": "Linus"}, "amount": 99.99, "event_at": "2026-07-12T23:59:59Z", "day": "2026-07-12", "tags": ["x"], "active": true}
not-json-a-bad-line
{"id": 4, "user": {"name": "Edsger", "plan": "pro"}, "amount": 0, "event_at": "2026-07-11T08:00:00Z", "day": "2026-07-11", "active": true}
```

It is intentionally ragged, so a single fixture shows off the whole coercion path:

- **Line 4 is not JSON.** It is a hard parse error. The run does **not** abort — the
  line is counted, streamed past, and preserved verbatim in a
  `<output>.rejects.jsonl` sidecar (see [The reject sidecar](#the-reject-sidecar)).
- **`user.plan` is missing for Linus**, **`day` and `tags` are missing for Edsger.**
  Inference and lenient coercion land those as nulls rather than failing.
- **`amount` mixes `12.50`, `3`, `99.99`, `0`** (float and int in one field) — inference
  widens the column to `float64`. Sum across the 4 good rows is **115.49**.
- **`event_at`** is RFC3339 → detected as `timestamp`; **`day`** is `YYYY-MM-DD` →
  detected as `date`; **`tags`** is an array → kept as a `json` leaf.

Net: **4 rows in, 1 parse error.**

### `billing.spec.yaml` — a strict spec with from-paths + transforms

A hand-written [DatasetSpec](../docs/SPEC.md) that pins the output shape instead of
inferring it. It writes **only** the listed columns and selects each from a
(possibly nested) source path, rewriting the value before coercion:

| column         | `from`      | `type`      | `transform`        | note                                   |
|----------------|-------------|-------------|--------------------|----------------------------------------|
| `id`           | `id`        | `int64`     | —                  | `required: true`                       |
| `plan`         | `user.plan` | `string`    | `uppercase`        | nested path; `pro` → `PRO`             |
| `customer`     | `user.name` | `string`    | —                  | nested path                            |
| `amount_cents` | `amount`    | `int64`     | `dollars_to_cents` | `12.50` → `1250`                       |
| `day`          | `day`       | `date`      | —                  | `"2026-07-13"` → a DATE column         |
| `event_at`     | `event_at`  | `timestamp` | —                  | RFC3339 → a TIMESTAMP column           |
| `active`       | `active`    | `bool`      | —                  |                                        |

`schema: strict` also switches on the strict coercion policy: a **required** column
that is missing/null, or **any** coercion failure, aborts the run. (Lenient/infer
mode instead soft-rejects just that row and nulls out bad optional values.) In this
fixture every row carries an integer `id` — the one required column — so all 4 good
rows pass; Linus's absent `user.plan` and Edsger's absent `day` simply become null.

## Build

Run everything below from the repository root:

```sh
cargo build -p shapeshift-cli          # produces target/debug/shapeshift
alias shapeshift=./target/debug/shapeshift
mkdir -p out
```

## (a) Infer a spec

Sample the input and print a ready-to-edit `DatasetSpec` (add `-o events.spec.yaml`
to write it instead of printing; `--no-flatten` to keep nested objects as `json`
leaves; `--sample N` to change the sample size, `0` = scan all):

```sh
shapeshift infer -i examples/events.jsonl
```

```yaml
dataset: events
source:
  format: jsonl
  path: examples/events.jsonl
output:
  format: parquet
  path: events.parquet
  compression: snappy
schema: infer
columns:
- name: active
  type: bool
  required: false
- name: amount
  type: float64
  required: false
- name: day
  type: date
  required: false
- name: event_at
  type: timestamp
  required: false
- name: id
  type: int64
  required: false
- name: tags
  type: json
  required: false
- name: user.name
  type: string
  required: false
- name: user.plan
  type: string
  required: false
options:
  flatten: true
  row_group_rows: 50000
  infer_sample: 1000
```

Flatten (on by default) expanded `user` into the dotted columns `user.name` /
`user.plan`, `tags` became a `json` leaf, and `day` / `event_at` were detected as
`date` / `timestamp`.

## (b) Shape to Parquet (inference on the fly)

With no `--spec`, `shape` infers on the fly, so `--input` and `--output` are
required:

```sh
shapeshift shape -i examples/events.jsonl -o out/events.parquet --to parquet
```

```text
shaped `events` → out/events.parquet (Parquet)
rows_in=4 rows_out=4 rejected=0 parse_errors=1 row_groups=1 bytes=2588
malformed / rejected rows → out/events.parquet.rejects.jsonl (1 parse, 0 shaped-out)
```

`rows_in=4 … parse_errors=1`: the 4 valid objects were shaped; the bad line was
counted and sidecarred. One batch → one Parquet row group (bounded RAM).

## (c) Shape to an Iceberg table

Same input, `--to iceberg` writes a self-contained Apache Iceberg **v2** table
directory — no catalog server required:

```sh
shapeshift shape -i examples/events.jsonl -o out/events_iceberg --to iceberg
```

```text
shaped `events` → out/events_iceberg (Iceberg)
rows_in=4 rows_out=4 rejected=0 parse_errors=1 row_groups=1 bytes=3212
malformed / rejected rows → out/events_iceberg.rejects.jsonl (1 parse, 0 shaped-out)
iceberg table metadata → out/events_iceberg/metadata/v1.metadata.json
```

The table is one unpartitioned append snapshot:

```text
out/events_iceberg/
  data/<uuid>.parquet                       # carries PARQUET:field_id 1..N
  metadata/<uuid>-m0.avro                    # manifest
  metadata/snap-<id>-1-<uuid>.avro           # manifest list
  metadata/v1.metadata.json
  metadata/version-hint.text
```

The `.avro` manifests are written by shapeshift's own Object-Container-File encoder
so the `field-id` attributes survive in the embedded Avro schema — the general Avro
crate drops them and DuckDB then can't read the table. See
[ARCHITECTURE](../ARCHITECTURE.md) for why.

Add `--partition-by <col>` (identity partitioning) to fan the rows out to one data file
per distinct value — the `data/` directory then holds Hive-style `data/<col>=<value>/`
subdirectories instead of a single file — and `--append` to commit the run as a new
snapshot onto an existing table rather than a fresh one.

## (d) Run the strict billing spec

Pass the spec with `-s`; the output path comes from the spec's `output.path`
(`./out/billing.parquet`), so only `-i` is needed (the spec's `source` has no
`path`):

```sh
shapeshift shape -s examples/billing.spec.yaml -i examples/events.jsonl
```

```text
shaped `billing` → out/billing.parquet (Parquet)
rows_in=4 rows_out=4 rejected=0 parse_errors=1 row_groups=1 bytes=2292
malformed / rejected rows → out/billing.parquet.rejects.jsonl (1 parse, 0 shaped-out)
```

Same 4-in / 1-parse-error, but the columns, names, types and transforms are exactly
the ones you declared — `plan` uppercased, `amount_cents` in integer cents.

## (e) Inspect each output

`inspect` auto-detects a Parquet file vs. an Iceberg table directory.

**Inferred Parquet:**

```sh
shapeshift inspect out/events.parquet
```

```text
parquet file: out/events.parquet
  rows: 4
  row-groups: 1
  columns (8):
    active: Boolean
    amount: Float64
    day: Date32
    event_at: Timestamp(Microsecond, None)
    id: Int64
    tags: Utf8
    user.name: Utf8
    user.plan: Utf8
```

**Iceberg table:**

```sh
shapeshift inspect out/events_iceberg
```

```text
iceberg table: out/events_iceberg
  format-version: 2
  table-uuid: <uuid>
  current-snapshot-id: <id>
  total-records: 4
  columns (8):
    active: boolean
    amount: double
    day: date
    event_at: timestamp
    id: long
    tags: string
    user.name: string
    user.plan: string
```

**Strict billing Parquet** — the declared column order is preserved:

```sh
shapeshift inspect out/billing.parquet
```

```text
parquet file: out/billing.parquet
  rows: 4
  row-groups: 1
  columns (7):
    id: Int64
    plan: Utf8
    customer: Utf8
    amount_cents: Int64
    day: Date32
    event_at: Timestamp(Microsecond, None)
    active: Boolean
```

## (f) Price the run with `shapeshift cost`

`cost` never guesses a price — you supply your managed vendor's effective
`$/million` MAR from your own plan. Count the fixture's billable rows straight from
the input:

```sh
shapeshift cost -i examples/events.jsonl --vendor-per-million 500 --self-host-cost 50
```

```text
billable rows (MAR-equivalent): 4
vendor:     $0.00  (@ $500.00/million MAR)
self-host:  $50.00
saved:      $-50.00  (-2499900.0%)
```

That negative number is honest, not a bug: 4 rows do not amortize a $50 self-host
bill. The economics flip the moment volume is real — the pitch is about MAR at
scale, so price it there with `--rows`:

```sh
shapeshift cost --rows 50000000 --vendor-per-million 500 --self-host-cost 200
```

```text
billable rows (MAR-equivalent): 50000000
vendor:     $25000.00  (@ $500.00/million MAR)
self-host:  $200.00
saved:      $24800.00  (99.2%)
```

## Verify with DuckDB

Both output paths carry the identical data — same rows, same aggregates. Confirmed
end to end with **DuckDB 1.5.4**:

```sql
INSTALL iceberg; LOAD iceberg;

-- Parquet path
SELECT count(*), round(sum(amount), 2) FROM read_parquet('out/events.parquet');
-- 4, 115.49

-- Iceberg path — same table, read through the catalog-less metadata
SELECT count(*), round(sum(amount), 2) FROM iceberg_scan('out/events_iceberg');
-- 4, 115.49

-- Strict spec: dollars_to_cents kept the money exact
SELECT sum(amount_cents) FROM read_parquet('out/billing.parquet');
-- 11549   (= 115.49 × 100)
```

Types, dates/timestamps, the flattened `user.*` columns, and the nulls (Linus's
`user.plan`, Edsger's `day`) all read back correctly on both paths.

## The reject sidecar

A `shape` writes `<output>.rejects.jsonl` **only when it sets a record aside** — a
malformed source line (`{"line", "error", "raw"}`) or a row that parsed cleanly but
failed shaping under the lenient policy (`{"error", "raw"}`, `raw` being the source
JSON). A clean run writes no sidecar; a stale one from a previous run is removed
first. The original text is preserved so nothing is silently lost:

```sh
cat out/events.parquet.rejects.jsonl
```

```json
{"error":"ExpectedNull at character 0 ('n')","line":4,"raw":"not-json-a-bad-line"}
```

A bad or rejected row never aborts the stream and never poisons the output — it is
counted (`parse_errors` / `rejected`) and set aside for you to inspect or replay.

---

Byte counts above are from a local debug build and can shift by a few bytes across
parquet-rs versions; row/record/error counts are exact. For the full spec model,
type mapping, and coercion rules see [`docs/SPEC.md`](../docs/SPEC.md); for the
Iceberg writer internals see [`ARCHITECTURE.md`](../ARCHITECTURE.md).

© 2026 Nicholas Lu Chee Seng and the shapeshift contributors — Apache-2.0.
