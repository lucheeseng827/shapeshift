# shapeshift — Dataset Transform Spec (reference)

The **dataset transform spec** is shapeshift's product boundary. It is one small YAML (or JSON)
document that describes how a stream of JSON records becomes exactly one columnar table: which
fields to select, what logical type each becomes, which named transform to apply on the way, and
where and how to write the result. You write it **once per source shape**; the engine then shapes
**unbounded rows for free** on your own hardware. There is no per-row rent because there is no
per-row decision left once the spec exists.

This document is the complete contract: every field with its type, default, and meaning; the full
logical→Arrow→Iceberg type table; the coercion rules; every transform with an input→output example;
path-selection syntax; the two schema modes; the lenient/strict reject policy; and the validation
errors. It is the OSS engine (Apache-2.0) — everything here runs in the single self-hostable
`shapeshift` binary with no row cap and no telemetry.

For the *why* behind these rules see [DESIGN.md](./DESIGN.md); for the internals (the
transactional per-row append, the field-id chain, the hand-rolled Avro encoder) see
[ARCHITECTURE.md](../ARCHITECTURE.md); for install and first run see the [README](../README.md).

---

## 1. Format and loading

- A spec is **YAML or JSON**. The loader auto-detects: a document whose first non-whitespace byte
  is `{` is parsed as JSON, otherwise as YAML. YAML is the canonical on-disk form.
- The spec is **validated on load** (§9). Structural errors (an empty `dataset`, strict mode with
  no columns, duplicate column names, or — when columns are declared — a `partition_by` naming an
  undeclared column) are reported before any input is read.
- `shapeshift infer …` emits a ready-to-edit YAML spec inferred from a sample; `shapeshift shape
  -s spec.yaml …` consumes one. `shape` with no `-s` infers a spec on the fly (then `-i/--input`
  and `-o/--output` are required).

### Top-level skeleton

```yaml
dataset: <name>                    # required, non-empty
source:                            # optional; defaults shown
  format: jsonl | json-array       # default jsonl
  path: <input path>               # optional; CLI --input overrides
output:                            # required
  format: parquet | iceberg        # required
  path: <output path>              # required
  compression: snappy | zstd | uncompressed   # default snappy
  partition_by: [<col> | <transform(col)>, ...]  # default [] (Iceberg; identity or bucket|truncate|year|month|day|hour)
schema: infer | strict             # default infer
columns:                           # default [] (all inferred)
  - name: <col>
    from: <path>                   # default = name
    type: <logical type>           # required
    transform: <named transform>   # optional
    required: <bool>               # default false
options:
  flatten: <bool>                  # default true
  row_group_rows: <int>            # default 50000
  infer_sample: <int>              # default 1000 (0 = whole input)
```

---

## 2. Field reference

### 2.1 `DatasetSpec` (top level)

| Field | Type | Default | Meaning |
|---|---|---|---|
| `dataset` | string | — (required) | Logical table name. Must not be empty/whitespace. Used as the Parquet file stem when `output.path` is a directory, and as the Iceberg table name. |
| `source` | `SourceSpec` | `{format: jsonl}` | How to read the input (§2.2). |
| `output` | `OutputSpec` | — (required) | How and where to write the output (§2.3). |
| `schema` | `infer` \| `strict` | `infer` | Schema strictness (§7). `infer` lets undeclared columns ride along; `strict` writes only declared columns. |
| `columns` | list of `ColumnSpec` | `[]` | Explicit column declarations (§2.4). In `infer` mode they refine/augment the inferred schema; in `strict` mode they **are** the schema. |
| `options` | `Options` | defaults | Dataset-wide shaping options (§2.5). |

### 2.2 `SourceSpec`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `format` | `jsonl` \| `json-array` | `jsonl` | `jsonl` = one JSON object per line (streaming, bounded RAM, blank lines skipped, a bad line is counted and its raw text preserved). `json-array` = a single top-level JSON array, **streamed element-at-a-time** (bounded RAM; an invalid/oversized element is counted and its raw bytes preserved, with the 1-based element index reported as the reject's `line`; structural problems — not an array, truncated array, trailing comma/garbage — are reported once). |
| `path` | string | *(none)* | Optional input path. The CLI `-i/--input` flag overrides it; when both are absent the input comes from the command line / stdin. |

### 2.3 `OutputSpec`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `format` | `parquet` \| `iceberg` | — (required) | `parquet` = one Parquet file. `iceberg` = a self-contained Apache Iceberg v2 table directory (no catalog server), one append snapshot per run (unpartitioned by default; partitioned per `partition_by` — identity or hidden transforms). |
| `path` | string | — (required) | Destination. For `parquet`: a file path, or a directory (then `<path>/<dataset>.parquet` is written). For `iceberg`: the table root directory (created if absent). |
| `compression` | `snappy` \| `zstd` \| `uncompressed` | `snappy` | Parquet page compression. **`zstd` needs the opt-in fat build** (`--features zstd`); the default musl-static binary has no C codecs, so selecting it there returns a clear error — use `snappy`/`uncompressed` or rebuild with the feature. |
| `partition_by` | list of string | `[]` | Partition fields (Iceberg output). Each entry is a column name (**identity**) or a **hidden-transform expression**: `bucket(N, col)` (spec-exact Murmur3; long/string/date/timestamp), `truncate(W, col)` (long/string), `year(col)`/`month(col)`/`day(col)` (date/timestamp), `hour(col)` (timestamp). When columns are declared, every *source* column must be declared (§9); in `infer` mode it resolves against the inferred schema. **Applied:** the table's partition spec records the real transform strings and rows fan out to one data file per **transformed** value under Hive-style directories (temporal segments human-readable, e.g. `data/event_at_day=2026-01-01/`). Ignored for `parquet` output (a single file is still written). |

### 2.4 `ColumnSpec`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | — (required) | Output column name. Must be unique within the spec (§9). |
| `from` | string | `= name` | Source path into each record (§6). Dotted, optionally `$`-prefixed; numeric segments index arrays. When omitted, the column name is used as the path. |
| `type` | logical type | — (required) | Target logical type (§3). One of `bool`, `int64`, `float64`, `string`, `date`, `timestamp`, `json`. |
| `transform` | named transform | *(none)* | Optional value→value rewrite applied **before** coercion (§5). |
| `required` | bool | `false` | When `true`, a missing/null (or, in lenient mode, uncoercible) value drops the row (lenient) or aborts (strict). When `false`, such a value is written as null. Also sets the Arrow field's nullability (`required: true` ⇒ non-nullable field). |

### 2.5 `Options`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `flatten` | bool | `true` | **Inference only.** When on, nested objects expand into dotted columns (`{"user":{"name":…}}` → `user.name`) and arrays become `json` leaves. When off, each top-level nested object becomes a single `json` column. Explicit `columns` are never affected by this. |
| `row_group_rows` | int | `50000` | Rows buffered before a `RecordBatch` (= one Parquet row group / one Iceberg data-file batch) is flushed. Bounds RAM: a 100M-row shape never holds more than this many rows at once. Values `< 1` are treated as 1. |
| `infer_sample` | int | `1000` | Records sampled when inferring the schema. `0` = scan the whole input. Ignored when every column is declared and `schema: strict`. |

---

## 3. Logical types

The closed set of logical types. Each maps cleanly onto Arrow and, downstream, onto both Parquet
and the Iceberg type system.

| Logical (`type:`) | Arrow physical | Iceberg type | Notes |
|---|---|---|---|
| `bool` | `Boolean` | `boolean` | JSON `true`/`false` only. |
| `int64` | `Int64` | `long` | 64-bit signed integer. |
| `float64` | `Float64` | `double` | IEEE-754 double. |
| `string` | `Utf8` | `string` | UTF-8 text. Scalars stringify; arrays/objects do **not** (use `json`). |
| `date` | `Date32` | `date` | Days since the Unix epoch (1970-01-01), no zone. |
| `timestamp` | `Timestamp(µs, no zone)` | `timestamp` | Microseconds since the epoch, zone-less (Iceberg `timestamp`, **not** `timestamptz`). |
| `json` | `Utf8` | `string` | Lossless escape hatch: any nested value is preserved as a compact JSON-encoded string. Never fails coercion. |

---

## 4. Coercion rules

Coercion runs **after** the optional transform, on the selected JSON value, targeting the column's
logical type. A missing path or a JSON `null` (or a transform that produced `null`) is treated as
**null** and never reaches coercion — its handling is governed by the reject policy (§8). A value
that is present but cannot be represented in the target type is a **coercion failure** (also §8).

| Target | Accepts | Result / failure |
|---|---|---|
| `bool` | JSON bool | `true`/`false`. Anything else fails. |
| `int64` | JSON number; numeric string | Integer number → i64. Integral float (`fract == 0`, finite) → i64. Non-integer float (e.g. `3.5`) fails. Numeric string (`"42"`) parses to i64; a non-integer string fails. Non-numeric types fail. |
| `float64` | JSON number; numeric string | Number → f64. Numeric string (`"3.14"`) parses to f64. Non-numeric string / other types fail. |
| `string` | string, bool, number | String kept as-is; bool → `"true"`/`"false"`; number → its JSON text. Array/object **fail** (message points you to `json`). |
| `date` | date string; integer | `YYYY-MM-DD`, or the date part of an RFC3339 string → epoch-days. A bare integer is read as **epoch-days** (Date32 range). Other shapes fail. |
| `timestamp` | timestamp string; integer | RFC3339 (offset required), or a zone-less `YYYY-MM-DDThh:mm:ss[.fff]` read as **UTC** → epoch-microseconds. A bare integer is read as **epoch-milliseconds** (×1000 → µs). Other shapes fail. |
| `json` | any value | String kept as-is; any other value → compact JSON text. **Never fails.** |

Notes:
- `int64` from an integral float means `10.0` coerces but `10.5` fails.
- Integer `date` = epoch-days; integer `timestamp` = epoch-milliseconds. These are deliberately
  different units — dates are day-granular, timestamps are millisecond-granular in common JSON.
- `timestamp` is zone-less throughout; an offset in the input is applied to reach UTC, then the
  zone is dropped.

---

## 5. Transforms

A transform is a pure `value → value` rewrite applied to the selected field **before** coercion.
Every transform is **total**: a value of a type it does not apply to passes through unchanged — a
genuine mismatch surfaces later in coercion, not here. Temporal parsing is *not* a transform; it is
driven by the target `date`/`timestamp` type in coercion (§4), which keeps the transform set small
and orthogonal.

| `transform:` | Effect | Example input → output |
|---|---|---|
| `lowercase` | Lowercase a string; non-strings unchanged | `"PRO"` → `"pro"` ; `42` → `42` |
| `uppercase` | Uppercase a string; non-strings unchanged | `"pro"` → `"PRO"` |
| `trim` | Trim leading/trailing whitespace from a string | `"  ada  "` → `"ada"` |
| `json_encode` | Serialize a composite value to compact JSON text; an existing scalar string is left as-is (not re-quoted) | `{"a":1}` → `"{\"a\":1}"` ; `["x","y"]` → `"[\"x\",\"y\"]"` ; `"hi"` → `"hi"` |
| `to_string` | Stringify a scalar: numbers/bools → their JSON text, strings unchanged, `null` stays `null`, composites → JSON text | `42` → `"42"` ; `true` → `"true"` ; `null` → `null` |
| `dollars_to_cents` | Multiply a number by 100 and round to the nearest integer (dollars → integer cents); non-numbers unchanged | `12.50` → `1250` ; `3` → `300` ; `-4.2` → `-420` |
| `abs` | Absolute value of a number (int stays int, float stays float); non-numbers unchanged | `-4` → `4` ; `-3.5` → `3.5` |
| `empty_to_null` | An empty string becomes `null` (so a `required` check can fire, or the cell writes null instead of `""`); everything else unchanged | `""` → `null` ; `"x"` → `"x"` |

Pattern: to keep a nested object/array losslessly, either set `type: json` (coercion stringifies it
for you) or apply `transform: json_encode` and use `type: string` — both yield the same JSON text.

---

## 6. Path selection (`from`)

`from` selects a value out of each JSON record. A path that does not resolve yields **null** (never
an error), which the reject policy (§8) turns into a written null or a row rejection.

- **Dotted** descent into objects: `user.name`, `geo.region`.
- **Optional leading `$` or `$.`** (JSON-pointer muscle memory): `$.user.name` and `$user.name`
  are the same as `user.name`.
- **`$` alone** (an empty path after the prefix is stripped) selects the **whole record** — the
  idiom for `from: $` with `type: json` to keep the entire object as one JSON column.
- A **numeric segment** indexes into an array: `tags.0` is the first element of `tags`; `items.2.sku`
  is the `sku` of the third element.
- Descent stops (→ null) when a segment is applied to a scalar, an object lacks the key, an array
  index is out of range, or a numeric segment is applied to a non-array.

When `from` is omitted, the column `name` is used as the path (so `name: id` reads `id`).

---

## 7. Schema modes (`schema`)

### `infer` (default)

The schema is inferred from a sample (§10) and undeclared columns ride along. Any columns you
**do** declare win over inferred columns of the same name — they keep their declared position,
type, transform, and `required` flag — and inferred columns not shadowed by a declaration are
appended after, in first-seen order. Use `infer` to reshape a few known fields while letting the
rest of a messy source through untouched. Coercion is **lenient** here (§8).

### `strict`

Only declared columns are written; the inferred schema is not consulted for the output shape. This
is the contract mode for downstream stability: the output columns and their types are exactly what
the spec says, run after run. It requires **at least one** declared column (§9). Coercion is
**strict** here (§8).

The merge of declared + inferred columns is the *effective column list* the shaper builds its Arrow
schema from. In `strict` mode that list is exactly `columns`; in `infer` mode it is `columns`
followed by the non-shadowed inferred columns.

---

## 8. Reject and coercion policy

Every cell of a row is computed and validated **before any value is appended**, so a rejected row
never leaves the column builders misaligned. The disposition of a value depends on three things:
whether it is *absent/null* vs *present-but-uncoercible*, whether its column is `required`, and the
schema mode.

### Lenient (`schema: infer`)

| Column | Missing / null | Uncoercible |
|---|---|---|
| optional (`required: false`) | written as **null** | written as **null** |
| `required: true` | **row rejected** (dropped, counted) | **row rejected** (dropped, counted) |

### Strict (`schema: strict`)

| Column | Missing / null | Uncoercible |
|---|---|---|
| optional (`required: false`) | written as **null** | **run aborts** |
| `required: true` | **run aborts** | **run aborts** |

In short: **lenient** never aborts — a bad optional value degrades to null, a required miss (or a
required bad value) drops just that row. **strict** never silently degrades a *value* — any
coercion failure on any column, or a required miss, aborts the whole run. A missing *optional*
value is always allowed and becomes null, in both modes.

### Rejects sidecar

`shapeshift shape` never aborts on a bad **source** line (a JSONL parse error). Such lines are
counted and written to `<output>.rejects.jsonl`, one JSON object per line:
`{"line": <n>, "error": <message>, "raw": <original text>}`. Rows dropped by a lenient soft-reject
are counted in the run report (`rejected=…`); the report line printed by `shape` is:

```text
rows_in=.. rows_out=.. rejected=.. parse_errors=.. row_groups=.. bytes=..
```

---

## 9. Validation errors

`DatasetSpec::validate()` runs on load, after serde has checked the document shape (a missing
required field — `dataset`, `output`, `output.format`, `output.path`, or a column's `type` — fails
there first). The structural checks and their messages:

| Condition | Message |
|---|---|
| `dataset` empty or whitespace | `` `dataset` must not be empty `` |
| `schema: strict` with no `columns` | `strict schema mode requires at least one declared column` |
| Two columns share a `name` | `` duplicate column name `<name>` `` |
| `partition_by` names a column not in `columns` (declared/`strict` mode only) | `` partition column `<name>` is not a declared column (declare it to partition by it) `` |

When columns are declared, a partition column must be among them so Iceberg has a stable field-id
for it. In `infer` mode there are no declared columns; the check is deferred and the sink resolves
each `partition_by` name against the inferred schema (erroring at write time if it is not present).

---

## 10. Inference (what `infer` produces)

Inference samples the first `infer_sample` records (`0` = all), watches the JSON shape at each path,
and proposes a `ColumnSpec` per path with `from` unset (path = name), no transform, and
`required: false`. The rules are conservative and **widening** — inference never proposes a type it
cannot losslessly hold, so a `strict` spec derived from an inferred one is safe.

- **Flattening** (`options.flatten`, default on): nested objects expand into dotted columns; arrays
  and empty objects become `json` leaves. Off: only top-level keys become columns and a nested
  object becomes one `json` column. A bare non-object line (e.g. a scalar JSONL line) contributes
  no columns.
- **Column order** is first-seen across the sample.
- **Per-path type decision** (over all sampled values at that path; JSON `null` is ignored):
  - any array/object observed → `json`;
  - two or more distinct scalar *categories* (bool / number / string) → widen to `string`;
  - all-string and every value is RFC3339-with-time → `timestamp`;
  - all-string and every value is bare `YYYY-MM-DD` → `date`;
  - all-string otherwise → `string`;
  - number category with any float value → `float64` (int + float widen to `float64`); all-integer
    → `int64`;
  - all-bool → `bool`;
  - only nulls ever seen → `string` (the lossless default).
- **Date vs timestamp detection**: a value counts as a date only if it is exactly ten characters
  `YYYY-MM-DD`; as a timestamp only if it contains `T` and parses as RFC3339.

Inference must yield at least one column for a run to proceed; an all-empty or all-scalar-line
sample in `infer` mode with no declared columns produces no schema and the run errors.

---

## 11. Spec vs CLI flags

The spec is the durable authored artifact; CLI flags on `shapeshift shape` override the matching
spec field for a single run without editing the file:

- `-i/--input` overrides `source.path`; `--format` overrides `source.format`.
- `-o/--output` overrides `output.path`; `--to` overrides `output.format`; `--compression`
  overrides `output.compression`.
- `--dataset` overrides `dataset`.

With no `-s/--spec`, `shape` infers a spec on the fly (then `--input` and `--output` are required),
which is equivalent to running `infer` and immediately shaping with the result.

---

## 12. Example specs

### 12.1 Minimal (infer mode)

Everything defaults: JSONL in, Parquet out, schema inferred, Snappy, flatten on, 50 000-row groups,
1 000-record sample. Every column is inferred.

```yaml
dataset: events
output:
  format: parquet
  path: ./out/events.parquet
```

### 12.2 Strict billing spec (declared columns + transforms)

Writes exactly these columns; because `schema: strict`, a required miss or any coercion failure
aborts the run. Nested selection, a money normalization, case-folding, and temporal typing are all
shown. (This is the spec shipped at `examples/billing.spec.yaml`.)

```yaml
dataset: billing
source:
  format: jsonl
output:
  format: parquet          # switch to `iceberg` to write an Iceberg v2 table dir
  path: ./out/billing.parquet
  compression: snappy
schema: strict
columns:
  - name: id
    from: id
    type: int64
    required: true         # a row without an integer id aborts (strict)
  - name: plan
    from: user.plan        # nested selection
    type: string
    transform: uppercase
  - name: customer
    from: user.name
    type: string
  - name: amount_cents
    from: amount
    type: int64
    transform: dollars_to_cents   # 12.50 → 1250
  - name: day
    from: day
    type: date             # "2026-07-13" → a DATE column
  - name: event_at
    from: event_at
    type: timestamp        # RFC3339 → a TIMESTAMP column
  - name: active
    from: active
    type: bool
options:
  row_group_rows: 50000
```

### 12.3 Iceberg output (infer mode, whole-record capture, declared partition column)

Writes a self-contained Iceberg v2 table directory, identity-partitioned by `region`: rows fan out
to one data file per distinct value under `data/region=<value>/`. `region` is declared so it can
appear in `partition_by`; the whole record is also captured losslessly in a `json` column.

```yaml
dataset: orders
source:
  format: jsonl
output:
  format: iceberg
  path: ./warehouse/orders     # Iceberg table root directory (no catalog server)
  compression: snappy
  partition_by: [region]       # identity partitioning: one data file per region under data/region=<value>/
schema: infer                  # order_id/region/total_cents/payload are pinned; other fields ride along
columns:
  - name: order_id
    from: id
    type: int64
    required: true
  - name: region
    from: geo.region           # nested selection
    type: string
    transform: lowercase
  - name: total_cents
    from: total
    type: int64
    transform: dollars_to_cents
  - name: payload
    from: $                     # whole record kept losslessly as a JSON string
    type: json
options:
  row_group_rows: 50000
```

Inspect either output with `shapeshift inspect <path>` (auto-detects a Parquet file vs an Iceberg
table directory).

---

## 13. v0.1 limits that touch the spec

- `partition_by` applies **identity and hidden (transform) partitioning** — one data file per
  distinct combination of the *transformed* partition values, under nested Hive-style directories.
  Transforms: `bucket(N, col)`, `truncate(W, col)`, `year|month|day|hour(col)` (§2.3 lists the
  type-applicability rules). A partitioned table
  needs **at least one row** (a zero-row input has no partition to write and is refused). For `parquet`
  output `partition_by` is ignored (a single file is written).
- Iceberg supports **append-to-existing / multi-snapshot** (`--append`) with **additive schema
  evolution** — new *optional* columns are welcomed (existing columns keep their field-ids, old
  rows read the new column as null); dropping, renaming, or re-typing a column, or adding a
  *required* column, is refused, and the partition spec must match — all checked at `create`
  time (before any data is written). Appends are
  **single-writer**: `version-hint.text` is published with no lock, so concurrent writers to one
  table would clobber each other — multi-writer commits need a catalog (roadmap REST catalog).
- `compression: zstd` is **off by default** (the musl-static binary links no C codecs) — it returns
  a clear error there; use `snappy`/`uncompressed`, or build with `--features zstd` (the opt-in fat
  build) to enable it for both Parquet and Iceberg.
- Both `source.format`s stream with bounded RAM: `jsonl` a line at a time, `json-array` an element
  at a time.
- Object-store output (S3/GCS/Azure) ships behind the CLI's `object_store` feature (off the default
  musl-static binary); Iceberg tables are **location-anchored** (absolute paths, valid where
  written). A moved or copied table is still **readable** via DuckDB's
  `iceberg_scan('<new path>', allow_moved_paths=true)` (verified for partitioned and multi-snapshot
  tables); **catalog-managed** relocation (re-anchoring paths so any engine reads with no flag, plus
  multi-writer commits) is the commercial-edition REST catalog.

---

*shapeshift v0.1 — © 2026 Nicholas Lu Chee Seng and the shapeshift contributors. Apache-2.0.
Repository: https://github.com/lucheeseng827/shapeshift*
