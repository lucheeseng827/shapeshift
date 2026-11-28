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
where: <predicate>                 # optional; which source records the run takes (§6a)
changes:                           # optional; the records are a feed of changes (§5b)
  preset: debezium-postgres | debezium-mysql | flink-cdc   # optional; fills in the rest
  op: <path>                       # the field that says what happened
  insert: [<value>, ...]           # op values per kind (strings, numbers or booleans)
  update: [<value>, ...]
  delete: [<value>, ...]
  skip: [<value>, ...]             # optional; counted, otherwise ignored
  row: <path>                      # optional; where the row is (default: the record)
  key_from: <path>                 # optional; where a delete's key is (default: row)
  order_by: [<col>, ...]           # optional; output columns that version a change
  tombstone_retention: <duration>  # optional, with order_by; default 7d
source:                            # optional; defaults shown
  format: jsonl | json-array | csv | tsv | xml   # default jsonl
  path: <input path>               # optional; CLI --input overrides
  csv:                             # csv/tsv only; every key optional (defaults shown)
    header: true                   # the first row names the columns
    columns: [<name>, ...]         # name them yourself (then a header row is skipped)
    delimiter: ","                 # "\t" for tsv
    quote: '"'
    null_values: [""]              # fields read as null
    types: auto | strings          # default auto
  xml:                             # xml only; required for xml
    record: /Orders/Order          # the element that is one record
output:                            # required
  format: parquet | iceberg        # required
  path: <output path>              # required, unless `catalog` names the table
  compression: snappy | zstd | uncompressed   # default snappy
  partition_by: [<col> | <transform(col)>, ...]  # default [] (Iceberg; identity or bucket|truncate|year|month|day|hour)
  merge_on: [<col>, ...]           # default [] (Iceberg; upsert on these key columns)
  apply: upsert | changelog        # with `changes:`; default upsert (§5b)
  catalog:                         # optional (Iceberg; commit through a REST catalog, no path)
    uri: <http(s)://host:port>
    table: <namespace>.<name>
    warehouse: <name>              # optional; credentials come from the environment (§5a)
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
| `where` | string | *(none)* | Which source records the run takes: a subset of SQL's `WHERE` over source paths (§6a). A record it does not keep is counted as filtered, not rejected. `--where` overrides it. |
| `changes` | `ChangesSpec` | *(none)* | The records are a feed of inserts, updates and deletes, applied to the table (§5b). |
| `source` | `SourceSpec` | `{format: jsonl}` | How to read the input (§2.2). |
| `output` | `OutputSpec` | — (required) | How and where to write the output (§2.3). |
| `schema` | `infer` \| `strict` | `infer` | Schema strictness (§7). `infer` lets undeclared columns ride along; `strict` writes only declared columns. |
| `columns` | list of `ColumnSpec` | `[]` | Explicit column declarations (§2.4). In `infer` mode they refine/augment the inferred schema; in `strict` mode they **are** the schema. |
| `options` | `Options` | defaults | Dataset-wide shaping options (§2.5). |
| `drift` | `{policy, rescue_column, max_examples}` | `{policy: warn}` | What a run does when the source's shape outgrows the schema: `policy` is `ignore`, `warn`, `rescue` (keep what the schema drops in a `json` column named `rescue_column`, default `_rescued`), `quarantine` (reject drifted rows into the sidecar) or `error` (fail at the first drift); `max_examples` (default 3) is how many example values the report keeps per field. `--on-drift` and `--rescue-column` override it. A field the spec reads (a `from` path, `where`, `$`) is never drift. |

### 2.2 `SourceSpec`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `format` | `jsonl` \| `json-array` \| `csv` \| `tsv` \| `xml` | `jsonl` | `jsonl` = one JSON object per line (streaming, bounded RAM, blank lines skipped, a bad line is counted and its raw text preserved). `json-array` = a single top-level JSON array, **streamed element-at-a-time** (bounded RAM; an invalid/oversized element is counted and its raw bytes preserved, with the 1-based element index reported as the reject's `line`; structural problems — not an array, truncated array, trailing comma/garbage — are reported once). |
| `csv` | `CsvOptions` | *(defaults)* | CSV and TSV only (§2.2.1). |
| `xml` | `XmlOptions` | *(none)* | XML only, and required for it (§2.2.2). |
| `http` | `HttpSourceSpec` | *(none)* | Pull the records from a REST endpoint instead of a file (§2.6). Not with `path`, and its body format is `http.format`, not `format`. |
| `mysql` | `MysqlSourceSpec` | *(none)* | Pull the rows from a MySQL query or table instead of a file (§2.7). Not with `path` or `http`, and no file format applies. |
| `postgres` | `PostgresSourceSpec` | *(none)* | The same from a Postgres query or table (§2.8). |
| `path` | string | *(none)* | Optional input path. The CLI `-i/--input` flag overrides it. Use `-` for standard input, so a shape composes in a pipeline (`producer \| shapeshift shape -i - -o out.parquet`); the source is read once, so a pipe shapes exactly what a file would. |

The other formats: `csv` and `tsv` (one record per row, §2.2.1) and `xml` (one record per
element at a path, §2.2.2). All three stream with bounded RAM, as the JSON formats do: one record at a time, and one
record may be no larger than a JSON one (256 MiB) — a CSV quote that never closes, or an XML record
path that matches the whole document, fails the run there instead of filling memory. A CSV or TSV
row that does not parse is counted and sidecarred with its line and its text (re-quoted, so it
reads back as the same fields), as a bad JSON line is. An XML document that does not parse fails
the run instead (§2.2.2). `shapeshift convert`
prints the records any of them produce as JSON Lines, to see exactly what a spec will be applied to.

#### 2.2.1 `CsvOptions` (`csv`, `tsv`)

A row becomes an object keyed by the column names. Quoting is RFC 4180's: a quoted field may hold
the delimiter, a doubled quote, or a line break. A UTF-8 byte-order mark is skipped.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `header` | bool | `true` | The first row names the columns. A header with an empty or repeated name is refused, naming it. |
| `columns` | list of strings | *(none)* | The column names, in order. With `header: true` the header row is then skipped. With `header: false` and no `columns`, the names are `column_1`, `column_2`, …. |
| `delimiter` | one ASCII character | `,` (csv), tab (tsv) | The field separator. |
| `quote` | one ASCII character | `"` | The quote character. Must differ from the delimiter. |
| `null_values` | list of strings | `[""]` | Fields read as null. By default an empty field is null; `[]` keeps it an empty string. |
| `types` | `auto` \| `strings` | `auto` | `auto`: a field that is plainly `true`, `false`, an integer that fits int64, or a decimal arrives as a JSON boolean or number, so inference sees types. "Plainly" means no `+`, no spaces, no exponent and no leading zero, so `02139` and `1e5` stay strings. A decimal becomes a float64, as a JSON number does, so one with more than about 15 significant digits is rounded, and `12.50` becomes `12.5`. `strings`: every field is a string, for the spec's column types to coerce (§4); use it where decimals must stay exact. |

A row with more or fewer fields than there are columns is rejected with its line, and the rows after
it are read as usual.

#### 2.2.2 `XmlOptions` (`xml`)

| Field | Type | Default | Meaning |
|---|---|---|---|
| `record` | string | *(required)* | The element that is one record, as a path of element names. `/Orders/Order` is anchored at the root; `Order` or `Orders/Order` match at any depth. A prefix in a step is dropped, as in the document (`/soap:Envelope/soap:Body` is `/Envelope/Body`). |

An element becomes JSON this way, so `from` paths (§6) reach into it as into any JSON record:

- its attributes are fields named `@name`;
- its child elements are fields by name, and a name that repeats becomes an array in document order;
- its text is the value itself when it has nothing else, or the field `#text` beside attributes and
  children; text is trimmed of the whitespace around it, and line breaks are normalised to `\n`;
- an empty element is null, or an object of its attributes;
- every value is a string (`"9.50"`): declare a column's type to coerce it, or let inference see
  strings.

Namespace prefixes are dropped (`soap:Body` is `Body`) and `xmlns` declarations are not
attributes, so a SOAP response's items are `record: /Envelope/Body/<Response>/<Item>`. Two
attributes that differ only by prefix (`a:id`, `b:id`) are both `@id`, and the last one wins.

**Safe by construction.** A document that declares a document type (`<!DOCTYPE …>`) is refused, so
no external entity is ever read and no entity expands. Only XML's five predefined entities and
character references are resolved; any other is an error. A record nested deeper than 256 elements
is refused (a repeated child adds an array level, so the JSON stays within the 512 levels a JSON
record may have). A document that is not well formed, or uses a refused entity, fails the run,
naming the record and the byte where it broke: nothing after that point can be read, so it is not
treated as one bad record, not even by `convert --skip-bad`.

### 2.3 `OutputSpec`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `format` | `parquet` \| `iceberg` | — (required) | `parquet` = one Parquet file. `iceberg` = a self-contained Apache Iceberg v2 table directory (no catalog server), one append snapshot per run (unpartitioned by default; partitioned per `partition_by` — identity or hidden transforms). |
| `path` | string | — (required unless `catalog`) | Destination. For `parquet`: a file path, or a directory (then `<path>/<dataset>.parquet` is written). For `iceberg`: the table root directory (created if absent). |
| `compression` | `snappy` \| `zstd` \| `uncompressed` | `snappy` | Parquet page compression. **`zstd` needs the opt-in fat build** (`--features zstd`); the default musl-static binary has no C codecs, so selecting it there returns a clear error — use `snappy`/`uncompressed` or rebuild with the feature. |
| `partition_by` | list of string | `[]` | Partition fields (Iceberg output). Each entry is a column name (**identity**) or a **hidden-transform expression**: `bucket(N, col)` (spec-exact Murmur3; long/string/date/timestamp), `truncate(W, col)` (long/string), `year(col)`/`month(col)`/`day(col)` (date/timestamp), `hour(col)` (timestamp). When columns are declared, every *source* column must be declared (§9); in `infer` mode it resolves against the inferred schema. **Applied:** the table's partition spec records the real transform strings and rows fan out to one data file per **transformed** value under Hive-style directories (temporal segments human-readable, e.g. `data/event_at_day=2026-01-01/`). Ignored for `parquet` output (a single file is still written). |
| `merge_on` | list of string | `[]` | Iceberg only: **upsert** on these key columns instead of appending. Each written row replaces any row already in the table with the same key (every key column equal); a key the input carries more than once keeps its **last** occurrence; a row with a null in any key column is appended and matches nothing. Creates the table if there is none. Key columns may be `int64`, `string`, `json`, `date`, `timestamp`, or `bool` — `float64` is refused. Rows are removed with Iceberg v2 position deletes (merge-on-read). Works on a local table and, in a build with the `object_store` feature, an object-store `output.path`. `--merge-on` overrides it. |
| `catalog` | `{uri, table, warehouse?}` | none | Iceberg only, in a build with the `object_store` feature: commit to the table `table` (`<namespace>.<name>`, a namespace may have several levels) that the Iceberg REST catalog at `uri` (`http(s)://host:port`) names, instead of a table directory — so leave out `path`. The catalog decides where the files go and is the commit point. A table it lacks is created empty in a namespace that must exist; one that already holds data needs `--append`. An append another writer beat to the commit is rebuilt onto the new head and committed again. With `merge_on` the merge commits through the catalog the same way, but is not rebuilt: another writer's commit refuses it (exit 75). `warehouse` names which of the catalog's warehouses holds the table (sent as `?warehouse=` with `GET /v1/config`; a Polaris catalog's name, a Lakekeeper warehouse). Credentials never go here (§5a). `--catalog` / `--table` / `--warehouse` override it. |
| `apply` | `upsert` \| `changelog` | `upsert` | With `changes:` only (§5b). `upsert` keeps the current state on `merge_on`; `changelog` appends every change with an `_op` column, to an Iceberg table, on every run (as with `--append`). |

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

### 2.6 `source.http` (an endpoint)

In place of `path`, a source can be a REST endpoint, read a page at a time (a build with the
`http` feature: `cargo build -p shapeshift-cli --features http`; the default binary links no HTTP
client). `shape --from <url>` sets `url`; without a spec it is one request whose JSON body is the
records.

```yaml
source:
  http:
    url: https://api.example.com/v1/orders
    auth: { bearer_env: ORDERS_API_TOKEN }
    records: data.items
    paging: { next: links.next }
    incremental: { param: updated_since, from: updated_at, order: asc }
output:
  format: iceberg
  path: warehouse/orders
  merge_on: [id]
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `url` | string | *(required)* | The first page. `http://` or `https://`, with no user info in it. |
| `method` | `GET` \| `POST` | `GET` | `body` (a string) is sent with `POST` on every request. |
| `headers` | map | *(none)* | Sent on every request. A name that looks like a credential is refused: use `auth`. Names are matched by word, so `Authorization`, `Cookie`, `X-Api-Key`, `api_key`, `access_token` or `client_secret` are refused, and `sort_key`, `keyword` or `Idempotency-Key` are not. |
| `query` | map | *(none)* | Query parameters added to every request (a sort order, a filter). Credential-looking names are refused the same way. |
| `auth` | one of the below | *(none)* | `bearer_env: VAR` (`Authorization: Bearer …`), `basic_env: VAR` (holding `user:password`), `header_env: {name, env}` or `query_env: {name, env}`. Each names an **environment variable**; a spec never holds a secret. |
| `format` | `json` \| `jsonl` \| `csv` \| `tsv` \| `xml` | `json` | How each body reads. `source.csv` and `source.xml` apply as for a file (§2.2.1, §2.2.2). |
| `records` | path | *(none)* | Where a JSON body's records are (`data.items`). Absent: an array body is the records, an object body is one record. Absent or null in a body: no records. |
| `paging` | one style | *(one request)* | `next: <path>` (the next page's URL in the body, absolute or relative), `link: true` (the `Link: <…>; rel="next"` header), `page: <param>` with `size: <param>` and `per_page: N` (pages from `first_page`, default 1), `offset: <param>` with `limit: <param>` and `per_page: N` (the offset moves by the records each page held), or `cursor: <path>` with `cursor_param: <param>` (a token from the body). Page and offset paging end at an empty page, since a server may cap its pages below `per_page` (one request more than a short last page would take); the same page twice fails the run, as from an endpoint that ignores the parameter. The others end where the link or token is absent, null or empty. `max_pages` (default 100,000) fails a run whose paging does not end; a next page equal to the current one fails at once. |
| `incremental` | object | *(none)* | Read only what is new since the table's position (below). |
| `soap` | object | *(none)* | A SOAP service: an envelope built around a template, with a WS-Security username token (below). |
| `timeout` | duration | `60s` | One request's limit. |
| `allow_other_origins` | bool | `false` | Follow a next-page link or a redirect to another origin than `url`'s. Off, such a link fails the run: it comes from the endpoint, and could name an address on this machine's network. On, it is followed without the credential or `headers`. |
| `retries` | integer | `5` | Retries on 429, a 5xx or a broken connection (a `POST` too, so it must be a read), waiting what `Retry-After` says (seconds or a date) or backing off from 500 ms, doubling to 30 s. A `Retry-After` over 300 s fails the run, saying so. |

Every page is held whole (at most 64 MiB), one at a time, so memory follows the page, not the
endpoint. A record that does not parse fails the run: its cursor value could not be judged.

**A SOAP service (`soap`).** Each request POSTs an envelope shapeshift builds around
`body_template`, and the response is read as XML from `source.xml.record`:

```yaml
source:
  http:
    url: https://shop.example.com/ws/orders
    soap:
      action: urn:shop#GetOrders
      body_template: >-
        <GetOrders xmlns="urn:shop"><since>{{since}}</since>
        <page>{{page}}</page><size>{{size}}</size></GetOrders>
      username_env: SHOP_SOAP_USER
      password_env: SHOP_SOAP_PASSWORD
    paging: { page: page, size: size, per_page: 100 }
    incremental: { param: since, from: updatedAt, order: asc, tiebreak: id }
  xml:
    record: /Envelope/Body/GetOrdersResponse/Order
```

| `soap` field | Meaning |
|---|---|
| `body_template` | The operation element the envelope's `Body` holds. A `{{name}}` is filled with the request parameter of that name, XML-escaped: `incremental.param`, a `page`/`size` or `offset`/`limit` paging parameter, or a `query` entry. A parameter the template names goes in the body instead of the URL. A placeholder no parameter fills is refused, and an incremental source must name its `param`. On a first run (no position yet) the lower bound's placeholder is empty. |
| `action` | The `SOAPAction` header (1.1), or the content type's `action` (1.2). |
| `version` | `1.1` (the default: `text/xml`) or `1.2` (`application/soap+xml`). |
| `username_env`, `password_env` | A WS-Security `UsernameToken` (`PasswordText`) in the envelope's header, from these environment variables. It goes only to `url`'s origin, and over plain http only to this machine, as any credential. |

A `Fault` in the response, whether its status is 500 or 200, fails the run at once. The error
names the fault code and string, and the request is not retried. A SOAP source is a `POST`,
`format` is `xml`, and `Content-Type` and `SOAPAction` come from `soap`. It pages by `page`,
`offset` or the `Link` header: `next` and `cursor` read a JSON body.

**Credentials.** They go only to the origin (scheme, host and port) of `url`: a redirect or a
next-page link to another origin fails the run before anything is sent there (unless
`allow_other_origins`, which sends neither the credential nor `headers` there), and so does one
from https down to http. Over plain http they go only to this machine, unless `--insecure-source` is given.
No message prints a URL's query or a credential's value. Redirects are followed by shapeshift (at
most 5), not by the HTTP client, so a header of the API's own is never sent on.

**`incremental`.** `param` is the query parameter the run's lower bound is sent in; `from` is
the path of the cursor field in each record; `order: asc` says the endpoint returns records in
ascending cursor order (and, with `tiebreak`, ascending key within a value), which is checked:
a record out of order fails the run.

**An endpoint sorted by a field that changes while it is paged** (`sort=updated`, a record
updated mid-run) can move records in two ways, and only one of them is seen:

- A record that comes back **below** a value already read fails the order check: the run
  commits nothing, and the next one starts again from the last commit.
- A record updated **forward**, past the page being read, shifts the ones after it back by one.
  With `page`/`size` or `offset`/`limit` paging (or a `Link` header that numbers pages), the one
  that shifts across the page boundary is never requested, and nothing tells the run: its
  position can pass a record it did not read.

Against such an endpoint, keep each run short (`--max-pages`, `--max-records`), so it reads a
narrow span of cursor values, and give the cursor a `lookback` that covers that span: a skipped
record keeps its own value, below the new position, and the next run reads that span again and
finds it (it upserts onto `merge_on`). A catch-up run over a long history spans more than any
lookback, so start such a source with bounded runs. An endpoint whose next-page token resumes
after the last record it returned (keyset paging, read with `next` or `cursor`) does not skip. A
cursor is a number, an RFC 3339 timestamp, or text
(compared byte by byte); in a CSV, TSV or XML body a numeric string is a number. The position is
written in the snapshot the rows commit in, as `shapeshift.source.http` (JSON: `{"at": v}` or
`{"inside": v, "after": k}`), and carried forward by every later snapshot, so the table is the
checkpoint: a failed run commits neither rows nor position, and the next run resumes from the last
commit. The position also records which source it is a position in (`"of"`: the URL without its
query and the cursor field, or a database's URL, table or query digest, and cursor); a run reading
another source into the table is refused, since resuming there would skip or repeat rows. An
incremental source needs Iceberg output.

The position rules, which every pulled source follows (the MySQL source too):

1. **A position** is `at v` (every record below `v` was read, and the group of records at `v` as
   it stood), or `inside v, after k` (only with `tiebreak`, a unique field: also every record at
   `v` with a key up to `k`).
2. **What is new.** From `at v` a run asks for values `>= v`: those at `v` are *replay* (a
   record may have appeared at `v` since), those above are *new*. `lookback` (a duration, for a
   timestamp cursor) widens the replay down to `v - lookback`, the *replay cutoff*. From `inside
   v, after k` the records at `v` past `k`, and those above `v`, are new; with a lookback the
   records from the cutoff up to (not at) `v` are replay, as from `at v`. A run that reads
   nothing new keeps the position it started from: replay never moves it.
3. **Replay never doubles.** An incremental source needs `output.merge_on` (replayed rows upsert
   onto themselves), or `strict: true` for a field that is unique and only grows (an
   auto-increment id): a record at the position's value was read already, and is skipped. Otherwise the spec is refused. A changelog
   (`apply: changelog`) needs `strict`.
4. **Bounds count new records only.** `--max-records` counts new records; `--max-pages` and
   `--max-wait` count from the first new one. Replay is never charged.
5. **Where a bounded run stops.** A bound needs `order: asc`; without it bounds are refused, and
   a run reads to the end and commits `at` the largest value it saw. With a tiebreak a run may
   stop after any record (at a page's end too) and commits `inside v, after k`. Without one it stops only at the end
   of a group: reaching its bound inside one, it reads on to the group's end and commits `at v`,
   and a group larger than `max_tie` (default 100,000) fails the run, naming the value and the
   fix (a tiebreak, or a finer field). A run that reaches the end commits `at` the last value.
6. **Progress.** A bounded run with anything new commits a later position. A run with nothing new
   commits the same one and says so (`nothing new`).
7. **A late record** with value `w` is read by the next run when `w >= v - lookback` for the
   position that run starts from, however late it appears; below the cutoff it is **missed**. So
   `lookback` must cover the furthest a record's value can trail the newest value already read
   when it becomes visible: the longest transaction, plus clock skew between writers, plus how far
   back an edit can be dated. There is no default. A change feed (§5b) has no such gap.

A catalog commit that another writer beats is not rebased when it carries a position (theirs may
be later): it fails as a conflict, nothing of it published, and the next run starts again from the
new head.

### 2.7 `source.mysql` (a database)

In place of `path`, a source can be a MySQL query or table (a build with the `mysql` feature:
`cargo build -p shapeshift-cli --features mysql`; the default binary links no database client).
`shape --from mysql://user@host/db` sets `url`, with `--query` or `--source-table`, and
`--watermark` for the cursor.

```yaml
source:
  mysql:
    url: mysql://app@db.example.com/shop
    table: orders                # or: query: select … where updated_at >= :cursor order by updated_at
    cursor: updated_at
    lookback: 5m
output:
  format: iceberg
  path: warehouse/orders
  merge_on: [id]
```

| Field | Type | Default | Meaning |
|---|---|---|---|
| `url` | string | *(required)* | `mysql://user@host[:port]/database`. A password in it is refused: it comes from `SHAPESHIFT_MYSQL_PASSWORD`, or the file `SHAPESHIFT_MYSQL_PASSWORD_FILE` names. |
| `query` | string | *(one of these)* | A `SELECT`. With `cursor` it names `:cursor`, bound to the run's lower bound, and orders by the cursor (the order is checked: a row out of order fails the run). |
| `table` | string | *(one of these)* | A table (`orders`, `db.orders`), read by `cursor` and its primary key. |
| `cursor` | column | *(none)* | The column the position follows (a watermark like `updated_at`, or an id). Set, the source is incremental (rules of §2.6). An integer, `FLOAT`/`DOUBLE`, `DATE`, `DATETIME` or `TIMESTAMP`: text sorts by its collation and a `DECIMAL` arrives as text, so neither can carry a position (refused). |
| `start` | string | *(none)* | The first run's lower bound (`2020-01-01`, `0`). A query's first run needs it, to bind `:cursor`; a table's first run reads every row without it. |
| `tiebreak` | column | *(none)* | A query's unique column ordering rows within one cursor value: an integer or temporal column (refused otherwise). A table's is its primary key, when the key is one such column. |
| `lookback` | duration | *(none)* | How far below the position the next run re-reads (a `DATETIME` or `TIMESTAMP` cursor). |
| `strict` | bool | `false` | The cursor is unique and only grows (an auto-increment id): no replay. |
| `max_tie` | integer | `100000` | As in §2.6. |
| `ca_file` | path | *(none)* | A PEM file of CA certificates the server's is checked against, beside the bundled public roots (Amazon RDS's, a private CA's). |

**One run, one snapshot.** A run opens one connection, sets its time zone to UTC, and reads in a
`REPEATABLE READ`, `READ ONLY` transaction started `WITH CONSISTENT SNAPSHOT`: it sees one state
of the database, and cannot change it. Rows stream through a queue of a few hundred, so memory does
not follow the result's size. A bounded run that stops early ends the server's read
(`KILL QUERY`) rather than drain it. Over the network the connection needs TLS (checked against
the bundled public roots and `ca_file`), unless `--insecure-source`; to this machine it is plain.
A `ca_file` always means TLS, even to this machine and even with `--insecure-source`: a CA named
is a CA to check. `--max-wait` ends a run even while the server is slow to send, and a
connection that goes quiet is found by TCP keepalive; a slow query is never cut.

**A table read.** The statement is
`SELECT * FROM t WHERE cursor IS NOT NULL [AND …] ORDER BY cursor, key`: from `at v`, `cursor >= v`
(`> v` when `strict`, `>= v - lookback` with a lookback); from `inside v, after k`,
`cursor > v OR (cursor = v AND key > k)`, so a bounded run over a group of any size stops inside it
and the next resumes there. **A row whose cursor is NULL is not read**: `COALESCE` it in a query if
it must be. A composite or text primary key orders the rows but is no tiebreak (`max_tie` applies). Column
names are quoted as identifiers, a backtick in one doubled. The session's `net_write_timeout` is
an hour, so a pause while the run writes does not end the server's read.

**Types.**

| MySQL | Record |
|---|---|
| `TINYINT(1)`, `BIT(1)` | boolean |
| other integers, `YEAR`, `BIT(n)` | number (`BIGINT UNSIGNED` exactly) |
| `FLOAT`, `DOUBLE` | number (a `FLOAT` as the shortest decimal that is the same `FLOAT`: `1.1`) |
| `DECIMAL` | string (`"12.50"`, exact): declare the column's type to coerce it |
| `DATETIME`, `TIMESTAMP` | `2026-11-01T06:39:00Z` (UTC; a `DATETIME` is read as UTC), microseconds when there are any; a zero date is null |
| `DATE` | `2026-11-01` |
| `TIME` | `[-]HH:MM:SS[.ffffff]` |
| `JSON` | the JSON value |
| `CHAR`, `VARCHAR`, `TEXT`, `ENUM`, `SET` | string |
| `BLOB`, `BINARY`, `VARBINARY`, geometry, `VECTOR` | refused before any row is read, naming the column and the cast (`HEX(…)`, `ST_AsText(…)`) |

Two columns of one name are refused: alias one.

**Deletes, and what a query cannot see.** A soft delete is a change (§5b): `changes: { op:
is_deleted, update: [false], delete: [true], order_by: [updated_at] }` on a query that returns the
column removes the row from the table. A **hard** `DELETE` leaves nothing for a query to read, and
is invisible to this source: it does not read the binlog. A change feed (Debezium, §5b) carries
hard deletes.

**What `lookback` must cover here.** `updated_at` is set when a row is written, not when its
transaction commits, so a row written by a transaction that commits after a run has read past its
stamp turns up late, behind the position (§2.6, rule 7). `lookback` must be at least the longest
transaction that writes the table, plus the clock skew between the servers that stamp it. A row
updated without moving its `updated_at`, or stamped in the past, is missed outright.

### 2.8 `source.postgres` (a database)

The Postgres twin of §2.7, in a build with the `postgres` feature (`--features postgres`).
`source.postgres` takes the same fields as `source.mysql`, and `shape --from
postgres://user@host/db` (or `postgresql://`) with `--query` or `--source-table` and
`--watermark` sets them. Where it differs:

- **The password** comes from `SHAPESHIFT_POSTGRES_PASSWORD`, or the file
  `SHAPESHIFT_POSTGRES_PASSWORD_FILE` names. TLS is needed off this machine, unless
  `--insecure-source`, and `ca_file` adds a CA.
- **A query** names `:cursor` as a MySQL query does; it is sent as the statement's `$1`, typed as
  Postgres infers it from the query. A `:cursor` inside a string literal, a quoted name, a
  comment or a dollar-quoted body is left alone, as is a `::` cast.
- **Names resolve through the role's search path**, which can change between runs, so a position
  names how they resolved: a table given without its schema as the schema it was found in
  (`table shop.orders`), a query with the search path it ran under, and the role. The read runs
  with that same search path and in that schema, so it reads the relation its position names.
  A table not on the search path is refused before anything is read. A position recorded under
  another resolution is refused, as for another source. A query's text is not parsed, so its
  position names the search path, not the tables its names find on it: a table created later
  in an earlier schema on the path shadows the one it read. Name the schema in a query
  (`FROM shop.orders`) to rule that out.
- **The URL** may set `connect_timeout`, `application_name` and `target_session_attrs` in its
  query, and nothing else: a password, a `passfile`, a `hostaddr`, `options` or an `sslmode` is
  refused, since the password comes from the environment and TLS from where the host is.
- **A table** is `orders` or `schema.orders`. Its names are quoted as written, so they match as
  Postgres stores them (lowercase, unless created quoted).
- **One run, one snapshot.** A run reads in a `REPEATABLE READ`, `READ ONLY` transaction in UTC,
  through a server-side portal, 500 rows a fetch. A run that stops at its bound stops fetching,
  and the rest is never sent.
- **Cursors and tiebreaks.** A cursor is an integer, a float, or a `date`, `timestamp` or
  `timestamptz`. A tiebreak is an integer, one of those dates, or a `uuid`, whose text sorts
  as its bytes do. `text` sorts by its collation and `numeric` arrives as text, so neither is
  either (refused). A table's one-column primary key of such a type is its tiebreak; a
  composite or text key only orders the rows.

**Types.**

| Postgres | Record |
|---|---|
| `boolean` | boolean |
| `smallint`, `integer`, `bigint`, `oid`, `real`, `double precision` | number (a `real` as its shortest decimal); `NaN`, `Infinity` and `-Infinity` as those strings, as a `numeric` prints them |
| `numeric` | string, exact (`"12.50"`) |
| `date` | `2026-11-01` |
| `timestamp`, `timestamptz` | `2026-11-01T06:39:00Z` (UTC), microseconds when there are any |
| `time` | `HH:MM:SS[.ffffff]` |
| `json`, `jsonb` | the JSON value |
| `text`, `varchar`, `char`, `"char"`, `name`, an enum | string |
| `uuid` | its text |
| `bytea`, arrays, anything else | refused before any row is read, naming the cast (`encode(…, 'hex')`, `to_json(…)`, `…::text`) |

What §2.7 says of deletes and `lookback` holds here too: a soft delete is a change (§5b), a hard
`DELETE` needs a change feed, and `lookback` must cover the longest transaction that writes the
table.

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

## 5a. Catalog credentials

A REST catalog that asks for credentials gets them from the **environment**, never from the
spec: a spec is meant to be committed, and a credential in it would be too.

| Variable | Meaning |
|---|---|
| `SHAPESHIFT_CATALOG_TOKEN` | A bearer token, sent as is with every catalog request. |
| `SHAPESHIFT_CATALOG_CREDENTIAL` | OAuth2 client credentials, `client_id:client_secret` (or a secret alone), exchanged for a bearer token with the `client_credentials` grant. The token is fetched again once nine tenths of the life the endpoint gives it (`expires_in`, a number or a numeric string) have passed, so a long run never sends one past it; a token issued without `expires_in` is kept for the run. |
| `SHAPESHIFT_CATALOG_OAUTH2_SERVER_URI` | The token endpoint, when it is not the catalog's own `{uri}/v1/oauth/tokens` (an identity provider in front of the catalog). With a credential only. |
| `SHAPESHIFT_CATALOG_SCOPE` | The scope asked for; default `catalog`, as Iceberg's own clients ask. Polaris wants `PRINCIPAL_ROLE:ALL`. With a credential only. |

- Setting both a token and a credential, or a token endpoint or scope without a credential, is
  refused before any request is made.
- The system's CA certificates must be installed (or `SSL_CERT_FILE` pointed at a PEM bundle):
  the HTTP client checks TLS against them, and without any it cannot start, so every catalog and
  object-store call fails, plain http included, with an error that names the missing
  certificates and what to do.
- No variable set means no credentials, which is what a catalog that asks for none (Iceberg's
  reference fixture) wants. A catalog that answers 401 or 403 is reported with which variable
  to set, or that the one set was refused.
- A catalog URI or token endpoint carrying credentials (`user:password@host`) is refused: URIs
  are printed in errors.
- A token or secret never appears in an error, a log line or `--summary-json`. A token
  endpoint's refusal is reported by its status and, when it is one of RFC 6749's, its error code
  (`invalid_client`); nothing else the endpoint wrote is shown, since it may echo the request.
- Credentials are not sent over plain `http://` to a host other than this machine: such a
  catalog or token endpoint is refused before any request. OAuth 2.0 requires TLS at the token
  endpoint. The insecure mode, `--insecure-catalog` on the command line of one run, allows it for
  a network trusted on its own terms (a compose network), and the run logs a warning; nothing in
  the environment or a spec turns it on. Without credentials, plain http is fine (Iceberg's
  reference fixture).
- A catalog or token endpoint on this machine is never reached through a proxy, whatever
  `HTTP_PROXY` / `NO_PROXY` say: no proxy can reach it, and the token would cross the network
  to the proxy in the clear. Other hosts follow the usual proxy variables. The token request never
  follows a redirect, since its body carries the secret; a catalog request that does follow one
  to another host drops the token, as `reqwest` does for any cross-host redirect.
- Object-store credentials for the table's files are separate: the same `AWS_*` / GCS / Azure
  environment as an object-store `--output`. The catalog does not vend them.

---

## 5b. Change feeds (`changes:`)

A feed of changes says more than a row: what happened to it. `changes:` reads that, and
`output.apply` says what the table keeps.

```yaml
changes:
  op: op                       # the field that says what happened
  insert: [c, r]               # the values of `op` that mean each kind
  update: [u]
  delete: [d]
  row: after                   # where the row is; absent: the record itself
  key_from: before             # where a delete's key is; absent or null: `row`
  order_by: [ts]               # output columns that version a change
output:
  format: iceberg
  path: warehouse/orders
  merge_on: [id]
  apply: upsert                # the default; or changelog
columns:
  - { name: id, type: int64 }
  - { name: status, type: string }
  - { name: ts, type: int64, from: $.source.ts_ms }
```

**Reading a change.**

- `op` is read from the record. Its value is matched against the four lists (a string, a number or
  a boolean: `delete: [true]` on an `is_deleted` field works). A record with no `op`, or an `op` in
  no list, is a reject. A value in `skip` is counted (`rows.skipped`) and otherwise ignored.
- Columns read their `from` paths against the **row**: `row`, or the record when `row` is not
  set. A path that starts with `$` reads the **whole record** instead, which is how a version
  or a timestamp that lives in the envelope (`$.source.ts_ms`) becomes a column.
- A delete reads its key from `key_from` when that is present and not null, else from the row.
  In `apply: upsert`, a delete whose key is null or missing is a reject: a delete that matches
  nothing by accident is refused.
- With `row`, schema drift (§7) is checked on the row, not the envelope; inference samples the
  rows too.

**`apply: upsert`** (needs `merge_on`). The table holds the current state. An insert or update
upserts on the key; a delete removes the key's live row (a position delete) and writes nothing.
Within one run, each key's winning change is its last one, or with `order_by` its newest.

**`order_by`** names output columns (`int64`, `date`, `timestamp` or `string`, compared left to
right; a string compares by its bytes, so zero-pad numbers) that are written to the table like
any column, since a later run compares with them:

- within a run, the change with the greatest version wins; on a tie, the later arrival;
- against the table, a change applies only if it is **newer than the key's live row**. One as
  new or older is **stale** (`rows.stale`) and the table keeps its row, so a feed replayed
  after a failure changes nothing;
- a change with a null version is a reject;
- with `key_from`, the version must be read from the envelope (`from: $.…`): a delete's
  `key_from` is the row before it changed, whose version is the live row's own, so a version
  read from it would make every delete stale. Such a spec is refused;
- the snapshot records the order (`shapeshift.changes.order-by`), and every later shapeshift
  write carries it forward (an append, a compaction), so expiring old snapshots keeps it. A
  later merge that orders changes another way, or not at all, or that is a plain `merge_on`, is
  refused. (A write by another engine records nothing: if every snapshot since the order was
  recorded comes from another engine and the one that recorded it is expired, the guard has
  nothing left to read.)

Without `order_by`, changes apply in arrival order, which is right only for a feed that is
already in order.

**Tombstones.** With `order_by`, a delete is remembered once its row is gone: the key and the
version that deleted it. A later change for that key that is not newer than its tombstone is
stale too, so a late update, or a replay of the feed, does not bring a deleted row back. A
newer change re-creates the row and retires the tombstone; a delete of a key the table never
had is remembered all the same.

- They are kept for `changes.tombstone_retention` (a duration: `7d`, `36h`; default 7 days,
  at least `1h`) and let go after it. The window counts from the merge that applied the delete,
  which stamps it seconds before its commit. A change that arrives later than that for a key deleted before it
  brings the key back: set the retention longer than the feed's worst delay.
- They live in one small file under `metadata/` (an Iceberg Puffin file, blob type
  `shapeshift-tombstones-v1`), named by every snapshot's summary (`shapeshift.tombstones`) and
  in the table's `statistics` list, which keeps it for other engines' orphan-file cleanup and
  for `relocate`; a reader that does not know the blob type skips it. Every shapeshift write
  names it again (appends and compactions included), and `expire` deletes a tombstone file
  only when no snapshot it keeps names it, in its summary or its `statistics` entry (so an
  engine's `ANALYZE` that replaces the entry does not lose the file). `expire` also keeps every
  snapshot from the current one back to the nearest that records the change-feed state, so
  another engine's writes on top cannot cut the table off from its tombstones. (Another
  engine's own expiry can; run expiry with shapeshift on a change-feed table.) Through a REST catalog it goes as `set-statistics`,
  and an expiry removes the expired snapshots' entries (`remove-statistics`).
- A merge that cannot read the table's tombstone file, or one written for another key or
  order, is refused, as is a merge without `order_by` into a table that has tombstones.
- The table records the format of this state (`shapeshift.changes.format`, `1`). A shapeshift
  that finds a newer format refuses to write the table at all.
- **shapeshift 0.5 does not know any of this.** A 0.5 merge into a table with tombstones
  ignores them and can bring deleted rows back, and nothing in the table can stop it: every
  writer of a change-feed table must be 0.6 or later.

**`apply: changelog`** keeps the history instead: every change is appended with a column
`_op` (`insert`, `update` or `delete`; a delete's row is read from `key_from`), nothing is
removed, and every run adds to the table rather than replacing it. It takes no `merge_on` and
no `order_by`, and needs `format: iceberg`.

**Presets.** A known feed needs only `preset:`. It fills in what the spec leaves out (anything
the spec sets itself wins), and adds the columns that version a change, unless the spec
declares columns of those names:

| Preset | Reads | Adds | Orders by |
|---|---|---|---|
| `debezium-postgres` | Debezium's envelope (the JSON converter with schemas off): `op` `c`/`r` insert, `u` update, `d` delete; `row: after`, `key_from: before` | `cdc_lsn` ← `$.source.lsn`, `cdc_ts_ms` ← `$.source.ts_ms` | `[cdc_lsn]` |
| `debezium-mysql` | the same envelope from MySQL | `cdc_file` ← `$.source.file`, `cdc_pos` ← `$.source.pos`, `cdc_row` ← `$.source.row`, `cdc_ts_ms` | `[cdc_file, cdc_pos, cdc_row]` |
| `flink-cdc` | Flink CDC's changelog JSON, `{data, op}`: `+I` insert, `+U` update, `-D` delete, `-U` skipped; `row: data` | nothing | arrival order. The feed carries no version, and a `-D` carries the row as it was, so a version read from the row would make every delete stale: `order_by` is refused with this preset |

A spec that sets an op list keeps it; each list it leaves empty takes the preset's.

The Debezium presets want the connector's `tombstones.on.delete: false` (a delete's key comes
from `before`). What their order assumes, stated:

- **Postgres:** a change's LSN. Writes to one key are serialized by the database, so their
  LSNs follow their commits.
- **MySQL:** the binlog file name, then the position, then the row within the event. File names
  compare as strings, which holds while the server keeps one basename and six-digit numbering;
  a changed `log_bin` basename, more than 999999 files, or a failover to another server (whose
  binlog positions are its own, GTID or not) breaks the order. After any of these, rebuild the
  table: apply a new snapshot and the stream after it to a new table. A snapshot applied to the
  old one changes no row a change has set (below).
- **Snapshot rows are older than every change.** A row the connector read from the table as it
  stood (`op: r`: the initial snapshot, and incremental snapshots) is not a change, and it
  takes the lowest version: `cdc_lsn` 0, or an empty `cdc_file` with `cdc_pos` and `cdc_row`
  0. Whatever order they arrive in, a change to a key wins over its snapshot row, and a
  snapshot row only fills a key no change has touched; one that arrives after its key's
  delete is refused by the delete's tombstone. So writes need not stop while a snapshot is
  taken. On Postgres they could not before: a transaction open across the snapshot commits
  after it with LSNs below the snapshot's, and its updates and deletes were stale against the
  snapshot rows (measured on a real feed, kept in `examples/changes/debezium/handoff/`). A spec that sets its own `order_by` gets the same
  rule: a snapshot row takes the lowest value of each column's type (0001-01-01 for a date or
  a timestamp). A table shows a snapshot row's version as that lowest value; the time it was
  read stays in `cdc_ts_ms`. A change versioned at exactly that lowest value in every
  `order_by` column would tie with a snapshot row, and lose: it is rejected instead, with the
  reason. No change from Postgres or MySQL carries it; a version of the spec's own can (an
  empty string). They are checked against real feeds from Postgres and MySQL, delivered out of
order, in `examples/changes/debezium/`. A Debezium `t` (truncate) is a reject.

**Counts.** The report and `--summary-json` add `changes` (inserts, updates, deletes, skipped),
`rows.deleted` (live rows a delete removed), `rows.stale` and `rows.skipped`, and the merge
report `keys_deleted`, `rows_deleted`, `changes_stale`, `order_by` and the tombstones written,
retired (a newer change re-created the key), trimmed (past the retention) and live.

A change feed runs from the command line in this release; the console refuses a spec with
`changes:`.

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
- A path has at most **512 segments**, as deep as a record may nest (§8); a longer one is a
  validation error.

When `from` is omitted, the column `name` is used as the path (so `name: id` reads `id`).

---

## 6a. Row filter (`where`)

`where` names the source records a run takes. One stream often carries several kinds of record
(a `record_type` field, an `op` field); a spec describes one kind, and `where` sets the others
aside:

```yaml
dataset: gate
where: record_type = 'gate' and verdict in ('pass', 'fail')
```

It is tested on each **source record, before shaping**, so it can test a field no column keeps,
and a record of another kind never reaches a required column, a strict coercion or drift
detection. A record it does not keep is **filtered**: counted (`filtered=` in the report,
`rows.filtered` in `--summary-json`), never written to the rejects sidecar, never seen by
inference or drift detection. `rows_in` is the sum of `rows_out`, `rejected` and `filtered`.
Inference (`infer`, and `shape` in `infer` mode) samples only the records the filter keeps. The
fields it reads count as read by the spec, so they are never reported as drift; a scalar where
it reads deeper (`payload` is `7`, the filter reads `payload.kind`) is read by nothing, and is.
A drift report's "first at record N" counts the filtered records too, so N is the record's
place in the input.

### Grammar

A small subset of SQL's `WHERE`, so it reads the way the table will be queried, and DuckDB runs
the same text:

```text
expr     := or
or       := and { "or" and }
and      := unary { "and" unary }
unary    := "not" unary | "(" expr ")" | test
test     := operand cmp operand
          | operand [ "not" ] "in" "(" literal { "," literal } ")"
          | operand "is" [ "not" ] "null"
cmp      := "=" | "!=" | "<" | "<=" | ">" | ">="
operand  := path | literal
literal  := 'string' ('' for a quote) | number | true | false
```

- **Paths** are `from:` paths (§6): `user.plan`, `tags.0`, an optional leading `$`. A name of
  letters, digits, `_` and `-` (not first) is written bare; any other name is double-quoted, a
  segment at a time: `"user name".first`, `"in" = 'x'` for a field called `in`. A path that does
  not resolve is null, as in `from:`.
- **Strings** take single quotes. A double-quoted word is a field, as in SQL: `kind = "gate"`
  compares two fields.
- **Keywords** (`and`, `or`, `not`, `in`, `is`, `null`, `true`, `false`) are case-insensitive;
  paths and strings are not. `and` binds tighter than `or`, and `not` tighter than `and`.
- **One spelling per operator.** `==`, `<>`, `&&`, `||` and `!` are refused, each naming the
  spelling to use; so is `= null` (use `is null`), a `null` in an `in` list, a comparison of two
  values with no field, and `<` (or `<=`, `>`, `>=`) against `true` or `false`.
- At most 64 KiB of text, 64 levels of `not` and parentheses, and 512 segments in a path. The
  expression is parsed when the spec loads; a mistake is a validation error that names the
  character, with the text and a caret. `shape --where` replaces the spec's before it is
  checked, so it can stand in for one the file gets wrong.

### What a comparison means

SQL's three-valued logic: a test is true, false or **unknown**, and a record is kept only when the
whole expression is **true**. `not unknown` is unknown; `unknown or true` is true; `unknown and
false` is false.

| Operands | Result |
|---|---|
| number, number | numeric (two integers exactly; otherwise as floats) |
| string, string | by code point |
| `true`/`false`, `true`/`false` | `=` and `!=` only |
| either side null or missing | unknown (`is null` and `is not null` say which) |
| different kinds, or an array or object | unknown, and counted as a **type mismatch** |

- `status != 200` therefore drops a record with no `status`, as every SQL engine does. To keep
  it: `status != 200 or status is null`.
- `x in (a, b)` is `x = a or x = b`; `not in` is its negation, so a null `x` is unknown either way.
- Dates and timestamps are compared as they are in the source: ISO-8601 strings in one format
  order correctly as strings, epoch numbers as numbers. There is no date type in the filter.
- **Type mismatches are reported**, because the likeliest mistake is `status = '500'` against a
  numeric field, which keeps nothing. The report says how many records met one and describes the
  first (`--summary-json`: `where.type_mismatches`, `where.first_mismatch`); a run that kept no
  record says so too. Neither changes the exit status.

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

`shapeshift shape` never aborts on a bad **source** line (a JSONL parse error). A line is bad
when it is not valid JSON, when it is larger than 256 MiB, or when it nests deeper than **512
levels** of objects and arrays together. Nothing real comes near that depth; the bound is what
keeps one deliberately deep line from exhausting the stack. Such lines are
counted and written to `<output>.rejects.jsonl`, one JSON object per line:
`{"line": <n>, "error": <message>, "raw": <original text>}`. Rows dropped by a lenient soft-reject
are counted in the run report (`rejected=…`); the report line printed by `shape` is:

```text
rows_in=.. rows_out=.. rejected=.. parse_errors=.. row_groups=.. bytes=..
```

---

## 9. Validation errors

`DatasetSpec::validate()` runs on load, after serde has checked the document shape (a missing
required field — `dataset`, `output`, `output.format`, a `catalog`'s `uri` or `table`, or a
column's `type` — fails there first). The structural checks and their messages:

| Condition | Message |
|---|---|
| `dataset` empty or whitespace | `` `dataset` must not be empty `` |
| `schema: strict` with no `columns` | `strict schema mode requires at least one declared column` |
| Two columns share a `name` | `` duplicate column name `<name>` `` |
| `partition_by` names a column not in `columns` (declared/`strict` mode only) | `` partition column `<name>` is not a declared column (declare it to partition by it) `` |
| Neither `path` nor `catalog` | `` `output.path` is required (or `output.catalog` for an Iceberg table a REST catalog names) `` |
| `catalog` with `format: parquet` | `` `output.catalog` applies only to Iceberg tables (`format: iceberg`) `` |
| `catalog` and `path` both set | `` `output.path` and `output.catalog` both name the table: the catalog decides its location, so drop the path `` |
| `catalog` with an empty `uri` or `table` | `` `output.catalog` needs both a `uri` and a `table` `` |
| `catalog` with an empty `warehouse` | `` `output.catalog.warehouse` is empty: name the warehouse, or leave the key out `` |
| `catalog` with a `token`, `credential` or other secret | `` unknown field `token` … A catalog's credentials never go in a spec, which is meant to be committed: set SHAPESHIFT_CATALOG_TOKEN, or SHAPESHIFT_CATALOG_CREDENTIAL (client_id:client_secret), in the environment `` |
| `merge_on` with `format: parquet` | `` `output.merge_on` applies only to Iceberg tables (`format: iceberg`) `` |
| `merge_on` names a column twice | `` `output.merge_on` names `<name>` twice `` |
| `merge_on` names a column not in `columns` (`strict` mode only) | `` merge key `<name>` is not a declared column (declare it to merge on it) `` |
| `output.apply` without `changes:` | `` `output.apply` says how to apply a feed of changes: it needs a `changes:` block `` |
| `changes:` with `apply: upsert` and no `merge_on` | `` `changes:` applies each change to a key: set `output.merge_on` (or `output.apply: changelog` to keep every change instead) `` |
| `apply: changelog` with `merge_on`, `order_by`, or a Parquet output | it names the one to drop, or that a changelog needs `format: iceberg` |
| `changes.op` empty; no value under `insert`, `update` or `delete`; a value in two lists, or not a scalar | it names the field or the value |
| `changes.order_by` names a column twice, or (`strict` mode) one not declared | it names the column |
| `changes.tombstone_retention` that is not a duration, or without `order_by` | it names the field |
| `changes.preset` that is not one of the three | the serde error names the three |
| A column named `__shapeshift_change`, or `_op` with `apply: changelog` | the name is shapeshift's: rename the column |
| `source.csv` with a format other than `csv` or `tsv` | `` `source.csv` applies to csv and tsv sources, not <format> `` |
| `format: xml` with no `source.xml` | `` an xml source needs `source.xml.record`: the repeating element, like `/Orders/Order` `` |
| `source.xml` with a format other than `xml` | `` `source.xml` applies to xml sources, not <format> `` |
| `source.xml.record` empty, or with an empty step (`/a//b`) | `` `source.xml.record` "<path>" is not a path of element names `` |
| A `delimiter` or `quote` that is not one ASCII character, or is a line break | `` `source.csv.<key>` must be one ASCII character other than a line break `` |
| `delimiter` equal to `quote` | `` `source.csv.delimiter` and `quote` must differ `` |
| `source.csv.columns` with an empty or repeated name | `` `source.csv.columns` has an empty name ``, or `` … names `<name>` twice `` |
| `where` does not parse (§6a) | `` `where`: <what is wrong> (at character <n>) ``, then the text with a caret under the character |

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
- With no spec, `--delimiter`, `--no-header`, `--columns a,b,c` and `--all-strings` set
  `source.csv`, and `--xml-record <path>` sets `source.xml.record`. `infer` writes them into the
  spec it prints, and `cost` and `convert` take them too. With `--spec` they are refused: the
  spec's `source.csv` and `source.xml` say how to read its input.
- `-o/--output` overrides `output.path`; `--to` overrides `output.format`; `--compression`
  overrides `output.compression`.
- `--dataset` overrides `dataset`.
- `--partition-by` overrides `output.partition_by`; `--merge-on` overrides `output.merge_on`
  (comma-separate a composite key).
- `--where <expr>` overrides `where`. `infer --where <expr>` samples only the records it keeps,
  and writes it into the spec it prints.
- `--catalog <uri> --table <ns>.<name>` (always together, never with `-o`) override
  `output.catalog`, set aside the spec's `output.path`, and make the output Iceberg unless `--to`
  says otherwise. `--warehouse <name>` overrides `output.catalog.warehouse`; it needs a catalog,
  from the flags or the spec. `--insecure-catalog` (no spec field) lets the catalog's credentials
  go over plain http to another machine (§5a); it needs a catalog too. `compact` and `expire`
  take `--catalog`, `--warehouse` and `--insecure-catalog` too.

- `--from <url>` sets `source.http.url`, or `source.mysql.url` for a `mysql://` URL (and sets
  aside `source.path`); `--records <path>` sets `source.http.records`; `--query`,
  `--source-table`, `--watermark` and `--start` set `source.mysql`'s `query`, `table`, `cursor`
  and `start`. `--max-records`, `--max-pages` and `--max-wait` bound one run of an
  incremental endpoint or database (§2.6, rules 4 and 5; a database has no pages), and
  `--insecure-source` lets an endpoint's credentials go over plain http, or a database
  connection go without TLS, to another machine. `--source-hosts h1,h2:5432` limits the hosts
  the source may reach, whatever the spec says: the first URL, each redirect and page (even with
  `allow_other_origins`), and a database's hosts as its driver reads them (several in one URL,
  or a `host=` parameter); a host alone takes any port. It is for a runner that holds runs to a
  list of its own. None of these has a spec field, since each is about one run.

`--run-id <id>` has no spec field either: it names one run of a spec, not the spec. It is recorded
in the snapshot (`shapeshift.run-id`), and a run whose id the table already holds commits nothing
and reports that snapshot (Iceberg only; 1–128 letters, digits, `.`, `_`, `:` or `-`).

With no `-s/--spec`, `shape` infers a spec on the fly (then `--input` and `--output`, or
`--catalog` and `--table`, are required),
which is equivalent to running `infer` and immediately shaping with the result.

Flags that only change how a run reports, not what it writes, have no spec field: `--summary-json`
prints one JSON document (`summary_version: 1`) on stdout in place of the text report — the
counts, the table version and snapshot committed, the merge and drift summaries, or the error
and its kind. The README's "Script it" section lists its fields. `--metrics-file <path>` writes
the same run's numbers to a file in the Prometheus text format on exit, failed runs included
(the README's "Monitor it" section).

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
  time (before any data is written). Concurrent writers are **guarded**: a commit creates the
  next `metadata.json` only if no other writer did, and the loser fails with a commit conflict
  (CLI exit status 75) having published nothing — run it again. Through a REST catalog
  (`output.catalog`) the catalog checks the commit, with the same exit status for a loser.
- `compression: zstd` is **off by default** (the musl-static binary links no C codecs) — it returns
  a clear error there; use `snappy`/`uncompressed`, or build with `--features zstd` (the opt-in fat
  build) to enable it for both Parquet and Iceberg.
- Both `source.format`s stream with bounded RAM: `jsonl` a line at a time, `json-array` an element
  at a time.
- Object-store output (S3/GCS/Azure) ships behind the CLI's `object_store` feature (off the default
  musl-static binary); Iceberg tables are **location-anchored** (absolute paths, valid where
  written). A moved or copied table is still **readable** via DuckDB's
  `iceberg_scan('<new path>', allow_moved_paths=true)` (verified for partitioned and multi-snapshot
  tables), and shapeshift reads and writes it where it is, however many times it has moved: a path
  recorded under any directory the table has been written at is read at the same key where it is
  now. `inspect` says the table has moved, and a write into it warns. `shapeshift relocate`
  (0.5; no spec) re-anchors it so any engine reads it with no flag: it rewrites the metadata,
  manifests and position-delete files that name the old places, never a data file, and keeps
  every snapshot.
- `merge_on` is guarded like `--append` (of two writers on one version, the second exits 75 having
  published nothing), and leaves position-delete files in place until the table is compacted
  (`shapeshift compact`, which takes no spec). A key column's type is checked when the schema is
  known (at write time in `infer` mode).

---

*shapeshift v0.1 — © 2026 Nicholas Lu Chee Seng and the shapeshift contributors. Apache-2.0.
Repository: https://github.com/lucheeseng827/shapeshift*
