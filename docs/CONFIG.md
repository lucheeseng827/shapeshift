# shapeshift configuration reference

Every setting of the `shapeshift` binary in one place: the command-line flags of each command,
the environment variables it reads, the build features that decide which flags exist, and the
dataset spec's top-level keys. The spec's fields are documented field by field in
[SPEC.md](./SPEC.md); this page points there rather than repeating them.

**Where a setting comes from.** A run's dataset is described by its spec (`--spec`, YAML or
JSON). A flag that names a spec field overrides the spec for that run (`--output`, `--merge-on`,
`--where`, …). Credentials are never in a spec or on the command line: they come only from the
environment variables below, so a spec can be committed and a command line logged.

**Features.** The release binary and image are the lean build. A flag marked with a feature
exists only in a build with it (`cargo build --release -p shapeshift-cli --features "…"`); in
the lean build it is an unknown flag.

| Feature | Adds |
|---|---|
| *(lean)* | `infer`, `shape`, `inspect`, `compact`, `expire`, `relocate`, `cost`, `convert`, `mcp`; JSON, JSON Lines, CSV, TSV and XML input; Parquet and local Iceberg output; Snappy |
| `object_store` | `s3://`, `gs://`, `az://`, `file://` tables; REST catalogs (`--catalog`) |
| `zstd` | `--compression zstd`, and merging into or compacting tables whose files are zstd |
| `http` | `--from https://…` and `source.http` (REST and SOAP) |
| `mysql` | `--from mysql://…` and `source.mysql` |
| `postgres` | `--from postgres://…` and `source.postgres` |
| `serve` | `shapeshift serve`, the local console |

## Every command

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `-v`, `--verbose` | count | off | More log output on standard error: `-v` debug, `-vv` trace. Taken by every command | Diagnosing a run |
| `-h`, `--help` | — | — | The command's flags | — |
| `-V`, `--version` | — | — | The version (top level only) | — |

## `shape`

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `-s`, `--spec` | path | none: infer | The dataset spec (YAML or JSON) | Always, once the shape is known: inference is for a first look |
| `-i`, `--input` | path or `-` | the spec's `source.path` | The input file; `-` is standard input | A different file, or a pipe |
| `-o`, `--output` | path or URL | the spec's `output.path` | A Parquet file or directory, or an Iceberg table directory or URL | A different destination |
| `--to` | `parquet` \| `iceberg` | the spec's `output.format` | Output format | — |
| `--format` | `jsonl` \| `json-array` \| `csv` \| `tsv` \| `xml` | `jsonl` | Input format when there is no spec | Inferring from a non-JSONL file |
| `--delimiter` | one character | `,` (csv), tab (tsv) | CSV/TSV field separator | `;` or `\|` files |
| `--no-header` | bool | off | CSV/TSV: the first row is data | A file with no header row |
| `--columns` | names, comma-separated | the header's | CSV/TSV column names, in order | No header, or renaming |
| `--all-strings` | bool | off | CSV/TSV: every field as text, no number or boolean detection | Exact decimals, codes with leading zeros |
| `--xml-record` | element path | none (required for XML) | The element that is one record | Every XML input |
| `--compression` | `snappy` \| `zstd` \| `uncompressed` | the spec's, else `snappy` | Parquet page codec; `zstd` needs the `zstd` feature | Smaller files (zstd), or debugging (uncompressed) |
| `--dataset` | name | the input file's stem | Dataset name when there is no spec | Standard input, or a better name |
| `--append` | bool | off | Add a snapshot to an existing Iceberg table instead of replacing it | Every incremental load |
| `--partition-by` | columns / transforms | the spec's `partition_by` | Iceberg partitioning: a column, `bucket(N, c)`, `truncate(W, c)`, `year\|month\|day\|hour(c)` | Large tables queried by a low-cardinality key |
| `--where` | expression | the spec's `where` | Keep only the source records it is true for | One kind out of a mixed stream |
| `--merge-on` | columns | the spec's `merge_on` | Upsert on these keys instead of appending | Sources with updates |
| `--catalog` | URI | the spec's `output.catalog.uri` | Commit through this Iceberg REST catalog. Feature `object_store` | A catalog-managed table |
| `--table` | `ns.name` | the spec's `output.catalog.table` | The catalog's table (with `--catalog`) | — |
| `--warehouse` | name | the spec's `output.catalog.warehouse` | Which of the catalog's warehouses | A catalog with several (Polaris, Lakekeeper) |
| `--insecure-catalog` | bool | off | Let catalog credentials go over plain http to another machine; warns each run | Only on a network trusted on its own terms |
| `--on-drift` | `ignore` \| `warn` \| `rescue` \| `quarantine` \| `error` | the spec's `drift.policy`, else `warn` | What a drift does ([SPEC §7](./SPEC.md)) | A contract table (`error`), or no loss (`rescue`) |
| `--rescue-column` | name | `_rescued` | The catch-all column under `rescue` | A name clash |
| `--pipeline` | `auto` \| `on` \| `off` | `auto` (on) | Overlap encoding with shaping on a second thread; never changes the output | `off` on a machine with one core to spare |
| `--run-id` | 1–128 of `[A-Za-z0-9._:-]` | none | Record the run in its snapshot; a retry of a committed run commits nothing | Every run an orchestrator may start twice |
| `--summary-json` | bool | off | One JSON document on standard output instead of the report | Scripts and orchestrators |
| `--metrics-file` | path | off | On exit, the run's numbers in the Prometheus text format | node_exporter's textfile collector |
| `--from` | URL | the spec's source URL | Pull from an http(s) endpoint or a `mysql://` / `postgres://` database. Features `http`, `mysql`, `postgres` | Every pulled source |
| `--records` | path | the spec's `source.http.records` | Where a JSON response's records are | Endpoints that wrap them (`data.items`) |
| `--query` | SQL | the spec's `query` | A database source's `SELECT`, naming `:cursor` | A join, a projection, a filter |
| `--source-table` | table | the spec's `table` | A database table, read by cursor and primary key | A whole table |
| `--watermark` | column | the spec's `cursor` | The column a database source's position follows | — |
| `--start` | value | the spec's `start` | A database source's first lower bound | A query's first run (needed), or skipping history |
| `--max-records` | count | unbounded | Stop after this many new records | Small, frequent runs |
| `--max-pages` | count | unbounded | Stop after this many pages from the first new one. An endpoint only: a database source refuses it | Rate-limited endpoints |
| `--max-wait` | duration | unbounded | Stop this long after the first new record | A run that must fit a schedule |
| `--insecure-source` | bool | off | Credentials over plain http to another machine, or a database without TLS | Only on a network trusted on its own terms |
| `--source-hosts` | `host[:port]`, comma-separated | no limit | The only hosts the source may reach, checked on every URL, redirect and page | A runner that holds runs to a list of its own |

## `infer`

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `-i`, `--input` | path or `-` | required | The sample to infer from | — |
| `--format`, `--delimiter`, `--no-header`, `--columns`, `--all-strings`, `--xml-record` | as for `shape` | as for `shape` | How the input reads | As for `shape` |
| `--sample` | count | `1000` | Records sampled; `0` reads the whole input | A field that appears late |
| `--no-flatten` | bool | off | Keep each nested object as one `json` column | Deep, irregular objects |
| `--dataset` | name | the input's stem | The spec's `dataset` | — |
| `--where` | expression | none | Sample only the records it keeps, and write it into the spec | One kind out of a mixed stream |
| `-o`, `--out` | path | standard output | Write the spec to a file | — |

## `inspect`

`shapeshift inspect <PATH>`: a Parquet file or an Iceberg table (a directory, or a URL in a build
with `object_store`). No flags of its own.

## `compact`

`shapeshift compact <TABLE>`: a directory, a URL (`object_store`), or with `--catalog` a
`ns.name`.

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `--catalog`, `--warehouse`, `--insecure-catalog` | as for `shape` | — | Maintain the table through a REST catalog. Feature `object_store` | A catalog-managed table |
| `--target-file-size` | size (`512MiB`, `64M`, bytes) | `512MiB` | Pack files smaller than this, per partition; `0` only rewrites files carrying deletes | Smaller targets for small tables |
| `--compression` | `snappy` \| `zstd` \| `uncompressed` | `snappy` | Codec of the rewritten files | Match the table's other writers |
| `--summary-json`, `--metrics-file` | as for `shape` | off | — | — |

## `expire`

`shapeshift expire <TABLE>`, as for `compact`.

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `--catalog`, `--warehouse`, `--insecure-catalog` | as for `shape` | — | Through a REST catalog. Feature `object_store` | — |
| `--older-than` | duration (`5d`, `12h`, `30m`, `0s`) | `5d` | Expire snapshots committed longer ago than this | Retention policy; keep it above any `--run-id` retry window |
| `--retain-last` | count | `1` | Always keep the newest N snapshots | Time travel needs |
| `--dry-run` | bool | off | Report, change nothing | Before the first real expiry |
| `--summary-json`, `--metrics-file` | as for `shape` | off | — | — |

## `relocate`

`shapeshift relocate <TABLE>`: the table where it is now.

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `--moved` | bool | — | The original is retired; keep the table's identity | When asked: the original still answers, or cannot be checked |
| `--copy` | bool | — | Both live on; give this one a new `table-uuid` | A copy that will be written to |
| `--dry-run` | bool | off | Report, change nothing | First |
| `--catalog`, `--warehouse`, `--insecure-catalog` | as for `shape` | — | Then register the table with a REST catalog. Feature `object_store` | — |
| `--table` | `ns.name` | — | The name to register it under (with `--catalog`) | — |
| `--summary-json`, `--metrics-file` | as for `shape` | off | — | — |

## `convert`

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `-i`, `--input` | path or `-` | required | The input | — |
| `--format` | as for `shape` | required | The input's format | — |
| `--delimiter`, `--no-header`, `--columns`, `--all-strings`, `--xml-record` | as for `shape` | as for `shape` | How it reads | — |
| `-o`, `--output` | path | standard output | Written whole once every record is read | — |
| `--skip-bad` | bool | off | Leave out a record that does not parse, reporting it, instead of stopping | Exploring a dirty file |
| `--limit` | count | all | Stop after this many records | A first look |

## `cost`

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `--rows` | count | — | Billable rows (or `--input`) | — |
| `-i`, `--input` | path or `-` | — | Count a file's parseable records instead | — |
| `--format`, `--delimiter`, `--no-header`, `--columns`, `--all-strings`, `--xml-record` | as for `shape` | as for `shape` | How `--input` reads | — |
| `--vendor-per-million` | dollars | required | Your plan's effective price per million MAR | — |
| `--self-host-cost` | dollars | `0` | What running shapeshift cost, for a net figure | Always, for an honest comparison |

## `mcp`

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `--root` | directory | required unless `--console` | The only directory the tools read under; anything outside, symlinks included, is refused | Point it at the data an agent may see, never `/` or a home directory |
| `--console` | `http://IP-or-localhost:port` | — | Read a running console's API instead | An agent watching a console |

## `serve` (feature `serve`)

| Flag | Type | Default | What it does | When to change it |
|---|---|---|---|---|
| `--addr` | `host:port` | `127.0.0.1:8087` | Where the console listens. It has no login | Only behind your own authentication |
| `--data-dir` | directory | `shapeshift-data` | Datasets, runs, outputs, rejects | A persistent location |
| `--allow-host` | host (repeatable) | loopback names and the bind IP | Another `Host` value to accept | Behind a proxy, at a name of your own |
| `--allow-any-host` | bool | off | Turn the `Host` check off | Only behind a trusted proxy |
| `--ingest-addr` | `host:port` | off | A second listener for push connectors (`POST /v1/ingest/<connector>`) | Producers that push records |
| `--catalog`, `--warehouse`, `--insecure-catalog` | as for `shape` | off | A REST catalog for the Catalog page, read-only. Feature `object_store` | — |
| `--allow-sources` | bool | off | Let datasets pull from the sources their specs name | Only where nobody else can reach the console |
| `--metrics-addr` | `host:port` | off | `GET /metrics` on a listener of its own, no login | Prometheus scraping, on a trusted network |

## Environment variables

| Variable | Read by | What it is |
|---|---|---|
| `SHAPESHIFT_CATALOG_TOKEN` | `--catalog` | A bearer token for the REST catalog. Not with a credential |
| `SHAPESHIFT_CATALOG_CREDENTIAL` | `--catalog` | OAuth2 client credentials, `client_id:client_secret`, exchanged for a token |
| `SHAPESHIFT_CATALOG_SCOPE` | `--catalog` | The scope asked for (default `catalog`; Polaris wants `PRINCIPAL_ROLE:ALL`). With a credential only |
| `SHAPESHIFT_CATALOG_OAUTH2_SERVER_URI` | `--catalog` | The token endpoint, when it is not the catalog's own. With a credential only |
| `SHAPESHIFT_MYSQL_PASSWORD`, `SHAPESHIFT_MYSQL_PASSWORD_FILE` | `source.mysql` | The password, or a file holding it |
| `SHAPESHIFT_POSTGRES_PASSWORD`, `SHAPESHIFT_POSTGRES_PASSWORD_FILE` | `source.postgres` | The password, or a file holding it |
| *named by the spec* | `source.http` | `auth.bearer_env`, `basic_env`, `header_env.env`, `query_env.env`, `soap.username_env`, `soap.password_env` name the variables an endpoint's credentials are read from |
| `SHAPESHIFT_READERS` | merges and compactions | Files read at once (local up to 4, object store 8 by default) |
| `AWS_*`, `GOOGLE_*`, `AZURE_*` | object-store tables | The object store's own variables, as its SDKs read them (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT`, `AWS_ALLOW_HTTP`, …) |
| `SSL_CERT_FILE` | an `object_store` build | A PEM bundle, where the system has no CA certificates |
| `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` | catalog and object-store requests | The usual proxy variables; a catalog or token endpoint on this machine is never reached through one ([SPEC §5a](./SPEC.md)) |

## The dataset spec

Top-level keys; every field is in [SPEC.md](./SPEC.md).

| Key | What it is | Reference |
|---|---|---|
| `dataset` | The name (required) | SPEC §2.1 |
| `source` | The input: `format`, `path`, `csv`, `xml`, or a pulled `http` / `mysql` / `postgres` | SPEC §2.2, §2.6–2.8 |
| `output` | `format`, `path` or `catalog`, `compression`, `partition_by`, `merge_on`, `apply` | SPEC §2.3 |
| `schema` | `infer` or `strict` | SPEC §7 |
| `columns` | The declared columns: `name`, `from`, `type`, `transform`, `required` | SPEC §2.4–6 |
| `where` | The row filter | SPEC §6a |
| `changes` | A change feed: `op` and its values, `row`, `key_from`, `order_by`, `preset`, `tombstone_retention` | SPEC §5b |
| `drift` | `policy`, `rescue_column`, `max_examples` | SPEC §2.1, §7 |
| `options` | `flatten`, `row_group_rows`, `infer_sample` | SPEC §2.5 |
