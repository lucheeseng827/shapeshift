# shapeshift for AI agents (MCP)

`shapeshift mcp` serves shapeshift's tools to an AI agent over the
[Model Context Protocol](https://modelcontextprotocol.io). It speaks JSON-RPC on stdio, the way
desktop agents and IDEs start a tool server, and it opens no port. It is in the released binary
and image, with nothing to rebuild.

It is **read-only**. Every tool looks; none writes, runs a pull, or deletes anything. Tools that
write and tools that delete are planned as separate tiers, each behind its own flag. A server
lists only the tools it offers and refuses any other call, and no tool changes what a server may
do.

## Two modes

| Mode | Command | What the tools reach |
|---|---|---|
| Local | `shapeshift mcp --root <dir>` | Files under `<dir>`, through the same functions `infer`, `convert` and `inspect` use |
| Console | `shapeshift mcp --console http://127.0.0.1:8087` | A running `shapeshift serve`: its datasets, runs, alerts, connectors, catalog and tables, through the API its pages use |

A server runs in one mode: give `--root` or `--console`, not both. `--root` has no default.
An agent's client may start the server from any directory, `/` or your home among them, so
the directory is always yours to name. Name the one that holds the samples and tables the
agent needs. A root that is a filesystem root or a home directory starts with a warning on
stderr.

## Connecting an agent

Claude Desktop (`claude_desktop_config.json`), or any client that starts a server by command:

```json
{
  "mcpServers": {
    "shapeshift": {
      "command": "shapeshift",
      "args": ["mcp", "--root", "/data/samples"]
    }
  }
}
```

To front a console instead, use `"args": ["mcp", "--console", "http://127.0.0.1:8087"]`. For the
container image, the command is `docker` with
`"args": ["run", "-i", "--rm", "-v", "/data/samples:/data", "mancube/shapeshift", "mcp", "--root", "/data"]`.

## The tools

Local mode:

| Tool | What it does |
|---|---|
| `shapeshift_infer` | A file under the root → a dataset spec in YAML: columns, types and nesting, ready to edit |
| `shapeshift_convert_sample` | The first records of a CSV, TSV, XML, JSON array or JSON Lines file, as the JSON records shapeshift would read (at most 50) |
| `shapeshift_inspect` | A Parquet file's or an Iceberg table's schema and counts |
| `shapeshift_table_history` | An Iceberg table's snapshots as events, newest first: kind, time, rows and files added and removed, schema changes |
| `shapeshift_preview` | The first rows of a Parquet file or an Iceberg table (at most 50, long values cut). A look, not a query: there is no SQL. A table a merge left delete files in is refused, since reading its data files whole would show deleted rows. `columns` gives the file's column order (each row's keys come sorted). Numbers, booleans and strings keep their JSON kind; dates, timestamps, decimals and nested values come as Parquet prints them. |

Console mode:

| Tool | What it does |
|---|---|
| `shapeshift_list_runs` | The run log, newest first, optionally for one dataset (at most 50) |
| `shapeshift_get_run` | One run, by id: phases, outcome, summary, error |
| `shapeshift_list_datasets` | Each dataset's definition, latest run and freshness |
| `shapeshift_get_dataset` | One dataset: definition, runs, freshness, and for a pulled one its source and position |
| `shapeshift_attention` | What needs a look now (the Overview): failed and conflicted runs, stale tables |
| `shapeshift_list_alerts` | Alert rules, targets (URLs masked) and incidents |
| `shapeshift_list_connectors` | Push connectors, what they hold and their counts. Never a token. |
| `shapeshift_browse_catalog` | The REST catalog's namespaces and tables, when the console has one |
| `shapeshift_inspect` | A file's or table's schema and counts, as the console names it |
| `shapeshift_table_history` | A table's card, schema and history, as the Tables page shows it |

Both modes:

| Tool | What it does |
|---|---|
| `shapeshift_validate_spec` | A spec in YAML → whether it parses and is consistent, with the CLI's errors. Nothing runs. |
| `shapeshift_cost` | MAR pricing from numbers you give: the vendor's cost, self-hosting's, the difference |

**Resources.** `shapeshift://docs/spec` is the spec reference (this repository's
`docs/SPEC.md`). An agent writing a spec should read it rather than guess.

**Prompts.**
Each mode offers the prompts its tools serve.
- `write_spec` (local): draft and check a spec for a sample.
- `why_stale` (console): find out why a dataset has no recent success.
- `table_changes` (both): say what changed in a table.

## Safety

- **Files under the root only.** Every path is resolved inside `--root`, links included. A path
  with `..`, an absolute path outside the root, a link that points out of it, or a URL is
  refused. The files a table names are held to the same rule, since a table copied in from
  elsewhere can name any path:
  - Before any of the three table tools (`shapeshift_inspect`, `shapeshift_table_history`,
    `shapeshift_preview`) reads a table, its `metadata` directory and everything under it must
    be under the root. A metadata file that is a link out of the root is refused.
  - So must the manifest list and every manifest the current snapshot names, inline ones
    included. A snapshot that names no manifest list is refused. Only preview reads these
    today; the other two are checked for them as well, so a later change cannot quietly widen
    what they reach.
  - `shapeshift_preview` opens each data file, then checks that the file it opened is the one
    under the root, and reads through that open file, so a link swapped in after the check
    cannot redirect the read.
- **A root is a boundary against the agent, not against whoever writes under it.** The other
  tools check a path and then read it through the engine. Someone who can write under the root
  while the server runs could swap a file for a link in between. Give the server a root only
  you (and the jobs you run) write to.
- **No tool takes a URL, a host, a connection string or a credential.** An agent cannot be
  talked into reading an internal address or sending data anywhere. The console's address is
  your flag, not the agent's.
- **Data is marked as data.** A result that carries values read from files or tables comes
  wrapped:

  ```json
  { "untrusted_content": true, "note": "…", "data": { … } }
  ```

  The note tells the agent that text in the data is not an instruction. A record that says
  "ignore your instructions" can do nothing against a read-only server anyway. An error from
  one of these tools (a parse error quoting a record, a run's error text) ends with the same
  warning, since it may quote what was read.
- **Results are capped.** Results hold at most 200 items in any list and 4,000 characters in any
  string, and say so where they were cut, with `"truncated": true` at the top of the result.
  The spec `shapeshift_infer` drafts is kept whole up to 128 K characters, since a part of a
  spec reads like a complete one. A whole result is at most 256 KiB; past that the call fails
  and says so. Errors are capped the same way.
- **The console is asked with a deadline.** In console mode each request has 30 seconds in all,
  connecting included, and an answer past 8 MiB is refused. A console that has gone away gives
  an error, not a hung server. `--console` takes an IP address (`http://127.0.0.1:8087`,
  `http://[::1]:8087`) or `localhost` (tried as 127.0.0.1, then ::1), never a name to look up: a lookup cannot be held to the
  deadline, and a resolver that hangs would hang every call.
- **Calls are limited** to 120 a minute.
- **Every call is logged** to stderr, one JSON line each: the tool, its arguments (long strings
  shortened), the outcome and the time taken.

On a shared machine, a policy gateway in front of the server (MCPdef, for one) can add per-user
rules on top.
