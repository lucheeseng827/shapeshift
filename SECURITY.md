# Security Policy

## Reporting a vulnerability

**Please report security issues privately. Do not open a public issue for a suspected vulnerability.**

- Preferred: open a private [GitHub Security Advisory](https://github.com/lucheeseng827/shapeshift/security/advisories/new)
  on the monorepo ("Report a vulnerability" on the Security tab), scoped to the
  `shapeshift` project.
- Alternatively, reach the maintainer via their GitHub profile
  ([@lucheeseng827](https://github.com/lucheeseng827)) to arrange a private
  channel, or write to the project security contact placeholder
  `security@shapeshift.example` (replace with the real address before release).

Include: the affected version or commit, a description, and a minimal
reproduction (ideally the input JSON/JSONL line and the spec) if possible. We aim
to acknowledge within a few business days and to ship a fix or mitigation for
confirmed, in-scope issues as soon as practical, crediting reporters who want it
once a fix is released.

## Supported versions

`shapeshift` is pre-1.0. The latest published `0.1.x` patch receives security
fixes; older `0.1` patches may be updated at the maintainer's discretion.

| Version | Supported |
|---------|-----------|
| `0.1.x` (latest) | ✅ |
| older   | ❌ |

## Security posture

The OSS engine (Apache-2.0) is a **pure data plane**. It reads JSON/JSONL from a
local path, shapes it through a declarative spec, and writes Parquet or a
self-contained Iceberg v2 table to a local path. Concretely:

- **No network in the OSS core.** The engine and CLI open no sockets, make no
  outbound calls, and ship no telemetry. Hosted control-plane and connector
  features live in the separate hosted/`ee` planes, not here.
- **No eval, no exec, no code loading.** The transform spec is data. Column
  transforms are a fixed, named library (`lowercase`, `trim`, `dollars_to_cents`,
  `json_encode`, …); there is no expression evaluator, no scripting, and no
  dynamic/plugin loading. A spec can only select paths and apply those named
  transforms.
- **No wall-clock dependence in the engine.** `chrono` is built *without* the
  `clock` feature; all time is derived from the data itself. The sole wall-clock
  read in the whole system is the Iceberg snapshot timestamp written by the
  Iceberg sink.
- **Memory-safe Rust, minimal dependencies.** The build is `#![forbid(unsafe_code)]`
  in the crates we author. The dependency surface is deliberately small — Parquet
  is compiled with `default-features = false` and only `["arrow","snap"]` (no
  zstd/brotli C codecs), and the Iceberg Avro manifests are written by a small
  **hand-rolled** Object-Container-File encoder rather than pulling in a general
  Avro crate. Fewer third-party crates means fewer moving parts to audit and track.

## Untrusted input

Input JSON is treated as **untrusted**. It is parsed by the `simd-json` SIMD lexer
(via its `serde_json::Value` bridge). The streaming JSONL reader is bounded-RAM: it
holds at most one record at a time and caps each record at a configurable ceiling
(256 MiB by default), so a pathological huge/unterminated line is rejected rather
than allowed to exhaust memory. It is **fault-tolerant by design**: a line that
fails to parse is counted and written verbatim to the `<output>.rejects.jsonl`
sidecar (`{"line","error","raw"}`) — a bad line never aborts the run. Soft row
rejects (a required column missing/null/uncoercible under the default lenient
policy) are likewise counted and written to the same sidecar (`{"error","raw"}`).

> **The reject sidecar is sensitive output.** It preserves rejected input
> **verbatim** — including any secrets, tokens, or PII a malformed or rejected line
> happened to contain — at a caller-controlled path. Treat `<output>.rejects.jsonl`
> with the same care as the source data: write it only to a trusted location, and
> restrict/rotate/scrub it as your data-handling policy requires. v0.1 does not yet
> redact it or offer an opt-out; per-field redaction and a disable flag are on the
> roadmap.

Both input formats stream with bounded RAM: JSONL a line at a time and `json-array`
an element at a time, each under the same per-record byte cap, so an untrusted-size
input (or a pathological single record) cannot OOM the process — an oversized record
is rejected and the reader recovers. If you enable `schema: strict`, any coercion
failure aborts the run by design — that is a correctness choice, not an error path
to exploit.

## Output paths

Output paths are **caller-controlled**. `shapeshift` writes exactly where the spec
or `--output` flag points (Parquet files, or an Iceberg table directory laid out
as `data/`, `metadata/`, and `version-hint.text`). It does not sandbox or
canonicalize the destination and, in v0.1, the Iceberg table records **absolute**
file paths (a moved table reads via DuckDB's `allow_moved_paths=true`; catalog-managed
relocation is roadmap). Write only to locations you trust and
control, and do not point the output at a shared or sensitive directory whose
contents could be overwritten. The tool logs to stderr and prints run summaries to
stdout; it reads no credentials and emits no secrets *of its own*. The one caveat is
the reject sidecar above, which echoes rejected **input** verbatim and so is only as
sensitive as the data you feed it.

## Third-party crates

Vulnerabilities in upstream dependencies (`arrow`, `parquet`, `simd-json`,
`clap`, `serde`, …) should be reported to those projects; we track advisories and
bump affected pins in `Cargo.lock`.

## No warranty

`shapeshift` is provided under the Apache License, Version 2.0, **"AS IS", without
warranties or conditions of any kind**, to the extent permitted by applicable law.
See `LICENSE` and `NOTICE`. Copyright © 2026 Nicholas Lu Chee Seng and the
shapeshift contributors.
