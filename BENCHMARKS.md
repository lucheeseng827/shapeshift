# Benchmarks — shapeshift

This is a **correctness proof, not a performance sheet.** For v0.1 the number that matters
is not throughput — it is *"does the file shapeshift writes actually read back, byte-for-byte
correct, in an engine we did not write?"* The answer is yes, and this document is the evidence:
a full round-trip of one deliberately-messy sample through both output paths (Parquet and a
self-contained Iceberg v2 table), read back with **DuckDB 1.5.4**, returning the **same rows and
the same aggregates**.

> **Why correctness first.** A JSON→columnar shaper that produces a file only *its own* reader can
> open is worthless — the whole point is to hand a Parquet/Iceberg dataset to the rest of the data
> ecosystem. Iceberg in particular has a sharp correctness edge (the `field-id` chain, §5) where a
> plausible-looking table is silently unreadable. So the acceptance gate for v0.1 is an **external
> engine**, not a self-round-trip. Performance is now **measured** with a reproducible harness —
> see [§7](#7-performance-measured) and [`benchmarks/`](./benchmarks/).

---

## 1. The sample

[`examples/events.jsonl`](./examples/events.jsonl) — **5 lines, one of them deliberately
un-parseable** (line 4). It exercises nested objects (`user.name`, `user.plan`), a missing nested
field (Linus has no `plan`), an array (`tags`, including an empty one), a missing top-level field
(Edsger has no `tags`), a mix of integer and fractional `amount`, an RFC3339 timestamp, a
`YYYY-MM-DD` date, and a boolean.

```jsonl
{"id": 1, "user": {"name": "Ada", "plan": "pro"}, "amount": 12.50, "event_at": "2026-07-13T09:15:00Z", "day": "2026-07-13", "tags": ["a","b"], "active": true}
{"id": 2, "user": {"name": "Grace", "plan": "free"}, "amount": 3, "event_at": "2026-07-13T10:00:00Z", "day": "2026-07-13", "tags": [], "active": false}
{"id": 3, "user": {"name": "Linus"}, "amount": 99.99, "event_at": "2026-07-12T23:59:59Z", "day": "2026-07-12", "tags": ["x"], "active": true}
not-json-a-bad-line
{"id": 4, "user": {"name": "Edsger", "plan": "pro"}, "amount": 0, "event_at": "2026-07-11T08:00:00Z", "day": "2026-07-11", "active": true}
```

**The invariant the whole round-trip checks:** 4 valid rows survive, line 4 is rejected (not
fatal), and

```text
sum(amount) = 12.50 + 3 + 99.99 + 0 = 115.49
```

must come out **identical** from the Parquet reader and the Iceberg reader.

---

## 2. Shape it (both output paths)

No `--spec` is passed, so shapeshift infers the spec on the fly (the inferred spec is shown in the
[README quickstart](./README.md#quickstart); it yields eight columns: `active bool`,
`amount float64`, `day date`, `event_at timestamp`, `id int64`, `tags json`, `user.name string`,
`user.plan string`). The two commands differ only in `--to`:

```bash
# Parquet
shapeshift shape -i examples/events.jsonl -o ./out/events.parquet --to parquet

# Iceberg v2 table directory
shapeshift shape -i examples/events.jsonl -o ./out/events_ice   --to iceberg
```

Real summary output (`shapeshift 0.1.0`, paths normalized to `./out`):

```text
shaped `events` → ./out/events.parquet (Parquet)
rows_in=4 rows_out=4 rejected=0 parse_errors=1 row_groups=1 bytes=2588
malformed / rejected rows → ./out/events.parquet.rejects.jsonl (1 parse, 0 shaped-out)
```

```text
shaped `events` → ./out/events_ice (Iceberg)
rows_in=4 rows_out=4 rejected=0 parse_errors=1 row_groups=1 bytes=3212
malformed / rejected rows → ./out/events_ice.rejects.jsonl (1 parse, 0 shaped-out)
iceberg table metadata → ./out/events_ice/metadata/v1.metadata.json
```

Both agree on the counts that matter: **`rows_in=4 rows_out=4 rejected=0 parse_errors=1`**. The one
un-parseable line was counted, sidecarred, and did **not** abort the run — exactly the honesty
contract.

---

## 3. The reject sidecar (the bad line is preserved, not swallowed)

The single parse error is written verbatim to `<output>.rejects.jsonl`, one JSON object per line.
Real content of `./out/events.parquet.rejects.jsonl`:

```jsonl
{"error":"ExpectedNull at character 0 ('n')","line":4,"raw":"not-json-a-bad-line"}
```

The record carries the three logical fields — the 1-based source `line`, the parser's `error`
message (here from the simd-json lexer, which read `n…` and tried to parse `null`), and the `raw`
line byte-for-byte so nothing is lost. Shaping rejects (a required column that resolves to
null/missing/uncoercible) land in the same sidecar; here there are none because inference makes
every column optional. This is what "the run never aborts on a bad line" looks like on disk: the
line is auditable, re-runnable, and counted, never silently dropped.

---

## 4. Read it back with DuckDB 1.5.4 (the external-engine gate)

`inspect` is shapeshift's own read-back (a self round-trip). The *acceptance* read is DuckDB — a
reader we did not write.

### 4a. shapeshift `inspect` (self round-trip)

```text
$ shapeshift inspect ./out/events.parquet
parquet file: ./out/events.parquet
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

$ shapeshift inspect ./out/events_ice
iceberg table: ./out/events_ice
  format-version: 2
  table-uuid: 0db916c9-72c2-48c7-806b-9496574bce47
  current-snapshot-id: 2549659052819107110
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

The logical → Arrow → Iceberg type mapping lands exactly as documented: `Float64`/`double`,
`Date32`/`date`, `Timestamp(µs, no zone)`/`timestamp`, the nested `tags` array kept as a JSON
string (`Utf8`/`string`), and the flattened `user.name` / `user.plan` columns.

### 4b. DuckDB `read_parquet()` and `iceberg_scan()` — same rows, same aggregate

```sql
-- Parquet path
SELECT count(*) AS rows, round(sum(amount), 2) AS total
FROM read_parquet('./out/events.parquet');
--  rows=4   total=115.49

-- Iceberg path (catalog-less; version-hint.text points at the current metadata)
INSTALL iceberg; LOAD iceberg;
SELECT count(*) AS rows, round(sum(amount), 2) AS total
FROM iceberg_scan('./out/events_ice');
--  rows=4   total=115.49
```

Verified with **DuckDB 1.5.4**: both paths return **4 rows** and **`sum(amount) = 115.49`** —
identical. Types, the `YYYY-MM-DD` → `DATE` and RFC3339 → `TIMESTAMP` conversions, the
nested-flattened `user.name` / `user.plan` columns, and the nulls (Linus's absent `user.plan`,
Edsger's absent `tags`) all read back correct in both engines. The Iceberg table is read
**end-to-end from the table directory with no catalog server** — DuckDB follows
`metadata/version-hint.text` → `metadata/v1.metadata.json` → the manifest list → the manifest →
the data Parquet, and joins the columns by field-id (§5).

> These DuckDB results are stated as **verified** (DuckDB 1.5.4). DuckDB is not installed in the
> build/CI image, so the SQL above is the reproduction recipe, not a captured CI log — run it
> locally against the two outputs from §2 to confirm.

---

## 5. The field-id chain (why the Iceberg table is *genuinely* readable)

Iceberg readers match manifest columns to data columns by **field-id**, never by name. shapeshift
threads one id space `1..=N` through four layers; this is the correctness edge that a hand-rolled
Avro encoder exists to protect (full rationale in [ARCHITECTURE.md §8](./ARCHITECTURE.md)).

**Evidence from the table written in §2** (`./out/events_ice`):

- **Iceberg schema (`metadata/v1.metadata.json`)** assigns ids `1..=8`:

  ```
  (1, active, boolean)  (2, amount, double)  (3, day, date)  (4, event_at, timestamp)
  (5, id, long)  (6, tags, string)  (7, user.name, string)  (8, user.plan, string)
  ```

- **Manifest Avro (`metadata/<uuid>-m0.avro`)** — its embedded schema JSON carries the
  `field-id` attributes on the `manifest_entry` fields (`grep -c` over the file's strings finds the
  `field-id` markers present in the header schema). These are the ids DuckDB uses to map manifest
  columns; if they were missing the scan fails with the internal error **"No default expression in
  FieldId Map."**
- **Data Parquet (`data/<uuid>.parquet`)** carries `PARQUET:field_id = 1..N` stamped onto each
  column before the file is written (verified: the ids appear on the Parquet columns).

The general Rust Avro crate (`apache-avro`) **drops** the custom `field-id` attributes when it
serializes the schema into the OCF header — which is precisely why shapeshift emits the Avro Object
Container File itself (~150 LOC) and preserves the field-id-carrying schema JSON byte-for-byte. The
successful `iceberg_scan()` in §4b **is** the end-to-end proof that this chain is intact: DuckDB
could not have joined manifest to data without it.

### Iceberg table layout produced

```text
out/events_ice/
  data/f7eb70a1-….parquet                       data (Parquet, PARQUET:field_id 1..8)
  metadata/da49769d-…-m0.avro                    manifest      (hand-rolled Avro, field-ids)
  metadata/snap-2549659052819107110-1-….avro     manifest list (hand-rolled Avro)
  metadata/v1.metadata.json                       table metadata (format-version 2)
  metadata/version-hint.text                      → 1
```

One append snapshot per run — unpartitioned here, since this run set no `--partition-by`
(identity partitioning would fan the data out under `data/<col>=<value>/`). `total-records=4`
and the single `current-snapshot-id` match the summary line from §2.

---

## 6. The `cost` command is honest by construction

Not a benchmark, but part of the same "no flattery" acceptance surface: `cost` **never guesses a
vendor price** (you supply your plan's effective `$/million MAR`), and it will print a **negative**
saving for a job too small to amortize. Both are real outputs.

A tiny job (the 4-row sample) against a self-host cost of one cent — honestly negative:

```text
$ shapeshift cost -i examples/events.jsonl --vendor-per-million 0.75 --self-host-cost 0.01
billable rows (MAR-equivalent): 4
vendor:     $0.00  (@ $0.75/million MAR)
self-host:  $0.01
saved:      $-0.01  (-333233.3%)
```

The same arithmetic at a realistic monthly volume (50M rows), where self-hosting amortizes:

```text
$ shapeshift cost --rows 50000000 --vendor-per-million 0.75 --self-host-cost 12.00
billable rows (MAR-equivalent): 50000000
vendor:     $37.50  (@ $0.75/million MAR)
self-host:  $12.00
saved:      $25.50  (68.0%)
```

`vendor_cost = rows/1e6 × price`, `saved = vendor_cost − self_host_cost`. The tiny-job case is not a
bug — it is the whole point: shapeshift shows you when self-hosting does *not* pay, instead of
selling you a number.

---

## 7. Performance: measured

**Measured with the reproducible harness in [`benchmarks/bench.py`](./benchmarks/bench.py)**
(python3 stdlib + cargo, no other deps — run it on your hardware and compare). Datasets are
generated by a deterministic formula in the shape of `examples/events.jsonl` (no RNG, no wall
clock); every child process is measured with `os.wait4` per-child `ru_maxrss`. These numbers are
from **one stated machine** — they are evidence the design properties hold, not a promise for
yours; the harness exists so you never have to extrapolate.

**Machine:** Intel Xeon @ 2.80 GHz (4 cores), 15.7 GiB RAM · rustc 1.94.1 · release build.
**Binary:** the shipped **musl-static** lean binary (`--target x86_64-unknown-linux-musl`,
~4.0 MB, stripped).

| bench | rows | input | wall | rows/s | input MB/s | peak RSS |
|---|---|---|---|---|---|---|
| jsonl→parquet | 10,000 | 1.5 MB | 0.06s | 157,980 | 24.3 | 9.9 MiB |
| jsonl→iceberg | 10,000 | 1.5 MB | 0.06s | 163,567 | 25.1 | 9.9 MiB |
| jsonl→parquet | 1,000,000 | 157 MB | 3.85s | 259,769 | 40.7 | 12.3 MiB |
| jsonl→iceberg | 1,000,000 | 157 MB | 3.84s | 260,646 | 40.8 | 12.4 MiB |
| jsonl→parquet | 8,000,000 | 1,260 MB | 29.3s | 273,495 | 43.1 | **14.1 MiB** |
| jsonl→iceberg | 8,000,000 | 1,260 MB | 29.7s | 269,506 | 42.4 | **14.1 MiB** |
| json-array→parquet | 1,000,000 | 157 MB | 8.15s | 122,752 | 19.2 | 12.0 MiB |

> The `json-array` row above is **pre-tape** and is kept for the record. That path now
> parses onto a tape as well — see [json-array on the tape too](#json-array-on-the-tape-too),
> measured separately because it was measured on a different machine.

- **Bounded RAM: verified.** Peak RSS goes 9.9 → 12.3 → 14.1 MiB while the input grows
  1.54 MB → 157 MB → 1,260 MB (**~820×**, exact byte counts from the harness's JSON output) —
  the flat curve the row-group==batch design promises, and the streamed json-array path sits on
  the same line. (Row-group == batch: the Shaper flushes a `RecordBatch` every `row_group_rows`,
  default 50,000, and the sink closes that row group immediately.)
- **Throughput: ~260–275k rows/s (~43 MB/s of input) on this machine**, steady from 1M to 8M
  rows, Parquet and Iceberg within a couple of percent of each other — the Iceberg path costs
  almost nothing over Parquet because it *is* the Parquet writer plus a metadata tail.
- **`json-array` input used to be the exception**, at ~123k rows/s in this sweep, because it
  still built an owned `serde_json::Value` per element while JSONL parsed onto a tape. That
  gap is now closed — see [json-array on the tape too](#json-array-on-the-tape-too). The row
  above is left as measured so the before/after is legible rather than quietly restated.
- **This number has moved a lot, in three steps.** The same sweep on the same machine measured
  121,185 rows/s at 8M rows before any of it. Removing the per-row allocations took it to
  149,649; overlapping the writer added nothing on musl at that point; parsing onto a tape took
  it to 273,495. Each step is A/B'd below.
- **Cold start: 3.0 ms** median (binary exec → 4 rows shaped → exit) — no JVM, no interpreter,
  no warm-up.
- **Snappy vs zstd** (glibc `--features zstd` fat build, same 1M rows): zstd wrote **11 MB vs
  Snappy's 20 MB (1.85× smaller)** at ~2% more wall time.
- **The allocator trade, much smaller than it was.** On a **glibc** (host) build the same
  engine runs ~1.34× faster (367k rows/s at 8M rows on this machine, against musl's 273k).
  That gap used to be ~2× on the parse path alone; the tape reader closed most of it by
  removing what there was to allocate. glibc malloc still *retains* the freed batch buffers
  (its dynamically rising mmap threshold), so its peak RSS appears to grow with input — 67 MiB
  at 8M rows against musl's flat 14 MiB. That is allocator retention, **not a leak**: `MALLOC_MMAP_THRESHOLD_=65536` on the same
  glibc binary is flat at 16 MiB, and the shipped musl binary is flat at 13 MiB. Pick your
  binary by what you're optimizing for; the harness measures both.

### Where the per-row cost went

The gap between the glibc and musl numbers above is the tell: a build that differs only in
its allocator should not differ in throughput unless the hot loop is *allocating*. Three
allocations per row were doing no work, and removing them is what moved the table above.

| removed | was | now |
|---|---|---|
| path re-split | `select()` split the column's dotted path into a fresh `Vec<&str>` per column, per row (8/row on this shape) | compiled once at build time into `Segment`s; the loop selects through them with no allocation |
| value clone | `coerce` took an owned `Value`, so every cell cloned out of the record — a heap allocation per string, a deep clone per array, including for `date`/`timestamp` columns that only parse the text and drop it | `coerce` reads `&Value`; `Cell::Str` holds a `Cow`, so a value that survives as-is borrows the record's bytes. Only a synthesized value (a transform's output, a stringified number, a serialized nested value) allocates |
| raw line copy | `JsonlReader` built the raw line `String` for **every** row, because simd-json parses destructively and the original bytes had to be saved before parsing — but they are only read on a reject | the parse runs on a reused scratch copy and `buf` keeps the pristine line, so a clean run never builds the string |

Bounded RAM is unaffected — the flat-RSS check still passes, and peak RSS came out marginally
lower. Output is unchanged: the shaped Parquet is byte-identical to the previous binary's for
both the Parquet and Iceberg paths, and the reject sidecar matches.

What is *not* done yet, and would be the next step change: the pipeline is still single-
threaded (parse, shape, encode, compress all on one core), and the per-row `Vec<Cell>` is the
one allocation left in `push`.

### Overlapping the writer with the shaper (`--pipeline`)

Encoding and compressing a row group used to take turns with shaping the next one on a
single core. `--pipeline` hands finished row groups to a writer thread so the two overlap.
It never changes the output — the shaped Parquet is byte-identical with it on and off, on
both targets — only who is idle.

**Same machine, 1M rows / 157 MB → Parquet, three interleaved reps, median:**

| build | `--pipeline off` | `--pipeline on` | |
|---|---|---|---|
| glibc | 226,947 rows/s (29.3 MiB) | **257,628 rows/s** (21.6 MiB) | **+13.5%** |
| musl (shipped) | 149,182 rows/s (11.7 MiB) | 141,298 rows/s (12.0 MiB) | **−5.3%** |

Over the full 8M-row sweep the glibc gain is **+16.4%** (222,833 → 259,380 rows/s), and peak
RSS *falls* (138.5 → 61.5 MiB) because the batch buffers turn over faster.

**The default was `auto`: on, except on musl — and the tape reader below changed that.**
musl's malloc takes a single global lock, so at the time a second allocating thread contended
with the first instead of scaling. What changed is not the allocator but how much the shaper
asks of it: parsing onto a reusable tape stopped allocating per record, so there is no longer
a lock to fight over. Re-measured after that change, five interleaved reps:

| build | `--pipeline off` | `--pipeline on` | |
|---|---|---|---|
| glibc | 281,649 rows/s | **344,550 rows/s** | **+22.3%** |
| musl (shipped) | 238,850 rows/s | **262,779 rows/s** | **+10.0%** |

So `auto` is now simply **on**, on both. `--pipeline on|off` overrides it either direction.
The earlier reasoning was not wrong; its premise was removed.

Memory stays bounded by the row group, not the input — at most two batches exist at once —
and the writer thread is spawned lazily on the *second* row group, so a single-row-group
run (every small input, including the 4-row cold-start case) never pays for a thread it
would have nothing to overlap with. Cold start is unchanged at ~3 ms.

**The negative result is the useful half.** The first attempt put the *source* on the far
thread instead, handing `serde_json::Value`s across. That measured **~2× slower** than
single-threaded on *both* glibc (0.47×) and musl (0.54×). A `Value` is a tree of small heap
allocations, so shipping one to another thread means every record is allocated on one core
and freed on another — cross-arena frees fight the allocator, and the consumer chases
pointers into memory another core just wrote. An Arrow `RecordBatch` is the opposite shape:
a few large contiguous buffers behind an `Arc`. *What* crosses the boundary mattered far
more than whether anything did.

### Not building a `Value` per record (the tape reader)

The engine's record model was `serde_json::Value` — a tree of small heap allocations, one map
per object and a `String` per key and per string value, built and thrown away for every record.
That was the single largest remaining cost, and it fell hardest on the shipped musl binary.

`shapeshift-json` now parses JSONL onto a **reusable simd-json tape**: a flat `Vec` of nodes,
reused between records, whose strings point back into the read buffer. Nothing is allocated per
record in the steady state. Parse only, 1M records of the benchmark shape
([`shapeshift-json/examples/parsebench.rs`](./shapeshift-json/examples/parsebench.rs) — run it
yourself):

| record model | glibc | musl |
|---|---|---|
| owned `serde_json::Value` | 540k rec/s | 256k rec/s |
| borrowed value | 855k rec/s | 488k rec/s |
| **tape, reused buffers** | **1,849k rec/s** | **1,592k rec/s** |

Read the *gap* as well as the rows: the owned model is 2.1× slower on musl than on glibc, the
tape only 1.16×. With nearly nothing to allocate, the allocator nearly stops mattering — which
is why this, and not a different allocator, was the answer to the section below.

End to end, 1M rows → Parquet, three interleaved reps, median:

| build | value model | tape | |
|---|---|---|---|
| **musl (shipped)** | 143,557 rows/s (11.6 MiB) | **241,211 rows/s** (11.7 MiB) | **+68.0%** |
| glibc | 238,817 rows/s (20.1 MiB) | **351,486 rows/s** (21.4 MiB) | **+47.2%** |

**The `serde_json::Value` contract did not change.** `shapeshift-core` reads records through a
`Record` trait that `&Value` implements, so `Shaper::push(&value)` behaves exactly as before and
an embedder sees no difference. Both models run the same coercion code, and
`tape_and_serde_agree_on_every_accessor` asserts they answer identically — it caught two real
divergences during development: simd-json's `as_f64` refuses an integer node (so `3` in a
`float64` column would have been written as **null**), and `u64` past `i64::MAX` converted to
null. Text rendering goes through serde_json rather than simd-json's encoder for the same
reason: the two disagree (`1.79e308` vs `1.79e+308`), and a column's bytes must not depend on
which reader produced them.

`json-array` input now takes the same path — see below.

### json-array on the tape too

The array reader already framed each element into a reusable buffer, exactly as the line
reader framed lines. So the only thing between `json-array` and the tape was that nothing had
been written to use it: the framing was factored out of the `Value` iterator (one
implementation of the array's state machine, shared, the same split JSONL uses), and a
`TapeJsonArrayReader` was built on top of it. A `TapeSource` trait lets the shape loop keep one
tape path rather than growing one per input format.

The element scanner, the streaming behaviour, and the error semantics are untouched: a bad
element is still recoverable and still reports its 1-based element index, a malformed array is
still reported once and ends iteration.

1M rows / 157 MB, three interleaved reps, median. **Different machine from the table above**
(Xeon @ 2.10 GHz, 4 cores, 16 GiB, rustc 1.94.1), so read the deltas, not the absolutes:

| build | before | after | |
|---|---|---|---|
| **musl (shipped)** | 151,268 rows/s | **308,298 rows/s** | **+103.8%** |
| glibc | 318,094 rows/s | **436,462 rows/s** | +37.2% |

Peak RSS is unmoved (11.9 → 12.2 MiB on musl).

musl doubling while glibc gains a third is the [allocator story](#the-allocator-was-the-standing-bottleneck-on-musl)
again, from the other end: an owned `Value` per element costs most where malloc takes a global
lock, so removing it is worth most there. On the same machine `json-array` now runs at **93.5%
of JSONL's rate on musl** (308,298 vs 329,779) against 46% before. What is left is the element
scanner — a byte-at-a-time depth/string walk, where JSONL splits lines with `memchr` — and that
is the next thing to look at if this path ever matters more.

**A second, smaller thing was fixed on the way.** The array reader materialized an owned
`String` of every element *before* parsing, so the raw text would be available if the parse
failed — while JSONL had been switched to a scratch copy months earlier precisely so the raw
line is built only on the error path. Bringing the array reader across removes an allocation
per element. On glibc it measured as **nothing** (312,385 vs 312,299 rows/s — one small
alloc/free against a 3.2 µs row budget is below noise); it is kept for consistency and because
the same class of allocation is what musl charges for, not because it showed up in a number.

### The allocator was the standing bottleneck on musl

Two independent measurements on this machine pointed at the same thing. Kept here because it
is what led to the tape reader above, and because the shape of the argument generalizes:

| workload (1M rows / 157 MB) | glibc | musl | musl penalty |
|---|---|---|---|
| `cost -i` (parse only — build a `Value` per record, drop it) | 1.96s | 12.06s | **6.2×** |
| `shape -i -o` (parse + shape + write) | 4.59s | 6.74s | 1.5× |

Parsing in isolation is **six times** slower on the shipped binary, because that workload is
almost pure allocate-and-free and musl serializes it. It is diluted to 1.5× in a full run
only because the rest of the work is not allocation-bound. This is also why the writer
thread loses on musl while winning on glibc.

The lever it pointed at was the allocator itself, not more threads — but every well-known
drop-in (mimalloc, snmalloc, rpmalloc) is C or C++, which the lean musl build deliberately does
not link. **Reducing allocation was the portable alternative, and it is the one taken above.**
The tape reader removed the `Value` tree, and with it most of the gap: the parse-only penalty
fell from 6.2× to 1.16×, and the end-to-end one from 1.5× to ~1.34×. The `json-array` source
was the last holdout on the owned model and has since been brought across too — where it
promptly doubled on musl and gained a third on glibc, which is this same argument arriving a
second time.

### Schema-drift detection: the cost of the default

`drift.policy` defaults to `warn`, so every run scans each record against the schema's consumed
paths. That is not free, and this is what it costs. **Same machine as above; glibc release build
(`cargo build --release -p shapeshift-cli`), 1,000,000 rows / 157 MB of the same
`examples/events.jsonl` shape → Parquet.** Runs are interleaved A/B/A/B so machine drift hits both
arms equally; the figure is the median of 6.

| run | policy | median wall | vs `ignore` | peak RSS |
|---|---|---|---|---|
| clean (schema covers the source) | `ignore` | 4.47s | 1.00× | 28.3 MiB |
| clean (schema covers the source) | `warn` | 4.75s | **1.06×** | 28.1 MiB |
| every row drifts (6 uncovered fields) | `ignore` | 2.30s | 1.00× | 11.1 MiB |
| every row drifts (6 uncovered fields) | `warn` | 3.38s | 1.47× | 11.1 MiB |
| every row drifts (6 uncovered fields) | `rescue` | 5.61s | 2.44× | 30.9 MiB |

- **On a run that has not drifted — the case that matters, because it is every healthy run — the
  default costs ~6% of wall time and nothing measurable in RAM.** The scan does one trie lookup per
  record key and only builds a path string on a miss, so a covered key costs a few short string
  comparisons and no allocation.
- **A run where every row drifts costs 1.47×**, and `rescue` on that same run 2.44× — it builds and
  serializes a JSON object per row. That is the price of not losing the data; it is also a spec
  that is badly out of date, and the report tells you exactly which columns to add.
- `--on-drift ignore` restores the v0.1 hot loop exactly: no tracker is constructed and `push` does
  no drift work at all.
- The drifting rows here are cheaper in absolute terms because that spec declares one column
  (`id`) instead of eight — compare down the `vs ignore` column, not across the rows.

---

## 8. Reproduce

```bash
cd shapeshift

# build the single binary
cargo build --release           # binary at target/release/shapeshift

# shape the sample both ways
./target/release/shapeshift shape -i examples/events.jsonl -o ./out/events.parquet --to parquet
./target/release/shapeshift shape -i examples/events.jsonl -o ./out/events_ice   --to iceberg

# shapeshift's own read-back
./target/release/shapeshift inspect ./out/events.parquet
./target/release/shapeshift inspect ./out/events_ice

# the bad line, preserved
cat ./out/events.parquet.rejects.jsonl

# the external-engine gate (DuckDB 1.5.4) — expect rows=4, total=115.49 from BOTH
duckdb -c "SELECT count(*), round(sum(amount),2) FROM read_parquet('./out/events.parquet');"
duckdb -c "INSTALL iceberg; LOAD iceberg;
           SELECT count(*), round(sum(amount),2) FROM iceberg_scan('./out/events_ice');"
```

The correctness suite that backs this — 91 tests (88 `#[test]` + 3 doctests), `cargo clippy
--workspace --all-targets -- -D warnings` clean, `cargo fmt --check` clean — runs with `cargo test
--workspace`.

---

*shapeshift v0.1 — © 2026 Nicholas Lu Chee Seng and the shapeshift contributors. Apache-2.0.
Repository: https://github.com/lucheeseng827/shapeshift*
