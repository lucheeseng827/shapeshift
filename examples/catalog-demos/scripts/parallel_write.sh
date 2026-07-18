#!/usr/bin/env bash
#
# parallel_write.sh — fan one input across N shapeshift instances writing in parallel.
#
# shapeshift is a **share-nothing** writer: each `shape` run produces its own data files
# and its own snapshot commit, with no shared state. So the safe way to scale writes across
# cores/hosts is to give each instance a **disjoint target** — one Iceberg table per shard —
# and union them at read time (catalog namespace, or a UNION view). Throughput scales ~linearly.
#
# (Do NOT point multiple concurrent `--append` runs at the SAME table dir — the commit is a
# last-writer-wins race with no locking; see 05-parallel-multi-write.md and the --race probe
# in this script.)
#
# Usage:
#   INPUT=events.jsonl SHARDS=4 OUT=warehouse/events ./parallel_write.sh
#     → warehouse/events_shard0 … _shard3, written by 4 parallel instances
#
#   ./parallel_write.sh --race INPUT=events.jsonl SHARDS=4    # demo the unsafe race, on purpose
#
# Env:
#   INPUT        source JSONL to fan out                 (required)
#   SHARDS       number of parallel writers              (default: nproc)
#   OUT          output table prefix                     (default ./warehouse/<input-stem>)
#   PARTITION_BY shapeshift --partition-by expr          (optional)
#   SPEC         shapeshift -s spec.yaml                 (optional; else infer on the fly)
set -euo pipefail

RACE=0
[ "${1:-}" = "--race" ] && { RACE=1; shift; }
# allow KEY=VALUE args after the flag for convenience
for kv in "$@"; do export "$kv"; done

: "${INPUT:?set INPUT=path/to/source.jsonl}"
SHARDS="${SHARDS:-$(nproc 2>/dev/null || echo 4)}"
stem="$(basename "${INPUT%.*}")"
OUT="${OUT:-./warehouse/$stem}"
SHAPESHIFT="${SHAPESHIFT:-shapeshift}"

part_args=(); [ -n "${PARTITION_BY:-}" ] && part_args=(--partition-by "$PARTITION_BY")
spec_args=(); [ -n "${SPEC:-}" ]         && spec_args=(-s "$SPEC")

workdir="$(mktemp -d)"; trap 'rm -rf "$workdir"' EXIT

# Reject empty input, and never launch more shards than there are rows: an empty shard
# file would make a shapeshift instance fail on a missing/empty input. Cap the active
# writer count at the row count so every launched shard file exists.
[ "$SHARDS" -ge 1 ] || { echo "error: SHARDS must be >= 1" >&2; exit 1; }
# grep -c prints the count and exits 1 on zero matches; `|| true` keeps its "0" without
# appending a second line (which `|| echo 0` would), so $in_rows stays a single integer.
in_rows=$(grep -c '' "$INPUT" 2>/dev/null || true)
[ -n "$in_rows" ] || in_rows=0
[ "$in_rows" -gt 0 ] || { echo "error: $INPUT has no rows to shape" >&2; exit 1; }
[ "$SHARDS" -le "$in_rows" ] || SHARDS="$in_rows"

# 1. Split the input into SHARDS round-robin pieces (line-oriented, so JSONL stays valid).
awk -v n="$SHARDS" -v d="$workdir" '{ print > (d "/shard" (NR % n) ".jsonl") }' "$INPUT"

secs() { date +%s.%N; }

if [ "$RACE" -eq 1 ]; then
  # ── Anti-pattern on purpose: one synchronous seed, then SHARDS-1 concurrent --append
  #    runs into the SAME table dir. They all read version-hint=1 and all write "v2";
  #    last writer wins, so the seed + one appender survive and the rest are orphaned. ──
  echo "RACE MODE: seed + $((SHARDS - 1)) concurrent --append into a single table '$OUT' (expect data loss)"
  "$SHAPESHIFT" shape ${spec_args[@]+"${spec_args[@]}"} -i "$workdir/shard0.jsonl" \
    -o "$OUT" --to iceberg ${part_args[@]+"${part_args[@]}"} >/dev/null
  for ((s=1; s<SHARDS; s++)); do
    "$SHAPESHIFT" shape ${spec_args[@]+"${spec_args[@]}"} -i "$workdir/shard$s.jsonl" \
      -o "$OUT" --to iceberg --append ${part_args[@]+"${part_args[@]}"} >/dev/null 2>&1 &
  done
  wait
  got=$("$SHAPESHIFT" inspect "$OUT" | awk '/total-records/{print $2}')
  echo "input rows=$in_rows   table total-records=$got   (mismatch ⇒ lost writes from the race)"
  exit 0
fi

# 2. Launch one shapeshift instance per shard, all in parallel, each to its OWN table.
echo "writing $INPUT across $SHARDS parallel shapeshift instances → ${OUT}_shard0..$((SHARDS-1))"
t0=$(secs)
for ((s=0; s<SHARDS; s++)); do
  "$SHAPESHIFT" shape ${spec_args[@]+"${spec_args[@]}"} -i "$workdir/shard$s.jsonl" \
    -o "${OUT}_shard$s" --to iceberg ${part_args[@]+"${part_args[@]}"} >/dev/null &
done
wait
t1=$(secs)

# 3. Verify: no rows lost — the shard totals must sum to the input row count.
tot=0
for ((s=0; s<SHARDS; s++)); do
  r=$("$SHAPESHIFT" inspect "${OUT}_shard$s" | awk '/total-records/{print $2}')
  tot=$((tot + r))
done
if [ "$tot" = "$in_rows" ]; then
  printf 'parallel wall-clock: %.2fs   shard totals sum=%s   input rows=%s   OK — no rows lost\n' \
    "$(echo "$t1 - $t0" | bc)" "$tot" "$in_rows"
  echo "read them as one dataset via a catalog namespace, or a UNION view (see read_back.sql)."
else
  printf 'parallel wall-clock: %.2fs   shard totals sum=%s   input rows=%s   MISMATCH — rows lost!\n' \
    "$(echo "$t1 - $t0" | bc)" "$tot" "$in_rows" >&2
  exit 1
fi
