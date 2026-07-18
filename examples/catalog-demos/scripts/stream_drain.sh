#!/usr/bin/env bash
#
# stream_drain.sh — turn a Kafka/Redpanda topic into Iceberg snapshots with shapeshift.
#
# shapeshift shapes JSONL and exits; a stream is unbounded. The bridge is micro-batching:
# drain the topic for a short window into a JSONL file, `shape --append` that batch as one
# Iceberg snapshot, repeat. Each window = one snapshot; the table grows append-only.
#
# Consumption uses a Kafka **consumer group** (via kcat -G), so offsets are committed and
# each window resumes where the last left off. A window is bounded by wall-clock time with
# `timeout`.
#
# DELIVERY SEMANTICS (important): kcat commits group offsets as it consumes — it has no hook
# to defer the commit until *after* the Iceberg write. So the honest guarantee here is
# at-most-once at the boundary: if `shapeshift shape` fails, this script aborts (set -e), but
# the offsets for that drained batch are already committed, so those messages are NOT
# re-read on restart. For a demo that is acceptable; for production, consume with a client
# library (confluent-kafka / sarama / franz-go) that commits offsets only after the shape
# commit succeeds (or writes each batch idempotently and dedups on the primary key at read
# time — see 01-streaming-to-iceberg-catalog.md).
#
# Requires: kcat (aka kafkacat), and shapeshift on PATH (or set $SHAPESHIFT).
#
# Usage:
#   BROKER=localhost:9092 TOPIC=events OUT=s3://lake/db/events \
#     ./stream_drain.sh
#
# Env:
#   BROKER          Kafka bootstrap server            (default localhost:9092)
#   TOPIC           topic to drain                     (required)
#   GROUP           consumer group id                  (default shapeshift-$TOPIC)
#   OUT             Iceberg table output (path or URL) (default ./"$TOPIC"_tbl)
#   PARTITION_BY    shapeshift --partition-by expr     (optional, e.g. 'day(event_at)')
#   SPEC            shapeshift -s spec.yaml            (optional; else infer on the fly)
#   DRAIN_SECONDS   window length per snapshot         (default 30)
#   IDLE_SLEEP      pause between empty windows         (default 5)
#   MAX_WINDOWS     stop after N windows (0 = forever) (default 0)
set -euo pipefail

: "${TOPIC:?set TOPIC to the Kafka topic to drain}"
BROKER="${BROKER:-localhost:9092}"
GROUP="${GROUP:-shapeshift-$TOPIC}"
OUT="${OUT:-./${TOPIC}_tbl}"
DRAIN_SECONDS="${DRAIN_SECONDS:-30}"
IDLE_SLEEP="${IDLE_SLEEP:-5}"
MAX_WINDOWS="${MAX_WINDOWS:-0}"
SHAPESHIFT="${SHAPESHIFT:-shapeshift}"

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
window=0

part_args=(); [ -n "${PARTITION_BY:-}" ] && part_args=(--partition-by "$PARTITION_BY")
spec_args=(); [ -n "${SPEC:-}" ]         && spec_args=(-s "$SPEC")

echo "draining $BROKER/$TOPIC (group=$GROUP) → $OUT  [${DRAIN_SECONDS}s windows]"
while :; do
  window=$((window + 1))
  batch="$workdir/batch.jsonl"

  # Drain up to DRAIN_SECONDS of messages, one JSON value per line, committing offsets.
  # -G <group> <topic> : consumer-group mode (durable offsets)
  # -o stored          : resume from committed offset
  # -e                 : exit at end of partition (so a quiet topic ends the window early)
  # -u -q              : unbuffered, quiet
  #
  # Distinguish an expected window timeout from a real kcat failure: `timeout` returns 124
  # when it ends the window, and kcat returns 0 on a clean end-of-partition exit — both are
  # normal. Any OTHER non-zero code is a broker/auth error, which we must NOT treat as an
  # empty window (that would silently skip the topic), so we fail loudly.
  set +e
  timeout "${DRAIN_SECONDS}s" \
    kcat -b "$BROKER" -G "$GROUP" "$TOPIC" -o stored -e -u -q > "$batch" 2>"$workdir/kcat.err"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ] && [ "$rc" -ne 124 ]; then
    echo "error: kcat exited $rc (broker/auth failure, not a timeout):" >&2
    cat "$workdir/kcat.err" >&2 || true
    exit "$rc"
  fi

  n=$(wc -l < "$batch" | tr -d ' ')
  if [ "$n" -eq 0 ]; then
    echo "window $window: 0 messages, idling ${IDLE_SLEEP}s"
    sleep "$IDLE_SLEEP"
  else
    # Append if the table already exists (probe with `inspect` — works for paths AND
    # s3://… URLs, and stays correct if this drainer is restarted); else create it.
    if "$SHAPESHIFT" inspect "$OUT" >/dev/null 2>&1; then append=(--append); else append=(); fi
    echo "window $window: shaping $n messages → $OUT"
    # ${arr[@]+"${arr[@]}"} keeps empty arrays safe under `set -u` on bash < 4.4 too.
    "$SHAPESHIFT" shape ${spec_args[@]+"${spec_args[@]}"} -i "$batch" -o "$OUT" \
      --to iceberg ${append[@]+"${append[@]}"} ${part_args[@]+"${part_args[@]}"}
  fi

  if [ "$MAX_WINDOWS" -ne 0 ] && [ "$window" -ge "$MAX_WINDOWS" ]; then
    echo "reached MAX_WINDOWS=$MAX_WINDOWS, stopping"
    break
  fi
done
