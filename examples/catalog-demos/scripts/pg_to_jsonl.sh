#!/usr/bin/env bash
#
# pg_to_jsonl.sh — export Postgres rows as JSONL (one JSON object per line) for shapeshift.
#
# A relational row set is just JSON once you ask for it that way. `row_to_json` +
# `COPY … TO STDOUT` streams the result set with no client-side buffering, so a 100M-row
# table exports in bounded memory — exactly what shapeshift then shapes in bounded memory.
#
# Two modes:
#   full         : dump the whole table/query.                        (creates the table)
#   incremental  : dump only rows with WATERMARK_COL > last-seen, and
#                  advance a local watermark file.                    (feeds shape --append)
#
# Requires: psql (libpq). Set PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE or PG_DSN.
#
# Usage:
#   # full load of a table:
#   TABLE=public.orders ./pg_to_jsonl.sh full > orders.jsonl
#
#   # arbitrary query:
#   QUERY='SELECT id, total, created_at FROM orders WHERE region = $$us$$' \
#     ./pg_to_jsonl.sh full > orders_us.jsonl
#
#   # incremental by a monotonic column (timestamp or bigint), tracked in .watermark:
#   TABLE=public.orders WATERMARK_COL=updated_at WATERMARK_FILE=.wm.orders \
#     ./pg_to_jsonl.sh incremental > delta.jsonl
set -euo pipefail

MODE="${1:-full}"
PSQL=(psql -X -q -t -A --no-psqlrc)
[ -n "${PG_DSN:-}" ] && PSQL+=("$PG_DSN")

# Build the row source: either a full TABLE or a custom QUERY.
if [ -n "${QUERY:-}" ]; then
  src="($QUERY) _q"
elif [ -n "${TABLE:-}" ]; then
  src="$TABLE _q"
else
  echo "set TABLE=schema.table or QUERY='SELECT …'" >&2; exit 1
fi

emit() {  # emit "<where-clause-or-empty>"
  local where="$1"
  "${PSQL[@]}" -c "COPY (SELECT row_to_json(_q) FROM $src ${where}) TO STDOUT"
}

case "$MODE" in
  full)
    emit ""
    ;;

  incremental)
    : "${WATERMARK_COL:?set WATERMARK_COL (a monotonic column, e.g. updated_at or id)}"
    WATERMARK_FILE="${WATERMARK_FILE:-.watermark}"
    # Read the last-seen watermark (epoch/ISO/bigint as text); default pulls everything.
    last="$(cat "$WATERMARK_FILE" 2>/dev/null || true)"

    # Capture the upper bound BEFORE exporting, and export the closed-open interval
    # (last, upper]. Sampling max() *after* the export would skip any row committed between
    # the export and the max() read; fixing the bound first closes that gap.
    upper="$("${PSQL[@]}" -c "SELECT max($WATERMARK_COL) FROM $src")"
    if [ -z "$upper" ]; then
      echo "watermark($WATERMARK_COL): no rows to export" >&2
      exit 0
    fi
    if [ -n "$last" ]; then
      where="WHERE $WATERMARK_COL > '$last' AND $WATERMARK_COL <= '$upper'"
    else
      where="WHERE $WATERMARK_COL <= '$upper'"
    fi
    emit "$where"

    # Advance the cursor to the captured upper bound.
    #
    # CAVEAT (at-least-once): this advances as soon as the export succeeds, NOT after the
    # downstream Iceberg append commits — so a failed `shapeshift shape` would skip this
    # delta. For an at-least-once pipeline, have the CALLER persist "$upper" only after the
    # append commits (WATERMARK_FILE is a plain text file it can write), and, for a
    # non-unique timestamp column, use a composite cursor (updated_at, id) or overlap the
    # lower bound by one tick and dedup on the primary key at read time to handle ties.
    printf '%s' "$upper" > "$WATERMARK_FILE"
    echo "watermark($WATERMARK_COL) → $upper" >&2
    ;;

  *)
    echo "unknown mode '$MODE' (use: full | incremental)" >&2; exit 1
    ;;
esac
