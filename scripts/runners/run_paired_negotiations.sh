#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TASK_DIR="$ROOT_DIR/data/inputs/tasks"
RUN_NAME="paired_$(date +%Y%m%d_%H%M%S)"
RUN_DIR=""
MAIN_SCRIPT="$ROOT_DIR/scripts/runners/main.py"
START_ID=1
END_ID=100

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3"
fi

EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --task-dir)
      TASK_DIR="$2"
      shift 2
      ;;
    --run-name)
      RUN_NAME="$2"
      shift 2
      ;;
    --run-dir)
      RUN_DIR="$2"
      shift 2
      ;;
    --out-no-guard)
      OUT_NO_GUARD="$2"
      shift 2
      ;;
    --out-with-guard)
      OUT_FULL_GUARD="$2"
      shift 2
      ;;
    --events-no-guard)
      EVENTS_NO_GUARD="$2"
      shift 2
      ;;
    --events-with-guard)
      EVENTS_FULL_GUARD="$2"
      shift 2
      ;;
    --start-id)
      START_ID="$2"
      shift 2
      ;;
    --end-id)
      END_ID="$2"
      shift 2
      ;;
    --)
      shift
      EXTRA_ARGS+=("$@")
      break
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$RUN_DIR" ]]; then
  RUN_DIR="$ROOT_DIR/outputs/$RUN_NAME"
fi

OUT_NO_GUARD="${OUT_NO_GUARD:-$RUN_DIR/processed/summaries/no_guard}"
OUT_FULL_GUARD="${OUT_FULL_GUARD:-$RUN_DIR/processed/summaries/full_guard}"
EVENTS_NO_GUARD="${EVENTS_NO_GUARD:-$RUN_DIR/raw/events/no_guard}"
EVENTS_FULL_GUARD="${EVENTS_FULL_GUARD:-$RUN_DIR/raw/events/full_guard}"

if (( START_ID > END_ID )); then
  echo "Invalid range: start-id (${START_ID}) is greater than end-id (${END_ID})." >&2
  exit 1
fi

mkdir -p "$OUT_NO_GUARD" "$OUT_FULL_GUARD" "$EVENTS_NO_GUARD" "$EVENTS_FULL_GUARD"

echo "=== Paired Negotiation Run ==="
echo "Task directory:      $TASK_DIR"
echo "Run directory:       $RUN_DIR"
echo "No-guard summaries:  $OUT_NO_GUARD"
echo "Full-guard summaries:$OUT_FULL_GUARD"
echo "No-guard events:     $EVENTS_NO_GUARD"
echo "Full-guard events:   $EVENTS_FULL_GUARD"
echo "Product range:       ${START_ID}-${END_ID}"

missing_tasks=0
failures_no_guard=0
failures_full_guard=0
completed_pairs=0

for (( product_id=START_ID; product_id<=END_ID; product_id++ )); do
  task_file="$TASK_DIR/product_${product_id}.json"
  if [[ ! -f "$task_file" ]]; then
    echo "Skipping product_${product_id}: missing task file at $task_file"
    missing_tasks=$((missing_tasks + 1))
    continue
  fi

  summary_name="product_${product_id}.txt"
  events_name="product_${product_id}.ndjson"

  summary_no_guard="$OUT_NO_GUARD/$summary_name"
  summary_full_guard="$OUT_FULL_GUARD/$summary_name"
  events_no_guard="$EVENTS_NO_GUARD/$events_name"
  events_full_guard="$EVENTS_FULL_GUARD/$events_name"

  echo "\n--- product_${product_id}: no-guard run ---"
  if "$PYTHON_BIN" "$MAIN_SCRIPT" \
      --task "$task_file" \
      --output "$summary_no_guard" \
      --events-output "$events_no_guard" \
      --disable-guard \
      "${EXTRA_ARGS[@]}"; then
    echo "No-guard run completed: $summary_no_guard"
  else
    echo "No-guard run failed for product_${product_id}" >&2
    failures_no_guard=$((failures_no_guard + 1))
    continue
  fi

  echo "--- product_${product_id}: full-guard run ---"
  if "$PYTHON_BIN" "$MAIN_SCRIPT" \
      --task "$task_file" \
      --output "$summary_full_guard" \
      --events-output "$events_full_guard" \
      --guard-mode full \
      "${EXTRA_ARGS[@]}"; then
    echo "Full-guard run completed: $summary_full_guard"
    completed_pairs=$((completed_pairs + 1))
  else
    echo "Full-guard run failed for product_${product_id}" >&2
    failures_full_guard=$((failures_full_guard + 1))
  fi
done

echo "\n=== Paired Run Summary ==="
echo "Completed pairs:   $completed_pairs"
echo "Missing tasks:     $missing_tasks"
echo "No-guard fails:    $failures_no_guard"
echo "Full-guard fails:  $failures_full_guard"

if (( failures_no_guard > 0 || failures_full_guard > 0 )); then
  exit 1
fi

echo "All requested paired runs completed successfully."
