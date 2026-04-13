#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TASK_DIR="$ROOT_DIR/data/inputs/tasks"
RUN_NAME="ablation_$(date +%Y%m%d_%H%M%S)"
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
    --out-root)
      OUT_ROOT="$2"
      shift 2
      ;;
    --events-root)
      EVENTS_ROOT="$2"
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

OUT_ROOT="${OUT_ROOT:-$RUN_DIR/processed/summaries}"
EVENTS_ROOT="${EVENTS_ROOT:-$RUN_DIR/raw/events}"

if (( START_ID > END_ID )); then
  echo "Invalid range: start-id (${START_ID}) is greater than end-id (${END_ID})." >&2
  exit 1
fi

OUT_NO_GUARD="$OUT_ROOT/no_guard"
OUT_BOUNDS_ONLY="$OUT_ROOT/bounds_only"
OUT_FULL_GUARD="$OUT_ROOT/full_guard"

EVENTS_NO_GUARD="$EVENTS_ROOT/no_guard"
EVENTS_BOUNDS_ONLY="$EVENTS_ROOT/bounds_only"
EVENTS_FULL_GUARD="$EVENTS_ROOT/full_guard"

mkdir -p \
  "$OUT_NO_GUARD" "$OUT_BOUNDS_ONLY" "$OUT_FULL_GUARD" \
  "$EVENTS_NO_GUARD" "$EVENTS_BOUNDS_ONLY" "$EVENTS_FULL_GUARD"

echo "=== Ablation Negotiation Run ==="
echo "Task directory:        $TASK_DIR"
echo "Run directory:         $RUN_DIR"
echo "Output root:           $OUT_ROOT"
echo "Event root:            $EVENTS_ROOT"
echo "Product range:         ${START_ID}-${END_ID}"
echo "Conditions:            no_guard, bounds_only, full_guard"

missing_tasks=0
fail_no_guard=0
fail_bounds_only=0
fail_full_guard=0
completed_triplets=0

for (( product_id=START_ID; product_id<=END_ID; product_id++ )); do
  task_file="$TASK_DIR/product_${product_id}.json"
  if [[ ! -f "$task_file" ]]; then
    echo "Skipping product_${product_id}: missing task file at $task_file"
    missing_tasks=$((missing_tasks + 1))
    continue
  fi

  summary_name="product_${product_id}.txt"
  events_name="product_${product_id}.ndjson"

  echo "\n--- product_${product_id}: no_guard ---"
  if "$PYTHON_BIN" "$MAIN_SCRIPT" \
      --task "$task_file" \
      --output "$OUT_NO_GUARD/$summary_name" \
      --events-output "$EVENTS_NO_GUARD/$events_name" \
      --disable-guard \
      "${EXTRA_ARGS[@]}"; then
    echo "Completed no_guard for product_${product_id}"
  else
    echo "Failed no_guard for product_${product_id}" >&2
    fail_no_guard=$((fail_no_guard + 1))
    continue
  fi

  echo "--- product_${product_id}: bounds_only ---"
  if "$PYTHON_BIN" "$MAIN_SCRIPT" \
      --task "$task_file" \
      --output "$OUT_BOUNDS_ONLY/$summary_name" \
      --events-output "$EVENTS_BOUNDS_ONLY/$events_name" \
      --guard-mode bounds-only \
      "${EXTRA_ARGS[@]}"; then
    echo "Completed bounds_only for product_${product_id}"
  else
    echo "Failed bounds_only for product_${product_id}" >&2
    fail_bounds_only=$((fail_bounds_only + 1))
    continue
  fi

  echo "--- product_${product_id}: full_guard ---"
  if "$PYTHON_BIN" "$MAIN_SCRIPT" \
      --task "$task_file" \
      --output "$OUT_FULL_GUARD/$summary_name" \
      --events-output "$EVENTS_FULL_GUARD/$events_name" \
      --guard-mode full \
      "${EXTRA_ARGS[@]}"; then
    echo "Completed full_guard for product_${product_id}"
    completed_triplets=$((completed_triplets + 1))
  else
    echo "Failed full_guard for product_${product_id}" >&2
    fail_full_guard=$((fail_full_guard + 1))
  fi
done

echo "\n=== Ablation Run Summary ==="
echo "Completed triplets:   $completed_triplets"
echo "Missing tasks:        $missing_tasks"
echo "No-guard failures:    $fail_no_guard"
echo "Bounds-only failures: $fail_bounds_only"
echo "Full-guard failures:  $fail_full_guard"

if (( fail_no_guard > 0 || fail_bounds_only > 0 || fail_full_guard > 0 )); then
  exit 1
fi

echo "Ablation runs completed successfully."
