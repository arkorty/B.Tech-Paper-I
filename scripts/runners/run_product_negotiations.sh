#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TASK_DIR="$ROOT_DIR/data/inputs/tasks"
RUN_NAME="no_guard_$(date +%Y%m%d_%H%M%S)"
OUTPUT_DIR=""
EVENTS_DIR=""
MAIN_SCRIPT="$ROOT_DIR/scripts/runners/main.py"

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
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --events-dir)
      EVENTS_DIR="$2"
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

if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$ROOT_DIR/outputs/$RUN_NAME/processed/summaries/no_guard"
fi

if [[ -z "$EVENTS_DIR" ]]; then
  EVENTS_DIR="$ROOT_DIR/outputs/$RUN_NAME/raw/events/no_guard"
fi

mkdir -p "$OUTPUT_DIR" "$EVENTS_DIR"

mapfile -t TASK_FILES < <(find "$TASK_DIR" -maxdepth 1 -type f -name "product_*.json" | sort -V)
if [[ ${#TASK_FILES[@]} -eq 0 ]]; then
  echo "No product tasks found in $TASK_DIR (expected files like product_1.json)."
  exit 1
fi

echo "Found ${#TASK_FILES[@]} tasks. Starting negotiations..."

failures=0
for task_file in "${TASK_FILES[@]}"; do
  task_stem="$(basename "$task_file" .json)"
  output_file="$OUTPUT_DIR/${task_stem}.txt"
  events_file="$EVENTS_DIR/${task_stem}.ndjson"

  echo "Running ${task_stem}.json ..."
  if "$PYTHON_BIN" "$MAIN_SCRIPT" \
      --task "$task_file" \
      --output "$output_file" \
      --events-output "$events_file" \
      --disable-guard \
      "${EXTRA_ARGS[@]}"; then
    echo "Saved summary to $output_file"
  else
    echo "Negotiation failed for ${task_stem}.json"
    failures=$((failures + 1))
  fi
done

if [[ $failures -gt 0 ]]; then
  echo "Completed with $failures failures."
  exit 1
fi

echo "All negotiations completed successfully. Summaries are in $OUTPUT_DIR"
