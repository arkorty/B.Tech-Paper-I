# engine-guard

Agent-to-agent negotiation engine with programmatic safety guards, experiment runners, and analysis tooling.

## Repository Layout

- `src/engine_guard/`: core package (`agents`, `core`, `guards`)
- `scripts/runners/`: negotiation and experiment execution entrypoints
- `scripts/analysis/`: post-run analytics and comparison scripts
- `scripts/utils/`: task generation and utility tools
- `scripts/scratch/`: non-canonical ad hoc scripts
- `data/inputs/`: source-of-truth inputs (`products.json`, `tasks/`)
- `outputs/`: run-versioned generated artifacts (ignored by git)
- `paper/`: manuscript assets (intentionally unchanged in this migration)

## Quick Start

Install dependencies:

```bash
conda create -n eg python=3.14
conda activate eg

pip -r requirements.txt
```

Run a single negotiation:

```bash
python3 scripts/runners/main.py \
  --task data/inputs/tasks/product_1.json \
  --output outputs/manual_run/processed/summaries/full_guard/product_1.txt \
  --events-output outputs/manual_run/raw/events/full_guard/product_1.ndjson
```

Run a parallel experiment (dry run):

```bash
python3 scripts/runners/run_parallel_multi_key_experiments.py \
  --dry-run --workers 1 --start-id 1 --end-id 2
```

Analyze a run directory:

```bash
python3 scripts/analysis/analyze_paired_negotiations.py \
  --run-dir outputs/<run_name> \
  --condition-a-label no_guard \
  --condition-b-label full_guard
```

## Reproducibility Policy

- Source code and input contracts are tracked.
- Generated artifacts under `outputs/` are local-only by default.
- Historical migration details are stored in `outputs/legacy_migration_20260414/metadata/`.
