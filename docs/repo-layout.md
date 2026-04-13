# Repository Organization Contract

This document defines canonical paths and responsibilities after the April 2026 reorganization.

## Canonical Source Paths

- Package code: `src/engine_guard/`
- Primary CLI: `scripts/runners/main.py`
- Experiment orchestration: `scripts/runners/`
- Analysis tools: `scripts/analysis/`
- Inputs: `data/inputs/products.json`, `data/inputs/tasks/`

## Output Contract

All generated runs should follow:

```
outputs/<run_name>/
  raw/events/<condition>/product_<id>.ndjson
  processed/summaries/<condition>/product_<id>.txt
  processed/logs/<condition>/product_<id>.log
  analysis/*.json|*.csv
  metadata/*.json
```

Recommended condition names:

- `no_guard`
- `bounds_only`
- `full_guard`

## Script Defaults

- Runners default to `data/inputs/tasks`.
- Runners default outputs to `outputs/<timestamped_run_name>/...`.
- Analysis scripts support `--run-dir` and infer summary/event paths automatically.

## Deferred Scope

The `paper/` directory was intentionally left unchanged in this reorganization pass.
