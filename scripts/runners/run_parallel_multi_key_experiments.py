#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


VARIATION_FLAGS: dict[str, list[str]] = {
    "no_guard": ["--disable-guard"],
    "bounds_only": ["--guard-mode", "bounds-only"],
    "full_guard": ["--guard-mode", "full"],
}


@dataclass
class Job:
    product_id: int
    variation: str
    task_file: Path
    summary_file: Path
    events_file: Path
    log_file: Path


@dataclass
class JobResult:
    product_id: int
    variation: str
    key_slot: int
    return_code: int
    duration_seconds: float
    summary_file: str
    events_file: str
    log_file: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run parallel negotiation experiments across multiple API keys and all guard variations. "
            "Each worker uses one API key and executes assigned jobs sequentially."
        )
    )
    parser.add_argument("--tasks-dir", type=Path, default=Path("data/inputs/tasks"))
    parser.add_argument("--output-root", type=Path, default=Path("outputs"))
    parser.add_argument("--run-name", type=str, default="")
    parser.add_argument("--start-id", type=int, default=1)
    parser.add_argument("--end-id", type=int, default=100)
    parser.add_argument(
        "--variations",
        type=str,
        default="no_guard,bounds_only,full_guard",
        help="Comma-separated variation set: no_guard,bounds_only,full_guard",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel workers. Recommended to match number of provided API keys.",
    )
    parser.add_argument(
        "--api-key",
        action="append",
        default=[],
        help="API key value; repeat exactly 4 times or rely on GOOGLE_API_KEY_1..4 env vars.",
    )
    parser.add_argument(
        "--api-key-file",
        type=Path,
        default=None,
        help="Optional file with one API key per line.",
    )
    parser.add_argument(
        "--python-bin",
        type=str,
        default="",
        help="Python interpreter for running scripts/runners/main.py (defaults to .venv/bin/python if present).",
    )
    parser.add_argument(
        "--main-script",
        type=Path,
        default=Path("scripts/runners/main.py"),
        help="Path to the negotiation entry script.",
    )
    parser.add_argument(
        "--main-arg",
        action="append",
        default=[],
        help="Additional argument to pass through to main.py (repeatable).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build and log commands without executing them.",
    )
    return parser.parse_args()


def resolve_path(root_dir: Path, path_value: Path) -> Path:
    if path_value.is_absolute():
        return path_value
    return root_dir / path_value


def load_api_keys(args: argparse.Namespace) -> list[str]:
    keys: list[str] = []

    for raw in args.api_key:
        normalized = raw.strip()
        if normalized:
            keys.append(normalized)

    if args.api_key_file:
        for line in args.api_key_file.read_text(encoding="utf-8").splitlines():
            normalized = line.strip()
            if normalized and not normalized.startswith("#"):
                keys.append(normalized)

    if not keys:
        for idx in range(1, 5):
            env_key = os.environ.get(f"GOOGLE_API_KEY_{idx}") or os.environ.get(f"GEMINI_API_KEY_{idx}")
            if env_key:
                keys.append(env_key.strip())

    # Preserve order while removing duplicates.
    deduped: list[str] = []
    for key in keys:
        if key and key not in deduped:
            deduped.append(key)

    if len(deduped) < args.workers:
        raise ValueError(
            f"Need at least {args.workers} API keys, found {len(deduped)}. "
            "Provide --api-key entries, --api-key-file, or GOOGLE_API_KEY_1..N env vars."
        )

    return deduped[: args.workers]


def resolve_python_bin(root_dir: Path, args: argparse.Namespace) -> str:
    if args.python_bin:
        return args.python_bin

    venv_python = root_dir / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)

    return "python3"


def parse_variations(raw: str) -> list[str]:
    variations = [item.strip() for item in raw.split(",") if item.strip()]
    if not variations:
        raise ValueError("At least one variation must be provided.")

    unsupported = [item for item in variations if item not in VARIATION_FLAGS]
    if unsupported:
        raise ValueError(
            f"Unsupported variation(s): {unsupported}. Supported: {sorted(VARIATION_FLAGS.keys())}"
        )

    # Preserve order, drop duplicates.
    unique: list[str] = []
    for item in variations:
        if item not in unique:
            unique.append(item)
    return unique


def build_jobs(
    root_dir: Path,
    tasks_dir: Path,
    run_dir: Path,
    start_id: int,
    end_id: int,
    variations: list[str],
) -> tuple[list[Job], list[int]]:
    jobs: list[Job] = []
    missing_tasks: list[int] = []

    for product_id in range(start_id, end_id + 1):
        task_file = tasks_dir / f"product_{product_id}.json"
        if not task_file.exists():
            missing_tasks.append(product_id)
            continue

        for variation in variations:
            summary_file = run_dir / "processed" / "summaries" / variation / f"product_{product_id}.txt"
            events_file = run_dir / "raw" / "events" / variation / f"product_{product_id}.ndjson"
            log_file = run_dir / "processed" / "logs" / variation / f"product_{product_id}.log"

            summary_file.parent.mkdir(parents=True, exist_ok=True)
            events_file.parent.mkdir(parents=True, exist_ok=True)
            log_file.parent.mkdir(parents=True, exist_ok=True)

            jobs.append(
                Job(
                    product_id=product_id,
                    variation=variation,
                    task_file=task_file,
                    summary_file=summary_file,
                    events_file=events_file,
                    log_file=log_file,
                )
            )

    return jobs, missing_tasks


def run_slot_jobs(
    slot_index: int,
    key_value: str,
    jobs: list[Job],
    root_dir: Path,
    python_bin: str,
    main_script: Path,
    pass_through_args: list[str],
    dry_run: bool,
) -> list[JobResult]:
    results: list[JobResult] = []

    env = os.environ.copy()
    env["GOOGLE_API_KEY"] = key_value
    # Keep precedence deterministic by dropping legacy key if present.
    env.pop("GEMINI_API_KEY", None)

    for job in jobs:
        command = [
            python_bin,
            str(main_script),
            "--task",
            str(job.task_file),
            "--output",
            str(job.summary_file),
            "--events-output",
            str(job.events_file),
            *VARIATION_FLAGS[job.variation],
            *pass_through_args,
        ]

        start_time = time.perf_counter()
        return_code = 0

        with job.log_file.open("w", encoding="utf-8") as log_handle:
            log_handle.write("COMMAND:\n")
            log_handle.write(" ".join(command) + "\n\n")
            log_handle.write(f"KEY_SLOT: {slot_index + 1}\n")
            log_handle.write(f"VARIATION: {job.variation}\n")
            log_handle.write(f"PRODUCT_ID: {job.product_id}\n\n")

            if dry_run:
                log_handle.write("DRY_RUN enabled: command not executed.\n")
            else:
                completed = subprocess.run(
                    command,
                    cwd=root_dir,
                    env=env,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                )
                return_code = completed.returncode

        duration = time.perf_counter() - start_time
        status = "OK" if return_code == 0 else "FAIL"
        print(
            f"[slot {slot_index + 1}] product_{job.product_id} {job.variation}: {status} ({duration:.2f}s)"
        )

        results.append(
            JobResult(
                product_id=job.product_id,
                variation=job.variation,
                key_slot=slot_index + 1,
                return_code=return_code,
                duration_seconds=duration,
                summary_file=str(job.summary_file),
                events_file=str(job.events_file),
                log_file=str(job.log_file),
            )
        )

    return results


def main() -> int:
    args = parse_args()

    if args.start_id > args.end_id:
        raise ValueError("--start-id must be less than or equal to --end-id")
    if args.workers <= 0:
        raise ValueError("--workers must be positive")

    root_dir = Path(__file__).resolve().parents[2]
    tasks_dir = resolve_path(root_dir, args.tasks_dir)
    output_root = resolve_path(root_dir, args.output_root)
    main_script = resolve_path(root_dir, args.main_script)

    if not main_script.exists():
        raise FileNotFoundError(f"Main script not found: {main_script}")
    if not tasks_dir.exists():
        raise FileNotFoundError(f"Tasks directory not found: {tasks_dir}")

    variations = parse_variations(args.variations)
    api_keys = load_api_keys(args)
    python_bin = resolve_python_bin(root_dir, args)

    run_name = (
        args.run_name.strip()
        if args.run_name
        else datetime.now(UTC).strftime("run_%Y%m%d_%H%M%S")
    )
    run_dir = output_root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)

    jobs, missing_tasks = build_jobs(
        root_dir=root_dir,
        tasks_dir=tasks_dir,
        run_dir=run_dir,
        start_id=args.start_id,
        end_id=args.end_id,
        variations=variations,
    )

    if not jobs:
        print("No runnable jobs found. Check task range and task files.")
        return 1

    buckets: list[list[Job]] = [[] for _ in range(args.workers)]
    for index, job in enumerate(jobs):
        buckets[index % args.workers].append(job)

    print("=== Parallel Multi-Key Experiment Run ===")
    print(f"Run directory:       {run_dir}")
    print(f"Tasks directory:     {tasks_dir}")
    print(f"Product range:       {args.start_id}-{args.end_id}")
    print(f"Variations:          {variations}")
    print(f"Workers:             {args.workers}")
    print(f"API keys loaded:     {len(api_keys)}")
    print(f"Total jobs:          {len(jobs)}")
    print(f"Missing task files:  {len(missing_tasks)}")
    if args.dry_run:
        print("DRY_RUN mode active: commands will not execute.")

    started_at = datetime.now(UTC).isoformat()
    wall_start = time.perf_counter()

    all_results: list[JobResult] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for slot_index in range(args.workers):
            futures.append(
                executor.submit(
                    run_slot_jobs,
                    slot_index,
                    api_keys[slot_index],
                    buckets[slot_index],
                    root_dir,
                    python_bin,
                    main_script,
                    args.main_arg,
                    args.dry_run,
                )
            )

        for future in futures:
            all_results.extend(future.result())

    wall_duration = time.perf_counter() - wall_start
    finished_at = datetime.now(UTC).isoformat()

    failures = [result for result in all_results if result.return_code != 0]
    successes = [result for result in all_results if result.return_code == 0]

    manifest = {
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": wall_duration,
        "config": {
            "tasks_dir": str(tasks_dir),
            "output_root": str(output_root),
            "run_dir": str(run_dir),
            "main_script": str(main_script),
            "python_bin": python_bin,
            "workers": args.workers,
            "api_keys_used": len(api_keys),
            "variations": variations,
            "start_id": args.start_id,
            "end_id": args.end_id,
            "dry_run": args.dry_run,
            "main_args": args.main_arg,
        },
        "summary": {
            "jobs_total": len(all_results),
            "jobs_succeeded": len(successes),
            "jobs_failed": len(failures),
            "missing_tasks": missing_tasks,
        },
        "results": [asdict(result) for result in all_results],
    }

    metadata_dir = run_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = metadata_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    failures_path = metadata_dir / "run_failures.json"
    failures_path.write_text(
        json.dumps([asdict(result) for result in failures], indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    print("\n=== Run Summary ===")
    print(f"Succeeded:           {len(successes)}")
    print(f"Failed:              {len(failures)}")
    print(f"Manifest:            {manifest_path}")
    print(f"Failures:            {failures_path}")
    print(f"Duration (seconds):  {wall_duration:.2f}")

    return 1 if failures else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Fatal error: {exc}", file=sys.stderr)
        raise SystemExit(1)
