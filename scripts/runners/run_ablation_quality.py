#!/usr/bin/env python3

import concurrent.futures
import copy
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
TASKS_DIR = ROOT_DIR / "data" / "inputs" / "tasks"
PRODUCTS_FILE = ROOT_DIR / "data" / "inputs" / "products.json"
MAIN_SCRIPT = ROOT_DIR / "scripts" / "runners" / "main.py"
RUN_DIR = ROOT_DIR / "outputs" / f"ablation_quality_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
SUMMARY_DIR = RUN_DIR / "processed" / "summaries" / "full_guard"
EVENTS_DIR = RUN_DIR / "raw" / "events" / "full_guard"

API_KEYS = [
    os.environ.get("GOOGLE_API_KEY_1") or os.environ.get("API_KEY_1") or "",
    os.environ.get("GOOGLE_API_KEY_2") or os.environ.get("API_KEY_2") or "",
]

if (ROOT_DIR / ".venv" / "bin" / "python").exists():
    PYTHON_BIN = str(ROOT_DIR / ".venv" / "bin" / "python")
else:
    PYTHON_BIN = "python3"


def generate_ablated_tasks(limit: int = 10) -> list[Path]:
    print("Generating ablated tasks (draft_quality_threshold 0.0 vs 0.9)...")
    TASKS_DIR.mkdir(parents=True, exist_ok=True)

    products = json.loads(PRODUCTS_FILE.read_text(encoding="utf-8"))
    tasks_to_run: list[Path] = []

    for product in products[:limit]:
        product_id = int(product["id"])
        base_task_file = TASKS_DIR / f"product_{product_id}.json"
        if not base_task_file.exists():
            continue

        task_payload = json.loads(base_task_file.read_text(encoding="utf-8"))

        task_low = copy.deepcopy(task_payload)
        task_low["agent_a"]["draft_quality_threshold"] = 0.0
        task_low["agent_b"]["draft_quality_threshold"] = 0.0

        task_high = copy.deepcopy(task_payload)
        task_high["agent_a"]["draft_quality_threshold"] = 0.9
        task_high["agent_b"]["draft_quality_threshold"] = 0.9

        file_q0 = TASKS_DIR / f"product_{product_id}_q0.json"
        file_q9 = TASKS_DIR / f"product_{product_id}_q9.json"

        file_q0.write_text(json.dumps(task_low, indent=4, ensure_ascii=True) + "\n", encoding="utf-8")
        file_q9.write_text(json.dumps(task_high, indent=4, ensure_ascii=True) + "\n", encoding="utf-8")

        tasks_to_run.extend([file_q0, file_q9])

    return tasks_to_run


def run_task(task_file: Path, api_key: str) -> None:
    summary_file = SUMMARY_DIR / f"{task_file.stem}.txt"
    events_file = EVENTS_DIR / f"{task_file.stem}.ndjson"

    env = os.environ.copy()
    env["GOOGLE_API_KEY"] = api_key
    env.pop("GEMINI_API_KEY", None)

    command = [
        PYTHON_BIN,
        str(MAIN_SCRIPT),
        "--task",
        str(task_file),
        "--output",
        str(summary_file),
        "--events-output",
        str(events_file),
        "--guard-mode",
        "full",
    ]

    print(f"Running {task_file.name}...")
    result = subprocess.run(
        command,
        cwd=ROOT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        print(f"Error in {task_file.name}:\n{result.stderr}")
    else:
        print(f"Completed {task_file.name}")


def main() -> int:
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)

    tasks = generate_ablated_tasks(limit=10)

    fallback_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY") or ""
    valid_keys = [key for key in API_KEYS if key]
    if fallback_key and fallback_key not in valid_keys:
        valid_keys.append(fallback_key)

    if not valid_keys:
        raise RuntimeError(
            "No API keys found. Set GOOGLE_API_KEY_1/2 (or API_KEY_1/2), or GOOGLE_API_KEY."
        )

    print(f"Executing {len(tasks)} tasks using {len(valid_keys)} concurrent workers...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(valid_keys)) as executor:
        futures = []
        for index, task_file in enumerate(tasks):
            key_value = valid_keys[index % len(valid_keys)]
            futures.append(executor.submit(run_task, task_file, key_value))

        concurrent.futures.wait(futures)

    print("All ablation quality tests completed.")
    print(f"Run outputs written under: {RUN_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
