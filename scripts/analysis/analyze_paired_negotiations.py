#!/usr/bin/env python3
import argparse
import csv
import json
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class SummaryData:
    status: str
    rounds: int
    accepted_terms: dict[str, Any]


@dataclass
class RunMetrics:
    product_id: int
    condition: str
    product_type: str
    retail_price_usd: float
    wholesale_price_usd: float
    agreement: bool
    rounds: int
    final_price: float | None
    midpoint: float
    final_violation: bool | None
    midpoint_deviation: float | None
    midpoint_deviation_pct: float | None
    generated_proposals: int
    unsafe_attempts: int
    unsafe_attempt_rate: float | None
    guard_failures: int
    guard_passes: int
    guard_rejection_rate: float | None


def parse_price(raw: Any) -> float:
    return float(str(raw).replace("$", "").replace(",", ""))


def safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def parse_summary(path: Path) -> SummaryData:
    status: str | None = None
    rounds: int | None = None
    accepted_terms: dict[str, Any] = {}

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if line.startswith("Status:"):
            status = line.split(":", 1)[1].strip()
        elif line.startswith("Total Rounds:"):
            rounds = int(line.split(":", 1)[1].strip())
        elif stripped.startswith("- ") and ":" in stripped:
            term_name, raw_value = stripped[2:].split(":", 1)
            raw = raw_value.strip()
            try:
                accepted_terms[term_name.strip()] = float(raw)
            except ValueError:
                accepted_terms[term_name.strip()] = raw

    if status is None or rounds is None:
        raise ValueError(f"Unable to parse summary file: {path}")

    return SummaryData(status=status, rounds=rounds, accepted_terms=accepted_terms)


def parse_ndjson(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        events.append(json.loads(stripped))
    return events


def evaluate_proposal_constraints(
    agent_id: str,
    terms: dict[str, Any],
    task_payload: dict[str, Any],
) -> bool:
    if not isinstance(terms, dict):
        return False

    if task_payload["agent_a"]["agent_id"] == agent_id:
        agent_key = "agent_a"
    elif task_payload["agent_b"]["agent_id"] == agent_id:
        agent_key = "agent_b"
    else:
        return False

    constraints = task_payload[agent_key].get("hard_constraints", {})
    required_terms = set(constraints.keys())
    offered_terms = set(terms.keys())

    if required_terms != offered_terms:
        return False

    for term_name, bounds in constraints.items():
        min_val, max_val = bounds
        if term_name not in terms:
            return False

        try:
            numeric = float(terms[term_name])
        except (TypeError, ValueError):
            return False

        if min_val is not None and numeric < float(min_val):
            return False
        if max_val is not None and numeric > float(max_val):
            return False

    return True


def extract_unit_price_bounds(task_payload: dict[str, Any]) -> tuple[float | None, float | None]:
    lower_bound = None
    upper_bound = None

    bounds_a = task_payload["agent_a"].get("hard_constraints", {}).get("unit_price_usd")
    bounds_b = task_payload["agent_b"].get("hard_constraints", {}).get("unit_price_usd")

    if bounds_a and bounds_a[0] is not None:
        lower_bound = float(bounds_a[0])
    if bounds_b and bounds_b[1] is not None:
        upper_bound = float(bounds_b[1])

    return lower_bound, upper_bound


def build_run_metrics(
    product_id: int,
    condition: str,
    summary: SummaryData,
    events: list[dict[str, Any]],
    task_payload: dict[str, Any],
    product_payload: dict[str, Any],
) -> RunMetrics:
    retail_price = parse_price(product_payload["Retail Price"])
    wholesale_price = parse_price(product_payload["Wholesale Price"])
    midpoint = (retail_price + wholesale_price) / 2.0

    agreement = summary.status.startswith("SUCCESS")
    final_price = None
    if agreement:
        value = summary.accepted_terms.get("unit_price_usd")
        if isinstance(value, (int, float)):
            final_price = float(value)

    lower_bound, upper_bound = extract_unit_price_bounds(task_payload)

    final_violation = None
    midpoint_deviation = None
    midpoint_deviation_pct = None
    if final_price is not None:
        out_of_lower = lower_bound is not None and final_price < lower_bound
        out_of_upper = upper_bound is not None and final_price > upper_bound
        final_violation = out_of_lower or out_of_upper
        midpoint_deviation = abs(final_price - midpoint)
        midpoint_deviation_pct = midpoint_deviation / midpoint if midpoint else None

    generated_proposals = 0
    unsafe_attempts = 0
    guard_failures = 0
    guard_passes = 0

    for event in events:
        event_type = event.get("type")
        if event_type == "PROPOSAL_GENERATED":
            generated_proposals += 1
            proposal = event.get("proposal", {})
            terms = proposal.get("terms", {})
            if not evaluate_proposal_constraints(event.get("agent_id", ""), terms, task_payload):
                unsafe_attempts += 1
        elif event_type == "GUARD_EVALUATION":
            if event.get("bypassed"):
                continue
            if event.get("passed"):
                guard_passes += 1
            else:
                guard_failures += 1

    unsafe_attempt_rate = safe_ratio(float(unsafe_attempts), float(generated_proposals))
    guard_rejection_rate = safe_ratio(
        float(guard_failures),
        float(guard_failures + guard_passes),
    )

    return RunMetrics(
        product_id=product_id,
        condition=condition,
        product_type=str(product_payload.get("Type", "Unknown")),
        retail_price_usd=retail_price,
        wholesale_price_usd=wholesale_price,
        agreement=agreement,
        rounds=summary.rounds,
        final_price=final_price,
        midpoint=midpoint,
        final_violation=final_violation,
        midpoint_deviation=midpoint_deviation,
        midpoint_deviation_pct=midpoint_deviation_pct,
        generated_proposals=generated_proposals,
        unsafe_attempts=unsafe_attempts,
        unsafe_attempt_rate=unsafe_attempt_rate,
        guard_failures=guard_failures,
        guard_passes=guard_passes,
        guard_rejection_rate=guard_rejection_rate,
    )


def aggregate(runs: list[RunMetrics]) -> dict[str, Any]:
    agreements = [run for run in runs if run.agreement]

    total_generated = sum(run.generated_proposals for run in runs)
    total_unsafe = sum(run.unsafe_attempts for run in runs)
    total_guard_fails = sum(run.guard_failures for run in runs)
    total_guard_evals = total_guard_fails + sum(run.guard_passes for run in runs)

    return {
        "runs": len(runs),
        "agreements": len(agreements),
        "agreement_rate": safe_ratio(float(len(agreements)), float(len(runs))) if runs else None,
        "avg_rounds": mean([float(run.rounds) for run in agreements]),
        "avg_midpoint_deviation": mean([
            float(run.midpoint_deviation)
            for run in agreements
            if run.midpoint_deviation is not None
        ]),
        "avg_midpoint_deviation_pct": mean([
            float(run.midpoint_deviation_pct)
            for run in agreements
            if run.midpoint_deviation_pct is not None
        ]),
        "final_violation_rate": mean([
            1.0 if run.final_violation else 0.0
            for run in agreements
            if run.final_violation is not None
        ]),
        "avg_generated_proposals": mean([float(run.generated_proposals) for run in runs]),
        "unsafe_attempt_rate_overall": safe_ratio(float(total_unsafe), float(total_generated)) if total_generated else None,
        "avg_unsafe_attempt_rate_per_run": mean([
            float(run.unsafe_attempt_rate)
            for run in runs
            if run.unsafe_attempt_rate is not None
        ]),
        "avg_guard_failures_per_run": mean([float(run.guard_failures) for run in runs]),
        "guard_rejection_rate": safe_ratio(float(total_guard_fails), float(total_guard_evals)) if total_guard_evals else None,
    }


def quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        raise ValueError("Cannot compute quantile of empty list")
    if len(sorted_values) == 1:
        return sorted_values[0]

    clamped_q = min(1.0, max(0.0, q))
    position = clamped_q * (len(sorted_values) - 1)
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(sorted_values) - 1)
    weight = position - lower_index

    lower_value = sorted_values[lower_index]
    upper_value = sorted_values[upper_index]
    return lower_value + (upper_value - lower_value) * weight


def bootstrap_mean_ci(
    values: list[float],
    confidence_level: float,
    samples: int,
    seed: int,
) -> dict[str, Any] | None:
    if not values:
        return None

    rng = random.Random(seed)
    n = len(values)
    sample_means: list[float] = []

    for _ in range(samples):
        total = 0.0
        for _ in range(n):
            total += values[rng.randrange(n)]
        sample_means.append(total / n)

    sample_means.sort()
    alpha = 1.0 - confidence_level

    return {
        "mean": mean(values),
        "ci_low": quantile(sample_means, alpha / 2.0),
        "ci_high": quantile(sample_means, 1.0 - alpha / 2.0),
        "n": n,
        "samples": samples,
        "confidence_level": confidence_level,
    }


def write_mixed_effects_csv(path: Path, runs: list[RunMetrics]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "product_id",
        "condition",
        "product_type",
        "retail_price_usd",
        "wholesale_price_usd",
        "midpoint_usd",
        "agreement",
        "rounds",
        "final_price_usd",
        "final_violation",
        "midpoint_deviation_usd",
        "midpoint_deviation_pct",
        "generated_proposals",
        "unsafe_attempts",
        "unsafe_attempt_rate",
        "guard_failures",
        "guard_passes",
        "guard_rejection_rate",
    ]

    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for run in sorted(runs, key=lambda item: (item.product_id, item.condition)):
            writer.writerow(
                {
                    "product_id": run.product_id,
                    "condition": run.condition,
                    "product_type": run.product_type,
                    "retail_price_usd": run.retail_price_usd,
                    "wholesale_price_usd": run.wholesale_price_usd,
                    "midpoint_usd": run.midpoint,
                    "agreement": int(run.agreement),
                    "rounds": run.rounds,
                    "final_price_usd": run.final_price,
                    "final_violation": "" if run.final_violation is None else int(run.final_violation),
                    "midpoint_deviation_usd": run.midpoint_deviation,
                    "midpoint_deviation_pct": run.midpoint_deviation_pct,
                    "generated_proposals": run.generated_proposals,
                    "unsafe_attempts": run.unsafe_attempts,
                    "unsafe_attempt_rate": run.unsafe_attempt_rate,
                    "guard_failures": run.guard_failures,
                    "guard_passes": run.guard_passes,
                    "guard_rejection_rate": run.guard_rejection_rate,
                }
            )


def resolve_repo_path(repo_root: Path, path_value: Path) -> Path:
    if path_value.is_absolute():
        return path_value
    return repo_root / path_value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze paired negotiation runs with event-level metrics, bootstrap CIs, and CSV export."
    )
    parser.add_argument("--products-file", type=Path, default=Path("data/inputs/products.json"))
    parser.add_argument("--tasks-dir", type=Path, default=Path("data/inputs/tasks"))
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Run directory in outputs/<run_name>. If provided, summaries/events paths are inferred.",
    )

    parser.add_argument("--summaries-no-guard", "--summaries-a", dest="summaries_a", type=Path, default=None)
    parser.add_argument("--summaries-with-guard", "--summaries-b", dest="summaries_b", type=Path, default=None)
    parser.add_argument("--events-no-guard", "--events-a", dest="events_a", type=Path, default=None)
    parser.add_argument("--events-with-guard", "--events-b", dest="events_b", type=Path, default=None)

    parser.add_argument("--condition-a-label", "--label-a", dest="label_a", type=str, default="no_guard")
    parser.add_argument("--condition-b-label", "--label-b", dest="label_b", type=str, default="full_guard")

    parser.add_argument("--start-id", type=int, default=1)
    parser.add_argument("--end-id", type=int, default=100)

    parser.add_argument("--bootstrap-samples", type=int, default=10000)
    parser.add_argument("--bootstrap-seed", type=int, default=123)
    parser.add_argument("--confidence-level", type=float, default=0.95)

    parser.add_argument("--json-output", type=Path, default=None)
    parser.add_argument("--csv-output", type=Path, default=None)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]

    if args.start_id > args.end_id:
        raise ValueError("--start-id must be less than or equal to --end-id")
    if args.label_a == args.label_b:
        raise ValueError("Condition labels must be distinct.")
    if args.bootstrap_samples <= 0:
        raise ValueError("--bootstrap-samples must be positive")
    if not (0.0 < args.confidence_level < 1.0):
        raise ValueError("--confidence-level must be in (0, 1)")

    products_file = resolve_repo_path(repo_root, args.products_file)
    tasks_dir = resolve_repo_path(repo_root, args.tasks_dir)

    if args.run_dir is not None:
        run_dir = resolve_repo_path(repo_root, args.run_dir)
        summaries_a_dir = (
            resolve_repo_path(repo_root, args.summaries_a)
            if args.summaries_a
            else run_dir / "processed" / "summaries" / args.label_a
        )
        summaries_b_dir = (
            resolve_repo_path(repo_root, args.summaries_b)
            if args.summaries_b
            else run_dir / "processed" / "summaries" / args.label_b
        )
        events_a_dir = (
            resolve_repo_path(repo_root, args.events_a)
            if args.events_a
            else run_dir / "raw" / "events" / args.label_a
        )
        events_b_dir = (
            resolve_repo_path(repo_root, args.events_b)
            if args.events_b
            else run_dir / "raw" / "events" / args.label_b
        )
        json_output = (
            resolve_repo_path(repo_root, args.json_output)
            if args.json_output
            else run_dir / "analysis" / "paired_analysis.json"
        )
        csv_output = (
            resolve_repo_path(repo_root, args.csv_output)
            if args.csv_output
            else run_dir / "analysis" / "mixed_effects.csv"
        )
    else:
        required_paths = [args.summaries_a, args.summaries_b, args.events_a, args.events_b]
        if any(path is None for path in required_paths):
            raise ValueError(
                "Provide --run-dir, or explicitly set --summaries-a, --summaries-b, --events-a, and --events-b."
            )
        summaries_a_dir = resolve_repo_path(repo_root, args.summaries_a)
        summaries_b_dir = resolve_repo_path(repo_root, args.summaries_b)
        events_a_dir = resolve_repo_path(repo_root, args.events_a)
        events_b_dir = resolve_repo_path(repo_root, args.events_b)
        json_output = resolve_repo_path(repo_root, args.json_output) if args.json_output else None
        csv_output = resolve_repo_path(repo_root, args.csv_output) if args.csv_output else None

    products = json.loads(products_file.read_text(encoding="utf-8"))
    product_by_id = {int(item["id"]): item for item in products}

    missing_required: list[dict[str, Any]] = []
    missing_event_logs: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []
    runs: list[RunMetrics] = []

    for product_id in range(args.start_id, args.end_id + 1):
        task_path = tasks_dir / f"product_{product_id}.json"
        summary_a_path = summaries_a_dir / f"product_{product_id}.txt"
        summary_b_path = summaries_b_dir / f"product_{product_id}.txt"
        events_a_path = events_a_dir / f"product_{product_id}.ndjson"
        events_b_path = events_b_dir / f"product_{product_id}.ndjson"

        required_paths = [task_path, summary_a_path, summary_b_path]
        missing_paths = [str(path) for path in required_paths if not path.exists()]
        if missing_paths:
            missing_required.append({"product_id": product_id, "missing": missing_paths})
            continue

        if product_id not in product_by_id:
            missing_required.append(
                {
                    "product_id": product_id,
                    "missing": [f"products.json entry id={product_id}"],
                }
            )
            continue

        missing_events = []
        if not events_a_path.exists():
            missing_events.append(str(events_a_path))
        if not events_b_path.exists():
            missing_events.append(str(events_b_path))
        if missing_events:
            missing_event_logs.append({"product_id": product_id, "missing": missing_events})

        try:
            task_payload = json.loads(task_path.read_text(encoding="utf-8"))
            summary_a = parse_summary(summary_a_path)
            summary_b = parse_summary(summary_b_path)
            events_a = parse_ndjson(events_a_path)
            events_b = parse_ndjson(events_b_path)
        except Exception as exc:
            parse_errors.append({"product_id": product_id, "error": str(exc)})
            continue

        product_payload = product_by_id[product_id]

        runs.append(
            build_run_metrics(
                product_id=product_id,
                condition=args.label_a,
                summary=summary_a,
                events=events_a,
                task_payload=task_payload,
                product_payload=product_payload,
            )
        )
        runs.append(
            build_run_metrics(
                product_id=product_id,
                condition=args.label_b,
                summary=summary_b,
                events=events_b,
                task_payload=task_payload,
                product_payload=product_payload,
            )
        )

    runs_a = [run for run in runs if run.condition == args.label_a]
    runs_b = [run for run in runs if run.condition == args.label_b]

    runs_by_product: dict[int, dict[str, RunMetrics]] = {}
    for run in runs:
        runs_by_product.setdefault(run.product_id, {})[run.condition] = run

    paired_products = sorted(
        product_id
        for product_id, item in runs_by_product.items()
        if args.label_a in item and args.label_b in item
    )

    delta_midpoint_deviation: list[float] = []
    delta_rounds: list[float] = []
    delta_unsafe_attempt_rate: list[float] = []
    delta_agreement: list[float] = []

    for product_id in paired_products:
        run_a = runs_by_product[product_id][args.label_a]
        run_b = runs_by_product[product_id][args.label_b]

        delta_rounds.append(float(run_b.rounds - run_a.rounds))
        delta_agreement.append(float(int(run_b.agreement) - int(run_a.agreement)))

        if run_a.midpoint_deviation is not None and run_b.midpoint_deviation is not None:
            delta_midpoint_deviation.append(float(run_b.midpoint_deviation - run_a.midpoint_deviation))

        if run_a.unsafe_attempt_rate is not None and run_b.unsafe_attempt_rate is not None:
            delta_unsafe_attempt_rate.append(float(run_b.unsafe_attempt_rate - run_a.unsafe_attempt_rate))

    deltas = {
        "avg_midpoint_deviation": mean(delta_midpoint_deviation),
        "avg_rounds": mean(delta_rounds),
        "avg_unsafe_attempt_rate": mean(delta_unsafe_attempt_rate),
        "avg_agreement_delta": mean(delta_agreement),
    }

    bootstrap = {
        "delta_avg_midpoint_deviation": bootstrap_mean_ci(
            delta_midpoint_deviation,
            confidence_level=args.confidence_level,
            samples=args.bootstrap_samples,
            seed=args.bootstrap_seed,
        ),
        "delta_avg_rounds": bootstrap_mean_ci(
            delta_rounds,
            confidence_level=args.confidence_level,
            samples=args.bootstrap_samples,
            seed=args.bootstrap_seed + 1,
        ),
        "delta_avg_unsafe_attempt_rate": bootstrap_mean_ci(
            delta_unsafe_attempt_rate,
            confidence_level=args.confidence_level,
            samples=args.bootstrap_samples,
            seed=args.bootstrap_seed + 2,
        ),
    }

    report = {
        "range": {"start_id": args.start_id, "end_id": args.end_id},
        "condition_labels": {"a": args.label_a, "b": args.label_b},
        "paired_products": len(paired_products),
        "missing_required": missing_required,
        "missing_event_logs": missing_event_logs,
        "parse_errors": parse_errors,
        "conditions": {
            args.label_a: aggregate(runs_a),
            args.label_b: aggregate(runs_b),
        },
        "deltas_b_minus_a": deltas,
        "bootstrap_cis_b_minus_a": bootstrap,
    }

    print("=== Paired Negotiation Analysis ===")
    print(f"Condition A ({args.label_a}) runs: {len(runs_a)}")
    print(f"Condition B ({args.label_b}) runs: {len(runs_b)}")
    print(f"Paired products analyzed: {len(paired_products)}")
    print(f"Missing required files: {len(missing_required)}")
    print(f"Missing event logs: {len(missing_event_logs)}")
    print(f"Parse errors: {len(parse_errors)}")

    print(f"\n{args.label_a} aggregate:")
    for key, value in report["conditions"][args.label_a].items():
        print(f"  - {key}: {value}")

    print(f"\n{args.label_b} aggregate:")
    for key, value in report["conditions"][args.label_b].items():
        print(f"  - {key}: {value}")

    print(f"\nDeltas ({args.label_b} - {args.label_a}):")
    for key, value in report["deltas_b_minus_a"].items():
        print(f"  - {key}: {value}")

    print("\nBootstrap CIs:")
    for key, value in report["bootstrap_cis_b_minus_a"].items():
        print(f"  - {key}: {value}")

    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(
            json.dumps(report, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
        print(f"\nJSON report written to: {json_output}")

    if csv_output:
        write_mixed_effects_csv(csv_output, runs)
        print(f"Mixed-effects CSV written to: {csv_output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
