#!/usr/bin/env python3
import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass
class NegotiationSummary:
    status: str | None
    total_rounds: int | None
    accepted_terms: dict[str, Any]


def parse_scalar(raw: str) -> Any:
    value = raw.strip()
    lowered = value.lower()

    if lowered == "none":
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        if any(ch in value for ch in (".", "e", "E")):
            return float(value)
        return int(value)
    except ValueError:
        return value


def parse_summary(file_path: Path) -> NegotiationSummary:
    text = file_path.read_text(encoding="utf-8")

    status: str | None = None
    total_rounds: int | None = None
    accepted_terms: dict[str, Any] = {}

    for line in text.splitlines():
        stripped = line.strip()

        if line.startswith("Status:"):
            status = line.split(":", 1)[1].strip()
            continue

        if line.startswith("Total Rounds:"):
            total_rounds = int(line.split(":", 1)[1].strip())
            continue

        if stripped.startswith("- "):
            term_payload = stripped[2:]
            if ":" not in term_payload:
                continue

            term_name, raw_value = term_payload.split(":", 1)
            accepted_terms[term_name.strip()] = parse_scalar(raw_value)

    if status is None:
        raise ValueError(f"Could not parse Status from {file_path}")
    if total_rounds is None:
        raise ValueError(f"Could not parse Total Rounds from {file_path}")

    return NegotiationSummary(
        status=status,
        total_rounds=total_rounds,
        accepted_terms=accepted_terms,
    )


def values_equal(left: Any, right: Any, tolerance: float = 1e-9) -> bool:
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return abs(float(left) - float(right)) <= tolerance
    return left == right


def compare_summaries(left: NegotiationSummary, right: NegotiationSummary) -> dict[str, Any]:
    differences: dict[str, Any] = {}

    if left.status != right.status:
        differences["status"] = {"left": left.status, "right": right.status}

    if left.total_rounds != right.total_rounds:
        differences["total_rounds"] = {
            "left": left.total_rounds,
            "right": right.total_rounds,
        }

    term_differences: dict[str, dict[str, Any]] = {}
    all_terms = sorted(set(left.accepted_terms) | set(right.accepted_terms))
    for term in all_terms:
        left_value = left.accepted_terms.get(term)
        right_value = right.accepted_terms.get(term)
        if not values_equal(left_value, right_value):
            term_differences[term] = {"left": left_value, "right": right_value}

    if term_differences:
        differences["accepted_terms"] = term_differences

    return differences


def resolve_repo_path(repo_root: Path, path_value: Path) -> Path:
    if path_value.is_absolute():
        return path_value
    return repo_root / path_value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare negotiation summary files across two output folders."
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Run directory in outputs/<run_name>. If provided, dir-a/dir-b default to condition summary folders.",
    )
    parser.add_argument("--condition-a", type=str, default="no_guard")
    parser.add_argument("--condition-b", type=str, default="full_guard")
    parser.add_argument("--dir-a", type=Path, default=None)
    parser.add_argument("--dir-b", type=Path, default=None)
    parser.add_argument("--start-id", type=int, default=1)
    parser.add_argument("--end-id", type=int, default=20)
    parser.add_argument("--file-prefix", type=str, default="product_")
    parser.add_argument("--file-suffix", type=str, default=".txt")
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path to write a machine-readable comparison report.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]

    if args.start_id > args.end_id:
        raise ValueError("--start-id must be less than or equal to --end-id")

    if args.run_dir is not None:
        run_dir = resolve_repo_path(repo_root, args.run_dir)
        dir_a = (
            resolve_repo_path(repo_root, args.dir_a)
            if args.dir_a
            else run_dir / "processed" / "summaries" / args.condition_a
        )
        dir_b = (
            resolve_repo_path(repo_root, args.dir_b)
            if args.dir_b
            else run_dir / "processed" / "summaries" / args.condition_b
        )
        json_output = (
            resolve_repo_path(repo_root, args.json_output)
            if args.json_output
            else run_dir / "analysis" / "comparison_summary.json"
        )
    else:
        if args.dir_a is None or args.dir_b is None:
            raise ValueError("Provide --run-dir, or explicitly provide both --dir-a and --dir-b.")
        dir_a = resolve_repo_path(repo_root, args.dir_a)
        dir_b = resolve_repo_path(repo_root, args.dir_b)
        json_output = resolve_repo_path(repo_root, args.json_output) if args.json_output else None

    if not dir_a.exists():
        raise FileNotFoundError(f"Missing directory: {dir_a}")
    if not dir_b.exists():
        raise FileNotFoundError(f"Missing directory: {dir_b}")

    missing_in_a: list[str] = []
    missing_in_b: list[str] = []
    parse_errors: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    matches = 0

    for product_id in range(args.start_id, args.end_id + 1):
        file_name = f"{args.file_prefix}{product_id}{args.file_suffix}"
        path_a = dir_a / file_name
        path_b = dir_b / file_name

        if not path_a.exists():
            missing_in_a.append(file_name)
        if not path_b.exists():
            missing_in_b.append(file_name)
        if not path_a.exists() or not path_b.exists():
            continue

        try:
            summary_a = parse_summary(path_a)
            summary_b = parse_summary(path_b)
        except Exception as exc:
            parse_errors.append(
                {
                    "file": file_name,
                    "error": str(exc),
                }
            )
            continue

        differences = compare_summaries(summary_a, summary_b)
        if differences:
            mismatches.append(
                {
                    "file": file_name,
                    "left": asdict(summary_a),
                    "right": asdict(summary_b),
                    "differences": differences,
                }
            )
        else:
            matches += 1

    total_requested = args.end_id - args.start_id + 1
    report = {
        "range": {"start_id": args.start_id, "end_id": args.end_id},
        "dir_a": str(dir_a),
        "dir_b": str(dir_b),
        "total_requested": total_requested,
        "matches": matches,
        "mismatches": mismatches,
        "missing_in_a": missing_in_a,
        "missing_in_b": missing_in_b,
        "parse_errors": parse_errors,
    }

    print("=== Comparison Report ===")
    print(f"Directory A: {dir_a}")
    print(f"Directory B: {dir_b}")
    print(f"Products checked: {args.start_id} to {args.end_id} (count={total_requested})")
    print(f"Exact matches: {matches}")
    print(f"Mismatches: {len(mismatches)}")
    print(f"Missing in A: {len(missing_in_a)}")
    print(f"Missing in B: {len(missing_in_b)}")
    print(f"Parse errors: {len(parse_errors)}")

    if missing_in_a:
        print("\nMissing files in directory A:")
        for file_name in missing_in_a:
            print(f"  - {file_name}")

    if missing_in_b:
        print("\nMissing files in directory B:")
        for file_name in missing_in_b:
            print(f"  - {file_name}")

    if parse_errors:
        print("\nFiles with parse errors:")
        for item in parse_errors:
            print(f"  - {item['file']}: {item['error']}")

    if mismatches:
        print("\nPer-file differences:")
        for item in mismatches:
            print(f"  - {item['file']}")
            differences = item["differences"]

            if "status" in differences:
                status_diff = differences["status"]
                print(f"      status: A='{status_diff['left']}' B='{status_diff['right']}'")

            if "total_rounds" in differences:
                rounds_diff = differences["total_rounds"]
                print(f"      total_rounds: A={rounds_diff['left']} B={rounds_diff['right']}")

            if "accepted_terms" in differences:
                print("      accepted_terms:")
                for term_name, term_diff in differences["accepted_terms"].items():
                    print(f"        - {term_name}: A={term_diff['left']} B={term_diff['right']}")

    if json_output:
        json_output.parent.mkdir(parents=True, exist_ok=True)
        json_output.write_text(
            json.dumps(report, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )
        print(f"\nJSON report written to {json_output}")

    has_issues = bool(mismatches or missing_in_a or missing_in_b or parse_errors)
    return 1 if has_issues else 0


if __name__ == "__main__":
    raise SystemExit(main())