import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dotenv import load_dotenv
from loguru import logger

from engine_guard.agents.base import AgentConfig, BaseAgent
from engine_guard.core.engine import NegotiationEngine
from engine_guard.guards.validators import ProgrammaticGuard


def _parse_constraints(raw_constraints: Any) -> dict[str, tuple[float | None, float | None]]:
    if not isinstance(raw_constraints, dict):
        raise ValueError(
            "Constraints must be a JSON object of term -> [min, max], where either value can be null."
        )

    parsed: dict[str, tuple[float | None, float | None]] = {}
    for term_name, bounds in raw_constraints.items():
        if not isinstance(bounds, (list, tuple)) or len(bounds) != 2:
            raise ValueError(
                f"Constraint for '{term_name}' must be a two-item list like [min, max], where either value can be null."
            )

        low_raw, high_raw = bounds
        low = None if low_raw is None else float(low_raw)
        high = None if high_raw is None else float(high_raw)

        if low is None and high is None:
            raise ValueError(
                f"Constraint for '{term_name}' must define at least one bound (min or max)."
            )
        if low is not None and high is not None and low > high:
            raise ValueError(
                f"Constraint for '{term_name}' has invalid bounds: min {low} is greater than max {high}."
            )

        parsed[str(term_name)] = (low, high)

    if not parsed:
        raise ValueError("At least one hard constraint must be defined.")

    return parsed


def _load_task(args: argparse.Namespace) -> dict[str, Any]:
    model_name = args.model or os.environ.get("GEMMA_MODEL", "gemma-3-4b-it")

    task_path = Path(args.task)
    if not task_path.exists():
        raise FileNotFoundError(f"Task file not found: {task_path}")

    with task_path.open("r", encoding="utf-8") as task_file:
        task_data = json.load(task_file)

    if "agent_a" not in task_data or "agent_b" not in task_data:
        raise ValueError("Task file must define both 'agent_a' and 'agent_b'.")
    if "task" not in task_data:
        raise ValueError("Task file must define 'task' description.")

    task_data.setdefault("max_retries_per_turn", 2)

    for agent_key in ("agent_a", "agent_b"):
        agent = task_data[agent_key]
        if "model_name" not in agent:
            agent["model_name"] = model_name

    return task_data


def _build_agent(config_payload: dict[str, Any]) -> BaseAgent:
    hard_constraints = _parse_constraints(config_payload.get("hard_constraints", {}))
    term_names = list(hard_constraints.keys())

    agent_config = AgentConfig(
        agent_id=config_payload["agent_id"],
        system_goal=config_payload["system_goal"],
        model_name=config_payload.get("model_name", "gemma-3-4b-it"),
        draft_models=config_payload.get("draft_models"),
        repair_models=config_payload.get("repair_models"),
        final_models=config_payload.get("final_models"),
        model_cooldown_seconds=config_payload.get("model_cooldown_seconds", 60),
        temperature=config_payload.get("temperature", 0.4),
        allowed_terms=term_names,
        hard_constraints=hard_constraints,
        target_goals=config_payload.get("target_goals"),
        parallel_drafts=config_payload.get("parallel_drafts", False),
        draft_quality_threshold=config_payload.get("draft_quality_threshold", 0.65),
    )
    return BaseAgent(agent_config)


async def _run(args: argparse.Namespace) -> int:
    env_path = Path(args.env_file) if args.env_file else REPO_ROOT / ".env"
    load_dotenv(dotenv_path=env_path, override=False)

    if not (os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")):
        logger.error("Missing GOOGLE_API_KEY (or legacy GEMINI_API_KEY) environment variable.")
        return 1

    task_def = _load_task(args)

    provider = _build_agent(task_def["agent_a"])
    client = _build_agent(task_def["agent_b"])

    guard = None
    if args.disable_guard:
        logger.warning("Programmatic guard is disabled; proposals will bypass safety validation.")
    else:
        guard = ProgrammaticGuard(mode=args.guard_mode)
        logger.info(f"Programmatic guard enabled in '{args.guard_mode}' mode.")
        guard.add_agent_constraints(
            provider.config.agent_id,
            _parse_constraints(task_def["agent_a"]["hard_constraints"]),
        )
        guard.add_agent_constraints(
            client.config.agent_id,
            _parse_constraints(task_def["agent_b"]["hard_constraints"]),
        )

    max_rounds_a = int(task_def["agent_a"].get("max_rounds", args.max_rounds_a))
    max_rounds_b = int(task_def["agent_b"].get("max_rounds", args.max_rounds_b))
    max_retries = int(task_def.get("max_retries_per_turn", args.max_retries_per_turn))

    engine = NegotiationEngine(
        agent_a=provider,
        agent_b=client,
        guard=guard,
        task_description=task_def["task"],
        max_rounds_a=max_rounds_a,
        max_rounds_b=max_rounds_b,
        max_retries_per_turn=max_retries,
    )

    captured_events: list[dict[str, Any]] = []
    should_capture_events = bool(args.events_output)

    async def on_event(event_dict: dict):
        if args.verbose_events:
            print(json.dumps(event_dict, ensure_ascii=True))
        if should_capture_events:
            captured_events.append(event_dict)

    result = await engine.run_negotiation(
        on_event=on_event if (args.verbose_events or should_capture_events) else None
    )
    
    summary_lines = []
    summary_lines.append("=== Negotiation Summary ===")
    summary_lines.append(f"Status: {result}")
    summary_lines.append(f"Total Rounds: {engine.context.round_number}")
    
    if result.startswith("SUCCESS") and engine.context.history:
        last_proposal = engine.context.history[-1]
        terms = engine.context.history[-2].offered_terms if (last_proposal.accept_previous and len(engine.context.history) >= 2) else last_proposal.offered_terms
        
        summary_lines.append("Accepted Terms:")
        if terms:
            for t in terms:
                summary_lines.append(f"  - {t.name}: {t.value}")
        else:
            summary_lines.append("  None")
    
    summary_text = "\n".join(summary_lines)
    print(summary_text)

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(summary_text + "\n", encoding="utf-8")
        logger.info(f"Summary written to {out_path}")

    if args.events_output:
        events_path = Path(args.events_output)
        events_path.parent.mkdir(parents=True, exist_ok=True)
        with events_path.open("w", encoding="utf-8") as events_file:
            for event in captured_events:
                events_file.write(json.dumps(event, ensure_ascii=True) + "\n")
        logger.info(f"Event log written to {events_path}")

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the standalone engine-guard negotiation engine from the command line."
    )
    parser.add_argument(
        "--task",
        type=str,
        required=True,
        help="Path to a JSON task file.",
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default="",
        help="Path to a .env file that defines GOOGLE_API_KEY or GEMINI_API_KEY.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="",
        help="Default model to use when a task file does not set per-agent model_name.",
    )
    parser.add_argument("--max-rounds-a", type=int, default=12)
    parser.add_argument("--max-rounds-b", type=int, default=12)
    parser.add_argument("--max-retries-per-turn", type=int, default=2)
    parser.add_argument(
        "--disable-guard",
        action="store_true",
        help="Disable ProgrammaticGuard safety validation and run negotiation without guard checks.",
    )
    parser.add_argument(
        "--guard-mode",
        type=str,
        default="full",
        choices=["full", "bounds-only"],
        help="Guard validation mode when guard is enabled.",
    )
    parser.add_argument(
        "--verbose-events",
        action="store_true",
        help="Print every engine event as JSON to stdout.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Path to a text file where the final output summary will be written.",
    )
    parser.add_argument(
        "--events-output",
        type=str,
        default="",
        help="Optional path to write per-event logs as NDJSON.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(_run(args))
    except Exception as exc:
        logger.error(f"CLI execution failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
