#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any


def resolve_repo_path(repo_root: Path, path_value: Path) -> Path:
    if path_value.is_absolute():
        return path_value
    return repo_root / path_value


def parse_price(raw_value: Any) -> float:
    if isinstance(raw_value, (int, float)):
        return float(raw_value)

    if not isinstance(raw_value, str):
        raise ValueError(f"Unsupported price value type: {type(raw_value).__name__}")

    normalized = raw_value.strip().replace("$", "").replace(",", "")
    if not normalized:
        raise ValueError("Price string is empty after normalization")

    return float(normalized)


def build_task(product: dict[str, Any], max_rounds_per_agent: int) -> dict[str, Any]:
    product_id = int(product["id"])
    product_name = str(product.get("Product Name", f"Product {product_id}"))
    product_type = str(product.get("Type", "Product"))
    features = str(product.get("Features", ""))
    reference = str(product.get("Reference", ""))

    retail_price = parse_price(product["Retail Price"])
    wholesale_price = parse_price(product["Wholesale Price"])

    if wholesale_price > retail_price:
        raise ValueError(
            f"Product {product_id} has wholesale price greater than retail price: "
            f"{wholesale_price} > {retail_price}"
        )

    spread = retail_price - wholesale_price
    seller_target = round(wholesale_price + (0.8 * spread), 2)
    buyer_target = round(wholesale_price + (0.25 * spread), 2)

    base_task = {
        "task": (
            f"Negotiate a purchase agreement for {product_name} ({product_type}). "
            f"The only negotiated term is unit_price_usd. "
            f"Catalog retail is ${retail_price:.2f} and wholesale is ${wholesale_price:.2f}. "
            f"Product features: {features}. Reference: {reference}"
        ),
        "agent_a": {
            "agent_id": "Supplier_Agent",
            "system_goal": (
                f"You are the supplier for {product_name}. Maximize unit_price_usd while preserving "
                "margin over wholesale cost and closing a deal quickly."
            ),
            "max_rounds": max_rounds_per_agent,
            "draft_models": [ "gemini-3.1-flash-lite-preview", "gemma-3-27b-it" ],
            "repair_models": [ "gemini-3-flash-preview", "gemma-4-31b-it" ],
            "final_models": [ "gemma-4-31b-it" ],
            "parallel_drafts": False,
            "hard_constraints": {
                "unit_price_usd": [round(wholesale_price, 2), None]
            },
            "target_goals": {
                "unit_price_usd": seller_target
            }
        },
        "agent_b": {
            "agent_id": "Buyer_Agent",
            "system_goal": (
                f"You are buying {product_name}. Minimize unit_price_usd while staying within budget "
                "and trying to close at a realistic market price."
            ),
            "max_rounds": max_rounds_per_agent,
            "draft_models": [ "gemini-3.1-flash-lite-preview", "gemma-3-27b-it" ],
            "repair_models": [ "gemini-3-flash-preview", "gemma-4-31b-it" ],
            "final_models": [ "gemma-4-31b-it" ],
            "parallel_drafts": False,
            "hard_constraints": {
                "unit_price_usd": [0.0, round(retail_price, 2)]
            },
            "target_goals": {
                "unit_price_usd": buyer_target
            }
        },
        "max_retries_per_turn": 2
    }

    return base_task


def load_products(products_file: Path) -> list[dict[str, Any]]:
    with products_file.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("products file must contain a JSON array")

    products: list[dict[str, Any]] = []
    for idx, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Product at index {idx} is not a JSON object")
        for required_field in ("id", "Product Name", "Retail Price", "Wholesale Price"):
            if required_field not in item:
                raise ValueError(f"Product at index {idx} is missing required field '{required_field}'")
        products.append(item)

    return products


def write_tasks(
    products: list[dict[str, Any]],
    output_dir: Path,
    prefix: str,
    suffix: str,
    max_rounds_per_agent: int,
    overwrite: bool,
) -> tuple[int, int]:
    output_dir.mkdir(parents=True, exist_ok=True)

    created = 0
    skipped = 0

    sorted_products = sorted(products, key=lambda p: int(p["id"]))
    for product in sorted_products:
        product_id = int(product["id"])
        output_file = output_dir / f"{prefix}{product_id}{suffix}"

        if output_file.exists() and not overwrite:
            skipped += 1
            continue

        task_payload = build_task(product, max_rounds_per_agent=max_rounds_per_agent)
        output_file.write_text(json.dumps(task_payload, indent=4, ensure_ascii=True) + "\n", encoding="utf-8")
        created += 1

    return created, skipped


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate per-product negotiation task files from products.json"
    )
    parser.add_argument(
        "--products-file",
        type=Path,
        default=Path("data/inputs/products.json"),
        help="Path to input products JSON file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/inputs/tasks"),
        help="Directory where generated task files will be written.",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="product_",
        help="Output filename prefix.",
    )
    parser.add_argument(
        "--suffix",
        type=str,
        default=".json",
        help="Output filename suffix.",
    )
    parser.add_argument(
        "--max-rounds-per-agent",
        type=int,
        default=4,
        help="Max rounds per agent for generated tasks.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing task files.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]

    products_file = resolve_repo_path(repo_root, args.products_file)
    output_dir = resolve_repo_path(repo_root, args.output_dir)
    if not products_file.exists():
        raise FileNotFoundError(f"Products file not found: {products_file}")

    products = load_products(products_file)
    created, skipped = write_tasks(
        products=products,
        output_dir=output_dir,
        prefix=args.prefix,
        suffix=args.suffix,
        max_rounds_per_agent=max(4, int(args.max_rounds_per_agent)),
        overwrite=bool(args.overwrite),
    )

    print(
        f"Generated tasks in '{output_dir}': created={created}, skipped={skipped}, total={len(products)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
