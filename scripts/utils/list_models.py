#!/usr/bin/env python3

import os
from pathlib import Path

from dotenv import load_dotenv
from google import genai


def main() -> int:
    repo_root = Path(__file__).resolve().parents[2]
    load_dotenv(dotenv_path=repo_root / ".env", override=False)

    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise ValueError("Missing GOOGLE_API_KEY (or legacy GEMINI_API_KEY) in environment or .env")

    client = genai.Client(api_key=api_key)

    print("Successfully connected. Available flash/gemma models:")
    for model in client.models.list():
        name = getattr(model, "name", "")
        normalized = name.lower()
        if "flash" in normalized or "gemma" in normalized:
            print(f" - {name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
