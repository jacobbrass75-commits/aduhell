#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


DEFAULT_INPUT = Path("/Users/brass/Downloads/Export-20260328-221034.csv")
DEFAULT_RULESET = Path("data/rulesets/la_city.json")


def run_step(command: list[str]) -> None:
    print("Running:", " ".join(command))
    subprocess.run(command, check=True)
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the LA City ADU analysis, enrichment, and marketing package pipeline.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to the source property CSV.")
    parser.add_argument("--ruleset", type=Path, default=DEFAULT_RULESET, help="Path to the LA City ruleset JSON.")
    args = parser.parse_args()

    python = sys.executable
    stem = args.input.stem

    run_step([python, "02_apply_la_adu_analysis.py", "--input", str(args.input), "--ruleset", str(args.ruleset)])
    run_step([python, "03_enrich_la_properties.py", "--input", str(args.input), "--ruleset", str(args.ruleset)])
    run_step([python, "04_generate_property_marketing_packages.py", "--input", f"data/enriched/{stem}_la_city_enriched.csv"])


if __name__ == "__main__":
    main()
