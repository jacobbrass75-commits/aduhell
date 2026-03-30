#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from adu_pipeline import build_analysis_row, build_summary, load_ruleset


DEFAULT_INPUT = Path("/Users/brass/Downloads/Export-20260328-221034.csv")
DEFAULT_RULESET = Path("data/rulesets/la_city.json")
DEFAULT_OUTPUT_DIR = Path("data/analysis")
SUMMARY_KEYS = [
    "recommended_primary_adu_path",
    "attached_adu_status",
    "detached_adu_status",
    "jadu_status",
    "garage_conversion_adu_status",
    "analysis_confidence",
    "city_rules_applied",
    "review_status",
    "marketing_ready",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply a best-effort LA City ADU screen to a property CSV.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to the property CSV.")
    parser.add_argument("--ruleset", type=Path, default=DEFAULT_RULESET, help="Path to the LA City ruleset JSON.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for analysis outputs.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    ruleset = load_ruleset(args.ruleset)

    with args.input.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        original_fields = reader.fieldnames or []
        original_rows = list(reader)

    analysis_fields = list(build_analysis_row({}, ruleset).keys())
    analyzed_rows = []
    for row in original_rows:
        analysis = build_analysis_row(row, ruleset)
        analyzed_rows.append({**row, **analysis})

    output_csv = args.output_dir / f"{args.input.stem}_la_city_best_effort_analysis.csv"
    output_summary = args.output_dir / f"{args.input.stem}_la_city_best_effort_summary.json"

    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=original_fields + analysis_fields)
        writer.writeheader()
        writer.writerows(analyzed_rows)

    summary = build_summary(
        [{key: row.get(key, "") for key in analysis_fields} for row in analyzed_rows],
        SUMMARY_KEYS,
    )
    output_summary.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Input rows: {len(original_rows)}")
    print(f"Analysis CSV: {output_csv}")
    print(f"Summary JSON: {output_summary}")
    print()
    for key, counts in summary["breakdowns"].items():
        print(key)
        for status, count in sorted(counts.items()):
            print(f"  {status}: {count}")
        print()


if __name__ == "__main__":
    main()
