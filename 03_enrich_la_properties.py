#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from adu_pipeline import build_enriched_row, build_summary, load_ruleset


DEFAULT_INPUT = Path("/Users/brass/Downloads/Export-20260328-221034.csv")
DEFAULT_RULESET = Path("data/rulesets/la_city.json")
DEFAULT_OUTPUT_DIR = Path("data/enriched")
SUMMARY_KEYS = [
    "selected_adu_path_for_marketing",
    "analysis_confidence",
    "review_status",
    "parcel_geometry_status",
    "overlay_join_status",
    "transit_join_status",
    "garage_evidence_status",
    "client_safe_image_available",
    "research_imagery_available",
    "render_ready",
    "marketing_ready",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an enriched LA City ADU marketing dataset from a property CSV.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to the property CSV.")
    parser.add_argument("--ruleset", type=Path, default=DEFAULT_RULESET, help="Path to the LA City ruleset JSON.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for enriched outputs.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    ruleset = load_ruleset(args.ruleset)

    with args.input.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        original_fields = reader.fieldnames or []
        original_rows = list(reader)

    enrichment_fields = list(build_enriched_row({}, ruleset).keys())
    enriched_rows = []
    for row in original_rows:
        enriched = build_enriched_row(row, ruleset)
        enriched_rows.append({**row, **enriched})

    output_csv = args.output_dir / f"{args.input.stem}_la_city_enriched.csv"
    output_summary = args.output_dir / f"{args.input.stem}_la_city_enriched_summary.json"

    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=original_fields + enrichment_fields)
        writer.writeheader()
        writer.writerows(enriched_rows)

    summary = build_summary(
        [{key: row.get(key, "") for key in enrichment_fields} for row in enriched_rows],
        SUMMARY_KEYS,
    )
    output_summary.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"Input rows: {len(original_rows)}")
    print(f"Enriched CSV: {output_csv}")
    print(f"Summary JSON: {output_summary}")
    print()
    for key, counts in summary["breakdowns"].items():
        print(key)
        for status, count in sorted(counts.items()):
            print(f"  {status}: {count}")
        print()


if __name__ == "__main__":
    main()
