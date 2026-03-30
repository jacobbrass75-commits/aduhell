#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from adu_demo_support import (
    DEFAULT_GEOJSON,
    DEFAULT_GEOCODED_CSV,
    DEFAULT_GEOCODE_CACHE,
    DEFAULT_RANKED_CSV,
    ensure_map_outputs,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Geocode ranked LA City ADU candidates for the local demo map.")
    parser.add_argument("--input", type=Path, default=DEFAULT_RANKED_CSV, help="Path to the ranked shortlist CSV.")
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_GEOCODED_CSV, help="Path to the geocoded CSV output.")
    parser.add_argument("--output-geojson", type=Path, default=DEFAULT_GEOJSON, help="Path to the GeoJSON output.")
    parser.add_argument("--cache", type=Path, default=DEFAULT_GEOCODE_CACHE, help="Path to the geocode cache JSON.")
    parser.add_argument(
        "--refresh-missing",
        action="store_true",
        help="Re-query addresses that are missing or failed in the current cache.",
    )
    args = parser.parse_args()

    result = ensure_map_outputs(
        ranked_csv=args.input,
        geocoded_csv=args.output_csv,
        geojson_path=args.output_geojson,
        cache_path=args.cache,
        refresh_missing=args.refresh_missing,
    )

    print(f"Ranked CSV: {result['ranked_csv']}")
    print(f"Geocoded CSV: {result['geocoded_csv']}")
    print(f"GeoJSON: {result['geojson_path']}")
    print(f"Cache JSON: {result['cache_path']}")
    print(f"Total rows: {result['total_rows']}")
    print(f"Matched rows: {result['matched_rows']}")


if __name__ == "__main__":
    main()
