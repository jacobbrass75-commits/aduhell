#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from adu_pipeline import clean_text, property_id_from_row


DEFAULT_SOURCE_STEM = "Export-20260328-221034"
DEFAULT_INPUT = Path(f"data/enriched/{DEFAULT_SOURCE_STEM}_la_city_enriched.csv")
DEFAULT_OUTPUT_DIR = Path("data/shortlists")
DEFAULT_BRIEFS_DIR = Path("data/briefs")
DEFAULT_TOP_N = 50
MAX_SHORTLIST_RAW_SCORE = 118


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    text = clean_text(str(value)).replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def status_points(value: str, mapping: dict[str, int]) -> int:
    return mapping.get(clean_text(value), 0)


def owner_type_points(owner_type: str) -> int:
    normalized = clean_text(owner_type).lower()
    if normalized in {"individual", "married", "multi-owner"}:
        return 2
    if normalized == "trust":
        return 1
    if normalized == "corporate":
        return -3
    return 0


def foreclosure_stage_points(stage: str) -> int:
    normalized = clean_text(stage).lower()
    if normalized == "bank owned":
        return -6
    if normalized == "3rd owned":
        return -4
    if normalized == "auction":
        return -2
    return 0


def lot_points(lot_sqft: float | None) -> int:
    if lot_sqft is None:
        return 0
    if lot_sqft >= 15000:
        return 8
    if lot_sqft >= 10000:
        return 7
    if lot_sqft >= 7000:
        return 6
    if lot_sqft >= 5000:
        return 4
    if lot_sqft >= 3000:
        return 2
    return 0


def open_area_points(open_area_sqft: float | None) -> int:
    if open_area_sqft is None:
        return 0
    if open_area_sqft >= 10000:
        return 20
    if open_area_sqft >= 7000:
        return 18
    if open_area_sqft >= 5000:
        return 15
    if open_area_sqft >= 3500:
        return 12
    if open_area_sqft >= 2000:
        return 8
    if open_area_sqft > 0:
        return 3
    return 0


def coverage_points(building_coverage_pct: float | None) -> int:
    if building_coverage_pct is None:
        return 0
    if building_coverage_pct <= 20:
        return 10
    if building_coverage_pct <= 30:
        return 8
    if building_coverage_pct <= 40:
        return 5
    if building_coverage_pct <= 50:
        return 2
    return 0


def shortlist_tier(score: int) -> str:
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 55:
        return "C"
    return "D"


def review_priority(tier: str) -> str:
    if tier == "A":
        return "review_first"
    if tier == "B":
        return "review_soon"
    if tier == "C":
        return "review_if_capacity"
    return "hold"


def build_reason_summary(row: dict[str, str], lot_sqft: float | None, open_area_sqft: float | None) -> str:
    reasons: list[str] = []
    detached_status = clean_text(row.get("detached_adu_status"))
    attached_status = clean_text(row.get("attached_adu_status"))
    jadu_status = clean_text(row.get("jadu_status"))
    confidence = clean_text(row.get("analysis_confidence"))

    if detached_status == "strong_candidate":
        reasons.append("detached ADU screens strong")
    elif detached_status == "candidate":
        reasons.append("detached ADU remains viable")

    if attached_status == "strong_candidate":
        reasons.append("attached ADU screens strong")

    if jadu_status == "strong_candidate":
        reasons.append("JADU path also looks strong")
    elif jadu_status == "candidate":
        reasons.append("JADU path remains viable")

    if lot_sqft is not None:
        reasons.append(f"lot is about {int(round(lot_sqft)):,} sf")
    if open_area_sqft is not None:
        reasons.append(f"open-area proxy is about {int(round(open_area_sqft)):,} sf")
    if confidence:
        reasons.append(f"analysis confidence is {confidence}")

    return "; ".join(reasons)


def build_caution_summary(row: dict[str, str], building_coverage_pct: float | None) -> str:
    cautions: list[str] = []
    if clean_text(row.get("parcel_geometry_status")) != "joined":
        cautions.append("parcel geometry is not joined")
    if clean_text(row.get("building_footprint_status")) != "joined":
        cautions.append("building footprints are not joined")
    if clean_text(row.get("overlay_join_status")) != "joined":
        cautions.append("overlays are not joined")
    if clean_text(row.get("transit_join_status")) != "joined":
        cautions.append("transit is not joined")
    if clean_text(row.get("garage_evidence_status")) != "joined":
        cautions.append("garage evidence is not joined")
    if building_coverage_pct is not None and building_coverage_pct > 50:
        cautions.append("building coverage proxy is relatively high")
    if clean_text(row.get("FCL Stage")).lower() == "bank owned":
        cautions.append("property is already bank owned")
    return "; ".join(cautions)


def score_row(row: dict[str, str], briefs_dir: Path) -> dict[str, str | int]:
    lot_sqft = parse_float(row.get("lot_sqft_numeric") or row.get("Lot SqFt"))
    open_area_sqft = parse_float(row.get("detached_adu_open_area_proxy_sqft"))
    building_coverage_pct = parse_float(row.get("building_coverage_proxy_pct"))

    raw_score = 0
    raw_score += status_points(row.get("city_rules_applied", ""), {"yes": 15})
    raw_score += status_points(row.get("property_model_status", ""), {"modeled_sfr_single_family": 15})
    raw_score += status_points(row.get("analysis_confidence", ""), {"medium_high": 10, "medium": 6})
    raw_score += status_points(row.get("detached_adu_status", ""), {"strong_candidate": 15, "candidate": 8})
    raw_score += status_points(row.get("attached_adu_status", ""), {"strong_candidate": 8, "candidate": 6})
    raw_score += status_points(row.get("jadu_status", ""), {"strong_candidate": 6, "candidate": 3})
    raw_score += status_points(row.get("recommended_primary_adu_path", ""), {"detached_adu": 5, "attached_adu": 3, "jadu": 2})
    raw_score += open_area_points(open_area_sqft)
    raw_score += coverage_points(building_coverage_pct)
    raw_score += lot_points(lot_sqft)
    raw_score += status_points(row.get("Primary Phone 1 Status", ""), {"Active": 4})
    raw_score += owner_type_points(row.get("Owner Type", ""))
    raw_score += foreclosure_stage_points(row.get("FCL Stage", ""))

    if lot_sqft is None:
        raw_score -= 6
    if parse_float(row.get("primary_sqft_numeric") or row.get("Sq Ft")) is None:
        raw_score -= 6

    normalized_score = round(max(raw_score, 0) / MAX_SHORTLIST_RAW_SCORE * 100)
    tier = shortlist_tier(normalized_score)
    property_id = row.get("property_id") or property_id_from_row(row)
    property_dir = briefs_dir / property_id

    return {
        "shortlist_score_raw": raw_score,
        "shortlist_score": normalized_score,
        "shortlist_tier": tier,
        "shortlist_review_priority": review_priority(tier),
        "shortlist_reason_summary": build_reason_summary(row, lot_sqft, open_area_sqft),
        "shortlist_caution_summary": build_caution_summary(row, building_coverage_pct),
        "shortlist_package_dir": str(property_dir),
        "shortlist_property_brief_path": str(property_dir / "property_brief.json"),
        "shortlist_asset_manifest_path": str(property_dir / "asset_manifest.json"),
        "shortlist_render_prompt_path": str(property_dir / "render_prompt.txt"),
    }


def write_markdown(path: Path, ranked_rows: list[dict[str, str]], top_n: int) -> None:
    top_rows = ranked_rows[:top_n]
    lines = [
        "# Ranked LA City Top Candidates",
        "",
        f"Generated at: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Scoring notes:",
        "- Best-effort LA City shortlist based on current ADU screen, lot/open-area proxies, building-coverage proxy, and light outreach/review signals.",
        "- This is still not a parcel-feasibility confirmation. Geometry, overlays, transit, and garage evidence remain unresolved.",
        "",
        f"## Top {len(top_rows)}",
        "",
        "| Rank | Score | Tier | Address | Recommended Path | Lot SqFt | Open-Area Proxy | Reason |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]

    for row in top_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    clean_text(str(row.get("shortlist_rank", ""))),
                    clean_text(str(row.get("shortlist_score", ""))),
                    clean_text(str(row.get("shortlist_tier", ""))),
                    clean_text(str(row.get("normalized_address") or row.get("Address", ""))).replace("|", "/"),
                    clean_text(str(row.get("recommended_primary_adu_path", ""))),
                    clean_text(str(row.get("lot_sqft_numeric") or row.get("Lot SqFt", ""))),
                    clean_text(str(row.get("detached_adu_open_area_proxy_sqft", ""))),
                    clean_text(str(row.get("shortlist_reason_summary", ""))).replace("|", "/"),
                ]
            )
            + " |"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rank LA City ADU candidates from the enriched marketing dataset.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to the enriched property CSV.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for shortlist outputs.")
    parser.add_argument("--briefs-dir", type=Path, default=DEFAULT_BRIEFS_DIR, help="Directory containing per-property briefs.")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N, help="Number of top rows to export into the top-candidates file.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    with args.input.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        original_fields = reader.fieldnames or []
        rows = list(reader)

    shortlist_fields = list(score_row({}, args.briefs_dir).keys())
    ranked_rows = []
    for row in rows:
        shortlist = score_row(row, args.briefs_dir)
        ranked_rows.append({**row, **shortlist})

    ranked_rows.sort(
        key=lambda row: (
            -int(row.get("shortlist_score", 0)),
            -int(parse_float(row.get("detached_adu_open_area_proxy_sqft")) or 0),
            clean_text(row.get("normalized_address") or row.get("Address")),
        )
    )

    for index, row in enumerate(ranked_rows, start=1):
        row["shortlist_rank"] = index
        row["shortlist_in_top_candidates"] = "yes" if index <= args.top_n else "no"

    top_rows = ranked_rows[: args.top_n]
    stem = args.input.stem.replace("_la_city_enriched", "")
    ranked_csv = args.output_dir / f"{stem}_la_city_ranked_candidates.csv"
    top_csv = args.output_dir / f"{stem}_la_city_top_candidates.csv"
    summary_json = args.output_dir / f"{stem}_la_city_top_candidates_summary.json"
    top_markdown = args.output_dir / f"{stem}_la_city_top_candidates.md"

    shortlist_output_fields = original_fields + shortlist_fields + ["shortlist_rank", "shortlist_in_top_candidates"]

    with ranked_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=shortlist_output_fields)
        writer.writeheader()
        writer.writerows(ranked_rows)

    with top_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=shortlist_output_fields)
        writer.writeheader()
        writer.writerows(top_rows)

    tier_counts: dict[str, int] = {}
    recommended_counts: dict[str, int] = {}
    for row in top_rows:
        tier = clean_text(str(row.get("shortlist_tier", ""))) or "unknown"
        recommended = clean_text(str(row.get("recommended_primary_adu_path", ""))) or "unknown"
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        recommended_counts[recommended] = recommended_counts.get(recommended, 0) + 1

    summary = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_csv": str(args.input),
        "ranked_csv": str(ranked_csv),
        "top_candidates_csv": str(top_csv),
        "top_candidates_markdown": str(top_markdown),
        "total_rows_ranked": len(ranked_rows),
        "top_n": args.top_n,
        "top_candidate_tier_breakdown": tier_counts,
        "top_candidate_recommended_path_breakdown": recommended_counts,
        "top_candidates": [
            {
                "shortlist_rank": row.get("shortlist_rank"),
                "property_id": row.get("property_id") or property_id_from_row(row),
                "apn": row.get("normalized_apn") or row.get("APN"),
                "address": row.get("normalized_address") or row.get("Address"),
                "shortlist_score": row.get("shortlist_score"),
                "shortlist_tier": row.get("shortlist_tier"),
                "recommended_primary_adu_path": row.get("recommended_primary_adu_path"),
                "shortlist_reason_summary": row.get("shortlist_reason_summary"),
                "shortlist_caution_summary": row.get("shortlist_caution_summary"),
                "property_brief_path": row.get("shortlist_property_brief_path"),
            }
            for row in top_rows[:20]
        ],
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_markdown(top_markdown, ranked_rows, args.top_n)

    print(f"Input rows ranked: {len(ranked_rows)}")
    print(f"Ranked CSV: {ranked_csv}")
    print(f"Top candidates CSV: {top_csv}")
    print(f"Summary JSON: {summary_json}")
    print(f"Top candidates Markdown: {top_markdown}")
    print()
    print("Top 10 candidates")
    for row in top_rows[:10]:
        print(
            f"  #{row['shortlist_rank']}: {row.get('normalized_address') or row.get('Address')} "
            f"(score {row['shortlist_score']}, path {row.get('recommended_primary_adu_path')})"
        )


if __name__ == "__main__":
    main()
