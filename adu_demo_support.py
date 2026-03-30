#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

from adu_pipeline import clean_text


DEFAULT_SOURCE_STEM = "Export-20260328-221034"
DEFAULT_RANKED_CSV = Path(f"data/shortlists/{DEFAULT_SOURCE_STEM}_la_city_ranked_candidates.csv")
DEFAULT_TOP_CANDIDATES_CSV = Path(f"data/shortlists/{DEFAULT_SOURCE_STEM}_la_city_top_candidates.csv")
DEFAULT_BRIEFS_DIR = Path("data/briefs")
DEFAULT_MAPS_DIR = Path("data/maps")
DEFAULT_GEOCODED_CSV = DEFAULT_MAPS_DIR / f"{DEFAULT_SOURCE_STEM}_la_city_geocoded_candidates.csv"
DEFAULT_GEOJSON = DEFAULT_MAPS_DIR / f"{DEFAULT_SOURCE_STEM}_la_city_property_points.geojson"
DEFAULT_GEOCODE_CACHE = DEFAULT_MAPS_DIR / f"{DEFAULT_SOURCE_STEM}_la_city_geocode_cache.json"
CENSUS_GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
ALLOWED_PACKAGE_FILES = {
    "property_brief.json",
    "property_brief.md",
    "asset_manifest.json",
    "render_prompt.txt",
    "review_checklist.md",
}


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


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_json(path: Path) -> dict[str, Any] | list[Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def property_dir(property_id: str, briefs_dir: Path = DEFAULT_BRIEFS_DIR) -> Path:
    return briefs_dir / clean_text(property_id)


def package_paths(property_id: str, briefs_dir: Path = DEFAULT_BRIEFS_DIR) -> dict[str, Path]:
    base = property_dir(property_id, briefs_dir)
    return {
        "dir": base,
        "property_brief_json": base / "property_brief.json",
        "property_brief_md": base / "property_brief.md",
        "asset_manifest_json": base / "asset_manifest.json",
        "render_prompt_txt": base / "render_prompt.txt",
        "review_checklist_md": base / "review_checklist.md",
    }


@lru_cache(maxsize=4)
def load_ranked_rows(path_str: str = str(DEFAULT_RANKED_CSV)) -> list[dict[str, str]]:
    return load_csv_rows(Path(path_str))


def resolve_property(rows: list[dict[str, str]], query: str) -> dict[str, str] | None:
    needle = clean_text(query).lower()
    if not needle:
        return None

    exact_keys = [
        "property_id",
        "normalized_apn",
        "APN",
        "normalized_address",
        "Address",
    ]
    for row in rows:
        for key in exact_keys:
            if clean_text(row.get(key)).lower() == needle:
                return row

    for row in rows:
        haystack = " ".join(
            [
                clean_text(row.get("property_id")),
                clean_text(row.get("normalized_apn")),
                clean_text(row.get("APN")),
                clean_text(row.get("normalized_address")),
                clean_text(row.get("Address")),
            ]
        ).lower()
        if needle in haystack:
            return row
    return None


def search_properties(rows: list[dict[str, str]], query: str, limit: int = 10) -> list[dict[str, str]]:
    needle = clean_text(query).lower()
    if not needle:
        return rows[:limit]

    scored: list[tuple[int, dict[str, str]]] = []
    for row in rows:
        exact = 0
        partial = 0
        for key in ["property_id", "normalized_apn", "APN", "normalized_address", "Address"]:
            value = clean_text(row.get(key)).lower()
            if not value:
                continue
            if value == needle:
                exact = max(exact, 3)
            elif needle in value:
                partial = max(partial, 1)
        score = exact or partial
        if score:
            scored.append((score, row))

    scored.sort(
        key=lambda item: (
            -item[0],
            -int(clean_text(str(item[1].get("shortlist_score"))) or 0),
            clean_text(item[1].get("normalized_address") or item[1].get("Address")),
        )
    )
    return [row for _, row in scored[:limit]]


def load_property_bundle(property_id: str, briefs_dir: Path = DEFAULT_BRIEFS_DIR) -> dict[str, Any]:
    paths = package_paths(property_id, briefs_dir)
    return {
        "property_id": property_id,
        "paths": {key: str(path) for key, path in paths.items()},
        "property_brief": read_json(paths["property_brief_json"]),
        "asset_manifest": read_json(paths["asset_manifest_json"]),
        "render_prompt": paths["render_prompt_txt"].read_text(encoding="utf-8") if paths["render_prompt_txt"].exists() else "",
        "review_checklist": paths["review_checklist_md"].read_text(encoding="utf-8") if paths["review_checklist_md"].exists() else "",
        "property_brief_markdown": paths["property_brief_md"].read_text(encoding="utf-8") if paths["property_brief_md"].exists() else "",
    }


def build_marketing_prompt(bundle: dict[str, Any], deliverable: str = "one_pager") -> str:
    brief = bundle.get("property_brief") or {}
    subject = brief.get("subject_property") or {}
    analysis = brief.get("adu_analysis") or {}
    handoff = brief.get("claude_handoff") or {}
    deliverables = (handoff.get("deliverables") or {})
    requested = deliverables.get(deliverable) or {}
    approved_claims = analysis.get("approved_claims") or []
    constraints = analysis.get("constraints") or []
    warnings = analysis.get("review_warnings") or []
    copy_constraints = handoff.get("copy_constraints") or []

    lines = [
        f"Create a {deliverable.replace('_', ' ')} for this LA City ADU opportunity.",
        "",
        "Use only the approved facts and claims below.",
        "",
        "Subject property:",
        f"- Address: {subject.get('address', '')}",
        f"- APN: {subject.get('apn', '')}",
        f"- Zoning: {subject.get('zoning', '')}",
        f"- Property type: {subject.get('property_type', '')}",
        f"- Lot size: {subject.get('lot_sqft', '')}",
        f"- Primary home size: {subject.get('primary_sqft', '')}",
        f"- Recommended ADU path: {analysis.get('recommended_primary_adu_path', '')}",
        f"- Analysis confidence: {subject.get('analysis_confidence', '')}",
        "",
        "Approved claims:",
    ]
    lines.extend(f"- {item}" for item in approved_claims)
    lines.append("")
    lines.append("Constraints and caveats:")
    lines.extend(f"- {item}" for item in constraints)
    lines.extend(f"- {item}" for item in warnings)
    lines.append("")
    lines.append("Copy constraints:")
    lines.extend(f"- {item}" for item in copy_constraints)
    lines.append("")
    if requested.get("goal"):
        lines.append(f"Deliverable goal: {requested['goal']}")
        lines.append("")
    if requested.get("template_markdown"):
        lines.append("Template:")
        lines.append(requested["template_markdown"])
        lines.append("")
    lines.append("Keep any render references conceptual and illustrative only.")
    return "\n".join(lines).strip() + "\n"


def compute_demo_summary(rows: list[dict[str, str]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "total_properties": len(rows),
        "geocoded_properties": 0,
        "top_50_count": 0,
        "tier_breakdown": {},
        "recommended_path_breakdown": {},
    }
    for row in rows:
        if parse_float(row.get("latitude")) is not None and parse_float(row.get("longitude")) is not None:
            summary["geocoded_properties"] += 1
        if clean_text(row.get("shortlist_in_top_candidates")) == "yes":
            summary["top_50_count"] += 1
        tier = clean_text(row.get("shortlist_tier")) or "unknown"
        path = clean_text(row.get("recommended_primary_adu_path")) or "unknown"
        summary["tier_breakdown"][tier] = summary["tier_breakdown"].get(tier, 0) + 1
        summary["recommended_path_breakdown"][path] = summary["recommended_path_breakdown"].get(path, 0) + 1
    return summary


def build_geojson(rows: list[dict[str, str]]) -> dict[str, Any]:
    features = []
    for row in rows:
        lat = parse_float(row.get("latitude"))
        lon = parse_float(row.get("longitude"))
        if lat is None or lon is None:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "property_id": row.get("property_id"),
                    "normalized_apn": row.get("normalized_apn"),
                    "normalized_address": row.get("normalized_address"),
                    "shortlist_score": row.get("shortlist_score"),
                    "shortlist_tier": row.get("shortlist_tier"),
                    "recommended_primary_adu_path": row.get("recommended_primary_adu_path"),
                    "shortlist_reason_summary": row.get("shortlist_reason_summary"),
                    "shortlist_caution_summary": row.get("shortlist_caution_summary"),
                    "shortlist_rank": row.get("shortlist_rank"),
                    "shortlist_in_top_candidates": row.get("shortlist_in_top_candidates"),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def load_geocode_cache(path: Path) -> dict[str, dict[str, Any]]:
    payload = read_json(path)
    if isinstance(payload, dict):
        return payload
    return {}


def geocode_address(address: str, session: requests.Session | None = None, retries: int = 3) -> dict[str, Any]:
    if not clean_text(address):
        return {"status": "missing_address"}

    client = session or requests.Session()
    params = {
        "address": address,
        "benchmark": "4",
        "format": "json",
    }
    for attempt in range(1, retries + 1):
        try:
            response = client.get(CENSUS_GEOCODER_URL, params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
            matches = (((payload or {}).get("result") or {}).get("addressMatches") or [])
            if not matches:
                return {"status": "no_match"}
            match = matches[0]
            coordinates = match.get("coordinates") or {}
            return {
                "status": "matched",
                "latitude": coordinates.get("y"),
                "longitude": coordinates.get("x"),
                "matched_address": match.get("matchedAddress"),
            }
        except Exception as exc:  # pragma: no cover - network path
            if attempt == retries:
                return {"status": "error", "error": str(exc)}
            time.sleep(0.5 * attempt)
    return {"status": "error", "error": "unknown"}


def ensure_map_outputs(
    ranked_csv: Path = DEFAULT_RANKED_CSV,
    geocoded_csv: Path = DEFAULT_GEOCODED_CSV,
    geojson_path: Path = DEFAULT_GEOJSON,
    cache_path: Path = DEFAULT_GEOCODE_CACHE,
    refresh_missing: bool = False,
    sleep_sec: float = 0.05,
) -> dict[str, Any]:
    ranked_rows = load_csv_rows(ranked_csv)
    cache = load_geocode_cache(cache_path)
    geocoded_rows: list[dict[str, Any]] = []
    session = requests.Session()
    updated_cache = False

    for row in ranked_rows:
        address = clean_text(row.get("normalized_address") or row.get("Address"))
        cached = cache.get(address)
        if cached and (not refresh_missing or cached.get("status") == "matched"):
            geocode = cached
        else:
            geocode = geocode_address(address, session=session)
            cache[address] = geocode
            updated_cache = True
            time.sleep(sleep_sec)

        geocoded_rows.append(
            {
                **row,
                "latitude": geocode.get("latitude", ""),
                "longitude": geocode.get("longitude", ""),
                "geocoder_status": geocode.get("status", ""),
                "geocoder_matched_address": geocode.get("matched_address", ""),
            }
        )

    geocoded_csv.parent.mkdir(parents=True, exist_ok=True)
    write_csv_rows(geocoded_csv, geocoded_rows)
    write_json(geojson_path, build_geojson(geocoded_rows))
    if updated_cache or not cache_path.exists():
        write_json(cache_path, cache)

    return {
        "ranked_csv": str(ranked_csv),
        "geocoded_csv": str(geocoded_csv),
        "geojson_path": str(geojson_path),
        "cache_path": str(cache_path),
        "total_rows": len(geocoded_rows),
        "matched_rows": sum(1 for row in geocoded_rows if clean_text(str(row.get("geocoder_status"))) == "matched"),
    }
