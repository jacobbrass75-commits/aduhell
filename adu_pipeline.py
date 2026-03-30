#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus


PIPELINE_VERSION = "la_city_marketing_pipeline_v1"
ANALYSIS_MODE = "best_effort_v1"
ANALYSIS_SCOPE = "la_city_only"
IMAGERY_STRATEGY = "hybrid_public_or_owned_for_client_google_for_internal_research"
CLIENT_SAFE_IMAGERY_STRATEGY = "public_open_or_owned_only"
CONCEPT_RENDER_DISCLAIMER = (
    "Conceptual / illustrative only. This is not an entitled design, permitted plan, "
    "construction drawing, or guaranteed development outcome."
)
CLAUDE_TONE_PROMPT = (
    "Write in a clear, warm, high-trust sales tone for homeowners. Be opportunity-oriented "
    "without hype. Never overclaim entitlement, cost, size, or buildability. Keep all "
    "render descriptions clearly labeled as conceptual and illustrative."
)
COPY_CONSTRAINTS = [
    "Use only the approved facts in this brief.",
    "Do not claim that the parcel is fully feasible without further diligence.",
    "Label all AI visuals as conceptual / illustrative only.",
    "Do not present transit incentives, overlays, or garage conversions as confirmed unless marked approved.",
    "End with a next-step CTA focused on property-specific feasibility review.",
]


def parse_number(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = "".join(ch for ch in value if ch.isdigit())
    if not cleaned:
        return None
    return int(cleaned)


def clean_text(value: str | None) -> str:
    return (value or "").strip()


def join_notes(parts: list[str]) -> str:
    return " | ".join(part for part in parts if part)


def split_notes(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split("|") if item.strip()]


def semicolon_join(items: list[str]) -> str:
    return "; ".join(item for item in items if item)


def semicolon_split(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(";") if item.strip()]


def load_ruleset(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_apn(value: str | None) -> str:
    raw = clean_text(value)
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 10:
        return f"{digits[:4]}-{digits[4:7]}-{digits[7:]}"
    return raw


def slugify(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "unknown-property"


def property_id_from_row(row: dict[str, str]) -> str:
    apn = normalize_apn(row.get("APN"))
    if apn:
        return apn.replace("-", "")
    address = clean_text(row.get("Address"))
    return slugify(address)


def canonical_address(row: dict[str, str]) -> str:
    parts = [
        clean_text(row.get("Address")),
        clean_text(row.get("City")),
        clean_text(row.get("State")) or "CA",
        clean_text(row.get("ZIP")),
    ]
    return ", ".join(part for part in parts if part)


def google_maps_search_url(row: dict[str, str]) -> str:
    query = canonical_address(row) or normalize_apn(row.get("APN"))
    if not query:
        return ""
    return f"https://www.google.com/maps/search/?api=1&query={quote_plus(query)}"


def infer_style_hint(year_built: int | None, primary_sqft: int | None) -> str:
    if year_built is None:
        return "Use a restrained Southern California residential infill style that complements the existing home."
    if year_built <= 1939:
        return (
            "Reference early Los Angeles residential character such as bungalow, Spanish revival, or simple cottage forms, "
            "but keep the ADU clean and contemporary enough to read as a respectful addition."
        )
    if year_built <= 1969:
        return (
            "Reference a modest mid-century Los Angeles infill aesthetic with simple stucco volumes, warm wood accents, "
            "and restrained rooflines."
        )
    if year_built <= 1999:
        return (
            "Use a practical late-century Southern California accessory dwelling style with simple massing and durable materials."
        )
    if primary_sqft is not None and primary_sqft >= 2500:
        return "Use a contemporary infill style scaled to a larger existing residence, with clean lines and understated materials."
    return "Use a contemporary but neighborhood-compatible Los Angeles ADU concept with simple massing and realistic proportions."


def status_for_detached(in_scope: bool, sfr_single: bool, lot_sqft: int | None, open_area_proxy_sqft: int | None) -> str:
    if not in_scope:
        return "out_of_scope"
    if not sfr_single:
        return "not_modeled"
    if lot_sqft is not None and open_area_proxy_sqft is not None:
        if lot_sqft >= 5000 and open_area_proxy_sqft >= 1200:
            return "strong_candidate"
    return "candidate"


def status_for_attached(in_scope: bool, sfr_single: bool) -> str:
    if not in_scope:
        return "out_of_scope"
    if not sfr_single:
        return "not_modeled"
    return "strong_candidate"


def status_for_jadu(in_scope: bool, sfr_single: bool, primary_sqft: int | None) -> str:
    if not in_scope:
        return "out_of_scope"
    if not sfr_single:
        return "not_modeled"
    if primary_sqft is not None and primary_sqft < 500:
        return "unlikely"
    if primary_sqft is not None and primary_sqft >= 1000:
        return "strong_candidate"
    return "candidate"


def status_for_garage_conversion(in_scope: bool, sfr_single: bool, year_built: int | None) -> str:
    if not in_scope:
        return "out_of_scope"
    if not sfr_single:
        return "not_modeled"
    if year_built is not None and year_built <= 1980:
        return "candidate"
    return "candidate"


def recommended_path(
    detached_status: str,
    attached_status: str,
    jadu_status: str,
    primary_sqft: int | None,
) -> str:
    if detached_status == "strong_candidate":
        return "detached_adu"
    if attached_status == "strong_candidate" and (primary_sqft or 0) >= 1400:
        return "attached_adu"
    if jadu_status == "strong_candidate":
        return "jadu"
    if attached_status in {"strong_candidate", "candidate"}:
        return "attached_adu"
    if detached_status == "candidate":
        return "detached_adu"
    return "manual_review"


def allowed_adu_types(
    detached_status: str,
    attached_status: str,
    jadu_status: str,
    garage_status: str,
) -> list[str]:
    allowed: list[str] = []
    if attached_status in {"strong_candidate", "candidate"}:
        allowed.append("attached_adu")
    if detached_status in {"strong_candidate", "candidate"}:
        allowed.append("detached_adu")
    if jadu_status in {"strong_candidate", "candidate"}:
        allowed.append("jadu")
    if garage_status in {"strong_candidate", "candidate"}:
        allowed.append("garage_conversion_adu")
    return allowed


def build_rule_constraints(ruleset: dict) -> list[str]:
    constraints = [
        f"Detached ADU best-effort cap: {ruleset.get('max_detached_adu_sqft') or 'unknown'} square feet.",
        "Attached ADU rule: 50% of existing primary dwelling floor area, with State-law minimum allowances up to 850 sf or 1,000 sf depending on bedroom count.",
        f"JADU cap: {ruleset.get('jadu_max_sqft') or 'unknown'} square feet.",
        f"Rear setback baseline: {ruleset.get('rear_setback_ft') or 'unknown'} feet.",
        f"Side setback baseline: {ruleset.get('side_setback_ft') or 'unknown'} feet.",
        "Owner occupancy is not required for ADUs in the current LA City ruleset, but JADUs still require a deed-restriction owner-occupancy condition.",
        "Transit-based parking exemptions and some height incentives may apply, but they are not parcel-verified in this pipeline.",
        "Lot coverage is zone-dependent and not resolved from the PropertyRadar CSV alone.",
    ]
    return constraints


def build_analysis_row(row: dict[str, str], ruleset: dict) -> dict[str, str | int | float | None]:
    state = clean_text(row.get("State")).upper()
    county = clean_text(row.get("County")).upper()
    city = clean_text(row.get("City")).upper()
    property_type = clean_text(row.get("Type")).upper()
    zoning = clean_text(row.get("Zoning")).upper()

    lot_sqft = parse_number(row.get("Lot SqFt"))
    primary_sqft = parse_number(row.get("Sq Ft"))
    year_built = parse_number(row.get("Yr Built"))
    units = parse_number(row.get("Units"))

    in_scope = state == "CA" and county == "LOS ANGELES" and city == "LOS ANGELES"
    sfr_single = property_type == "SFR" and units == 1

    open_area_proxy_sqft = None
    building_coverage_proxy_pct = None
    if lot_sqft is not None and primary_sqft is not None and lot_sqft > 0:
        open_area_proxy_sqft = max(lot_sqft - primary_sqft, 0)
        building_coverage_proxy_pct = round((primary_sqft / lot_sqft) * 100, 1)

    detached_status = status_for_detached(in_scope, sfr_single, lot_sqft, open_area_proxy_sqft)
    attached_status = status_for_attached(in_scope, sfr_single)
    jadu_status = status_for_jadu(in_scope, sfr_single, primary_sqft)
    garage_status = status_for_garage_conversion(in_scope, sfr_single, year_built)

    attached_rule = ruleset.get("max_attached_adu_sqft_rule") or {}
    attached_min_override = (attached_rule.get("state_override_max_sqft") or {}).get("studio_or_one_bedroom")
    attached_max_override = (attached_rule.get("state_override_max_sqft") or {}).get("more_than_one_bedroom")

    attached_50pct_sqft = round(primary_sqft * 0.5) if primary_sqft is not None else None
    attached_best_effort_min_sqft = None
    attached_best_effort_max_sqft = None
    if attached_50pct_sqft is not None and attached_min_override is not None and attached_max_override is not None:
        attached_best_effort_min_sqft = max(attached_50pct_sqft, attached_min_override)
        attached_best_effort_max_sqft = max(attached_50pct_sqft, attached_max_override)

    analysis_notes: list[str] = []
    missing_data: list[str] = []

    if in_scope:
        analysis_notes.append("LA City rules applied using a best-effort screen.")
    else:
        analysis_notes.append(
            "LA City rules were not confidently applied because City of Los Angeles jurisdiction is not confirmed from this CSV alone."
        )

    if sfr_single:
        analysis_notes.append(
            "Property looks like a single-family residence with one unit, which matches the current v1 LA ADU screening model."
        )
    else:
        analysis_notes.append("Property is outside the current v1 single-family screening model.")

    if lot_sqft is None:
        missing_data.append("Missing lot square footage.")
    if primary_sqft is None:
        missing_data.append("Missing primary building square footage.")

    missing_data.extend(
        [
            "No parcel geometry in CSV, so detached fit and exact setbacks are not confirmed.",
            "No building footprint data, so lot coverage is not confirmed.",
            "No garage-specific field, so garage conversion is only screened as a possibility.",
            "No parcel coordinates or transit join, so transit-based incentives are not evaluated per parcel.",
            "No overlay join, so hillside, VHFHSZ, HPOZ, and specific-plan impacts are not evaluated per parcel.",
        ]
    )

    if open_area_proxy_sqft is not None:
        analysis_notes.append(
            "Detached ADU screening uses a conservative open-area proxy of lot square footage minus primary building square footage; this is a proxy, not a footprint measurement."
        )

    if zoning:
        analysis_notes.append(f"Current CSV zoning value retained as reference: {zoning}.")

    if year_built is not None and year_built <= 1980 and garage_status == "candidate":
        analysis_notes.append(
            "Older home vintage may increase the chance of an existing garage, but garage presence is not confirmed from this export."
        )

    confidence = "medium"
    if in_scope and sfr_single and lot_sqft is not None and primary_sqft is not None:
        confidence = "medium_high"
    elif not in_scope or not sfr_single:
        confidence = "low"

    recommended = recommended_path(detached_status, attached_status, jadu_status, primary_sqft)
    allowed = allowed_adu_types(detached_status, attached_status, jadu_status, garage_status)

    research_imagery_available = "yes" if canonical_address(row) else "no"
    review_required = "yes"
    review_status = "pending_human_review"
    client_safe_image_available = "no"
    render_ready = "no"
    marketing_ready = "no"

    return {
        "pipeline_version": PIPELINE_VERSION,
        "analysis_scope": ANALYSIS_SCOPE,
        "analysis_mode": ANALYSIS_MODE,
        "city_rules_applied": "yes" if in_scope else "no",
        "jurisdiction_assumption": "assumed_city_of_los_angeles_from_csv_city_county_state" if in_scope else "not_confirmed",
        "property_model_status": "modeled_sfr_single_family" if sfr_single else "outside_current_v1_model",
        "analysis_confidence": confidence,
        "allowed_adu_types": semicolon_join(allowed),
        "rule_based_constraints": semicolon_join(build_rule_constraints(ruleset)),
        "recommended_primary_adu_path": recommended,
        "attached_adu_status": attached_status,
        "attached_adu_50pct_of_primary_sqft": attached_50pct_sqft,
        "attached_adu_best_effort_min_sqft": attached_best_effort_min_sqft,
        "attached_adu_best_effort_max_sqft": attached_best_effort_max_sqft,
        "attached_adu_rule_summary": "50% of existing dwelling, with State-law allowance up to 850 sf or 1,000 sf depending on bedroom count.",
        "detached_adu_status": detached_status,
        "detached_adu_max_sqft": ruleset.get("max_detached_adu_sqft"),
        "detached_adu_open_area_proxy_sqft": open_area_proxy_sqft,
        "detached_adu_default_height_ft": 16,
        "detached_adu_transit_height_ft": 18,
        "detached_adu_transit_aligned_roof_height_ft": 20,
        "jadu_status": jadu_status,
        "jadu_max_sqft": ruleset.get("jadu_max_sqft"),
        "garage_conversion_adu_status": garage_status,
        "owner_occupancy_required_for_adu": "no" if ruleset.get("owner_occupancy_required") is False else "unknown",
        "transit_parking_exemption_possible": "yes_but_not_evaluated_per_parcel" if ruleset.get("transit_half_mile_parking_exempt") else "unknown",
        "building_coverage_proxy_pct": building_coverage_proxy_pct,
        "max_lot_coverage_pct": ruleset.get("max_lot_coverage_pct"),
        "max_lot_coverage_rule_summary": "Zone-dependent in LA City; not resolved from this CSV alone.",
        "review_required": review_required,
        "review_status": review_status,
        "client_safe_image_available": client_safe_image_available,
        "research_imagery_available": research_imagery_available,
        "render_ready": render_ready,
        "marketing_ready": marketing_ready,
        "analysis_notes": join_notes(analysis_notes),
        "missing_data_reasons": join_notes(missing_data),
    }


def build_enriched_row(row: dict[str, str], ruleset: dict) -> dict[str, str | int | float | None]:
    analysis = build_analysis_row(row, ruleset)
    lot_sqft = parse_number(row.get("Lot SqFt"))
    primary_sqft = parse_number(row.get("Sq Ft"))
    units = parse_number(row.get("Units"))
    year_built = parse_number(row.get("Yr Built"))

    selected_adu_path = clean_text(str(analysis["recommended_primary_adu_path"]))
    attached_min = analysis.get("attached_adu_best_effort_min_sqft")
    attached_max = analysis.get("attached_adu_best_effort_max_sqft")
    detached_max = analysis.get("detached_adu_max_sqft")
    jadu_max = analysis.get("jadu_max_sqft")

    render_program_parts = []
    if "detached_adu" in semicolon_split(str(analysis["allowed_adu_types"])):
        render_program_parts.append(f"Detached ADU concept up to {detached_max or 'unknown'} sf")
    if "attached_adu" in semicolon_split(str(analysis["allowed_adu_types"])):
        render_program_parts.append(
            f"Attached ADU concept in the {attached_min or 'unknown'} to {attached_max or 'unknown'}+ sf best-effort range"
        )
    if "jadu" in semicolon_split(str(analysis["allowed_adu_types"])):
        render_program_parts.append(f"JADU concept up to {jadu_max or 'unknown'} sf")
    if "garage_conversion_adu" in semicolon_split(str(analysis["allowed_adu_types"])):
        render_program_parts.append("Garage conversion ADU concept if an existing qualifying garage is confirmed")

    approved_claims = []
    if clean_text(str(analysis["city_rules_applied"])) == "yes":
        if clean_text(str(analysis["attached_adu_status"])) in {"strong_candidate", "candidate"}:
            approved_claims.append("Attached ADU appears supportable under a best-effort LA City screening, pending site-specific diligence.")
        if clean_text(str(analysis["detached_adu_status"])) == "strong_candidate":
            approved_claims.append("Detached ADU appears to be a strong candidate under this best-effort screen.")
        elif clean_text(str(analysis["detached_adu_status"])) == "candidate":
            approved_claims.append("Detached ADU may be possible, but parcel geometry and setbacks still need verification.")
        if clean_text(str(analysis["jadu_status"])) in {"strong_candidate", "candidate"}:
            approved_claims.append("A JADU path appears possible if the primary residence layout can support it.")
        if clean_text(str(analysis["garage_conversion_adu_status"])) in {"strong_candidate", "candidate"}:
            approved_claims.append("Garage conversion ADU remains a possible path if an eligible garage is confirmed.")

    enrichment = {
        "property_id": property_id_from_row(row),
        "normalized_apn": normalize_apn(row.get("APN")),
        "normalized_address": canonical_address(row),
        "normalized_city": clean_text(row.get("City")).upper(),
        "normalized_state": clean_text(row.get("State")).upper(),
        "normalized_zip": clean_text(row.get("ZIP")),
        "normalized_county": clean_text(row.get("County")).upper(),
        "property_type_normalized": clean_text(row.get("Type")).upper(),
        "zoning_code": clean_text(row.get("Zoning")).upper(),
        "lot_sqft_numeric": lot_sqft,
        "primary_sqft_numeric": primary_sqft,
        "units_numeric": units,
        "year_built_numeric": year_built,
        "jurisdiction_validation_status": "mailing_city_based_assumption" if clean_text(str(analysis["city_rules_applied"])) == "yes" else "not_verified",
        "parcel_geometry_status": "not_joined",
        "building_footprint_status": "not_joined",
        "overlay_join_status": "not_joined",
        "transit_join_status": "not_joined",
        "garage_evidence_status": "not_joined",
        "public_map_export_status": "not_generated",
        "google_maps_search_url": google_maps_search_url(row),
        "google_maps_satellite_research_url": google_maps_search_url(row),
        "imagery_strategy": IMAGERY_STRATEGY,
        "client_safe_imagery_strategy": CLIENT_SAFE_IMAGERY_STRATEGY,
        "selected_adu_path_for_marketing": selected_adu_path,
        "concept_render_style_hint": infer_style_hint(year_built, primary_sqft),
        "concept_render_program_summary": semicolon_join(render_program_parts),
        "concept_render_disclaimer": CONCEPT_RENDER_DISCLAIMER,
        "approved_claims_summary": semicolon_join(approved_claims),
        "review_warnings_summary": semicolon_join(split_notes(str(analysis["missing_data_reasons"]))),
        "brief_created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    return {**analysis, **enrichment}


def analysis_fieldnames() -> list[str]:
    return list(build_analysis_row({}, {}).keys())


def enrichment_fieldnames() -> list[str]:
    return list(build_enriched_row({}, {}).keys())


def build_summary(rows: list[dict[str, str]], keys: list[str]) -> dict:
    breakdowns: dict[str, dict[str, int]] = {}
    for key in keys:
        counter = Counter()
        for row in rows:
            counter[clean_text(str(row.get(key, ""))) or ""] += 1
        breakdowns[key] = dict(counter)
    return {"total_rows": len(rows), "breakdowns": breakdowns}
