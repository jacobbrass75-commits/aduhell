#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from adu_pipeline import (
    CLAUDE_TONE_PROMPT,
    CLIENT_SAFE_IMAGERY_STRATEGY,
    CONCEPT_RENDER_DISCLAIMER,
    COPY_CONSTRAINTS,
    IMAGERY_STRATEGY,
    canonical_address,
    property_id_from_row,
    semicolon_split,
    split_notes,
)


DEFAULT_SOURCE_STEM = "Export-20260328-221034"
DEFAULT_INPUT = Path(f"data/enriched/{DEFAULT_SOURCE_STEM}_la_city_enriched.csv")
DEFAULT_OUTPUT_DIR = Path("data/briefs")
ONE_PAGER_TEMPLATE = Path("templates/claude_one_pager_template.md")
PITCH_DECK_TEMPLATE = Path("templates/claude_pitch_deck_template.md")


def load_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def make_render_prompt(row: dict[str, str]) -> str:
    address = row.get("normalized_address") or canonical_address(row)
    selected_path = row.get("selected_adu_path_for_marketing") or row.get("recommended_primary_adu_path") or "manual_review"
    style_hint = row.get("concept_render_style_hint") or "Use a neighborhood-compatible Los Angeles ADU concept."
    program_summary = row.get("concept_render_program_summary") or "ADU options require manual review."
    zoning = row.get("zoning_code") or row.get("Zoning") or "unknown"
    lot_sqft = row.get("lot_sqft_numeric") or row.get("Lot SqFt") or "unknown"
    primary_sqft = row.get("primary_sqft_numeric") or row.get("Sq Ft") or "unknown"
    year_built = row.get("year_built_numeric") or row.get("Yr Built") or "unknown"

    return "\n".join(
        [
            f"Project: Conceptual ADU marketing render for {address}",
            "",
            f"Primary concept to emphasize: {selected_path}",
            f"Program summary: {program_summary}",
            f"Existing property facts: zoning {zoning}, lot approximately {lot_sqft} sf, primary residence approximately {primary_sqft} sf, year built {year_built}.",
            f"Style guidance: {style_hint}",
            "",
            "Rendering requirements:",
            "- Show a realistic Los Angeles residential setting and massing that feels plausible for an ADU concept.",
            "- Keep the composition grounded and restrained rather than flashy or futuristic.",
            "- Do not imply permit approval, exact lot placement, exact setbacks, or guaranteed feasibility.",
            "- Use the existing residence only as contextual inspiration, not as an exact architectural survey.",
            "- Prefer one clear concept image over multiple speculative variants.",
            "",
            f"Required disclaimer: {CONCEPT_RENDER_DISCLAIMER}",
        ]
    )


def build_asset_manifest(row: dict[str, str]) -> dict:
    address = row.get("normalized_address") or canonical_address(row)
    google_maps_url = row.get("google_maps_search_url") or ""
    google_satellite_url = row.get("google_maps_satellite_research_url") or google_maps_url

    return {
        "property_id": row.get("property_id") or property_id_from_row(row),
        "imagery_strategy": IMAGERY_STRATEGY,
        "client_safe_imagery_strategy": CLIENT_SAFE_IMAGERY_STRATEGY,
        "approval_state": {
            "review_required": row.get("review_required", "yes"),
            "review_status": row.get("review_status", "pending_human_review"),
            "client_safe_image_available": row.get("client_safe_image_available", "no"),
            "research_imagery_available": row.get("research_imagery_available", "no"),
            "render_ready": row.get("render_ready", "no"),
            "marketing_ready": row.get("marketing_ready", "no"),
        },
        "research_only": [
            {
                "id": "google_maps_search",
                "type": "link",
                "url": google_maps_url,
                "label": "Google Maps property search",
                "approved_for_client_use": False,
                "notes": "Internal research only. Use to review frontage, access, and neighborhood context.",
            },
            {
                "id": "google_maps_satellite",
                "type": "link",
                "url": google_satellite_url,
                "label": "Google Maps / Satellite review entry point",
                "approved_for_client_use": False,
                "notes": "Internal research only. Manually switch to satellite / aerial view as needed.",
            },
            {
                "id": "zimas_manual_lookup",
                "type": "link",
                "url": "https://zimas.lacity.org/",
                "label": "ZIMAS manual lookup",
                "approved_for_client_use": False,
                "notes": f"Use internal review to confirm zoning and overlays for {address}.",
            },
        ],
        "client_safe": {
            "approved_assets": [],
            "recommended_assets_to_collect": [
                "Owned exterior property photo",
                "Public/open aerial or GIS parcel-context export with reuse rights verified",
                "Generated parcel or massing diagram based on approved source data",
            ],
            "notes": "Do not rely on Google-owned imagery as the final client-facing asset by default.",
        },
        "ai_mockup_inputs": {
            "selected_path": row.get("selected_adu_path_for_marketing", ""),
            "program_summary": row.get("concept_render_program_summary", ""),
            "style_hint": row.get("concept_render_style_hint", ""),
            "disclaimer": row.get("concept_render_disclaimer", CONCEPT_RENDER_DISCLAIMER),
            "prompt_file": "render_prompt.txt",
        },
    }


def build_property_brief(row: dict[str, str], one_pager_template: str, pitch_deck_template: str) -> dict:
    approved_claims = semicolon_split(row.get("approved_claims_summary"))
    review_warnings = semicolon_split(row.get("review_warnings_summary"))
    allowed_adu_types = semicolon_split(row.get("allowed_adu_types"))
    constraints = semicolon_split(row.get("rule_based_constraints"))

    if not approved_claims:
        approved_claims = [
            "No client-facing ADU opportunity claim is approved until human review confirms jurisdiction, scope, and messaging.",
        ]

    facts = {
        "apn": row.get("normalized_apn") or row.get("APN"),
        "address": row.get("normalized_address") or canonical_address(row),
        "county": row.get("normalized_county") or row.get("County"),
        "zoning": row.get("zoning_code") or row.get("Zoning"),
        "property_type": row.get("property_type_normalized") or row.get("Type"),
        "lot_sqft": row.get("lot_sqft_numeric") or row.get("Lot SqFt"),
        "primary_sqft": row.get("primary_sqft_numeric") or row.get("Sq Ft"),
        "units": row.get("units_numeric") or row.get("Units"),
        "year_built": row.get("year_built_numeric") or row.get("Yr Built"),
        "estimated_value": row.get("Est Value"),
        "recommended_primary_adu_path": row.get("recommended_primary_adu_path"),
        "analysis_confidence": row.get("analysis_confidence"),
    }

    return {
        "property_id": row.get("property_id") or property_id_from_row(row),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "workflow": {
            "analysis_scope": row.get("analysis_scope"),
            "analysis_mode": row.get("analysis_mode"),
            "pipeline_version": row.get("pipeline_version"),
            "imagery_strategy": IMAGERY_STRATEGY,
            "review_required": row.get("review_required"),
            "review_status": row.get("review_status"),
            "render_ready": row.get("render_ready"),
            "marketing_ready": row.get("marketing_ready"),
        },
        "subject_property": facts,
        "adu_analysis": {
            "allowed_adu_types": allowed_adu_types,
            "recommended_primary_adu_path": row.get("recommended_primary_adu_path"),
            "approved_claims": approved_claims,
            "constraints": constraints,
            "review_warnings": review_warnings,
            "analysis_notes": split_notes(row.get("analysis_notes")),
            "missing_data_reasons": split_notes(row.get("missing_data_reasons")),
            "detached_adu": {
                "status": row.get("detached_adu_status"),
                "max_sqft": row.get("detached_adu_max_sqft"),
                "open_area_proxy_sqft": row.get("detached_adu_open_area_proxy_sqft"),
            },
            "attached_adu": {
                "status": row.get("attached_adu_status"),
                "best_effort_min_sqft": row.get("attached_adu_best_effort_min_sqft"),
                "best_effort_max_sqft": row.get("attached_adu_best_effort_max_sqft"),
                "rule_summary": row.get("attached_adu_rule_summary"),
            },
            "jadu": {
                "status": row.get("jadu_status"),
                "max_sqft": row.get("jadu_max_sqft"),
            },
            "garage_conversion_adu": {
                "status": row.get("garage_conversion_adu_status"),
            },
        },
        "imagery": {
            "selected_client_safe_assets": [],
            "approved_research_references": [],
            "research_only_links": [
                row.get("google_maps_search_url", ""),
                row.get("google_maps_satellite_research_url", ""),
                "https://zimas.lacity.org/",
            ],
            "concept_render": {
                "style_hint": row.get("concept_render_style_hint"),
                "program_summary": row.get("concept_render_program_summary"),
                "disclaimer": row.get("concept_render_disclaimer"),
            },
        },
        "claude_handoff": {
            "tone_prompt": CLAUDE_TONE_PROMPT,
            "copy_constraints": COPY_CONSTRAINTS,
            "deliverables": {
                "one_pager": {
                    "template_markdown": one_pager_template,
                    "goal": "Produce a concise homeowner mailer / one-sheet from only the approved facts and claims in this brief.",
                },
                "pitch_deck": {
                    "template_markdown": pitch_deck_template,
                    "goal": "Produce a homeowner-facing pitch deck narrative from only the approved facts and claims in this brief.",
                },
            },
        },
    }


def render_brief_markdown(brief: dict) -> str:
    facts = brief["subject_property"]
    analysis = brief["adu_analysis"]
    handoff = brief["claude_handoff"]

    lines = [
        f"# Property Brief: {facts['address']}",
        "",
        "## Property Facts",
        f"- APN: {facts['apn']}",
        f"- County: {facts['county']}",
        f"- Zoning: {facts['zoning']}",
        f"- Property Type: {facts['property_type']}",
        f"- Lot SqFt: {facts['lot_sqft']}",
        f"- Primary SqFt: {facts['primary_sqft']}",
        f"- Units: {facts['units']}",
        f"- Year Built: {facts['year_built']}",
        f"- Estimated Value: {facts['estimated_value']}",
        "",
        "## Likely ADU Paths",
        f"- Allowed ADU types: {', '.join(analysis['allowed_adu_types']) or 'none approved'}",
        f"- Recommended primary path: {analysis['recommended_primary_adu_path']}",
        f"- Analysis confidence: {facts['analysis_confidence']}",
        "",
        "## Approved Claims",
    ]
    lines.extend([f"- {claim}" for claim in analysis["approved_claims"]])
    lines.append("")
    lines.append("## Constraints")
    lines.extend([f"- {constraint}" for constraint in analysis["constraints"]])
    lines.append("")
    lines.append("## Review Warnings")
    lines.extend([f"- {warning}" for warning in analysis["review_warnings"]])
    lines.append("")
    lines.append("## Claude Handoff")
    lines.append(f"- Tone prompt: {handoff['tone_prompt']}")
    lines.append("- Copy constraints:")
    lines.extend([f"- {constraint}" for constraint in handoff["copy_constraints"]])
    lines.append("")
    lines.append("## Concept Render")
    lines.append(f"- Style hint: {brief['imagery']['concept_render']['style_hint']}")
    lines.append(f"- Program summary: {brief['imagery']['concept_render']['program_summary']}")
    lines.append(f"- Disclaimer: {brief['imagery']['concept_render']['disclaimer']}")
    lines.append("")
    lines.append("## Deliverables")
    lines.append("- One-pager and pitch deck should both be generated from this brief after human review approval.")
    return "\n".join(lines) + "\n"


def render_review_checklist(row: dict[str, str]) -> str:
    address = row.get("normalized_address") or canonical_address(row)
    allowed_types = semicolon_split(row.get("allowed_adu_types"))

    lines = [
        f"# Review Checklist: {address}",
        "",
        "## Jurisdiction and Scope",
        "- [ ] Confirm the parcel is truly within City of Los Angeles jurisdiction.",
        "- [ ] Confirm the property should remain in the SFR single-family screening model.",
        "",
        "## Claims and ADU Emphasis",
        f"- [ ] Approve or revise the recommended primary ADU path: `{row.get('recommended_primary_adu_path', '')}`.",
        f"- [ ] Approve or revise the allowed ADU types: {', '.join(allowed_types) or 'none listed'}.",
        "- [ ] Decide whether detached, attached, JADU, and garage-conversion claims are acceptable for client-facing use.",
        "",
        "## Imagery",
        "- [ ] Approve research-only image references for internal use.",
        "- [ ] Select client-safe final imagery (owned, public/open, or generated with clear rights).",
        "- [ ] Confirm no Google-owned imagery is being used as final client-facing art without clearance.",
        "",
        "## Render",
        "- [ ] Decide whether to include a concept render.",
        "- [ ] Confirm the render disclaimer will remain visible and clear.",
        "- [ ] Approve the ADU concept path emphasized in the render prompt.",
        "",
        "## Final Output",
        "- [ ] Approve the one-pager generation.",
        "- [ ] Approve the pitch deck generation.",
        "- [ ] Mark the property as marketing ready only after the above items are complete.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate property marketing packages for Claude from an enriched LA CSV.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Path to the enriched property CSV.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for brief outputs.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    one_pager_template = load_template(ONE_PAGER_TEMPLATE)
    pitch_deck_template = load_template(PITCH_DECK_TEMPLATE)

    with args.input.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    index = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_csv": str(args.input),
        "brief_count": len(rows),
        "property_dirs": [],
    }

    for row in rows:
        property_id = row.get("property_id") or property_id_from_row(row)
        property_dir = args.output_dir / property_id
        property_dir.mkdir(parents=True, exist_ok=True)

        brief = build_property_brief(row, one_pager_template, pitch_deck_template)
        asset_manifest = build_asset_manifest(row)
        render_prompt = make_render_prompt(row)
        review_checklist = render_review_checklist(row)
        brief_markdown = render_brief_markdown(brief)

        (property_dir / "property_brief.json").write_text(json.dumps(brief, indent=2), encoding="utf-8")
        (property_dir / "property_brief.md").write_text(brief_markdown, encoding="utf-8")
        (property_dir / "asset_manifest.json").write_text(json.dumps(asset_manifest, indent=2), encoding="utf-8")
        (property_dir / "render_prompt.txt").write_text(render_prompt, encoding="utf-8")
        (property_dir / "review_checklist.md").write_text(review_checklist, encoding="utf-8")

        index["property_dirs"].append(
            {
                "property_id": property_id,
                "dir": str(property_dir),
                "recommended_primary_adu_path": row.get("recommended_primary_adu_path"),
                "review_status": row.get("review_status"),
                "marketing_ready": row.get("marketing_ready"),
            }
        )

    index_path = args.output_dir / "_brief_index.json"
    index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")

    print(f"Input rows: {len(rows)}")
    print(f"Brief output dir: {args.output_dir}")
    print(f"Index file: {index_path}")
    if rows:
        sample_id = rows[0].get("property_id") or property_id_from_row(rows[0])
        print(f"Sample property dir: {args.output_dir / sample_id}")


if __name__ == "__main__":
    main()
