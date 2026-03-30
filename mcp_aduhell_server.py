#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from adu_demo_support import (
    DEFAULT_BRIEFS_DIR,
    DEFAULT_RANKED_CSV,
    build_marketing_prompt,
    load_property_bundle,
    load_ranked_rows,
    resolve_property,
    search_properties,
)
from adu_pipeline import clean_text


ROOT = Path(os.environ.get("ADUHELL_ROOT", Path(__file__).resolve().parent)).resolve()
RANKED_CSV = ROOT / DEFAULT_RANKED_CSV
BRIEFS_DIR = ROOT / DEFAULT_BRIEFS_DIR
mcp = FastMCP("aduhell")


def ranked_rows() -> list[dict[str, str]]:
    return load_ranked_rows(str(RANKED_CSV))


def row_to_summary(row: dict[str, str]) -> dict[str, Any]:
    return {
        "property_id": row.get("property_id"),
        "apn": row.get("normalized_apn") or row.get("APN"),
        "address": row.get("normalized_address") or row.get("Address"),
        "shortlist_rank": row.get("shortlist_rank"),
        "shortlist_score": row.get("shortlist_score"),
        "shortlist_tier": row.get("shortlist_tier"),
        "recommended_primary_adu_path": row.get("recommended_primary_adu_path"),
        "analysis_confidence": row.get("analysis_confidence"),
        "allowed_adu_types": row.get("allowed_adu_types"),
        "shortlist_reason_summary": row.get("shortlist_reason_summary"),
        "shortlist_caution_summary": row.get("shortlist_caution_summary"),
    }


def lookup_row(query: str) -> dict[str, str] | None:
    return resolve_property(ranked_rows(), query)


@mcp.tool(description="List the top-ranked LA City ADU candidates from the local shortlist.", structured_output=True)
def list_top_candidates(limit: int = 10, tier: str | None = None, recommended_path: str | None = None) -> dict[str, Any]:
    rows = ranked_rows()
    filtered = []
    for row in rows:
        if tier and clean_text(row.get("shortlist_tier")) != clean_text(tier):
            continue
        if recommended_path and clean_text(row.get("recommended_primary_adu_path")) != clean_text(recommended_path):
            continue
        filtered.append(row)
    return {
        "count": min(limit, len(filtered)),
        "candidates": [row_to_summary(row) for row in filtered[:limit]],
    }


@mcp.tool(description="Search LA City ADU shortlist properties by APN, property id, or address.", structured_output=True)
def search_adu_properties(query: str, limit: int = 10) -> dict[str, Any]:
    matches = search_properties(ranked_rows(), query, limit=limit)
    return {
        "query": query,
        "count": len(matches),
        "matches": [row_to_summary(row) for row in matches],
    }


@mcp.tool(description="Get the full local property packet, including the brief, assets, and render prompt.", structured_output=True)
def get_property_packet(query: str) -> dict[str, Any]:
    row = lookup_row(query)
    if not row:
        return {"error": f"No property matched query: {query}"}

    bundle = load_property_bundle(clean_text(row.get("property_id")), BRIEFS_DIR)
    return {
        "property": row_to_summary(row),
        "paths": bundle.get("paths"),
        "property_brief": bundle.get("property_brief"),
        "asset_manifest": bundle.get("asset_manifest"),
        "render_prompt": bundle.get("render_prompt"),
        "review_checklist": bundle.get("review_checklist"),
    }


@mcp.tool(description="Build a ready-to-use Claude marketing prompt from the approved property packet.", structured_output=True)
def get_marketing_prompt(query: str, deliverable: str = "one_pager") -> dict[str, Any]:
    row = lookup_row(query)
    if not row:
        return {"error": f"No property matched query: {query}"}

    bundle = load_property_bundle(clean_text(row.get("property_id")), BRIEFS_DIR)
    return {
        "property": row_to_summary(row),
        "deliverable": deliverable,
        "prompt": build_marketing_prompt(bundle, deliverable=deliverable),
    }


@mcp.tool(description="Return a concise shortlist explanation for one property.", structured_output=True)
def explain_candidate(query: str) -> dict[str, Any]:
    row = lookup_row(query)
    if not row:
        return {"error": f"No property matched query: {query}"}
    return {
        "property": row_to_summary(row),
        "reason_summary": row.get("shortlist_reason_summary"),
        "caution_summary": row.get("shortlist_caution_summary"),
        "review_status": row.get("review_status"),
        "marketing_ready": row.get("marketing_ready"),
    }


if __name__ == "__main__":
    mcp.run()
