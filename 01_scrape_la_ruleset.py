#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urldefrag, urljoin, urlparse

import pdfplumber
import requests
from bs4 import BeautifulSoup
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


OUTPUT_DIR = Path("data/rulesets")
OUTPUT_JSON = OUTPUT_DIR / "la_city.json"
OUTPUT_RAW = OUTPUT_DIR / "la_city_raw.txt"

SEED_URLS = [
    "https://ladbs.org/services/check-my-project/accessory-dwelling-units",
    "https://planning.lacity.gov/ordinances/adu",
]

# The City Planning ADU ordinance URL above currently resolves to a 404 page.
# Keep the requested URL in the crawl, but also fetch the current Housing page
# that links to the active ADU memo and ordinance PDFs.
DISCOVERY_URLS = [
    "https://planning.lacity.gov/plans-policies/initiatives-policies/housing",
    "https://dbs.lacity.gov/forms-and-publications?field_title_search_value=ADU",
    "https://dbs.lacity.gov/forms-and-publications?field_faq_category_target_id%5B%5D=200",
    "https://dbs.lacity.gov/forms-and-publications?field_faq_category_target_id%5B%5D=201",
]

# These are official LA City documents discovered from the planning and LADBS
# pages above. Keeping them as fallbacks makes the scraper resilient if page
# markup changes.
FALLBACK_DOCUMENT_URLS = [
    "https://planning.lacity.gov/odocument/184600d8-71d7-4d74-baf1-1f9cd2603320/ZA%20Memo%20No%20143-%20Implementation%20of%202019%20ADU%20Ord%20and%20State%20ADU%20Law%20(Updated%20based%20on%202022,%202023%20an.pdf",
    "https://planning.lacity.gov/odocument/be4db130-aa38-4637-b35a-d24304d50ff2/ZA_Memo_143_Revision_1_Chapter1.pdf",
    "https://planning.lacity.gov/odocument/ec892d01-7873-455a-8e15-78a771b2c7ac/ADU_Memo_2020_Final_2.26.20_(1).pdf",
    "https://planning.lacity.gov/odocument/58134843-3bb4-4fb0-8870-fd5bee42974a/ZA%20Memo.142.pdf",
    "https://cityclerk.lacity.org/onlinedocs/2016/16-1468_ORD_186481_12-19-2019.pdf",
    "https://dbs.lacity.gov/sites/default/files/efs/pdf/publications/misc/Sample-Plan-for-ADU.pdf",
    "https://dbs.lacity.gov/sites/default/files/efs/pdf/publications/adu/you-adu/YOU-ADU-Standard-Plan.pdf",
    "https://dbs.lacity.gov/sites/default/files/efs/pdf/publications/adu/you-adu/G-0.0-COVER-SHEET---YOUADU_Sample.pdf",
    "https://dbs.lacity.gov/sites/default/files/efs/pdf/publications/adu/you-adu/ADU-BP-Checklist.7.7.2023.pdf",
    "https://dbs.lacity.gov/sites/default/files/efs/forms/pc17/Doc_Subm_New_SFD_ADU_Std_Pln.pdf",
]

OFFICIAL_HOSTS = {
    "ladbs.org",
    "www.ladbs.org",
    "dbs.lacity.gov",
    "planning.lacity.gov",
    "cityclerk.lacity.org",
}

LINK_HINTS = (
    "adu",
    "accessory dwelling",
    "junior accessory",
    "jadu",
    "ordinance no. 186481",
    "sample-plan-for-adu",
    "sample plan for adu",
    "you-adu",
    "standard-plan",
    "standard plan",
    "movable tiny house",
)


@dataclass
class Document:
    requested_url: str
    final_url: str
    status_code: int
    content_type: str
    kind: str
    title: str
    text: str


def build_session() -> requests.Session:
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=0.75,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def normalize_text(text: str) -> str:
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_url(url: str) -> str:
    return re.sub(r"(?<!:)/{2,}", "/", url)


def is_official_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in OFFICIAL_HOSTS


def response_is_pdf(response: Response) -> bool:
    content_type = response.headers.get("content-type", "").lower()
    path = urlparse(response.url).path.lower()
    return "pdf" in content_type or path.endswith(".pdf")


def extract_html_text(response: Response) -> tuple[str, str]:
    soup = BeautifulSoup(response.text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    text = normalize_text(soup.get_text("\n", strip=True))
    return title, text


def extract_pdf_text(raw_bytes: bytes) -> str:
    pages: list[str] = []
    with pdfplumber.open(io.BytesIO(raw_bytes)) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            page_text = normalize_text(page.extract_text() or "")
            if page_text:
                pages.append(f"[Page {page_number}]\n{page_text}")
    return "\n\n".join(pages).strip()


def fetch_document(session: requests.Session, url: str) -> Document:
    response = session.get(url, timeout=(15, 120), allow_redirects=True)
    content_type = response.headers.get("content-type", "")

    if response_is_pdf(response):
        title = Path(urlparse(response.url).path).name
        try:
            text = extract_pdf_text(response.content)
        except Exception as exc:  # pragma: no cover - defensive path
            text = f"PDF extraction failed: {exc}"
        kind = "pdf"
    else:
        title, text = extract_html_text(response)
        kind = "html"

    return Document(
        requested_url=url,
        final_url=clean_url(response.url),
        status_code=response.status_code,
        content_type=content_type,
        kind=kind,
        title=title,
        text=text,
    )


def discover_links_from_html(session: requests.Session, url: str) -> list[str]:
    response = session.get(url, timeout=(15, 120), allow_redirects=True)
    soup = BeautifulSoup(response.text, "html.parser")
    discovered: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        label = " ".join(anchor.get_text(" ", strip=True).split()).lower()
        href = clean_url(urldefrag(urljoin(response.url, anchor["href"]))[0])
        blob = f"{label} {href}".lower()
        if not href.startswith(("http://", "https://")):
            continue
        if not is_official_url(href):
            continue
        if not any(hint in blob for hint in LINK_HINTS):
            continue
        discovered.add(href)

    return sorted(discovered)


def combine_text(documents: Iterable[Document], *needles: str) -> str:
    lowered_needles = [needle.lower() for needle in needles]
    chunks = []
    for document in documents:
        haystack = " ".join(
            [
                document.requested_url.lower(),
                document.final_url.lower(),
                document.title.lower(),
            ]
        )
        if lowered_needles and not any(needle in haystack for needle in lowered_needles):
            continue
        chunks.append(document.text)
    return "\n\n".join(chunk for chunk in chunks if chunk).strip()


def first_match(patterns: Iterable[str], text: str) -> re.Match[str] | None:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match
    return None


def parse_int(value: str) -> int:
    return int(re.sub(r"[^0-9]", "", value))


def build_ruleset(documents: list[Document]) -> dict:
    authoritative_text = combine_text(documents, "za memo no 143", "za_memo_143", "housing")
    all_text = "\n\n".join(document.text for document in documents if document.text)
    standard_plan_text = combine_text(
        documents,
        "forms-and-publications",
        "you-adu",
        "sample-plan-for-adu",
        "sample plan for adu",
        "doc_subm_new_sfd_adu_std_pln",
    )

    notes: list[str] = []

    min_lot_size_sqft = None
    if re.search(
        r"shall not include minimum lot size|minimum lot size requirement .*? shall not be the basis of a denial",
        authoritative_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        min_lot_size_sqft = 0
        notes.append(
            "Minimum lot size appears to be effectively waived for State ADUs in LA City; 0 is used here to mean no minimum lot size requirement for State ADU approval."
        )

    detached_sqft_match = first_match(
        [
            r"Floor Area for a detached ADU shall not exceed\s+1[, ]?200",
            r"detached ADU square footage.*?1[, ]?200",
            r"ADU Square Footage\s+Limit.*?1[, ]?200\s*SF",
            r"\b1[, ]?200\s*SF\b",
            r"\b1[, ]?200\s*square feet\b",
        ],
        all_text,
    )
    max_detached_adu_sqft = 1200 if detached_sqft_match else None

    attached_formula_found = re.search(
        r"50%\s*of\s*existing.*?dwelling|attached ADU may not exceed 50 percent of the existing primary dwelling",
        all_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    max_attached_adu_sqft = None
    max_attached_adu_sqft_rule = None
    if attached_formula_found:
        max_attached_adu_sqft_rule = {
            "type": "formula",
            "base_rule": "50% of the existing primary dwelling floor area",
            "new_building_rule": "No fixed local square-foot cap when part of a new building, subject to other applicable zoning limits.",
            "state_override_max_sqft": {
                "studio_or_one_bedroom": 850,
                "more_than_one_bedroom": 1000,
            },
            "programmatic_interpretation": "Use the greater of the local 50% rule and the applicable State-law override, while respecting scenario-specific zoning constraints.",
        }
        notes.append(
            "TODO: Attached ADU floor area is formula-based, not a single citywide scalar. ZA Memo 143 says 50% of the existing dwelling, or no fixed cap when built with a new dwelling, with 850 sf / 1,000 sf State-law allowances overriding stricter local limits."
        )

    max_adu_height_ft = None
    max_adu_height_ft_rule = None
    if re.search(
        r"up to\s+25\s+feet.*?whichever is lower|16 feet; or|18 feet.*?transit",
        authoritative_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        max_adu_height_ft_rule = {
            "type": "conditional",
            "cases": [
                {
                    "scenario": "attached_ordinance_adu",
                    "max_height_ft": 25,
                    "qualifier": "or the applicable zoning height limit, whichever is lower",
                },
                {
                    "scenario": "detached_state_adu_default",
                    "max_height_ft": 16,
                    "qualifier": "baseline State detached ADU allowance",
                },
                {
                    "scenario": "detached_state_adu_within_half_mile_of_major_transit_or_high_quality_transit",
                    "max_height_ft": 18,
                    "qualifier": "applies to qualifying transit proximity cases",
                },
                {
                    "scenario": "detached_state_adu_with_qualifying_transit_and_aligned_roof_pitch",
                    "max_height_ft": 20,
                    "qualifier": "18 ft baseline plus 2 additional feet when roof pitch aligns with the primary dwelling",
                },
                {
                    "scenario": "other_adu_contexts",
                    "max_height_ft": None,
                    "qualifier": "use applicable zoning height rules and specific ADU type context",
                },
            ],
        }
        notes.append(
            "TODO: Height varies by ADU type and context. Attached ordinance ADUs may be up to 25 ft or zoning height, whichever is lower; detached State ADUs are generally 16 ft, 18 ft near qualifying transit, or 20 ft with an aligned roof pitch."
        )

    four_foot_setback = re.search(
        r"four-foot rear and side yard setbacks|4 feet away from the rear and side lot lines|rear and side setbacks of no more than four feet",
        authoritative_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    rear_setback_ft = 4 if four_foot_setback else None
    side_setback_ft = 4 if four_foot_setback else None

    max_lot_coverage_pct = None
    max_lot_coverage_pct_rule = None
    if re.search(r"lot coverage", authoritative_text, flags=re.IGNORECASE):
        max_lot_coverage_pct_rule = {
            "type": "zone_dependent",
            "citywide_scalar_pct": None,
            "rule": "Underlying zone lot coverage rules may apply, but they cannot preclude the minimum State ADU allowances described in current LA Planning guidance.",
            "programmatic_interpretation": "Treat lot coverage as parcel-zoning dependent rather than a citywide constant; evaluate against the property's base zoning rules after applying State ADU minimum allowances.",
        }
        notes.append(
            "TODO: Maximum lot coverage is zone-specific and not a single citywide percentage. ZA Memo 143 says lot coverage rules may still apply so long as they do not preclude minimum State ADU allowances."
        )

    jadu_allowed = bool(
        re.search(
            r"one JADU is permitted per residential Lot|Junior Accessory Dwelling Units",
            authoritative_text,
            flags=re.IGNORECASE,
        )
    )

    jadu_max_match = first_match(
        [
            r"JADU is a unit that is no more than\s+(\d{3})\s+square feet",
            r"JADU.*?(\d{3})\s*SF",
        ],
        authoritative_text,
    )
    jadu_max_sqft = parse_int(jadu_max_match.group(1)) if jadu_max_match else None

    detached_adu_allowed = bool(
        re.search(r"\bdetached ADU\b", authoritative_text, flags=re.IGNORECASE)
    )
    attached_adu_allowed = bool(
        re.search(r"\battached ADU\b", authoritative_text, flags=re.IGNORECASE)
    )

    garage_conversion_allowed = bool(
        re.search(
            r"attics, basements, or garages|existing living area or accessory structure .*? converted to an ADU|garage, carport, covered parking structure .*? demolished in conjunction with ADU construction",
            authoritative_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )

    owner_occupancy_required = None
    if re.search(
        r"permanently removed the authority .*? owner-occupancy of an ADU",
        authoritative_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        owner_occupancy_required = False
        notes.append(
            "Owner occupancy does not appear to be required for ADUs after AB 976, but JADUs still require a deed restriction tying owner occupancy to either the primary residence or the JADU."
        )

    max_adus_per_lot = 1 if re.search(r"one ADU and one JADU per lot", authoritative_text, flags=re.IGNORECASE) else None
    if re.search(
        r"up to eight detached ADUs|up to 25% of existing units in the building|up to 2 per Lot",
        authoritative_text,
        flags=re.IGNORECASE,
    ):
        notes.append(
            "Single-family baseline is used for max_adus_per_lot = 1. TODO: model multifamily parcels separately because LA allows more attached/detached ADUs on some multifamily lots."
        )

    max_jadus_per_lot = 1 if re.search(r"one JADU is permitted per residential Lot", authoritative_text, flags=re.IGNORECASE) else None

    transit_half_mile_parking_exempt = bool(
        re.search(
            r"within\s*[½1/2]\s*mile walking distance from a bus or rail stop|parking incentive",
            authoritative_text,
            flags=re.IGNORECASE,
        )
    )

    standard_plan_program_available = bool(
        re.search(
            r"YOU-ADU|ADU Standard Plans|Sample Plan for ADU|Doc_Subm_New_SFD_ADU_Std_Pln",
            standard_plan_text,
            flags=re.IGNORECASE,
        )
    )
    if standard_plan_program_available:
        notes.append(
            "LADBS standard plan materials were found, including YOU-ADU and Sample Plan for ADU documents."
        )

    if any(
        document.requested_url == "https://planning.lacity.gov/ordinances/adu"
        and document.status_code >= 400
        for document in documents
    ):
        notes.append(
            "The requested Planning URL https://planning.lacity.gov/ordinances/adu returned a 404 page during scraping; the scraper used the current Housing page and linked ADU memos as the active Planning source."
        )

    ruleset = {
        "min_lot_size_sqft": min_lot_size_sqft,
        "max_detached_adu_sqft": max_detached_adu_sqft,
        "max_attached_adu_sqft": max_attached_adu_sqft,
        "max_attached_adu_sqft_rule": max_attached_adu_sqft_rule,
        "max_adu_height_ft": max_adu_height_ft,
        "max_adu_height_ft_rule": max_adu_height_ft_rule,
        "rear_setback_ft": rear_setback_ft,
        "side_setback_ft": side_setback_ft,
        "max_lot_coverage_pct": max_lot_coverage_pct,
        "max_lot_coverage_pct_rule": max_lot_coverage_pct_rule,
        "jadu_allowed": jadu_allowed,
        "jadu_max_sqft": jadu_max_sqft,
        "detached_adu_allowed": detached_adu_allowed,
        "attached_adu_allowed": attached_adu_allowed,
        "garage_conversion_allowed": garage_conversion_allowed,
        "owner_occupancy_required": owner_occupancy_required,
        "max_adus_per_lot": max_adus_per_lot,
        "max_jadus_per_lot": max_jadus_per_lot,
        "transit_half_mile_parking_exempt": transit_half_mile_parking_exempt,
        "standard_plan_program_available": standard_plan_program_available,
        "notes": notes,
        "source_urls": sorted(
            {
                document.final_url
                for document in documents
                if document.status_code < 400 and document.text
            }
        ),
        "last_scraped": datetime.now(timezone.utc).isoformat(),
    }

    if ruleset["max_detached_adu_sqft"] is None:
        notes.append("TODO: Verify the maximum detached ADU square footage manually from the current memo tables.")
    if ruleset["rear_setback_ft"] is None or ruleset["side_setback_ft"] is None:
        notes.append("TODO: Verify rear and side setback requirements manually from the current memo tables.")
    if ruleset["jadu_max_sqft"] is None:
        notes.append("TODO: Verify JADU maximum square footage manually.")
    if ruleset["owner_occupancy_required"] is None:
        notes.append("TODO: Verify owner-occupancy requirements manually.")

    return ruleset


def build_raw_dump(documents: list[Document]) -> str:
    chunks = []
    for document in documents:
        chunks.append(
            "\n".join(
                [
                    "=" * 100,
                    f"REQUESTED URL: {document.requested_url}",
                    f"FINAL URL: {document.final_url}",
                    f"STATUS: {document.status_code}",
                    f"KIND: {document.kind}",
                    f"CONTENT-TYPE: {document.content_type}",
                    f"TITLE: {document.title}",
                    "",
                    document.text or "[NO TEXT EXTRACTED]",
                    "",
                ]
            )
        )
    return "\n".join(chunks)


def print_summary(ruleset: dict) -> None:
    ignored = {"notes", "source_urls", "last_scraped"}
    populated = [key for key, value in ruleset.items() if key not in ignored and value is not None]
    missing = [key for key, value in ruleset.items() if key not in ignored and value is None]

    print("LA City ADU ruleset scrape complete.")
    print(f"Structured output: {OUTPUT_JSON}")
    print(f"Raw text dump: {OUTPUT_RAW}")
    print(f"Source documents used: {len(ruleset['source_urls'])}")
    print()
    print("Populated fields:")
    for key in populated:
        print(f"  - {key}: {ruleset[key]}")
    print()
    print("Fields requiring manual review:")
    for key in missing:
        print(f"  - {key}")
    print()
    print("Notes:")
    for note in ruleset["notes"]:
        print(f"  - {note}")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    session = build_session()
    seen: set[str] = set()
    documents: list[Document] = []

    initial_urls = SEED_URLS + DISCOVERY_URLS
    for url in initial_urls:
        if url in seen:
            continue
        seen.add(url)
        documents.append(fetch_document(session, url))

    discovered_urls: set[str] = set()
    for url in initial_urls:
        for discovered_url in discover_links_from_html(session, url):
            discovered_urls.add(discovered_url)

    discovered_urls.update(FALLBACK_DOCUMENT_URLS)

    for url in sorted(discovered_urls):
        if url in seen:
            continue
        seen.add(url)
        try:
            documents.append(fetch_document(session, url))
        except Exception as exc:
            documents.append(
                Document(
                    requested_url=url,
                    final_url=url,
                    status_code=0,
                    content_type="",
                    kind="error",
                    title="fetch_failed",
                    text=f"Fetch failed: {exc}",
                )
            )

    ruleset = build_ruleset(documents)

    OUTPUT_JSON.write_text(json.dumps(ruleset, indent=2, sort_keys=False), encoding="utf-8")
    OUTPUT_RAW.write_text(build_raw_dump(documents), encoding="utf-8")

    print_summary(ruleset)


if __name__ == "__main__":
    main()
