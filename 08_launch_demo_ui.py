#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, send_from_directory

from adu_demo_support import (
    ALLOWED_PACKAGE_FILES,
    DEFAULT_BRIEFS_DIR,
    DEFAULT_GEOJSON,
    DEFAULT_GEOCODED_CSV,
    DEFAULT_GEOCODE_CACHE,
    DEFAULT_MAPS_DIR,
    DEFAULT_RANKED_CSV,
    compute_demo_summary,
    ensure_map_outputs,
    load_csv_rows,
    load_property_bundle,
)


def create_app(
    ranked_csv: Path,
    briefs_dir: Path,
    geocoded_csv: Path,
    geojson_path: Path,
    cache_path: Path,
    refresh_geocodes: bool,
) -> Flask:
    app = Flask(__name__, template_folder="web_templates")
    app.config["RANKED_CSV"] = ranked_csv
    app.config["BRIEFS_DIR"] = briefs_dir
    app.config["GEOCODED_CSV"] = geocoded_csv
    app.config["GEOJSON_PATH"] = geojson_path
    app.config["GEOCODE_CACHE"] = cache_path

    def refresh_state() -> None:
        ensure_map_outputs(
            ranked_csv=app.config["RANKED_CSV"],
            geocoded_csv=app.config["GEOCODED_CSV"],
            geojson_path=app.config["GEOJSON_PATH"],
            cache_path=app.config["GEOCODE_CACHE"],
            refresh_missing=refresh_geocodes,
        )
        rows = load_csv_rows(app.config["GEOCODED_CSV"])
        app.config["PROPERTY_ROWS"] = rows
        app.config["PROPERTY_INDEX"] = {
            (row.get("property_id") or ""): row for row in rows if row.get("property_id")
        }
        app.config["DEMO_SUMMARY"] = compute_demo_summary(rows)

    refresh_state()

    @app.get("/")
    def index():
        return render_template("demo_map.html", summary=app.config["DEMO_SUMMARY"])

    @app.get("/property/<property_id>")
    def property_detail(property_id: str):
        row = app.config["PROPERTY_INDEX"].get(property_id)
        if not row:
            abort(404)
        bundle = load_property_bundle(property_id, app.config["BRIEFS_DIR"])
        return render_template("property_detail.html", row=row, bundle=bundle)

    @app.get("/api/summary")
    def api_summary():
        return jsonify(app.config["DEMO_SUMMARY"])

    @app.get("/api/properties")
    def api_properties():
        return jsonify(app.config["PROPERTY_ROWS"])

    @app.get("/api/properties/<property_id>")
    def api_property(property_id: str):
        row = app.config["PROPERTY_INDEX"].get(property_id)
        if not row:
            abort(404)
        bundle = load_property_bundle(property_id, app.config["BRIEFS_DIR"])
        return jsonify({"row": row, "bundle": bundle})

    @app.get("/files/<property_id>/<filename>")
    def package_file(property_id: str, filename: str):
        if filename not in ALLOWED_PACKAGE_FILES:
            abort(404)
        property_path = app.config["BRIEFS_DIR"] / property_id
        if not property_path.exists():
            abort(404)
        return send_from_directory(property_path, filename)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Launch the local LA City ADU demo UI.")
    parser.add_argument("--input", type=Path, default=DEFAULT_RANKED_CSV, help="Path to the ranked shortlist CSV.")
    parser.add_argument("--briefs-dir", type=Path, default=DEFAULT_BRIEFS_DIR, help="Directory containing property brief folders.")
    parser.add_argument("--maps-dir", type=Path, default=DEFAULT_MAPS_DIR, help="Directory for geocode and map outputs.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind the local web app.")
    parser.add_argument("--port", type=int, default=5055, help="Port for the local web app.")
    parser.add_argument(
        "--refresh-geocodes",
        action="store_true",
        help="Refresh missing geocodes before launching the app.",
    )
    args = parser.parse_args()

    maps_dir = args.maps_dir
    maps_dir.mkdir(parents=True, exist_ok=True)

    app = create_app(
        ranked_csv=args.input,
        briefs_dir=args.briefs_dir,
        geocoded_csv=DEFAULT_GEOCODED_CSV if args.maps_dir == DEFAULT_MAPS_DIR else maps_dir / DEFAULT_GEOCODED_CSV.name,
        geojson_path=DEFAULT_GEOJSON if args.maps_dir == DEFAULT_MAPS_DIR else maps_dir / DEFAULT_GEOJSON.name,
        cache_path=DEFAULT_GEOCODE_CACHE if args.maps_dir == DEFAULT_MAPS_DIR else maps_dir / DEFAULT_GEOCODE_CACHE.name,
        refresh_geocodes=args.refresh_geocodes,
    )
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
