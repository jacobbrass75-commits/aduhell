# Local Demo And Claude Setup

## What this gives you
- a local map UI with all ranked properties color coded by shortlist tier
- per-property detail pages that open the generated package files
- a local MCP server for Claude Desktop so Claude can pull the shortlist and property briefs without using API credits

## Run the pipeline
```bash
cd /Users/brass/Downloads/aduhell
python 05_run_la_city_marketing_pipeline.py --input /Users/brass/Downloads/Export-20260328-221034.csv --top-n 50
```

## Geocode the ranked list for the map
```bash
python 07_geocode_la_city_candidates.py
```

This uses the U.S. Census geocoder and writes:
- `data/maps/Export-20260328-221034_la_city_geocoded_candidates.csv`
- `data/maps/Export-20260328-221034_la_city_property_points.geojson`

## Launch the local demo UI
```bash
python 08_launch_demo_ui.py
```

Open:
- `http://127.0.0.1:5055`

## Connect Claude Desktop

### Easy install
```bash
python 10_install_claude_desktop_mcp.py
```

Then restart Claude Desktop.

### Manual install
Merge the contents of:
- `claude_desktop_config.aduhell.json`

Into:
- `~/Library/Application Support/Claude/claude_desktop_config.json`

## Suggested Claude prompts
- `Use the aduhell MCP server to list the top 10 ADU opportunities.`
- `Use aduhell to get the property packet for 4378-012-009.`
- `Use aduhell to build a one_pager marketing prompt for 4378-012-009, then draft the one-pager.`
- `Use aduhell to search for 2850 MORAGA DR and summarize the approved ADU claims only.`

## Important caveat
This stack is still a best-effort LA City screen. It does not yet confirm parcel geometry, overlays, transit, or garage evidence.
