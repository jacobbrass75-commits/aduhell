#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


CONFIG_PATH = Path.home() / "Library/Application Support/Claude/claude_desktop_config.json"
WORKSPACE_ROOT = Path("/Users/brass/Downloads/aduhell")
PYTHON_PATH = Path("/opt/anaconda3/bin/python")


def main() -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)

    if CONFIG_PATH.exists():
        backup_path = CONFIG_PATH.with_suffix(f".backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json")
        backup_path.write_text(CONFIG_PATH.read_text(encoding="utf-8"), encoding="utf-8")
        payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        print(f"Backed up existing config to: {backup_path}")
    else:
        payload = {}

    mcp_servers = payload.setdefault("mcpServers", {})
    mcp_servers["aduhell"] = {
        "command": str(PYTHON_PATH),
        "args": [str(WORKSPACE_ROOT / "mcp_aduhell_server.py")],
        "env": {
            "ADUHELL_ROOT": str(WORKSPACE_ROOT),
        },
    }

    CONFIG_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Updated Claude Desktop config: {CONFIG_PATH}")
    print("Restart Claude Desktop to load the ADUHell MCP server.")


if __name__ == "__main__":
    main()
