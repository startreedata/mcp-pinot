#!/bin/bash
set -euo pipefail

printf '%s\n' \
  "query_simple.sh now uses the supported MCP client example." \
  "The server does not expose the former /api/tools/* REST endpoints." \
  "Connecting to MCP_URL=${MCP_URL:-http://127.0.0.1:8080/mcp} ..."

exec uv run python examples/example_client.py
