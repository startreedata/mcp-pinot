#!/bin/bash
set -euo pipefail

printf '%s\n' \
  "test_rest_api.sh is retained as a compatibility wrapper." \
  "mcp-pinot exposes standard MCP over STDIO or Streamable HTTP at /mcp;" \
  "it does not expose /api/tools/list or /api/tools/call." \
  "Running the supported FastMCP client smoke test instead ..."

exec uv run python examples/example_client.py
