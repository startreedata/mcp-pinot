#!/bin/bash
set -euo pipefail

printf '%s\n' \
  "The legacy /sse session script has been retired." \
  "The supported HTTP endpoint is Streamable HTTP at /mcp." \
  "Running the bundled MCP client, which handles initialization and sessions ..."

exec uv run python examples/example_client.py
