# Legacy prototype: `mcp_pinot_ops`

This directory contains the original pre-FastMCP prototype and is retained only
for historical reference. It is not included in the `mcp-pinot-server` Python
package, is not the entry point used by Docker, MCPB, Helm, or the MCP Registry,
and its hyphenated tool names are not part of the supported v4 contract.

Do not run or import this server for new integrations. Use:

- implementation: `mcp_pinot/`
- command: `uv run mcp-pinot`
- local Streamable HTTP endpoint: `http://127.0.0.1:8080/mcp`
- supported client example: `uv run python examples/example_client.py`

The authoritative tool names are documented in the root `README.md` and exposed
to MCP clients through `tools/list`.
