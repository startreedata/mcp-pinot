#!/usr/bin/env python3
"""Compatibility notice for the removed custom REST example.

Streamable HTTP MCP requires initialization, capability negotiation, and session
handling. Use an MCP SDK rather than sending one-off requests with ``urllib``.
"""


def main() -> None:
    raise SystemExit(
        "This legacy urllib example targeted removed /api/tools/* endpoints. "
        "Start the server at http://127.0.0.1:8080/mcp and run "
        "'uv run python examples/example_client.py' instead."
    )


if __name__ == "__main__":
    main()
