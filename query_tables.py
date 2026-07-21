#!/usr/bin/env python3
"""List Pinot tables through the supported Streamable HTTP MCP endpoint."""

import asyncio
import os

from fastmcp import Client


async def main() -> None:
    endpoint = os.environ.get("MCP_URL", "http://127.0.0.1:8080/mcp")
    async with Client(endpoint) as client:
        result = await client.call_tool("list_tables", {"limit": 100})

    content = result.structured_content or {}
    tables = content.get("tables", [])
    for table in tables:
        print(table)

    if content.get("has_more"):
        print(
            "More tables are available; use an MCP client to continue with the "
            "returned pagination metadata."
        )


if __name__ == "__main__":
    asyncio.run(main())
