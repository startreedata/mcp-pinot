#!/usr/bin/env python3
"""
Example MCP Pinot Client

This example demonstrates how to connect to and interact with the MCP Pinot server
using the FastMCP client library.

Usage:
    python examples/example_client.py

Prerequisites:
    - MCP Pinot server running on localhost:8080
    - FastMCP client library installed
"""

from fastmcp import Client
import asyncio


async def main():
    """Main example function demonstrating MCP Pinot client usage."""
    print("🚀 MCP Pinot Client Example")
    print("=" * 40)
    
    async with Client("http://localhost:8080/mcp") as client:
        print("✓ Connected to MCP Pinot server!")
        print()

        # List available tools
        print("📋 Available tools:")
        tools = await client.list_tools()
        for tool in tools:
            print(f"  - {tool.name}: {tool.description}")
        print()

        # Test listing tables
        print("📊 Listing tables:")
        try:
            tables_result = await client.call_tool("list_tables")
            print(f"Tables: {tables_result}")
        except Exception as e:
            print(f"Error listing tables: {e}")
        print()

        # Test querying a table
        print("📊 Querying a table:")
        try:
            tables_result = await client.call_tool("read_query", {"query": "SELECT * FROM airlineStats LIMIT 5"})
            print(f"Query result: {tables_result}")
        except Exception as e:
            print(f"Error querying table: {e}")
        print()

        print("✅ Example completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())
