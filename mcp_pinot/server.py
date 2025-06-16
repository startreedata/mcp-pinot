# --------------------------
# File: mcp_pinot/server.py
# --------------------------
import asyncio
from typing import Any

import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
import mcp.server.stdio
from mcp_pinot.utils.pinot_client import (
    Pinot
)
from mcp_pinot.utils.logging_config import get_logger
from mcp_pinot.prompts import PROMPT_TEMPLATE

# Get the configured logger
logger = get_logger()

# Use the imported Pinot class and connection values
pinot_instance = Pinot()

async def main():
    logger.info("Starting Pinot MCP Server")
    server = Server("pinot_mcp_claude")

    @server.list_prompts()
    async def handle_list_prompts() -> list[types.Prompt]:
        logger.debug("Handling list_prompts request")
        return [
            types.Prompt(
                name="pinot-query",
                description="A prompt to Query the pinot database with an Pinot MCP Server + Claude",
                arguments=[],
            )
        ]

    @server.get_prompt()
    async def handle_get_prompt(name: str, arguments: dict[str, str] | None) -> types.GetPromptResult:
        if name != "pinot-query":
            raise ValueError(f"Unknown prompt: {name}")
        return types.GetPromptResult(
            description="Pinot query assistance template",
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.TextContent(type="text", text=PROMPT_TEMPLATE.strip()),
                )
            ],
        )

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="test-connection",
                description="Test the Pinot connection and return diagnostic information",
                inputSchema={"type": "object", "properties": {}},
            ),
            types.Tool(
                name="read-query",
                description="Execute a SELECT query on the Pinot database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "SELECT SQL query to execute"},
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="list-tables",
                description="List all tables in Pinot",
                inputSchema={"type": "object", "properties": {}},
            ),
            types.Tool(
                name="table-details",
                description="Get table size details",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string", "description": "Table name"},
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="segment-list",
                description="List segments for a table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string", "description": "Table name"},
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="index-column-details",
                description="Get index/column details for a segment",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string"},
                        "segmentName": {"type": "string"},
                    },
                    "required": ["tableName", "segmentName"],
                },
            ),
            types.Tool(
                name="segment-metadata-details",
                description="Get metadata for segments of a table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string"},
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="tableconfig-schema-details",
                description="Get table config and schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string"},
                    },
                    "required": ["tableName"],
                },
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """Handle tool execution requests"""
        try:
            if name == "test-connection":
                results = pinot_instance.test_connection()
                return [types.TextContent(type="text", text=str(results))]

            elif name == "read-query":
                if not arguments["query"].strip().upper().startswith("SELECT"):
                    raise ValueError("Only SELECT queries are allowed for read-query")
                results = pinot_instance._execute_query(query=arguments["query"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "table-details":
                results = pinot_instance._get_table_detail(tableName=arguments["tableName"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "segment-list":
                results = pinot_instance._get_segments(tableName=arguments["tableName"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "index-column-details":
                results = pinot_instance._get_index_column_detail(
                    tableName=arguments["tableName"],
                    segmentName=arguments["segmentName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "segment-metadata-details":
                results = pinot_instance._get_segment__metadata_detail(tableName=arguments["tableName"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "tableconfig-schema-details":
                results = pinot_instance._get_tableconfig_schema_detail(tableName=arguments["tableName"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "list-tables":
                results = pinot_instance._get_tables()
                return [types.TextContent(type="text", text=str(results))]

            else:
                raise ValueError(f"Unknown tool: {name}")

        except Exception as e:
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    try:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            logger.info("Server running with stdio transport")
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="pinot_mcp_claude",
                    server_version="0.1.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )
    except Exception as e:
        import traceback
        import sys
        logger.error(f"Error running MCP server: {e}")
        logger.error(traceback.format_exc())
        print(f"Error running MCP server: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        raise

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
