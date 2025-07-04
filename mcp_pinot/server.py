# --------------------------
# File: mcp_pinot/server.py
# --------------------------
from typing import Any

from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
import mcp.server.stdio
import mcp.types as types

from mcp_pinot.config import load_pinot_config
from mcp_pinot.pinot_client import PinotClient
from mcp_pinot.prompts import PROMPT_TEMPLATE
from mcp_pinot.utils.logging_config import get_logger

# Get the configured logger
logger = get_logger()

# Initialize configuration and create client
pinot_config = load_pinot_config()
pinot_client = PinotClient(pinot_config)


async def main():
    logger.info("Starting Pinot MCP Server")
    server = Server("pinot_mcp_claude")

    @server.list_prompts()
    async def handle_list_prompts() -> list[types.Prompt]:
        logger.debug("Handling list_prompts request")
        return [
            types.Prompt(
                name="pinot-query",
                description="Query Pinot database with MCP Server + Claude",
                arguments=[],
            )
        ]

    @server.get_prompt()
    async def handle_get_prompt(
        name: str, arguments: dict[str, str] | None
    ) -> types.GetPromptResult:
        if name != "pinot-query":
            raise ValueError(f"Unknown prompt: {name}")
        return types.GetPromptResult(
            description="Pinot query assistance template",
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.TextContent(
                        type="text", text=PROMPT_TEMPLATE.strip()
                    ),
                )
            ],
        )

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="test-connection",
                description="Test Pinot connection and return diagnostics",
                inputSchema={"type": "object", "properties": {}},
            ),
            types.Tool(
                name="read-query",
                description="Execute a SELECT query on the Pinot database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SELECT SQL query to execute",
                        },
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
            types.Tool(
                name="create-schema",
                description="Create a new schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schemaJson": {"type": "string"},
                        "override": {"type": "boolean", "default": True},
                        "force": {"type": "boolean", "default": False},
                    },
                    "required": ["schemaJson"],
                },
            ),
            types.Tool(
                name="update-schema",
                description="Update an existing schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schemaName": {"type": "string"},
                        "schemaJson": {"type": "string"},
                        "reload": {"type": "boolean", "default": False},
                        "force": {"type": "boolean", "default": False},
                    },
                    "required": ["schemaName", "schemaJson"],
                },
            ),
            types.Tool(
                name="get-schema",
                description="Fetch a schema by name",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schemaName": {"type": "string"},
                    },
                    "required": ["schemaName"],
                },
            ),
            types.Tool(
                name="create-table-config",
                description="Create table configuration",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableConfigJson": {"type": "string"},
                        "validationTypesToSkip": {"type": "string"},
                    },
                    "required": ["tableConfigJson"],
                },
            ),
            types.Tool(
                name="update-table-config",
                description="Update table configuration",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string"},
                        "tableConfigJson": {"type": "string"},
                        "validationTypesToSkip": {"type": "string"},
                    },
                    "required": ["tableName", "tableConfigJson"],
                },
            ),
            types.Tool(
                name="get-table-config",
                description="Get table configuration",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string"},
                        "tableType": {"type": "string"},
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
                results = pinot_client.test_connection()
                return [types.TextContent(type="text", text=str(results))]

            elif name == "read-query":
                if not arguments["query"].strip().upper().startswith("SELECT"):
                    raise ValueError("Only SELECT queries are allowed for read-query")
                results = pinot_client.execute_query(query=arguments["query"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "table-details":
                results = pinot_client.get_table_detail(
                    tableName=arguments["tableName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "segment-list":
                results = pinot_client.get_segments(tableName=arguments["tableName"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "index-column-details":
                results = pinot_client.get_index_column_detail(
                    tableName=arguments["tableName"],
                    segmentName=arguments["segmentName"],
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "segment-metadata-details":
                results = pinot_client.get_segment_metadata_detail(
                    tableName=arguments["tableName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "tableconfig-schema-details":
                results = pinot_client.get_tableconfig_schema_detail(
                    tableName=arguments["tableName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "create-schema":
                results = pinot_client.create_schema(
                    arguments["schemaJson"],
                    arguments.get("override", True),
                    arguments.get("force", False),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "update-schema":
                results = pinot_client.update_schema(
                    arguments["schemaName"],
                    arguments["schemaJson"],
                    arguments.get("reload", False),
                    arguments.get("force", False),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "get-schema":
                results = pinot_client.get_schema(schemaName=arguments["schemaName"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "create-table-config":
                results = pinot_client.create_table_config(
                    arguments["tableConfigJson"],
                    arguments.get("validationTypesToSkip"),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "update-table-config":
                results = pinot_client.update_table_config(
                    arguments["tableName"],
                    arguments["tableConfigJson"],
                    arguments.get("validationTypesToSkip"),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "get-table-config":
                results = pinot_client.get_table_config(
                    tableName=arguments["tableName"],
                    tableType=arguments.get("tableType"),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "list-tables":
                results = pinot_client.get_tables()
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
        import sys
        import traceback

        logger.error(f"Error running MCP server: {e}")
        logger.error(traceback.format_exc())
        print(f"Error running MCP server: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        raise


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
