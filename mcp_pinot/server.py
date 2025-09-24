# --------------------------
# File: mcp_pinot/server.py
# --------------------------
import asyncio
import json
import ssl
from typing import Any

from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
import mcp.server.sse
import mcp.server.stdio
import mcp.types as types
import uvicorn

from mcp_pinot.config import load_pinot_config, load_server_config
from mcp_pinot.pinot_client import PinotClient
from mcp_pinot.prompts import PROMPT_TEMPLATE
from mcp_pinot.utils.logging_config import get_logger

# Get the configured logger
logger = get_logger()

# Initialize configurations and create client
pinot_config = load_pinot_config()
server_config = load_server_config()
pinot_client = PinotClient(pinot_config)


def get_tools_list() -> list[types.Tool]:
    """Get the list of available tools"""
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


def create_server() -> Server:
    """Create and configure the MCP server with all handlers"""
    server = Server("pinot_mcp")

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
        return get_tools_list()

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

    return server


async def run_stdio_server():
    """Run the MCP server with STDIO transport"""
    logger.info("Starting MCP server with STDIO transport")
    server = create_server()

    try:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            logger.info("Server running with stdio transport")
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="pinot_mcp",
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


async def handle_rest_api_call(scope, receive, send, server):
    """Handle REST API tool calls"""
    try:
        # Read the request body
        body = b""
        while True:
            message = await receive()
            if message["type"] == "http.request":
                body += message.get("body", b"")
                if not message.get("more_body", False):
                    break

        # Parse JSON request
        try:
            request_data = json.loads(body.decode())
        except json.JSONDecodeError:
            await send(
                {
                    "type": "http.response.start",
                    "status": 400,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": json.dumps({"error": "Invalid JSON"}).encode(),
                }
            )
            return

        # Extract tool name and arguments
        tool_name = request_data.get("name")
        arguments = request_data.get("arguments", {})

        if not tool_name:
            await send(
                {
                    "type": "http.response.start",
                    "status": 400,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": json.dumps({"error": "Missing 'name' field"}).encode(),
                }
            )
            return

        # Call the tool directly using our pinot_client
        try:
            if tool_name == "test-connection":
                result = pinot_client.test_connection()
            elif tool_name == "list-tables":
                result = pinot_client.get_tables()
            elif tool_name == "read-query":
                query = arguments.get("query")
                if not query:
                    result = {"error": "Missing 'query' argument"}
                elif not query.strip().upper().startswith("SELECT"):
                    result = {"error": "Only SELECT queries are allowed"}
                else:
                    result = pinot_client.execute_query(query)
            elif tool_name == "table-details":
                table_name = arguments.get("tableName")
                if not table_name:
                    result = {"error": "Missing 'tableName' argument"}
                else:
                    result = pinot_client.get_table_detail(table_name)
            else:
                result = {"error": f"Unknown tool: {tool_name}"}

            # Send successful response
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": json.dumps({"result": result}).encode(),
                }
            )

        except Exception as e:
            # Send error response
            await send(
                {
                    "type": "http.response.start",
                    "status": 500,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send(
                {
                    "type": "http.response.body",
                    "body": json.dumps({"error": str(e)}).encode(),
                }
            )

    except Exception as e:
        logger.error(f"Error in REST API handler: {e}")
        await send(
            {
                "type": "http.response.start",
                "status": 500,
                "headers": [[b"content-type", b"application/json"]],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": json.dumps({"error": "Internal server error"}).encode(),
            }
        )


async def handle_rest_api_list_tools(scope, receive, send, server):
    """Handle REST API tools list"""
    try:
        tools_list = get_tools_list()
        tools = [
            {
                "name": tool.name,
                "description": tool.description,
            }
            for tool in tools_list
        ]

        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [[b"content-type", b"application/json"]],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": json.dumps({"tools": tools}).encode(),
            }
        )

    except Exception as e:
        logger.error(f"Error in REST API list tools: {e}")
        await send(
            {
                "type": "http.response.start",
                "status": 500,
                "headers": [[b"content-type", b"application/json"]],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": json.dumps({"error": str(e)}).encode(),
            }
        )


async def run_http_server():
    """Run the MCP server with HTTP/SSE transport"""
    logger.info(
        f"Starting MCP server with HTTP transport on "
        f"{server_config.host}:{server_config.port}"
    )
    server = create_server()
    transport = mcp.server.sse.SseServerTransport(server_config.endpoint)

    # Create ASGI application
    async def app(scope, receive, send):
        if scope["type"] == "http":
            path = scope["path"]
            method = scope["method"]

            if path == server_config.endpoint and method == "GET":
                # Handle SSE connection
                async with transport.connect_sse(scope, receive, send) as streams:
                    read_stream, write_stream = streams
                    try:
                        await server.run(
                            read_stream,
                            write_stream,
                            InitializationOptions(
                                server_name="pinot_mcp",
                                server_version="0.1.0",
                                capabilities=server.get_capabilities(
                                    notification_options=NotificationOptions(),
                                    experimental_capabilities={},
                                ),
                            ),
                        )
                    except Exception as e:
                        logger.error(f"Error in SSE connection: {e}")
                        raise

            elif path == server_config.endpoint and method == "POST":
                # Handle POST messages
                await transport.handle_post_message(scope, receive, send)

            elif path == "/api/tools/call" and method == "POST":
                # Simple REST API endpoint for direct tool calls
                await handle_rest_api_call(scope, receive, send, server)

            elif path == "/api/tools/list" and method == "GET":
                # List available tools via REST API
                await handle_rest_api_list_tools(scope, receive, send, server)

            else:
                # Return 404 for other paths
                await send(
                    {
                        "type": "http.response.start",
                        "status": 404,
                        "headers": [[b"content-type", b"text/plain"]],
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": b"Not Found",
                    }
                )
        else:
            # Handle non-HTTP requests (shouldn't happen)
            logger.error(f"Received non-HTTP request: {scope['type']}")

    # Configure SSL context if certificates are provided
    ssl_context = None
    if server_config.ssl_keyfile and server_config.ssl_certfile:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(
            server_config.ssl_certfile, server_config.ssl_keyfile
        )
        logger.info("HTTPS/TLS enabled")
    else:
        logger.info("Running in HTTP mode (no SSL certificates provided)")

    # Run the server
    config = uvicorn.Config(
        app,
        host=server_config.host,
        port=server_config.port,
        ssl_keyfile=server_config.ssl_keyfile,
        ssl_certfile=server_config.ssl_certfile,
        log_level="info",
    )
    server_instance = uvicorn.Server(config)
    await server_instance.serve()


async def main():
    """Main entry point that chooses transport based on configuration"""
    logger.info(f"Starting Pinot MCP Server with {server_config.transport} transport")

    if server_config.transport == "http":
        await run_http_server()
    elif server_config.transport == "stdio":
        await run_stdio_server()
    elif server_config.transport == "both":
        # Run both transports simultaneously
        logger.info("Running both STDIO and HTTP transports concurrently")

        # Create tasks for both transports
        stdio_task = asyncio.create_task(run_stdio_server())
        http_task = asyncio.create_task(run_http_server())

        try:
            # Wait for either task to complete or fail
            done, pending = await asyncio.wait(
                [stdio_task, http_task], return_when=asyncio.FIRST_COMPLETED
            )

            # If we get here, one of the tasks completed or failed
            for task in done:
                if task.exception():
                    logger.error(f"Transport task failed: {task.exception()}")
                    # Cancel remaining tasks
                    for pending_task in pending:
                        pending_task.cancel()
                    raise task.exception()
                else:
                    # Task completed normally, shutdown the other transport
                    logger.info("One transport completed, shutting down the other")
                    for pending_task in pending:
                        pending_task.cancel()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down both transports")
            stdio_task.cancel()
            http_task.cancel()

            # Wait for tasks to finish cancellation
            await asyncio.gather(stdio_task, http_task, return_exceptions=True)
            logger.info("Both transports shut down successfully")

    else:
        raise ValueError(
            f"Unknown transport: {server_config.transport}. "
            f"Valid options: 'stdio', 'http', 'both'"
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
