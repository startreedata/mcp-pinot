# --------------------------
# File: mcp_pinot_ops/server.py
# --------------------------
import asyncio
import logging
import sys
from typing import Any

from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
import mcp.server.stdio
import mcp.types as types

from mcp_pinot_ops.prompts import PROMPT_TEMPLATE
from mcp_pinot_ops.utils.pinot_client import Pinot

logger = logging.getLogger("pinot_mcp_table_ops_claude")
logger.setLevel(logging.INFO)

# Use the imported Pinot class and connection values
pinot_instance = Pinot()


async def main():
    logger.info("Starting Pinot MCP Table Ops Server")
    server = Server("pinot_mcp_table_ops_claude")

    @server.list_prompts()
    async def handle_list_prompts() -> list[types.Prompt]:
        logger.debug("Handling list_prompts request")
        return [
            types.Prompt(
                name="pinot-query",
                description=(
                    "A prompt to query the Pinot database with a Pinot MCP "
                    "Server + Claude"
                ),
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
                name="pause_consumption",
                description="Pause consumption of a realtime table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table",
                        },
                        "comment": {
                            "type": "string",
                            "description": "Optional comment",
                        },
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="resume_consumption",
                description="Resume consumption of a realtime table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table",
                        },
                        "comment": {
                            "type": "string",
                            "description": "Optional comment",
                        },
                        "consumeFrom": {
                            "type": "string",
                            "description": "lastConsumed | smallest | largest",
                            "enum": ["lastConsumed", "smallest", "largest"],
                        },
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="force_commit",
                description="Force commit the current consuming segments",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table",
                        },
                        "partitions": {
                            "type": "string",
                            "description": (
                                "Comma separated list of partition group IDs"
                            ),
                        },
                        "segments": {
                            "type": "string",
                            "description": (
                                "Comma separated list of consuming segments"
                            ),
                        },
                        "batchSize": {
                            "type": "integer",
                            "description": "Max segments to commit at once",
                        },
                        "batchStatusCheckIntervalSec": {
                            "type": "integer",
                            "description": "Interval to check batch status",
                        },
                        "batchStatusCheckTimeoutSec": {
                            "type": "integer",
                            "description": "Timeout for batch status check",
                        },
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="get_pause_status",
                description="Return pause status of a realtime table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table",
                        },
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="get_consuming_segments_info",
                description=(
                    "Gets the status of consumers from all servers for a realtime table"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Realtime table name with or without type",
                        },
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="reload-table-segments",
                description=(
                    "Reload all segments for a table (applies config changes, can "
                    "force download)"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table",
                        },
                        "type": {
                            "type": "string",
                            "description": "OFFLINE or REALTIME",
                            "enum": ["OFFLINE", "REALTIME"],
                        },
                        "forceDownload": {
                            "type": "boolean",
                            "description": (
                                "Whether to force servers to re-download segments"
                            ),
                            "default": False,
                        },
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="rebalance-table",
                description="Rebalances a table (reassign instances and segments)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table to rebalance",
                        },
                        "type": {
                            "type": "string",
                            "description": "OFFLINE or REALTIME",
                            "enum": ["OFFLINE", "REALTIME"],
                        },
                        "dryRun": {
                            "type": "boolean",
                            "description": "Dry run mode",
                            "default": False,
                        },
                        "reassignInstances": {
                            "type": "boolean",
                            "description": "Reassign instances before segments",
                            "default": True,
                        },
                        "includeConsuming": {
                            "type": "boolean",
                            "description": (
                                "Reassign CONSUMING segments (REALTIME only)"
                            ),
                            "default": True,
                        },
                        "bootstrap": {
                            "type": "boolean",
                            "description": (
                                "Bootstrap mode (ignore minimal data movement)"
                            ),
                            "default": False,
                        },
                        "downtime": {
                            "type": "boolean",
                            "description": "Allow downtime",
                            "default": False,
                        },
                        "minAvailableReplicas": {
                            "type": "integer",
                            "description": "Min replicas during no-downtime rebalance",
                            "default": -1,
                        },
                        # Add other rebalance parameters as needed
                    },
                    "required": ["tableName", "type"],
                },
            ),
            types.Tool(
                name="reset-table-segments",
                description=(
                    "Resets segments for a table (disable->wait->enable). Use "
                    "tableNameWithType (e.g., myTable_REALTIME)"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableNameWithType": {
                            "type": "string",
                            "description": (
                                "Table name with type suffix (e.g., myTable_REALTIME)"
                            ),
                        },
                        "errorSegmentsOnly": {
                            "type": "boolean",
                            "description": "Reset only segments in ERROR state",
                            "default": False,
                        },
                    },
                    "required": ["tableNameWithType"],
                },
            ),
            types.Tool(
                name="list-supported-indices",
                description="List the types of indices supported by Pinot",
                inputSchema={"type": "object", "properties": {}},
            ),
            types.Tool(
                name="create-schema",
                description="Adds a new schema to Pinot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schemaJson": {
                            "type": "string",
                            "description": "The schema definition in JSON format",
                        },
                        "override": {
                            "type": "boolean",
                            "description": "Override if schema exists",
                            "default": True,
                        },
                        "force": {
                            "type": "boolean",
                            "description": "Force override even if incompatible",
                            "default": False,
                        },
                    },
                    "required": ["schemaJson"],
                },
            ),
            types.Tool(
                name="update-schema",
                description="Updates an existing schema in Pinot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schemaName": {
                            "type": "string",
                            "description": "Name of the schema to update",
                        },
                        "schemaJson": {
                            "type": "string",
                            "description": (
                                "The updated schema definition in JSON format"
                            ),
                        },
                        "reload": {
                            "type": "boolean",
                            "description": "Reload table after update",
                            "default": False,
                        },
                        "force": {
                            "type": "boolean",
                            "description": "Force update even if incompatible",
                            "default": False,
                        },
                    },
                    "required": ["schemaName", "schemaJson"],
                },
            ),
            types.Tool(
                name="create-table-config",
                description="Adds a new table configuration to Pinot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableConfigJson": {
                            "type": "string",
                            "description": ("The table configuration in JSON format"),
                        },
                        "validationTypesToSkip": {
                            "type": "string",
                            "description": (
                                "Comma-separated validation types to skip "
                                "(ALL|TASK|UPSERT)"
                            ),
                        },
                    },
                    "required": ["tableConfigJson"],
                },
            ),
            types.Tool(
                name="update-table-config",
                description=(
                    "Updates an existing table configuration in Pinot (can be used "
                    "to add/modify indices)"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table to update",
                        },
                        "tableConfigJson": {
                            "type": "string",
                            "description": (
                                "The updated table configuration in JSON format"
                            ),
                        },
                        "validationTypesToSkip": {
                            "type": "string",
                            "description": (
                                "Comma-separated validation types to skip "
                                "(ALL|TASK|UPSERT)"
                            ),
                        },
                    },
                    "required": ["tableName", "tableConfigJson"],
                },
            ),
            types.Tool(
                name="add-index",
                description=(
                    "Adds a specified index type to one or more columns in a table "
                    "config and optionally reloads"
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table (without type suffix)",
                        },
                        "tableType": {
                            "type": "string",
                            "description": (
                                "OFFLINE or REALTIME (required if table has both types)"
                            ),
                            "enum": ["OFFLINE", "REALTIME"],
                        },
                        "indexType": {
                            "type": "string",
                            "description": "Type of index to add",
                            "enum": [
                                "inverted",
                                "range",
                                "text",
                                "json",
                                "bloom",
                                "fst",
                                "sorted",
                            ],
                        },
                        "columns": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": ("List of column names to add the index to"),
                        },
                        "triggerReload": {
                            "type": "boolean",
                            "description": (
                                "Reload the table segments after updating config"
                            ),
                            "default": True,
                        },
                        # Specific index configs (e.g., for JSON, FST) could be added
                        # here if needed
                    },
                    "required": ["tableName", "indexType", "columns"],
                },
            ),
            types.Tool(
                name="add-startree-index",
                description=(
                    "Adds a Star-Tree index configuration to a table config and "
                    "optionally reloads."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {
                            "type": "string",
                            "description": "Name of the table (without type suffix)",
                        },
                        "tableType": {
                            "type": "string",
                            "description": (
                                "OFFLINE or REALTIME (required if table has both types)"
                            ),
                            "enum": ["OFFLINE", "REALTIME"],
                        },
                        "dimensionsSplitOrder": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "List of dimension columns defining the tree structure"
                            ),
                        },
                        "functionColumnPairs": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                'Optional. Aggregations like ["SUM__colA", '
                                '"COUNT__*"]. Use this OR aggregationConfigsJson.'
                            ),
                            "default": [],
                        },
                        "aggregationConfigsJson": {
                            "type": "string",
                            "description": (
                                "Optional. JSON string for the "
                                "'aggregationConfigs' array (alternative to "
                                "functionColumnPairs)."
                            ),
                        },
                        "skipStarNodeCreationForDimensions": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "Optional. Dimensions for which to skip the "
                                "Star-node creation."
                            ),
                            "default": [],
                        },
                        "maxLeafRecords": {
                            "type": "integer",
                            "description": (
                                "Optional. Threshold T to determine whether to split "
                                "nodes further."
                            ),
                            "default": 10000,
                        },
                        "triggerReload": {
                            "type": "boolean",
                            "description": (
                                "Reload the table segments after updating config "
                                "(Note: Star-Tree often needs segment regeneration)"
                            ),
                            "default": True,
                        },
                    },
                    "required": ["tableName", "dimensionsSplitOrder"],
                },
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """Handle tool execution requests"""
        try:
            if name == "table-details":
                results = pinot_instance._get_table_detail(
                    tableName=arguments["tableName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "segment-list":
                results = pinot_instance._get_segments(tableName=arguments["tableName"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "index-column-details":
                results = pinot_instance._get_index_column_detail(
                    tableName=arguments["tableName"],
                    segmentName=arguments["segmentName"],
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "segment-metadata-details":
                results = pinot_instance._get_segment_metadata_detail(
                    tableName=arguments["tableName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "tableconfig-schema-details":
                results = pinot_instance._get_tableconfig_schema_detail(
                    tableName=arguments["tableName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "list-tables":
                results = pinot_instance._get_tables()
                return [types.TextContent(type="text", text=str(results))]

            elif name == "pause_consumption":
                results = pinot_instance._pause_consumption(
                    tableName=arguments["tableName"], comment=arguments.get("comment")
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "resume_consumption":
                results = pinot_instance._resume_consumption(
                    tableName=arguments["tableName"],
                    comment=arguments.get("comment"),
                    consumeFrom=arguments.get("consumeFrom"),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "force_commit":
                results = pinot_instance._force_commit(
                    tableName=arguments["tableName"],
                    partitions=arguments.get("partitions"),
                    segments=arguments.get("segments"),
                    batchSize=arguments.get("batchSize"),
                    batchStatusCheckIntervalSec=arguments.get(
                        "batchStatusCheckIntervalSec"
                    ),
                    batchStatusCheckTimeoutSec=arguments.get(
                        "batchStatusCheckTimeoutSec"
                    ),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "get_pause_status":
                results = pinot_instance._get_pause_status(
                    tableName=arguments["tableName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "get_consuming_segments_info":
                results = pinot_instance._get_consuming_segments_info(
                    tableName=arguments["tableName"]
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "reload-table-segments":
                results = pinot_instance._reload_table_segments(
                    tableName=arguments["tableName"],
                    tableType=arguments.get("type"),  # API uses 'type' query param
                    forceDownload=arguments.get("forceDownload", False),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "rebalance-table":
                results = pinot_instance._rebalance_table(
                    tableName=arguments["tableName"],
                    tableType=arguments["type"],
                    dryRun=arguments.get("dryRun", False),
                    reassignInstances=arguments.get("reassignInstances", True),
                    includeConsuming=arguments.get("includeConsuming", True),
                    bootstrap=arguments.get("bootstrap", False),
                    downtime=arguments.get("downtime", False),
                    minAvailableReplicas=arguments.get("minAvailableReplicas", -1),
                    # Pass other params as needed
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "reset-table-segments":
                results = pinot_instance._reset_table_segments(
                    tableNameWithType=arguments["tableNameWithType"],
                    errorSegmentsOnly=arguments.get("errorSegmentsOnly", False),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "list-supported-indices":
                # Based on web search and swagger definitions
                supported_indices = [
                    (
                        "Forward Index (Dictionary-encoded, Sorted, Raw Value) - "
                        "Default, based on encoding/sorting"
                    ),
                    "Inverted Index (Bitmap, Sorted) - For exact match filtering",
                    "Range Index - For range filtering (<, >, <=, >=)",
                    "Text Index (Native/Lucene) - For text search queries",
                    "JSON Index - For filtering fields within JSON blobs",
                    (
                        "Geospatial Index (H3) - For geospatial "
                        "distance/containment queries"
                    ),
                    "Timestamp Index - Optimized time filtering",
                    "Vector Index - For vector similarity search",
                    "Bloom Filter - Probabilistic filter to skip segments",
                    "Star-Tree Index - Pre-aggregation cube.",
                    (
                        "FST Index - For prefix/regex matching on "
                        "dictionary-encoded columns"
                    ),
                ]
                return [
                    types.TextContent(type="text", text="\n".join(supported_indices))
                ]

            elif name == "create-schema":
                results = pinot_instance._create_schema(
                    schemaJson=arguments["schemaJson"],
                    override=arguments.get("override", True),
                    force=arguments.get("force", False),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "update-schema":
                results = pinot_instance._update_schema(
                    schemaName=arguments["schemaName"],
                    schemaJson=arguments["schemaJson"],
                    reload=arguments.get("reload", False),
                    force=arguments.get("force", False),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "create-table-config":
                results = pinot_instance._create_table_config(
                    tableConfigJson=arguments["tableConfigJson"],
                    validationTypesToSkip=arguments.get("validationTypesToSkip"),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "update-table-config":
                results = pinot_instance._update_table_config(
                    tableName=arguments["tableName"],
                    tableConfigJson=arguments["tableConfigJson"],
                    validationTypesToSkip=arguments.get("validationTypesToSkip"),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "add-index":
                results = pinot_instance._add_index(
                    tableName=arguments["tableName"],
                    tableType=arguments.get("tableType"),
                    indexType=arguments["indexType"],
                    columns=arguments["columns"],
                    triggerReload=arguments.get("triggerReload", True),
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "add-startree-index":
                # Ensure only one of functionColumnPairs/aggregationConfigsJson is set
                if arguments.get("functionColumnPairs") and arguments.get(
                    "aggregationConfigsJson"
                ):
                    raise ValueError(
                        "Provide either 'functionColumnPairs' or "
                        "'aggregationConfigsJson', not both."
                    )

                results = pinot_instance._add_star_tree_index(
                    tableName=arguments["tableName"],
                    tableType=arguments.get("tableType"),
                    dimensionsSplitOrder=arguments["dimensionsSplitOrder"],
                    functionColumnPairs=arguments.get("functionColumnPairs", []),
                    aggregationConfigsJson=arguments.get("aggregationConfigsJson"),
                    skipStarNodeCreationForDimensions=arguments.get(
                        "skipStarNodeCreationForDimensions", []
                    ),
                    maxLeafRecords=arguments.get("maxLeafRecords", 10000),
                    triggerReload=arguments.get("triggerReload", True),
                )
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
                    server_name="pinot_mcp_table_ops_claude",
                    server_version="0.1.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )
    except Exception as e:
        import traceback

        logger.error(f"Error running MCP server: {e}")
        logger.error(traceback.format_exc())
        print(f"Error running MCP server: {e}", file=sys.stderr)
        print(traceback.format_exc(), file=sys.stderr)
        raise


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
