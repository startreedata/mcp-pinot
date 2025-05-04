import logging
from typing import Any
import pandas as pd
import requests
import mcp.types as types
from pinotdb import connect
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger("pinot_mcp_claude")

# Get configuration from environment variables
PINOT_CONTROLLER_URL = os.getenv("PINOT_CONTROLLER_URL")
PINOT_BROKER_HOST = os.getenv("PINOT_BROKER_HOST")
PINOT_BROKER_PORT = int(os.getenv("PINOT_BROKER_PORT", "443"))
PINOT_BROKER_SCHEME = os.getenv("PINOT_BROKER_SCHEME", "https")
PINOT_USERNAME = os.getenv("PINOT_USERNAME")
PINOT_PASSWORD = os.getenv("PINOT_PASSWORD")
PINOT_USE_MSQE = os.getenv("PINOT_USE_MSQE", "false").lower() == "true"
PINOT_DATABASE = os.getenv("PINOT_DATABASE", "")
PINOT_TOKEN = os.getenv("PINOT_TOKEN", "")

HEADERS = {
    "accept": "application/json",
}
if PINOT_TOKEN:
    HEADERS["Authorization"] = PINOT_TOKEN

if PINOT_DATABASE:
    HEADERS["database"] = PINOT_DATABASE

conn = connect(
    host=PINOT_BROKER_HOST,
    port=PINOT_BROKER_PORT,
    path="/query/sql",
    scheme=PINOT_BROKER_SCHEME,
    username=PINOT_USERNAME,
    password=PINOT_PASSWORD,
    use_multistage_engine=PINOT_USE_MSQE,
    database=PINOT_DATABASE,
)


class Pinot:
    def __init__(self):
        self.insights: list[str] = []

    def _execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        logger.debug(f"Executing query: {query}")
        curs = conn.cursor()
        if PINOT_DATABASE:
            # Remove database name from query
            query = query.replace(f"{PINOT_DATABASE}.", "")
        curs.execute(query)
        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
        return df.to_dict(orient="records")

    def _get_tables(self, params: dict[str, Any] | None = None) -> list[str]:
        url = f"{PINOT_CONTROLLER_URL}/tables"
        return requests.get(url, headers=HEADERS).json()["tables"]

    def _get_table_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/size"
        return requests.get(url, headers=HEADERS).json()

    def _get_segment__metadata_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}/metadata"
        return requests.get(url, headers=HEADERS).json()

    def _get_segments(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}"
        return requests.get(url, headers=HEADERS).json()

    def _get_index_column_detail(self, tableName: str, segmentName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        for type_suffix in ["REALTIME", "OFFLINE"]:
            url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}_{type_suffix}/{segmentName}/metadata?columns=*"
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                return response.json()
        raise ValueError("Index column detail not found")

    def _get_tableconfig_schema_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tableConfigs/{tableName}"
        return requests.get(url, headers=HEADERS).json()

    def list_tools(self) -> list[types.Tool]:
        return [
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
            types.Tool(name="list-tables", description="List all Pinot tables", inputSchema={"type": "object", "properties": {}}),
            types.Tool(
                name="table-details",
                description="Get details about a single table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string"},
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
                        "tableName": {"type": "string"},
                    },
                    "required": ["tableName"],
                },
            ),
            types.Tool(
                name="index-column-details",
                description="Get index/column details",
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
                description="Get metadata for segments",
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
                description="Get table config/schema",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tableName": {"type": "string"},
                    },
                    "required": ["tableName"],
                },
            ),
        ]

    def handle_tool(self, name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
        match name:
            case "read-query":
                return [types.TextContent(type="text", text=str(self._execute_query(arguments["query"])))]
            case "list-tables":
                return [types.TextContent(type="text", text=str(self._get_tables()))]
            case "table-details":
                return [types.TextContent(type="text", text=str(self._get_table_detail(arguments["tableName"])))]
            case "segment-list":
                return [types.TextContent(type="text", text=str(self._get_segments(arguments["tableName"])))]
            case "index-column-details":
                return [types.TextContent(type="text", text=str(self._get_index_column_detail(arguments["tableName"], arguments["segmentName"])))]
            case "segment-metadata-details":
                return [types.TextContent(type="text", text=str(self._get_segment__metadata_detail(arguments["tableName"])))]
            case "tableconfig-schema-details":
                return [types.TextContent(type="text", text=str(self._get_tableconfig_schema_detail(arguments["tableName"])))]
            case _:
                raise ValueError(f"Unknown tool: {name}")
