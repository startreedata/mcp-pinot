# --------------------------
# File: mcp_pinot/server.py
# --------------------------
"""
FastMCP-based implementation for the Apache Pinot MCP Server.
"""

import json
from typing import Optional

from fastmcp import FastMCP
from fastmcp.server.auth.providers.jwt import JWTVerifier
import uvicorn

from mcp_pinot.config import load_pinot_config, load_server_config
from mcp_pinot.pinot_client import PinotClient
from mcp_pinot.prompts import PROMPT_TEMPLATE

# Initialize configurations and create client
pinot_config = load_pinot_config()
server_config = load_server_config()
pinot_client = PinotClient(pinot_config)


mcp = FastMCP("Pinot MCP Server")


@mcp.tool
def test_connection() -> str:
    """Test Pinot connection and return diagnostics"""
    try:
        results = pinot_client.test_connection()
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def read_query(query: str) -> str:
    """Execute a SELECT query on the Pinot database"""
    try:
        if not query.strip().upper().startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed for read-query")
        results = pinot_client.execute_query(query=query)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def list_tables() -> str:
    """List all tables in Pinot"""
    try:
        results = pinot_client.get_tables()
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def table_details(tableName: str) -> str:
    """Get table size details"""
    try:
        results = pinot_client.get_table_detail(tableName=tableName)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def segment_list(tableName: str) -> str:
    """List segments for a table"""
    try:
        results = pinot_client.get_segments(tableName=tableName)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def index_column_details(tableName: str, segmentName: str) -> str:
    """Get index/column details for a segment"""
    try:
        results = pinot_client.get_index_column_detail(
            tableName=tableName,
            segmentName=segmentName,
        )
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def segment_metadata_details(tableName: str) -> str:
    """Get metadata for segments of a table"""
    try:
        results = pinot_client.get_segment_metadata_detail(tableName=tableName)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def tableconfig_schema_details(tableName: str) -> str:
    """Get table config and schema"""
    try:
        results = pinot_client.get_tableconfig_schema_detail(tableName=tableName)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def create_schema(schemaJson: str, override: bool = True, force: bool = False) -> str:
    """Create a new schema"""
    try:
        results = pinot_client.create_schema(
            schemaJson,
            override,
            force,
        )
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def update_schema(
    schemaName: str, schemaJson: str, reload: bool = False, force: bool = False
) -> str:
    """Update an existing schema"""
    try:
        results = pinot_client.update_schema(
            schemaName,
            schemaJson,
            reload,
            force,
        )
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def get_schema(schemaName: str) -> str:
    """Fetch a schema by name"""
    try:
        results = pinot_client.get_schema(schemaName=schemaName)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def create_table_config(
    tableConfigJson: str, validationTypesToSkip: Optional[str] = None
) -> str:
    """Create table configuration"""
    try:
        results = pinot_client.create_table_config(
            tableConfigJson,
            validationTypesToSkip,
        )
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def update_table_config(
    tableName: str,
    tableConfigJson: str,
    validationTypesToSkip: Optional[str] = None,
) -> str:
    """Update table configuration"""
    try:
        results = pinot_client.update_table_config(
            tableName,
            tableConfigJson,
            validationTypesToSkip,
        )
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def get_table_config(tableName: str, tableType: Optional[str] = None) -> str:
    """Get table configuration"""
    try:
        results = pinot_client.get_table_config(
            tableName=tableName,
            tableType=tableType,
        )
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.prompt
def pinot_query() -> str:
    """Query Pinot database with MCP Server + Claude"""
    return PROMPT_TEMPLATE.strip()


def main():
    """Main entry point for FastMCP Pinot Server"""
    tls_enabled = server_config.ssl_keyfile and server_config.ssl_certfile
    if (
        server_config.transport == "http"
        or server_config.transport == "streamable-http"
    ) and tls_enabled:
        app = mcp.http_app()
        uvicorn.run(
            app,
            host=server_config.host,
            port=server_config.port,
            ssl_keyfile=server_config.ssl_keyfile,
            ssl_certfile=server_config.ssl_certfile,
        )
    else:
        mcp.run(
            transport=server_config.transport,
            host=server_config.host,
            port=server_config.port,
        )


if __name__ == "__main__":
    main()
