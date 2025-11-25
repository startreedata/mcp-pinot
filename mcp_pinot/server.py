# --------------------------
# File: mcp_pinot/server.py
# --------------------------
"""
FastMCP-based implementation for the Apache Pinot MCP Server.
"""

import json
from datetime import datetime, timedelta
from typing import Optional

from fastmcp import FastMCP
from fastmcp.server.auth.oidc_proxy import OAuthProxy
from fastmcp.server.auth.providers.jwt import JWTVerifier
import uvicorn

from mcp_pinot.config import load_oauth_config, load_pinot_config, load_server_config
from mcp_pinot.pinot_client import PinotClient
from mcp_pinot.prompts import PROMPT_TEMPLATE

# Initialize configurations and create client
pinot_config = load_pinot_config()
server_config = load_server_config()
pinot_client = PinotClient(pinot_config)


mcp = FastMCP("Pinot MCP Server")

if server_config.oauth_enabled:
    oauth_config = load_oauth_config()

    token_verifier = JWTVerifier(
        jwks_uri=oauth_config.jwks_uri,
        issuer=oauth_config.issuer,
        audience=oauth_config.audience,
    )

    mcp.auth = OAuthProxy(
        upstream_authorization_endpoint=oauth_config.upstream_authorization_endpoint,
        upstream_token_endpoint=oauth_config.upstream_token_endpoint,
        upstream_client_id=oauth_config.client_id,
        upstream_client_secret=oauth_config.client_secret,
        token_verifier=token_verifier,
        extra_authorize_params=oauth_config.extra_authorize_params,
        base_url=oauth_config.base_url,
    )


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


@mcp.tool
def search_volume_by_term(
    search_term: str
) -> str:
    """Get search volume and average number of results from the keyword_research_search_data table for a search term for the last 7 days"""
    try:
        # Calculate date range for last 7 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Format dates for SQL (assuming ISO format or timestamp format)
        # Pinot typically uses timestamp in milliseconds or ISO format
        # Adjust format based on your schema - using ISO format here
        start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # Escape single quotes in search_term_id to prevent SQL injection
        # Replace single quotes with two single quotes (SQL escape)
        escaped_search_term = search_term.replace("'", "''")
        
        # Build the query - matching the original template structure
        # Using search_term = ? as per template (search_term_id parameter contains the ID value)
        query = f"""SELECT search_term, 
            SUM(search_count) AS search_volume,
            SUM(search_results_total) / SUM(search_count) AS avg_search_results
            FROM keyword_research_search_data
            WHERE search_term = '{escaped_search_term}'
            AND search_date_hour BETWEEN '{start_date_str}' AND '{end_date_str}'
            GROUP BY search_term
            LIMIT 1"""
        
        results = pinot_client.execute_query(query=query)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error: {str(e)}"


@mcp.tool
def top_search_terms_by_volume() -> str:
    """Get the top 20 search terms by search volume from the keyword_research_search_data table for the last 7 days"""
    try:
        # Calculate date range for last 7 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Format dates for SQL (assuming ISO format or timestamp format)
        # Pinot typically uses timestamp in milliseconds or ISO format
        # Adjust format based on your schema - using ISO format here
        start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # Build the query to get top 20 search terms by volume
        query = f"""SELECT search_term, 
            SUM(search_count) AS search_volume,
            SUM(search_results_total) / SUM(search_count) AS avg_search_results
            FROM keyword_research_search_data
            WHERE search_date_hour BETWEEN '{start_date_str}' AND '{end_date_str}'
            GROUP BY search_term
            ORDER BY search_volume DESC
            LIMIT 20"""
        
        results = pinot_client.execute_query(query=query)
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
    if tls_enabled:
        app = mcp.http_app(path=server_config.path)
        uvicorn.run(
            app,
            host=server_config.host,
            port=server_config.port,
            ssl_keyfile=server_config.ssl_keyfile,
            ssl_certfile=server_config.ssl_certfile,
        )
    elif server_config.transport == "stdio":
        # stdio transport - no configuration needed
        mcp.run(transport=server_config.transport)
    else:
        mcp.run(
            transport=server_config.transport,
            host=server_config.host,
            port=server_config.port,
            path=server_config.path,
        )


if __name__ == "__main__":
    main()
