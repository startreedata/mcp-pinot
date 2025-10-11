import json
from unittest.mock import patch

from fastmcp import Client
import pytest

from mcp_pinot.server import main, mcp


@pytest.fixture
def mock_pinot_client():
    """Fixture to mock the PinotClient."""
    with patch("mcp_pinot.server.pinot_client") as mock_client:
        mock_client.test_connection.return_value = {"status": "connected"}
        mock_client.execute_query.return_value = {
            "resultTable": {"rows": [["test", "data"]]},
            "numRowsResultSet": 1,
        }
        mock_client.get_tables.return_value = {"tables": ["test_table"]}
        mock_client.get_table_detail.return_value = {
            "tableName": "test_table",
            "columnCount": 5,
        }
        mock_client.get_segments.return_value = {"segments": ["segment1"]}
        mock_client.get_index_column_detail.return_value = {"indexes": ["index1"]}
        mock_client.get_segment_metadata_detail.return_value = {"metadata": "test"}
        mock_client.get_tableconfig_schema_detail.return_value = {"config": "test"}
        mock_client.create_schema.return_value = {"status": "created"}
        mock_client.update_schema.return_value = {"status": "updated"}
        mock_client.get_schema.return_value = {"schema": "test"}
        mock_client.create_table_config.return_value = {"status": "created"}
        mock_client.update_table_config.return_value = {"status": "updated"}
        mock_client.get_table_config.return_value = {"config": "test"}
        yield mock_client


class TestFastMCPServer:
    """Test the FastMCP-based server implementation"""

    def test_mcp_instance_creation(self):
        """Test that the FastMCP instance is created correctly"""
        assert mcp is not None
        assert mcp.name == "Pinot MCP Server"

    @pytest.mark.asyncio
    async def test_tools_registration(self):
        """Test that all tools are properly registered"""
        # Get the registered tools
        tools = await mcp.get_tools()

        # Check that all expected tools are registered
        expected_tools = [
            "test_connection",
            "read_query",
            "list_tables",
            "table_details",
            "segment_list",
            "index_column_details",
            "segment_metadata_details",
            "tableconfig_schema_details",
            "create_schema",
            "update_schema",
            "get_schema",
            "create_table_config",
            "update_table_config",
            "get_table_config",
        ]

        for tool_name in expected_tools:
            assert tool_name in tools, f"Tool {tool_name} not found in registered tools"

    @pytest.mark.asyncio
    async def test_prompts_registration(self):
        """Test that prompts are properly registered"""
        # Get the registered prompts
        prompts = await mcp.get_prompts()

        # Check that the expected prompt is registered
        assert "pinot_query" in prompts, (
            "Prompt pinot_query not found in registered prompts"
        )

    @pytest.mark.asyncio
    async def test_tool_test_connection(self, mock_pinot_client):
        """Test the test_connection tool"""
        async with Client(mcp) as client:
            result = await client.call_tool("test_connection", {})

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert data["status"] == "connected"

    @pytest.mark.asyncio
    async def test_tool_test_connection_error(self, mock_pinot_client):
        """Test the test_connection tool with error"""
        # Mock client to raise exception
        mock_pinot_client.test_connection.side_effect = Exception("Connection failed")

        async with Client(mcp) as client:
            result = await client.call_tool("test_connection", {})

            # Should return error message
            assert "Error: Connection failed" in result.data

    @pytest.mark.asyncio
    async def test_tool_read_query(self, mock_pinot_client):
        """Test the read_query tool"""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "read_query", {"query": "SELECT * FROM test_table"}
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert "resultTable" in data

    @pytest.mark.asyncio
    async def test_tool_read_query_invalid(self, mock_pinot_client):
        """Test the read_query tool with invalid query"""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "read_query", {"query": "INSERT INTO test_table VALUES (1)"}
            )

            # Should return error message
            assert "Error: Only SELECT queries are allowed" in result.data

    @pytest.mark.asyncio
    async def test_tool_read_query_error(self, mock_pinot_client):
        """Test the read_query tool with error"""
        # Mock client to raise exception
        mock_pinot_client.execute_query.side_effect = Exception("Query failed")

        async with Client(mcp) as client:
            result = await client.call_tool(
                "read_query", {"query": "SELECT * FROM test_table"}
            )

            # Should return error message
            assert "Error: Query failed" in result.data

    @pytest.mark.asyncio
    async def test_tool_list_tables(self, mock_pinot_client):
        """Test the list_tables tool"""
        async with Client(mcp) as client:
            result = await client.call_tool("list_tables", {})

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert "tables" in data

    @pytest.mark.asyncio
    async def test_tool_table_details(self, mock_pinot_client):
        """Test the table_details tool"""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "table_details", {"tableName": "test_table"}
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert data["tableName"] == "test_table"

    @pytest.mark.asyncio
    async def test_tool_segment_list(self, mock_pinot_client):
        """Test the segment_list tool"""
        async with Client(mcp) as client:
            result = await client.call_tool("segment_list", {"tableName": "test_table"})

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert "segments" in data

    @pytest.mark.asyncio
    async def test_tool_index_column_details(self, mock_pinot_client):
        """Test the index_column_details tool"""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "index_column_details",
                {"tableName": "test_table", "segmentName": "segment1"},
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert "indexes" in data

    @pytest.mark.asyncio
    async def test_tool_segment_metadata_details(self, mock_pinot_client):
        """Test the segment_metadata_details tool"""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "segment_metadata_details", {"tableName": "test_table"}
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert "metadata" in data

    @pytest.mark.asyncio
    async def test_tool_tableconfig_schema_details(self, mock_pinot_client):
        """Test the tableconfig_schema_details tool"""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "tableconfig_schema_details", {"tableName": "test_table"}
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert "config" in data

    @pytest.mark.asyncio
    async def test_tool_create_schema(self, mock_pinot_client):
        """Test the create_schema tool"""
        schema_json = '{"schemaName": "test", "dimensionFieldSpecs": []}'

        async with Client(mcp) as client:
            result = await client.call_tool(
                "create_schema", {"schemaJson": schema_json}
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert data["status"] == "created"

    @pytest.mark.asyncio
    async def test_tool_update_schema(self, mock_pinot_client):
        """Test the update_schema tool"""
        schema_json = '{"schemaName": "test", "dimensionFieldSpecs": []}'

        async with Client(mcp) as client:
            result = await client.call_tool(
                "update_schema", {"schemaName": "test", "schemaJson": schema_json}
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert data["status"] == "updated"

    @pytest.mark.asyncio
    async def test_tool_get_schema(self, mock_pinot_client):
        """Test the get_schema tool"""
        async with Client(mcp) as client:
            result = await client.call_tool("get_schema", {"schemaName": "test"})

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert "schema" in data

    @pytest.mark.asyncio
    async def test_tool_create_table_config(self, mock_pinot_client):
        """Test the create_table_config tool"""
        config_json = '{"tableName": "test", "tableType": "OFFLINE"}'

        async with Client(mcp) as client:
            result = await client.call_tool(
                "create_table_config", {"tableConfigJson": config_json}
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert data["status"] == "created"

    @pytest.mark.asyncio
    async def test_tool_update_table_config(self, mock_pinot_client):
        """Test the update_table_config tool"""
        config_json = '{"tableName": "test", "tableType": "OFFLINE"}'

        async with Client(mcp) as client:
            result = await client.call_tool(
                "update_table_config",
                {"tableName": "test", "tableConfigJson": config_json},
            )

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert data["status"] == "updated"

    @pytest.mark.asyncio
    async def test_tool_get_table_config(self, mock_pinot_client):
        """Test the get_table_config tool"""
        async with Client(mcp) as client:
            result = await client.call_tool("get_table_config", {"tableName": "test"})

            # Should return JSON string
            assert isinstance(result.data, str)
            data = json.loads(result.data)
            assert "config" in data

    @pytest.mark.asyncio
    async def test_prompt_pinot_query(self):
        """Test the pinot_query prompt"""
        async with Client(mcp) as client:
            result = await client.get_prompt("pinot_query", {})

            # Should return the prompt template
            assert len(result.messages) > 0
            assert hasattr(result.messages[0].content, "text")
            assert len(result.messages[0].content.text) > 0

    @pytest.mark.asyncio
    async def test_tool_test_connection_error(self, mock_pinot_client):
        """Test the test_connection tool with error"""
        # Mock client to raise exception
        mock_pinot_client.test_connection.side_effect = Exception("Connection failed")

class TestMainFunction:
    """Test the main function with different configurations"""

    def test_main_function_http_transport(self, mock_pinot_client):
        """Test the main function with HTTP transport"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "http"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.ssl_keyfile = None
            mock_server_config.ssl_certfile = None

            with patch("mcp_pinot.server.mcp.run") as mock_mcp_run:
                # Call the main function
                main()

                # Verify mcp.run was called
                mock_mcp_run.assert_called_once()
                call_args = mock_mcp_run.call_args
                assert call_args[1]["transport"] == "http"

    def test_main_function_http_transport_with_ssl(self, mock_pinot_client):
        """Test the main function with HTTP transport and SSL"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "http"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.ssl_keyfile = "/path/to/key.pem"
            mock_server_config.ssl_certfile = "/path/to/cert.pem"

            with patch("mcp_pinot.server.uvicorn.run") as mock_uvicorn_run:
                # Call the main function
                main()

                # Verify uvicorn.run was called with SSL parameters
                mock_uvicorn_run.assert_called_once()
                call_args = mock_uvicorn_run.call_args
                assert call_args[1]["ssl_keyfile"] == "/path/to/key.pem"
                assert call_args[1]["ssl_certfile"] == "/path/to/cert.pem"

    def test_main_function_streamable_http_transport(self, mock_pinot_client):
        """Test the main function with streamable-http transport"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "streamable-http"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.ssl_keyfile = None
            mock_server_config.ssl_certfile = None

            with patch("mcp_pinot.server.mcp.run") as mock_mcp_run:
                # Call the main function
                main()

                # Verify mcp.run was called
                mock_mcp_run.assert_called_once()
                call_args = mock_mcp_run.call_args
                assert call_args[1]["transport"] == "streamable-http"

    def test_main_function_stdio_transport(self, mock_pinot_client):
        """Test the main function with STDIO transport"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "stdio"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000

            with patch("mcp_pinot.server.mcp.run") as mock_mcp_run:
                # Call the main function
                main()

                # Verify mcp.run was called
                mock_mcp_run.assert_called_once()
                call_args = mock_mcp_run.call_args
                assert call_args[1]["transport"] == "stdio"
