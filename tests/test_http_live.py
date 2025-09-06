"""
Live HTTP endpoint tests for MCP Pinot Server
These tests require both Pinot quickstart and MCP server to be running
"""

import json
import urllib.request

import pytest


def is_server_ready():
    """Check if MCP HTTP server is ready"""
    try:
        with urllib.request.urlopen(
            "http://127.0.0.1:8080/api/tools/list", timeout=2
        ) as response:
            return response.status == 200
    except Exception:
        return False


def is_pinot_ready():
    """Check if Pinot is ready"""
    try:
        import requests

        response = requests.get("http://localhost:9000/health", timeout=2)
        return response.status_code == 200
    except Exception:
        return False


@pytest.mark.skipif(not is_pinot_ready(), reason="Pinot quickstart not running")
@pytest.mark.skipif(not is_server_ready(), reason="MCP HTTP server not running")
class TestLiveHttpEndpoints:
    """Live tests for HTTP endpoints with real Pinot data"""

    def test_tools_list_endpoint(self):
        """Test GET /api/tools/list endpoint"""
        with urllib.request.urlopen("http://127.0.0.1:8080/api/tools/list") as response:
            assert response.status == 200
            data = json.loads(response.read().decode())
            tools = data.get("tools", [])
            assert len(tools) >= 4
            tool_names = [tool["name"] for tool in tools]
            assert "list-tables" in tool_names
            assert "read-query" in tool_names
            assert "test-connection" in tool_names

    def test_list_tables_via_http(self):
        """Test listing tables via HTTP API"""
        request_data = {"name": "list-tables", "arguments": {}}
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:8080/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            assert response.status == 200
            result = json.loads(response.read().decode())
            tables = result.get("result", [])
            assert len(tables) >= 5
            assert "airlineStats" in tables
            assert "githubEvents" in tables

    def test_query_execution_via_http(self):
        """Test executing queries via HTTP API"""
        request_data = {
            "name": "read-query",
            "arguments": {"query": "SELECT COUNT(*) as total FROM airlineStats"},
        }
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:8080/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            assert response.status == 200
            result = json.loads(response.read().decode())
            query_result = result.get("result", [])
            assert len(query_result) > 0
            assert "total" in query_result[0]
            count = query_result[0]["total"]
            assert count > 0

    def test_connection_via_http(self):
        """Test connection testing via HTTP API"""
        request_data = {"name": "test-connection", "arguments": {}}
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:8080/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            assert response.status == 200
            result = json.loads(response.read().decode())
            conn_result = result.get("result", {})
            assert conn_result.get("connection_test") is True
            assert conn_result.get("query_test") is True
            assert conn_result.get("tables_test") is True
            assert conn_result.get("tables_count", 0) > 0

    def test_sample_data_query_via_http(self):
        """Test querying sample data via HTTP API"""
        request_data = {
            "name": "read-query",
            "arguments": {"query": "SELECT * FROM githubEvents LIMIT 3"},
        }
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:8080/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            assert response.status == 200
            result = json.loads(response.read().decode())
            query_result = result.get("result", [])
            assert len(query_result) >= 3
            # Verify we have GitHub event data
            for event in query_result[:3]:
                assert "id" in event or "type" in event

    def test_table_details_via_http(self):
        """Test getting table details via HTTP API"""
        request_data = {
            "name": "table-details",
            "arguments": {"tableName": "airlineStats"},
        }
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:8080/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            assert response.status == 200
            result = json.loads(response.read().decode())
            table_details = result.get("result", {})
            assert "tableName" in table_details
            assert table_details["tableName"] == "airlineStats"

    def test_error_handling_invalid_tool(self):
        """Test error handling for invalid tool names"""
        request_data = {"name": "invalid-tool", "arguments": {}}
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:8080/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            assert response.status == 200  # Server responds but with error
            result = json.loads(response.read().decode())
            assert "result" in result
            error_result = result["result"]
            assert "error" in str(error_result).lower()

    def test_error_handling_invalid_query(self):
        """Test error handling for invalid SQL queries"""
        request_data = {
            "name": "read-query",
            "arguments": {"query": "DROP TABLE airlineStats"},  # Not allowed
        }
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            "http://127.0.0.1:8080/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            assert response.status == 200  # Server responds but with error
            result = json.loads(response.read().decode())
            assert "result" in result
            error_result = result["result"]
            assert (
                "error" in str(error_result).lower()
                or "only select" in str(error_result).lower()
            )
