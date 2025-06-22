from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from mcp_pinot.config import load_pinot_config
from mcp_pinot.pinot_client import PinotClient

# Mock data for testing
MOCK_TABLE_DATA = [
    {"id": 1, "name": "Table 1"},
    {"id": 2, "name": "Table 2"}
]

MOCK_QUERY_RESULT = pd.DataFrame([
    {"id": 1, "name": "Test 1"},
    {"id": 2, "name": "Test 2"}
])

@pytest.fixture
def mock_connection():
    """Fixture to mock the Pinot connection."""
    with patch("mcp_pinot.utils.pinot_client.connect") as mock_connect:
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.__iter__.return_value = [(1, "Test 1"), (2, "Test 2")]
        mock_connect.return_value.cursor.return_value = mock_cursor
        yield mock_connect

@pytest.fixture
def mock_requests():
    """Fixture to mock the requests module."""
    with patch("mcp_pinot.utils.pinot_client.requests") as mock_req:
        mock_response = MagicMock()
        mock_response.json.return_value = {"tables": ["table1", "table2"]}
        mock_req.get.return_value = mock_response
        yield mock_req

def test_pinot_init():
    """Test that PinotClient class initializes correctly."""
    config = load_pinot_config()
    pinot = PinotClient(config)
    assert isinstance(pinot.insights, list)
    assert len(pinot.insights) == 0

@patch.object(PinotClient, "http_request")
def test_execute_query(mock_http_request):
    """Test the execute_query function."""
    # Mock HTTP request to fail so it falls back to PinotDB
    mock_http_request.side_effect = Exception("HTTP failed")

    config = load_pinot_config()
    pinot = PinotClient(config)

    # Mock the get_connection method
    with patch.object(pinot, 'get_connection') as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test 1"), (2, "Test 2")]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = pinot.execute_query("SELECT * FROM my_table")
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Test 1"

@patch.object(PinotClient, "http_request")
def test_execute_query_empty_result(mock_http_request):
    """Test execute_query with an empty result set."""
    # Mock HTTP request to fail so it falls back to PinotDB
    mock_http_request.side_effect = Exception("HTTP failed")

    config = load_pinot_config()
    pinot = PinotClient(config)

    # Mock the get_connection method
    with patch.object(pinot, 'get_connection') as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        result = pinot.execute_query("SELECT * FROM my_table WHERE id = 999")
        assert isinstance(result, list)
        assert len(result) == 0

@patch.object(PinotClient, "http_request")
def test_execute_query_with_error(mock_http_request):
    """Test execute_query with a database error."""
    # Mock HTTP request to fail
    mock_http_request.side_effect = Exception("HTTP failed")

    config = load_pinot_config()
    pinot = PinotClient(config)

    # Mock the get_connection method to also fail
    with patch.object(pinot, 'get_connection') as mock_get_conn:
        mock_get_conn.side_effect = Exception("Database error")

        with pytest.raises(Exception, match="Database error"):
            pinot.execute_query("SELECT * FROM my_table")

@patch("mcp_pinot.config.load_pinot_config")
@patch.object(PinotClient, "http_request")
def test_pinot_get_tables(mock_http_request, mock_config):
    """Test the get_tables method."""
    # Mock config
    from mcp_pinot.config import PinotConfig
    mock_config.return_value = PinotConfig(
        controller_url="http://localhost:9000",
        broker_host="localhost",
        broker_port=8099,
        broker_scheme="http",
        username=None,
        password=None,
        token=None,
        database="",
        use_msqe=False
    )

    # Mock HTTP response
    mock_response = MagicMock()
    mock_response.json.return_value = {"tables": ["table1", "table2"]}
    mock_http_request.return_value = mock_response

    config = load_pinot_config()
    pinot = PinotClient(config)
    tables = pinot.get_tables()
    assert isinstance(tables, list)
    assert "table1" in tables
    assert "table2" in tables

@patch("mcp_pinot.config.load_pinot_config")
@patch.object(PinotClient, "http_request")
def test_pinot_get_table_detail(mock_http_request, mock_config):
    """Test the get_table_detail method."""
    # Mock config
    from mcp_pinot.config import PinotConfig
    mock_config.return_value = PinotConfig(
        controller_url="http://localhost:9000",
        broker_host="localhost",
        broker_port=8099,
        broker_scheme="http",
        username=None,
        password=None,
        token=None,
        database="",
        use_msqe=False
    )

    # Mock HTTP response
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "tableName": "test_table",
        "columnCount": 5
    }
    mock_http_request.return_value = mock_response

    config = load_pinot_config()
    pinot = PinotClient(config)
    detail = pinot.get_table_detail("test_table")
    assert isinstance(detail, dict)
    assert detail["tableName"] == "test_table"
    assert detail["columnCount"] == 5

def test_pinot_list_tools():
    """Test the list_tools method."""
    config = load_pinot_config()
    pinot = PinotClient(config)
    tools = pinot.list_tools()
    assert isinstance(tools, list)
    assert len(tools) > 0

    # Check that each tool has the required attributes
    for tool in tools:
        assert hasattr(tool, "name")
        assert hasattr(tool, "description")
        assert hasattr(tool, "inputSchema")
