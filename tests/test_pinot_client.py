import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from mcp_pinot.utils.pinot_client import Pinot

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
    with patch("mcp_pinot.utils.pinot_client.initialize_connection") as mock_init_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.__iter__.return_value = [(1, "Test 1"), (2, "Test 2")]
        mock_conn.cursor.return_value = mock_cursor
        mock_init_conn.return_value = mock_conn
        yield mock_init_conn

@pytest.fixture
def mock_requests():
    """Fixture to mock the requests module."""
    with patch("mcp_pinot.utils.pinot_client.requests") as mock_req:
        mock_response = MagicMock()
        mock_response.json.return_value = {"tables": ["table1", "table2"]}
        mock_req.get.return_value = mock_response
        yield mock_req

def test_pinot_init():
    """Test that Pinot class initializes correctly."""
    pinot = Pinot()
    assert isinstance(pinot.insights, list)
    assert len(pinot.insights) == 0

def test_execute_query(mock_connection):
    """Test the execute_query function."""
    pinot = Pinot()
    result = pinot._execute_query("SELECT * FROM my_table")
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[0]["name"] == "Test 1"

def test_execute_query_empty_result(mock_connection):
    """Test execute_query with an empty result set."""
    # Modify the mock to return empty results
    mock_conn = mock_connection.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.__iter__.return_value = []
    
    pinot = Pinot()
    result = pinot._execute_query("SELECT * FROM my_table WHERE id = 999")
    assert isinstance(result, list)
    assert len(result) == 0

def test_execute_query_with_error(mock_connection):
    """Test execute_query with a database error."""
    mock_connection.side_effect = Exception("Database error")
    
    pinot = Pinot()
    with pytest.raises(Exception, match="Database error"):
        pinot._execute_query("SELECT * FROM my_table")

def test_pinot_get_tables(mock_requests):
    """Test the _get_tables method."""
    pinot = Pinot()
    tables = pinot._get_tables()
    assert isinstance(tables, list)
    assert "table1" in tables
    assert "table2" in tables

def test_pinot_get_table_detail(mock_requests):
    """Test the _get_table_detail method."""
    mock_requests.get.return_value.json.return_value = {
        "tableName": "test_table",
        "columnCount": 5
    }
    
    pinot = Pinot()
    detail = pinot._get_table_detail("test_table")
    assert isinstance(detail, dict)
    assert detail["tableName"] == "test_table"
    assert detail["columnCount"] == 5

def test_pinot_list_tools():
    """Test the list_tools method."""
    pinot = Pinot()
    tools = pinot.list_tools()
    assert isinstance(tools, list)
    assert len(tools) > 0
    
    # Check that each tool has the required attributes
    for tool in tools:
        assert hasattr(tool, "name")
        assert hasattr(tool, "description")
        assert hasattr(tool, "inputSchema") 