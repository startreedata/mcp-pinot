from unittest.mock import MagicMock, patch

import pytest

from mcp_pinot.config import PinotConfig
from mcp_pinot.pinot_client import PinotClient


@pytest.fixture
def mock_pinot_config():
    """Fixture to create a mock PinotConfig."""
    return PinotConfig(
        controller_url="http://localhost:9000",
        broker_host="localhost",
        broker_port=8000,
        broker_scheme="http",
        username=None,
        password=None,
        token=None,
        database="",
        use_msqe=False,
        request_timeout=60,
        connection_timeout=60,
        query_timeout=60,
    )


@pytest.fixture
def mock_connection():
    """Fixture to mock the Pinot connection."""
    with patch("mcp_pinot.pinot_client.create_connection") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.fetchall.return_value = [(1, "Test 1"), (2, "Test 2")]
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_conn


@pytest.fixture
def mock_requests():
    """Fixture to mock the requests module."""
    with patch("mcp_pinot.pinot_client.requests") as mock_req:
        mock_response = MagicMock()
        mock_response.json.return_value = {"tables": ["table1", "table2"]}
        mock_response.raise_for_status.return_value = None
        mock_req.get.return_value = mock_response
        mock_req.post.return_value = mock_response
        mock_req.put.return_value = mock_response
        yield mock_req


class TestPinotClient:
    """Test the PinotClient class"""

    def test_pinot_client_init(self, mock_pinot_config):
        """Test that PinotClient initializes correctly."""
        pinot = PinotClient(mock_pinot_config)
        assert pinot.config == mock_pinot_config
        assert isinstance(pinot.insights, list)
        assert len(pinot.insights) == 0
        assert pinot._conn is None

    def test_create_auth_headers_no_auth(self, mock_pinot_config):
        """Test auth headers creation with no authentication."""
        pinot = PinotClient(mock_pinot_config)
        headers = pinot._create_auth_headers()

        assert headers["accept"] == "application/json"
        assert headers["Content-Type"] == "application/json"
        assert "Authorization" not in headers

    def test_create_auth_headers_with_token(self, mock_pinot_config):
        """Test auth headers creation with token authentication."""
        mock_pinot_config.token = "Bearer test_token"
        pinot = PinotClient(mock_pinot_config)
        headers = pinot._create_auth_headers()

        assert headers["Authorization"] == "Bearer test_token"

    def test_create_auth_headers_with_username_password(self, mock_pinot_config):
        """Test auth headers creation with username/password authentication."""
        mock_pinot_config.username = "test_user"
        mock_pinot_config.password = "test_pass"
        pinot = PinotClient(mock_pinot_config)
        headers = pinot._create_auth_headers()

        assert headers["Authorization"].startswith("Basic ")
        # Decode and verify the basic auth
        import base64

        decoded = base64.b64decode(headers["Authorization"][6:]).decode()
        assert decoded == "test_user:test_pass"

    def test_create_auth_headers_with_database(self, mock_pinot_config):
        """Test auth headers creation with database."""
        mock_pinot_config.database = "test_db"
        pinot = PinotClient(mock_pinot_config)
        headers = pinot._create_auth_headers()

        assert headers["database"] == "test_db"

    def test_http_request_get(self, mock_pinot_config, mock_requests):
        """Test HTTP GET request."""
        pinot = PinotClient(mock_pinot_config)
        response = pinot.http_request("http://test.com/api")

        mock_requests.get.assert_called_once()
        assert response == mock_requests.get.return_value

    def test_http_request_post(self, mock_pinot_config, mock_requests):
        """Test HTTP POST request."""
        pinot = PinotClient(mock_pinot_config)
        data = {"test": "data"}
        response = pinot.http_request("http://test.com/api", "POST", data)

        mock_requests.post.assert_called_once()
        assert response == mock_requests.post.return_value

    def test_get_connection_creates_new(self, mock_pinot_config, mock_connection):
        """Test get_connection creates new connection when none exists."""
        pinot = PinotClient(mock_pinot_config)
        conn = pinot.get_connection()

        assert conn == mock_connection
        assert pinot._conn == mock_connection

    def test_get_connection_reuses_existing(self, mock_pinot_config, mock_connection):
        """Test get_connection reuses existing connection."""
        pinot = PinotClient(mock_pinot_config)
        pinot._conn = mock_connection

        conn = pinot.get_connection()
        assert conn == mock_connection

    def test_get_connection_creates_new_on_error(self, mock_pinot_config):
        """Test get_connection creates new connection when existing fails."""
        with patch("mcp_pinot.pinot_client.create_connection") as mock_create:
            with patch("mcp_pinot.pinot_client.test_connection_query") as mock_test:
                # Mock connection that will fail the test
                mock_conn = MagicMock()
                mock_create.return_value = mock_conn

                # Make test_connection_query fail to trigger new connection creation
                mock_test.side_effect = Exception("Connection test failed")

                pinot = PinotClient(mock_pinot_config)
                pinot._conn = MagicMock()  # Set existing connection that will fail test

                conn = pinot.get_connection()
                assert conn == mock_conn
                assert pinot._conn == mock_conn
                # Should have called create_connection once to create new connection
                mock_create.assert_called_once()

    def test_test_connection_success(
        self, mock_pinot_config, mock_connection, mock_requests
    ):
        """Test successful connection test."""
        pinot = PinotClient(mock_pinot_config)

        # Mock get_tables to return some tables
        with patch.object(pinot, "get_tables") as mock_get_tables:
            mock_get_tables.return_value = ["table1", "table2"]

            result = pinot.test_connection()

            assert result["connection_test"] is True
            assert result["query_test"] is True
            assert result["tables_test"] is True
            assert result["error"] is None
            assert result["tables_count"] == 2

    def test_test_connection_failure(self, mock_pinot_config):
        """Test connection test with failure."""
        pinot = PinotClient(mock_pinot_config)

        with patch.object(pinot, "get_connection") as mock_get_conn:
            mock_get_conn.side_effect = Exception("Connection failed")

            result = pinot.test_connection()

            assert result["connection_test"] is False
            assert result["query_test"] is False
            assert result["tables_test"] is False
            assert result["error"] == "Connection failed"

    def test_execute_query_http_success(self, mock_pinot_config, mock_requests):
        """Test successful HTTP query execution."""
        pinot = PinotClient(mock_pinot_config)

        # Mock HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "resultTable": {
                "dataSchema": {"columnNames": ["id", "name"]},
                "rows": [[1, "Test 1"], [2, "Test 2"]],
            }
        }
        mock_requests.post.return_value = mock_response

        result = pinot.execute_query_http("SELECT * FROM test_table")

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["name"] == "Test 1"
        assert result[1]["id"] == 2
        assert result[1]["name"] == "Test 2"

    def test_execute_query_http_with_exceptions(self, mock_pinot_config, mock_requests):
        """Test HTTP query execution with exceptions in response."""
        pinot = PinotClient(mock_pinot_config)

        # Mock HTTP response with exceptions
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "exceptions": ["Query error: Table not found"]
        }
        mock_requests.post.return_value = mock_response

        with pytest.raises(Exception, match="Query error"):
            pinot.execute_query_http("SELECT * FROM nonexistent_table")

    def test_execute_query_http_no_result_table(self, mock_pinot_config, mock_requests):
        """Test HTTP query execution with no result table."""
        pinot = PinotClient(mock_pinot_config)

        # Mock HTTP response without resultTable
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_requests.post.return_value = mock_response

        result = pinot.execute_query_http("SELECT * FROM test_table")
        assert result == []

    def test_execute_query_http_fallback_to_pinotdb(
        self, mock_pinot_config, mock_connection
    ):
        """Test execute_query falls back to PinotDB when HTTP fails."""
        pinot = PinotClient(mock_pinot_config)

        # Mock HTTP request to fail
        with patch.object(pinot, "http_request") as mock_http_request:
            mock_http_request.side_effect = Exception("HTTP failed")

            # Mock PinotDB execution
            with patch.object(pinot, "execute_query_pinotdb") as mock_pinotdb:
                mock_pinotdb.return_value = [{"id": 1, "name": "Test"}]

                result = pinot.execute_query("SELECT * FROM test_table")

                assert result == [{"id": 1, "name": "Test"}]
                mock_pinotdb.assert_called_once()

    def test_execute_query_both_methods_fail(self, mock_pinot_config):
        """Test execute_query when both HTTP and PinotDB fail."""
        pinot = PinotClient(mock_pinot_config)

        with patch.object(pinot, "http_request") as mock_http_request:
            mock_http_request.side_effect = Exception("HTTP failed")

            with patch.object(pinot, "execute_query_pinotdb") as mock_pinotdb:
                mock_pinotdb.side_effect = Exception("PinotDB failed")

                # The actual exception raised is the last one (PinotDB failed)
                with pytest.raises(Exception, match="PinotDB failed"):
                    pinot.execute_query("SELECT * FROM test_table")

    def test_preprocess_query_removes_database_prefix(self, mock_pinot_config):
        """Test query preprocessing removes database prefix."""
        mock_pinot_config.database = "test_db"
        pinot = PinotClient(mock_pinot_config)

        query = "SELECT * FROM test_db.my_table"
        processed = pinot.preprocess_query(query)

        assert "test_db." not in processed
        assert "my_table" in processed

    def test_preprocess_query_adds_timeout(self, mock_pinot_config):
        """Test query preprocessing adds timeout option."""
        pinot = PinotClient(mock_pinot_config)

        query = "SELECT * FROM my_table"
        processed = pinot.preprocess_query(query)

        assert "OPTION(timeoutMs=60000)" in processed

    def test_preprocess_query_preserves_existing_timeout(self, mock_pinot_config):
        """Test query preprocessing preserves existing timeout."""
        pinot = PinotClient(mock_pinot_config)

        query = "SELECT * FROM my_table OPTION(timeoutMs=30000)"
        processed = pinot.preprocess_query(query)

        assert processed == query  # Should not be modified

    def test_execute_query_pinotdb_success(self, mock_pinot_config, mock_connection):
        """Test successful PinotDB query execution."""
        pinot = PinotClient(mock_pinot_config)

        with patch.object(pinot, "get_connection") as mock_get_conn:
            mock_get_conn.return_value = mock_connection

            result = pinot.execute_query_pinotdb("SELECT * FROM test_table")

            assert len(result) == 2
            assert result[0]["id"] == 1
            assert result[0]["name"] == "Test 1"

    def test_execute_query_pinotdb_error_resets_connection(self, mock_pinot_config):
        """Test PinotDB query execution resets connection on error."""
        pinot = PinotClient(mock_pinot_config)
        pinot._conn = MagicMock()

        with patch.object(pinot, "get_connection") as mock_get_conn:
            mock_get_conn.side_effect = Exception("Connection failed")

            with pytest.raises(Exception):
                pinot.execute_query_pinotdb("SELECT * FROM test_table")

            assert pinot._conn is None

    def test_get_tables(self, mock_pinot_config, mock_requests):
        """Test get_tables method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"tables": ["table1", "table2"]}
        mock_requests.get.return_value = mock_response

        tables = pinot.get_tables()

        assert tables == ["table1", "table2"]
        mock_requests.get.assert_called_once()

    def test_get_table_detail(self, mock_pinot_config, mock_requests):
        """Test get_table_detail method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"tableName": "test_table", "columnCount": 5}
        mock_requests.get.return_value = mock_response

        detail = pinot.get_table_detail("test_table")

        assert detail["tableName"] == "test_table"
        assert detail["columnCount"] == 5

    def test_get_segments(self, mock_pinot_config, mock_requests):
        """Test get_segments method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"segments": ["segment1", "segment2"]}
        mock_requests.get.return_value = mock_response

        segments = pinot.get_segments("test_table")

        assert segments["segments"] == ["segment1", "segment2"]

    def test_get_segment_metadata_detail(self, mock_pinot_config, mock_requests):
        """Test get_segment_metadata_detail method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"metadata": "test_metadata"}
        mock_requests.get.return_value = mock_response

        metadata = pinot.get_segment_metadata_detail("test_table")

        assert metadata["metadata"] == "test_metadata"

    def test_get_index_column_detail_success(self, mock_pinot_config, mock_requests):
        """Test get_index_column_detail method with success."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"indexes": ["index1", "index2"]}
        mock_requests.get.return_value = mock_response

        detail = pinot.get_index_column_detail("test_table", "segment1")

        assert detail["indexes"] == ["index1", "index2"]

    def test_get_index_column_detail_not_found(self, mock_pinot_config, mock_requests):
        """Test get_index_column_detail method when not found."""
        pinot = PinotClient(mock_pinot_config)

        mock_requests.get.side_effect = Exception("Not found")

        with pytest.raises(ValueError, match="Index column detail not found"):
            pinot.get_index_column_detail("test_table", "segment1")

    def test_get_tableconfig_schema_detail(self, mock_pinot_config, mock_requests):
        """Test get_tableconfig_schema_detail method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"config": "test_config"}
        mock_requests.get.return_value = mock_response

        config = pinot.get_tableconfig_schema_detail("test_table")

        assert config["config"] == "test_config"

    def test_create_schema(self, mock_pinot_config, mock_requests):
        """Test create_schema method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "created"}
        mock_requests.post.return_value = mock_response

        schema_json = '{"schemaName": "test", "dimensionFieldSpecs": []}'
        result = pinot.create_schema(schema_json)

        assert result["status"] == "created"

    def test_update_schema(self, mock_pinot_config, mock_requests):
        """Test update_schema method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "updated"}
        mock_requests.put.return_value = mock_response

        schema_json = '{"schemaName": "test", "dimensionFieldSpecs": []}'
        result = pinot.update_schema("test", schema_json)

        assert result["status"] == "updated"

    def test_get_schema(self, mock_pinot_config, mock_requests):
        """Test get_schema method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"schema": "test_schema"}
        mock_requests.get.return_value = mock_response

        schema = pinot.get_schema("test")

        assert schema["schema"] == "test_schema"

    def test_create_table_config(self, mock_pinot_config, mock_requests):
        """Test create_table_config method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "created"}
        mock_requests.post.return_value = mock_response

        config_json = '{"tableName": "test", "tableType": "OFFLINE"}'
        result = pinot.create_table_config(config_json)

        assert result["status"] == "created"

    def test_update_table_config(self, mock_pinot_config, mock_requests):
        """Test update_table_config method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "updated"}
        mock_requests.put.return_value = mock_response

        config_json = '{"tableName": "test", "tableType": "OFFLINE"}'
        result = pinot.update_table_config("test", config_json)

        assert result["status"] == "updated"

    def test_get_table_config(self, mock_pinot_config, mock_requests):
        """Test get_table_config method."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"OFFLINE": {"config": "test"}}
        mock_requests.get.return_value = mock_response

        config = pinot.get_table_config("test", "OFFLINE")

        assert config["config"] == "test"

    def test_get_table_config_no_type(self, mock_pinot_config, mock_requests):
        """Test get_table_config method without table type."""
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"OFFLINE": {"config": "test"}}
        mock_requests.get.return_value = mock_response

        config = pinot.get_table_config("test")

        assert "OFFLINE" in config
        assert config["OFFLINE"]["config"] == "test"
