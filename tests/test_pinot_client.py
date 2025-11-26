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
        included_tables=None,
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

    def test_is_table_filtering_enabled_with_none(self, mock_pinot_config):
        """Test that _is_table_filtering_enabled returns False when included_tables is None."""
        mock_pinot_config.included_tables = None
        pinot = PinotClient(mock_pinot_config)

        assert pinot._is_table_filtering_enabled() is False

    def test_is_table_filtering_enabled_with_empty_list(self, mock_pinot_config):
        """Test that _is_table_filtering_enabled returns False when included_tables is empty."""
        mock_pinot_config.included_tables = []
        pinot = PinotClient(mock_pinot_config)

        assert pinot._is_table_filtering_enabled() is False

    def test_is_table_filtering_enabled_with_patterns(self, mock_pinot_config):
        """Test that _is_table_filtering_enabled returns True when patterns are configured."""
        mock_pinot_config.included_tables = ["table1", "table2*"]
        pinot = PinotClient(mock_pinot_config)

        assert pinot._is_table_filtering_enabled() is True

    def test_filter_tables_no_filter_configured(self, mock_pinot_config):
        """Test that _filter_tables returns all tables when no filter configured."""
        pinot = PinotClient(mock_pinot_config)
        tables = ["table1", "table2", "table3"]

        result = pinot._filter_tables(tables)

        assert result == tables

    def test_filter_tables_with_patterns(self, mock_pinot_config):
        """Test that _filter_tables applies patterns correctly."""
        mock_pinot_config.included_tables = ["prod_*", "important_table"]
        pinot = PinotClient(mock_pinot_config)
        tables = ["prod_users", "prod_orders", "dev_users", "important_table"]

        result = pinot._filter_tables(tables)

        assert result == ["prod_users", "prod_orders", "important_table"]

    def test_filter_tables_excludes_non_matching(self, mock_pinot_config):
        """Test that _filter_tables excludes tables not in the filter."""
        mock_pinot_config.included_tables = ["allowed_table"]
        pinot = PinotClient(mock_pinot_config)
        tables = ["allowed_table", "excluded_table", "another_excluded"]

        result = pinot._filter_tables(tables)

        assert result == ["allowed_table"]
        assert "excluded_table" not in result
        assert "another_excluded" not in result

    def test_filter_tables_empty_list(self, mock_pinot_config):
        """Test that _filter_tables handles empty table list."""
        mock_pinot_config.included_tables = ["prod_*"]
        pinot = PinotClient(mock_pinot_config)

        result = pinot._filter_tables([])

        assert result == []

    def test_get_tables_with_filtering(self, mock_pinot_config, mock_requests):
        """Test that get_tables applies filtering when configured."""
        mock_pinot_config.included_tables = ["prod_*"]
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "tables": ["prod_users", "prod_orders", "dev_users"]
        }
        mock_requests.get.return_value = mock_response

        tables = pinot.get_tables()

        assert tables == ["prod_users", "prod_orders"]
        assert "dev_users" not in tables

    def test_execute_query_blocks_unauthorized_table_in_from(
        self, mock_pinot_config, mock_requests
    ):
        """Test that execute_query blocks queries with unauthorized table in FROM clause."""
        mock_pinot_config.included_tables = ["allowed_table", "another_allowed"]
        pinot = PinotClient(mock_pinot_config)

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "resultTable": {
                "dataSchema": {"columnNames": ["col1"]},
                "rows": [["data"]],
            }
        }
        mock_requests.post.return_value = mock_response

        # Query with unauthorized table should raise ValueError
        with pytest.raises(ValueError, match="unauthorized tables"):
            pinot.execute_query("SELECT * FROM unauthorized_table")

    def test_execute_query_blocks_unauthorized_table_in_join(
        self, mock_pinot_config, mock_requests
    ):
        """Test that execute_query blocks queries with unauthorized table in JOIN clause."""
        mock_pinot_config.included_tables = ["allowed_table"]
        pinot = PinotClient(mock_pinot_config)

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "resultTable": {
                "dataSchema": {"columnNames": ["col1"]},
                "rows": [["data"]],
            }
        }
        mock_requests.post.return_value = mock_response

        # Query joining unauthorized table should raise ValueError
        query = """
            SELECT a.*, b.name
            FROM allowed_table a
            JOIN unauthorized_table b ON a.id = b.id
        """
        with pytest.raises(ValueError, match="unauthorized tables"):
            pinot.execute_query(query)

    def test_execute_query_allows_authorized_tables(
        self, mock_pinot_config, mock_requests
    ):
        """Test that execute_query allows queries with authorized tables."""
        mock_pinot_config.included_tables = ["table1", "table2"]
        pinot = PinotClient(mock_pinot_config)

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "resultTable": {
                "dataSchema": {"columnNames": ["col1"]},
                "rows": [["data"]],
            }
        }
        mock_requests.post.return_value = mock_response

        # Query with authorized tables should succeed
        result = pinot.execute_query(
            "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id"
        )

        assert result == [{"col1": "data"}]

    def test_execute_query_allows_all_when_no_filter(
        self, mock_pinot_config, mock_requests
    ):
        """Test that execute_query allows any table when filtering is not configured."""
        mock_pinot_config.included_tables = None
        pinot = PinotClient(mock_pinot_config)

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "resultTable": {
                "dataSchema": {"columnNames": ["col1"]},
                "rows": [["data"]],
            }
        }
        mock_requests.post.return_value = mock_response

        # Should allow any table when no filter configured
        result = pinot.execute_query("SELECT * FROM any_table_name")

        assert result == [{"col1": "data"}]

    def test_execute_query_blocks_multiple_unauthorized_tables(
        self, mock_pinot_config, mock_requests
    ):
        """Test that execute_query reports all unauthorized tables in error message."""
        mock_pinot_config.included_tables = ["allowed_table"]
        pinot = PinotClient(mock_pinot_config)

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "resultTable": {
                "dataSchema": {"columnNames": ["col1"]},
                "rows": [["data"]],
            }
        }
        mock_requests.post.return_value = mock_response

        # Query with multiple unauthorized tables
        query = """
            SELECT * FROM unauthorized1
            JOIN unauthorized2 ON unauthorized1.id = unauthorized2.id
        """
        with pytest.raises(ValueError, match="unauthorized tables"):
            pinot.execute_query(query)

    def test_execute_query_blocks_unauthorized_table_in_subquery(
        self, mock_pinot_config, mock_requests
    ):
        """Test that execute_query blocks queries with unauthorized table in subquery."""
        mock_pinot_config.included_tables = ["allowed_table"]
        pinot = PinotClient(mock_pinot_config)

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "resultTable": {
                "dataSchema": {"columnNames": ["col1"]},
                "rows": [["data"]],
            }
        }
        mock_requests.post.return_value = mock_response

        # Query with unauthorized table in subquery should raise ValueError
        query = """
            SELECT * FROM allowed_table
            WHERE id IN (SELECT id FROM unauthorized_table WHERE active = 1)
        """
        with pytest.raises(ValueError, match="unauthorized tables"):
            pinot.execute_query(query)

    def test_execute_query_allows_authorized_table_in_subquery(
        self, mock_pinot_config, mock_requests
    ):
        """Test that execute_query allows queries with authorized table in subquery."""
        mock_pinot_config.included_tables = ["allowed_table", "another_allowed"]
        pinot = PinotClient(mock_pinot_config)

        # Mock the HTTP response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "resultTable": {
                "dataSchema": {"columnNames": ["col1"]},
                "rows": [["data"]],
            }
        }
        mock_requests.post.return_value = mock_response

        # Query with authorized tables in subquery should succeed
        query = """
            SELECT * FROM allowed_table
            WHERE id IN (SELECT id FROM another_allowed WHERE active = 1)
        """
        result = pinot.execute_query(query)

        assert result == [{"col1": "data"}]

    def test_extract_table_names_comma_separated(self, mock_pinot_config):
        """Test extracting table names from comma-separated tables in FROM"""
        pinot = PinotClient(mock_pinot_config)
        query = "SELECT * FROM table1, table2, table3"

        result = pinot._extract_sql_table_names(query)

        assert set(result) == {"table1", "table2", "table3"}

    def test_extract_table_names_with_cte(self, mock_pinot_config):
        """Test extracting table names from WITH clause (CTE)"""
        pinot = PinotClient(mock_pinot_config)
        query = "WITH cte AS (SELECT * FROM unauthorized_table) SELECT * FROM cte"

        result = pinot._extract_sql_table_names(query)

        # Should find both the CTE source table and the CTE itself
        assert "unauthorized_table" in result

    def test_extract_table_names_nested_subquery(self, mock_pinot_config):
        """Test extracting table names from nested subquery"""
        pinot = PinotClient(mock_pinot_config)
        query = "SELECT * FROM (SELECT * FROM unauthorized_table) AS subq"

        result = pinot._extract_sql_table_names(query)

        assert "unauthorized_table" in result

    def test_extract_table_names_different_join_types(self, mock_pinot_config):
        """Test extracting table names from different JOIN types"""
        pinot = PinotClient(mock_pinot_config)
        queries = [
            "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id",
            "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id",
            "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id",
            "SELECT * FROM t1 OUTER JOIN t2 ON t1.id = t2.id",
            "SELECT * FROM t1 CROSS JOIN t2",
        ]

        for query in queries:
            result = pinot._extract_sql_table_names(query)
            assert set(result) == {"t1", "t2"}, f"Failed for query: {query}"

    def test_extract_table_names_union_query(self, mock_pinot_config):
        """Test extracting table names from UNION query"""
        pinot = PinotClient(mock_pinot_config)
        query = "SELECT * FROM table1 UNION SELECT * FROM table2"

        result = pinot._extract_sql_table_names(query)

        assert set(result) == {"table1", "table2"}

    def test_extract_table_names_multiple_schemas(self, mock_pinot_config):
        """Test extracting table names with schema prefix"""
        pinot = PinotClient(mock_pinot_config)
        query = "SELECT * FROM database.schema.table_name"

        result = pinot._extract_sql_table_names(query)

        assert "table_name" in result

    def test_extract_table_names_removes_comments(self, mock_pinot_config):
        """Test that SQL comments are removed before extraction"""
        pinot = PinotClient(mock_pinot_config)
        query = """
            -- This is a comment with FROM fake_table
            SELECT * FROM real_table
            /* Multi-line comment
               FROM another_fake_table */
        """

        result = pinot._extract_sql_table_names(query)

        assert "real_table" in result
        assert "fake_table" not in result
        assert "another_fake_table" not in result

    def test_extract_table_names_case_insensitive(self, mock_pinot_config):
        """Test that FROM and JOIN keywords are case insensitive"""
        pinot = PinotClient(mock_pinot_config)
        queries = [
            "select * from table1",
            "SELECT * FROM table2",
            "SeLeCt * FrOm table3",
            "SELECT * from table4 join table5",
        ]

        results = []
        for query in queries:
            results.extend(pinot._extract_sql_table_names(query))

        assert "table1" in results
        assert "table2" in results
        assert "table3" in results
        assert "table4" in results
        assert "table5" in results

    def test_extract_table_names_double_quoted(self, mock_pinot_config):
        """Test extracting table names with double quotes"""
        pinot = PinotClient(mock_pinot_config)
        queries = [
            'SELECT * FROM "table_name"',
            'SELECT * FROM "table with spaces"',
            'SELECT * FROM t1 JOIN "quoted_table" ON t1.id = t2.id',
        ]

        result1 = pinot._extract_sql_table_names(queries[0])
        assert "table_name" in result1

        result2 = pinot._extract_sql_table_names(queries[1])
        assert "table with spaces" in result2

        result3 = pinot._extract_sql_table_names(queries[2])
        assert "t1" in result3
        assert "quoted_table" in result3

    def test_extract_table_names_backtick_quoted(self, mock_pinot_config):
        """Test extracting table names with backticks (MySQL style)"""
        pinot = PinotClient(mock_pinot_config)
        queries = [
            "SELECT * FROM `table_name`",
            "SELECT * FROM `table with spaces`",
            "SELECT * FROM t1 JOIN `quoted_table` ON t1.id = t2.id",
        ]

        result1 = pinot._extract_sql_table_names(queries[0])
        assert "table_name" in result1

        result2 = pinot._extract_sql_table_names(queries[1])
        assert "table with spaces" in result2

        result3 = pinot._extract_sql_table_names(queries[2])
        assert "t1" in result3
        assert "quoted_table" in result3

    def test_extract_table_names_mixed_quoted_unquoted(self, mock_pinot_config):
        """Test extracting mix of quoted and unquoted table names"""
        pinot = PinotClient(mock_pinot_config)
        query = 'SELECT * FROM normal_table, "quoted table", `backtick_table`'

        result = pinot._extract_sql_table_names(query)

        assert "normal_table" in result
        assert "quoted table" in result
        assert "backtick_table" in result

    def test_validate_table_name_access_integration(
        self, mock_pinot_config, mock_requests
    ):
        """Test _validate_table_name_access integration with table operations"""
        mock_pinot_config.included_tables = ["allowed_table"]
        pinot = PinotClient(mock_pinot_config)

        # Blocks unauthorized table
        with pytest.raises(ValueError, match="Access denied to table"):
            pinot.get_table_detail("unauthorized_table")

        # Allows authorized table
        mock_response = MagicMock()
        mock_response.json.return_value = {"tableName": "allowed_table"}
        mock_requests.get.return_value = mock_response

        result = pinot.get_table_detail("allowed_table")
        assert result == {"tableName": "allowed_table"}

    def test_table_operations_allow_all_when_no_filter(
        self, mock_pinot_config, mock_requests
    ):
        """Test that table operations allow any table when filtering not configured"""
        mock_pinot_config.included_tables = None
        pinot = PinotClient(mock_pinot_config)

        mock_response = MagicMock()
        mock_response.json.return_value = {"tableName": "any_table"}
        mock_requests.get.return_value = mock_response

        # Should not raise - no filtering configured
        result = pinot.get_table_detail("any_table_name")
        assert result == {"tableName": "any_table"}

    def test_create_schema_validates_schema_name_from_json(
        self, mock_pinot_config, mock_requests
    ):
        """Test that create_schema validates schema name extracted from JSON"""
        mock_pinot_config.included_tables = ["prod_*"]
        pinot = PinotClient(mock_pinot_config)

        # Should block unauthorized schema
        with pytest.raises(ValueError, match="Access denied to table"):
            pinot.create_schema('{"schemaName": "dev_unauthorized"}')

        # Should allow authorized schema
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_requests.post.return_value = mock_response

        result = pinot.create_schema('{"schemaName": "prod_authorized"}')
        assert result == {"status": "success"}

    def test_create_table_config_validates_table_name_from_json(
        self, mock_pinot_config, mock_requests
    ):
        """Test that create_table_config validates table name extracted from JSON"""
        mock_pinot_config.included_tables = ["prod_*"]
        pinot = PinotClient(mock_pinot_config)

        # Should block unauthorized table
        with pytest.raises(ValueError, match="Access denied to table"):
            pinot.create_table_config('{"tableName": "dev_unauthorized"}')

        # Should allow authorized table
        mock_response = MagicMock()
        mock_response.json.return_value = {"status": "success"}
        mock_requests.post.return_value = mock_response

        result = pinot.create_table_config('{"tableName": "prod_authorized"}')
        assert result == {"status": "success"}
