import base64
import json
import re
from fnmatch import fnmatch
from threading import Lock
from typing import Any, Dict, Tuple

import pandas as pd
from pinotdb import connect
import requests

from .config import PinotConfig, get_logger, reload_table_filters_from_file

logger = get_logger()


def get_auth_credentials(config: PinotConfig) -> Tuple[str | None, str | None]:
    """Extract authentication credentials for PinotDB connection"""
    if config.token:
        if config.token.startswith("Bearer "):
            return "", config.token  # Empty username, full Bearer token as password
        else:
            return "", config.token
    elif config.username and config.password:
        return config.username, config.password
    return None, None


def test_connection_query(connection) -> None:
    """Test connection with a simple query"""
    test_cursor = connection.cursor()
    test_cursor.execute("SELECT 1")
    test_result = test_cursor.fetchall()
    logger.debug(f"Connection test successful: {test_result}")


# URL pattern constants
class PinotEndpoints:
    QUERY_SQL = "query/sql"
    TABLES = "tables"
    SCHEMAS = "schemas"
    TABLE_SIZE = "tables/{}/size"
    SEGMENTS = "segments/{}"
    SEGMENT_METADATA = "segments/{}/metadata"
    SEGMENT_DETAIL = "segments/{}_{}/{}/metadata?columns=*"
    TABLE_CONFIG = "tableConfigs/{}"


def create_connection(config: PinotConfig) -> connect:
    """Create Pinot connection with proper authentication handling"""
    try:
        auth_username, auth_password = get_auth_credentials(config)

        logger.debug(
            f"Creating connection to {config.broker_host}:{config.broker_port} "
            f"with MSQE={config.use_msqe}"
        )
        auth_method = "token" if config.token else "username/password"
        logger.debug(f"Database: {config.database}, Auth method: {auth_method}")

        connection = connect(
            host=config.broker_host,
            port=config.broker_port,
            path="/query/sql",
            scheme=config.broker_scheme,
            username=auth_username,
            password=auth_password,
            use_multistage_engine=config.use_msqe,
            database=config.database,
            extra_conn_args={
                "timeout": config.query_timeout,
                "verify": True,
                "retries": 3,
                "backoff_factor": 1.0,
            },
        )

        test_connection_query(connection)
        return connection

    except Exception as e:
        logger.error(f"Failed to create Pinot connection: {e}")
        logger.error(
            f"Connection details - Host: {config.broker_host}, "
            f"Port: {config.broker_port}, Scheme: {config.broker_scheme}"
        )
        raise


class PinotClient:
    def __init__(self, config: PinotConfig):
        self.config = config
        self.insights: list[str] = []
        self._conn = None
        # Store filters separately to avoid mutating config
        self._included_tables = config.included_tables
        self._config_lock = Lock()  # For thread-safe filter updates

    def reload_table_filters(self) -> dict[str, Any]:
        """Reload table filters from the configured filter file without restarting.

        This allows dynamic updates to the table access list by:
        1. Editing the YAML filter file
        2. Calling this method to reload the configuration

        Returns:
            dict: Status information with previous and new filter counts

        Raises:
            ValueError: If no table filter file is configured
            FileNotFoundError: If the filter file doesn't exist
            yaml.YAMLError: If the file contains invalid YAML
        """
        if not self.config.table_filter_file:
            raise ValueError(
                "No table filter file configured. "
                "Set PINOT_TABLE_FILTER_FILE to enable hot-reload."
            )

        logger.info(f"Reloading table filters from {self.config.table_filter_file}")

        # Load new filters (validates file exists and parses YAML)
        new_filters = reload_table_filters_from_file(self.config.table_filter_file)

        # Atomically update the filters with lock
        with self._config_lock:
            old_filters = self._included_tables
            old_count = len(old_filters) if old_filters else 0
            self._included_tables = new_filters
            new_count = len(new_filters) if new_filters else 0

        logger.info(f"Table filters reloaded: {old_count} -> {new_count} tables")

        return {
            "status": "success",
            "message": "Table filters reloaded successfully",
            "previous_filter_count": old_count,
            "new_filter_count": new_count,
            "previous_filters": old_filters,
            "new_filters": new_filters,
        }

    def _create_auth_headers(self) -> Dict[str, str]:
        """Create HTTP headers with authentication based on configuration"""
        headers = {"accept": "application/json", "Content-Type": "application/json"}

        if self.config.token:
            headers["Authorization"] = self.config.token
        elif self.config.username and self.config.password:
            creds_str = f"{self.config.username}:{self.config.password}"
            credentials = base64.b64encode(creds_str.encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"

        if self.config.database:
            headers["database"] = self.config.database

        return headers

    def http_request(
        self,
        url: str,
        method: str = "GET",
        json_data: Dict = None,
    ) -> requests.Response:
        """Make HTTP request with authentication headers and timeout handling"""
        headers = self._create_auth_headers()

        try:
            if method.upper() == "POST":
                response = requests.post(
                    url,
                    headers=headers,
                    json=json_data,
                    timeout=(
                        self.config.connection_timeout,
                        self.config.request_timeout,
                    ),
                    verify=True,
                )
            else:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=(
                        self.config.connection_timeout,
                        self.config.request_timeout,
                    ),
                    verify=True,
                )
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            logger.error(f"HTTP request timeout for {url}")
            raise
        except Exception as e:
            logger.error(f"HTTP request failed for {url}: {e}")
            raise

    def get_connection(self):
        """Get or create a reusable connection"""
        try:
            if self._conn is None:
                self._conn = create_connection(self.config)
            else:
                # Test if connection is still alive
                test_connection_query(self._conn)
            return self._conn
        except Exception as e:
            logger.warning(f"Connection test failed, creating new connection: {e}")
            self._conn = create_connection(self.config)
            return self._conn

    def test_connection(self) -> dict[str, Any]:
        """Test the connection and return diagnostic information"""
        result = {
            "connection_test": False,
            "query_test": False,
            "tables_test": False,
            "error": None,
            "config": {
                "broker_host": self.config.broker_host,
                "broker_port": self.config.broker_port,
                "broker_scheme": self.config.broker_scheme,
                "controller_url": self.config.controller_url,
                "database": self.config.database,
                "use_msqe": self.config.use_msqe,
                "has_token": bool(self.config.token),
                "has_username": bool(self.config.username),
                "timeout_config": {
                    "connection": self.config.connection_timeout,
                    "request": self.config.request_timeout,
                    "query": self.config.query_timeout,
                },
            },
        }

        try:
            # Test basic connection
            conn = self.get_connection()
            result["connection_test"] = True

            # Test simple query
            curs = conn.cursor()
            curs.execute("SELECT 1 as test_column")
            test_result = curs.fetchall()
            result["query_test"] = True
            result["query_result"] = test_result

            # Test tables listing
            tables = self.get_tables()
            result["tables_test"] = True
            result["tables_count"] = len(tables)
            result["sample_tables"] = tables[:5] if tables else []

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Connection test failed: {e}")

        return result

    def execute_query_http(self, query: str) -> list[dict[str, Any]]:
        """Alternative query execution using HTTP requests directly to broker"""
        broker_url = f"{self.config.broker_scheme}://{self.config.broker_host}:{self.config.broker_port}/{PinotEndpoints.QUERY_SQL}"
        logger.debug(f"Executing query via HTTP: {query[:100]}...")

        payload = {
            "sql": query,
            "queryOptions": f"timeoutMs={self.config.query_timeout * 1000}",
        }

        response = self.http_request(broker_url, "POST", payload)
        result_data = response.json()

        # Check for query errors in response
        if "exceptions" in result_data and result_data["exceptions"]:
            raise Exception(f"Query error: {result_data['exceptions']}")

        # Parse the result into pandas-like format
        if "resultTable" in result_data:
            columns = result_data["resultTable"]["dataSchema"]["columnNames"]
            rows = result_data["resultTable"]["rows"]

            # Convert to list of dictionaries
            result = [dict(zip(columns, row)) for row in rows]
            logger.debug(f"HTTP query returned {len(result)} rows")
            return result
        else:
            logger.warning("No resultTable in response, returning empty result")
            return []

    def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        logger.debug(f"Executing query: {query[:100]}...")  # Log first 100 chars

        # Validate table access authorization
        self._validate_table_access(query)

        # Use HTTP as primary method since it works reliably with authenticated clusters
        try:
            return self.execute_query_http(query)
        except Exception as e:
            logger.warning(f"HTTP query failed: {e}, trying PinotDB fallback")
            try:
                return self.execute_query_pinotdb(query, params)
            except Exception as pinotdb_error:
                error_msg = (
                    f"Both HTTP and PinotDB queries failed. "
                    f"HTTP: {e}, PinotDB: {pinotdb_error}"
                )
                logger.error(error_msg)
                raise

    def preprocess_query(self, query: str) -> str:
        """Preprocess query by removing database prefix and adding timeout options"""
        # Remove database prefix if present
        if self.config.database and f"{self.config.database}." in query:
            query = query.replace(f"{self.config.database}.", "")
            logger.debug(f"Removed database prefix, query now: {query[:100]}...")

        # Add query timeout hint if not present
        if "SET timeoutMs" not in query.upper() and "OPTION" not in query.upper():
            timeout_ms = self.config.query_timeout * 1000  # Convert to milliseconds
            if query.strip().endswith(";"):
                query = query.rstrip(";")
            query = f"{query} OPTION(timeoutMs={timeout_ms})"
            logger.debug(f"Added timeout option: {timeout_ms}ms")

        return query

    def execute_query_pinotdb(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Original pinotdb-based query execution"""
        logger.debug(f"Executing query via PinotDB: {query[:100]}...")
        try:
            current_conn = self.get_connection()
            curs = current_conn.cursor()

            query = self.preprocess_query(query)
            logger.debug(f"Final query: {query}")

            curs.execute(query)

            # Get column names and fetch results
            columns = [item[0] for item in curs.description] if curs.description else []
            df = pd.DataFrame(curs.fetchall(), columns=columns)

            result = df.to_dict(orient="records")
            logger.debug(f"Query executed successfully, returned {len(result)} rows")
            return result

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query was: {query}")
            # Reset connection on error
            self._conn = None
            raise

    def _matches_patterns(self, table: str, patterns: list[str]) -> bool:
        """Check if table matches any pattern."""
        return any(fnmatch(table, pattern) for pattern in patterns)

    def _is_table_filtering_enabled(self) -> bool:
        """Check if table filtering is configured and enabled.

        Returns:
            bool: True if filtering is enabled (included_tables is configured),
                  False otherwise (None, empty list, or any falsy value)
        """
        return bool(self._included_tables)

    def _extract_sql_table_names(self, query: str) -> list[str]:
        """Extract table names from a SQL query.

        Handles table references in FROM, JOIN, and subquery clauses.
        Supports quoted identifiers (double quotes, backticks).

        Args:
            query: SQL query string

        Returns:
            list[str]: Unique list of table names found in the query
        """
        # Remove comments and normalize whitespace
        query = re.sub(r"--.*?$", "", query, flags=re.MULTILINE)
        query = re.sub(r"/\*.*?\*/", "", query, flags=re.DOTALL)
        query = " ".join(query.split())

        matches = []

        # Pattern 1: Unquoted tables (after FROM/JOIN or comma-separated)
        # Matches: FROM table, JOIN table, table1, table2
        # Uses negative lookahead to exclude SQL keywords (LEFT, RIGHT, INNER, etc.)
        unquoted_pattern = r"(?:\b(?:FROM|JOIN)\s+|,\s*)(?:[\w.]+\.)?(?!(?:LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|WHERE|GROUP|ORDER|HAVING|LIMIT)\b)(\w+)"
        matches.extend(re.findall(unquoted_pattern, query, re.IGNORECASE))

        # Pattern 2: Double-quoted tables (after FROM/JOIN or comma-separated)
        # Matches: FROM "table name", "quoted_table", "another table"
        double_quoted_pattern = r'(?:\b(?:FROM|JOIN)\s+|,\s*)(?:[\w.]+\.)?"([^"]+)"'
        matches.extend(re.findall(double_quoted_pattern, query, re.IGNORECASE))

        # Pattern 3: Backtick-quoted tables (after FROM/JOIN or comma-separated)
        # Matches: FROM `table_name`, `quoted table`, `another table`
        backtick_pattern = r"(?:\b(?:FROM|JOIN)\s+|,\s*)(?:[\w.]+\.)?`([^`]+)`"
        matches.extend(re.findall(backtick_pattern, query, re.IGNORECASE))

        return list(set(matches))

    def _validate_table_name_access(self, table_name: str) -> None:
        """Validate that a table name is allowed by filtering rules.

        Args:
            table_name: Table name to validate

        Raises:
            ValueError: If table is not in included_tables filter
        """
        if not self._is_table_filtering_enabled():
            return

        if not self._matches_patterns(table_name, self._included_tables):
            allowed = ", ".join(self._included_tables)
            raise ValueError(
                f"Access denied to table '{table_name}'. "
                f"Allowed tables: {allowed}"
            )

    def _extract_and_validate_name_from_json(
        self, json_str: str, key: str
    ) -> None:
        """Extract and validate table/schema name from JSON.

        Args:
            json_str: JSON string containing table or schema name
            key: JSON key to extract ("tableName" or "schemaName")

        Raises:
            ValueError: If name extraction fails or access is denied
        """
        try:
            data = json.loads(json_str)
            name = data.get(key)
            if not name:
                raise ValueError(f"Missing required field '{key}' in JSON")
            self._validate_table_name_access(name)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}")

    def _validate_table_access(self, query: str) -> None:
        """Validate that query only accesses allowed tables.

        Args:
            query: SQL query string to validate

        Raises:
            ValueError: If query references tables not in included_tables filter
        """
        if not self._is_table_filtering_enabled():
            return

        table_names = self._extract_sql_table_names(query)

        if not table_names:
            return

        unauthorized_tables = [
            table
            for table in table_names
            if not self._matches_patterns(table, self._included_tables)
        ]

        if unauthorized_tables:
            allowed = ", ".join(self._included_tables)
            unauthorized = ", ".join(unauthorized_tables)
            raise ValueError(
                f"Query references unauthorized tables: {unauthorized}. "
                f"Allowed tables: {allowed}"
            )

    def _filter_tables(self, tables: list[str]) -> list[str]:
        """Filter tables based on included_tables configuration."""
        if not tables or not self._is_table_filtering_enabled():
            return tables

        return [
            t for t in tables if self._matches_patterns(t, self._included_tables)
        ]

    def get_tables(self, params: dict[str, Any] | None = None) -> list[str]:
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLES}"
        logger.debug(f"Fetching tables from: {url}")
        response = self.http_request(url)
        tables = response.json()["tables"]
        logger.debug(f"Successfully fetched {len(tables)} tables")
        return self._filter_tables(tables)

    def get_table_detail(
        self,
        tableName: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_table_name_access(tableName)
        endpoint = PinotEndpoints.TABLE_SIZE.format(tableName)
        url = f"{self.config.controller_url}/{endpoint}"
        logger.debug(f"Fetching table details for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def get_segment_metadata_detail(
        self,
        tableName: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_table_name_access(tableName)
        endpoint = PinotEndpoints.SEGMENT_METADATA.format(tableName)
        url = f"{self.config.controller_url}/{endpoint}"
        logger.debug(f"Fetching segment metadata for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def get_segments(
        self,
        tableName: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_table_name_access(tableName)
        endpoint = PinotEndpoints.SEGMENTS.format(tableName)
        url = f"{self.config.controller_url}/{endpoint}"
        logger.debug(f"Fetching segments for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def get_index_column_detail(
        self,
        tableName: str,
        segmentName: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_table_name_access(tableName)
        for type_suffix in ["REALTIME", "OFFLINE"]:
            endpoint = PinotEndpoints.SEGMENT_DETAIL.format(
                tableName, type_suffix, segmentName
            )
            url = f"{self.config.controller_url}/{endpoint}"
            logger.debug(f"Trying to fetch index column details from: {url}")
            try:
                response = self.http_request(url)
                return response.json()
            except Exception as e:
                error_msg = (
                    f"Failed to fetch index column details for "
                    f"{tableName}_{type_suffix}/{segmentName}: {e}"
                )
                logger.error(error_msg)
                continue
        raise ValueError("Index column detail not found")

    def get_tableconfig_schema_detail(
        self,
        tableName: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._validate_table_name_access(tableName)
        endpoint = PinotEndpoints.TABLE_CONFIG.format(tableName)
        url = f"{self.config.controller_url}/{endpoint}"
        logger.debug(f"Fetching table config for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def create_schema(
        self,
        schemaJson: str,
        override: bool = True,
        force: bool = False,
    ) -> dict[str, Any]:
        self._extract_and_validate_name_from_json(schemaJson, "schemaName")
        url = f"{self.config.controller_url}/{PinotEndpoints.SCHEMAS}"
        params = {"override": str(override).lower(), "force": str(force).lower()}
        headers = self._create_auth_headers()
        headers["Content-Type"] = "application/json"
        response = requests.post(
            url,
            headers=headers,
            params=params,
            data=schemaJson,
            timeout=(self.config.connection_timeout, self.config.request_timeout),
            verify=True,
        )
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {
                "status": "success",
                "message": "Schema creation request processed.",
                "response_body": response.text,
            }

    def update_schema(
        self,
        schemaName: str,
        schemaJson: str,
        reload: bool = False,
        force: bool = False,
    ) -> dict[str, Any]:
        self._validate_table_name_access(schemaName)
        url = f"{self.config.controller_url}/{PinotEndpoints.SCHEMAS}/{schemaName}"
        params = {"reload": str(reload).lower(), "force": str(force).lower()}
        headers = self._create_auth_headers()
        headers["Content-Type"] = "application/json"
        response = requests.put(
            url,
            headers=headers,
            params=params,
            data=schemaJson,
            timeout=(self.config.connection_timeout, self.config.request_timeout),
            verify=True,
        )
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {
                "status": "success",
                "message": "Schema update request processed.",
                "response_body": response.text,
            }

    def get_schema(self, schemaName: str) -> dict[str, Any]:
        self._validate_table_name_access(schemaName)
        url = f"{self.config.controller_url}/{PinotEndpoints.SCHEMAS}/{schemaName}"
        headers = self._create_auth_headers()
        response = requests.get(
            url,
            headers=headers,
            timeout=(self.config.connection_timeout, self.config.request_timeout),
            verify=True,
        )
        response.raise_for_status()
        return response.json()

    def create_table_config(
        self,
        tableConfigJson: str,
        validationTypesToSkip: str | None = None,
    ) -> dict[str, Any]:
        self._extract_and_validate_name_from_json(tableConfigJson, "tableName")
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLES}"
        params: dict[str, str] = {}
        if validationTypesToSkip:
            params["validationTypesToSkip"] = validationTypesToSkip
        headers = self._create_auth_headers()
        headers["Content-Type"] = "application/json"
        response = requests.post(
            url,
            headers=headers,
            params=params,
            data=tableConfigJson,
            timeout=(self.config.connection_timeout, self.config.request_timeout),
            verify=True,
        )
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {
                "status": "success",
                "message": "Table config creation request processed.",
                "response_body": response.text,
            }

    def update_table_config(
        self,
        tableName: str,
        tableConfigJson: str,
        validationTypesToSkip: str | None = None,
    ) -> dict[str, Any]:
        self._validate_table_name_access(tableName)
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLES}/{tableName}"
        params: dict[str, str] = {}
        if validationTypesToSkip:
            params["validationTypesToSkip"] = validationTypesToSkip
        headers = self._create_auth_headers()
        headers["Content-Type"] = "application/json"
        response = requests.put(
            url,
            headers=headers,
            params=params,
            data=tableConfigJson,
            timeout=(self.config.connection_timeout, self.config.request_timeout),
            verify=True,
        )
        response.raise_for_status()
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return {
                "status": "success",
                "message": "Table config update request processed.",
                "response_body": response.text,
            }

    def get_table_config(
        self,
        tableName: str,
        tableType: str | None = None,
    ) -> dict[str, Any]:
        self._validate_table_name_access(tableName)
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLES}/{tableName}"
        params: dict[str, str] = {}
        if tableType:
            params["type"] = tableType
        headers = self._create_auth_headers()
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=(self.config.connection_timeout, self.config.request_timeout),
            verify=True,
        )
        response.raise_for_status()
        raw_response = response.json()
        if tableType and tableType.upper() in raw_response:
            return raw_response[tableType.upper()]
        if not tableType and ("OFFLINE" in raw_response or "REALTIME" in raw_response):
            return raw_response
        return raw_response
