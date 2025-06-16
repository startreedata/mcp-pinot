from typing import Any, Dict, Tuple
import pandas as pd
import mcp.types as types
from pinotdb import connect
import base64
import requests
from .config import PinotConfig
from .utils.logging_config import get_logger

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
    TABLE_SIZE = "tables/{}/size"
    SEGMENTS = "segments/{}"
    SEGMENT_METADATA = "segments/{}/metadata"
    SEGMENT_DETAIL = "segments/{}_{}/{}/metadata?columns=*"
    TABLE_CONFIG = "tableConfigs/{}"


def create_connection(config: PinotConfig) -> connect:
    """Create Pinot connection with proper authentication handling"""
    try:
        auth_username, auth_password = get_auth_credentials(config)
        
        logger.debug(f"Creating connection to {config.broker_host}:{config.broker_port} with MSQE={config.use_msqe}")
        logger.debug(f"Database: {config.database}, Auth method: {'token' if config.token else 'username/password'}")
        
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
                'timeout': config.query_timeout,
                'verify': True,
                'retries': 3,
                'backoff_factor': 1.0
            }
        )
        
        test_connection_query(connection)
        return connection
        
    except Exception as e:
        logger.error(f"Failed to create Pinot connection: {e}")
        logger.error(f"Connection details - Host: {config.broker_host}, Port: {config.broker_port}, Scheme: {config.broker_scheme}")
        raise



class PinotClient:
    def __init__(self, config: PinotConfig):
        self.config = config
        self.insights: list[str] = []
        self._conn = None
    
    def _create_auth_headers(self) -> Dict[str, str]:
        """Create HTTP headers with authentication based on configuration"""
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json"
        }
        
        if self.config.token:
            headers["Authorization"] = self.config.token
        elif self.config.username and self.config.password:
            credentials = base64.b64encode(f"{self.config.username}:{self.config.password}".encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"
        
        if self.config.database:
            headers["database"] = self.config.database
        
        return headers
    
    def http_request(self, url: str, method: str = "GET", json_data: Dict = None) -> requests.Response:
        """Make HTTP request with authentication headers and timeout handling"""
        headers = self._create_auth_headers()
        
        try:
            if method.upper() == "POST":
                response = requests.post(
                    url,
                    headers=headers,
                    json=json_data,
                    timeout=(self.config.connection_timeout, self.config.request_timeout),
                    verify=True
                )
            else:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=(self.config.connection_timeout, self.config.request_timeout),
                    verify=True
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
                    "query": self.config.query_timeout
                }
            }
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
            "queryOptions": f"timeoutMs={self.config.query_timeout * 1000}"
        }
        
        response = self.http_request(broker_url, "POST", payload)
        result_data = response.json()
        
        # Check for query errors in response
        if 'exceptions' in result_data and result_data['exceptions']:
            raise Exception(f"Query error: {result_data['exceptions']}")
        
        # Parse the result into pandas-like format
        if 'resultTable' in result_data:
            columns = result_data['resultTable']['dataSchema']['columnNames']
            rows = result_data['resultTable']['rows']
            
            # Convert to list of dictionaries
            result = [dict(zip(columns, row)) for row in rows]
            logger.debug(f"HTTP query executed successfully, returned {len(result)} rows")
            return result
        else:
            logger.warning("No resultTable in response, returning empty result")
            return []

    def execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        logger.debug(f"Executing query: {query[:100]}...")  # Log first 100 chars
        
        # Use HTTP as primary method since it works reliably with authenticated clusters
        try:
            return self.execute_query_http(query)
        except Exception as e:
            logger.warning(f"HTTP query failed: {e}, trying PinotDB fallback")
            try:
                return self.execute_query_pinotdb(query, params)
            except Exception as pinotdb_error:
                logger.error(f"Both HTTP and PinotDB queries failed. HTTP: {e}, PinotDB: {pinotdb_error}")
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
            if query.strip().endswith(';'):
                query = query.rstrip(';')
            query = f"{query} OPTION(timeoutMs={timeout_ms})"
            logger.debug(f"Added timeout option: {timeout_ms}ms")
        
        return query
    
    def execute_query_pinotdb(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
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

    def get_tables(self, params: dict[str, Any] | None = None) -> list[str]:
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLES}"
        logger.debug(f"Fetching tables from: {url}")
        response = self.http_request(url)
        tables = response.json()["tables"]
        logger.debug(f"Successfully fetched {len(tables)} tables")
        return tables

    def get_table_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLE_SIZE.format(tableName)}"
        logger.debug(f"Fetching table details for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def get_segment_metadata_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.config.controller_url}/{PinotEndpoints.SEGMENT_METADATA.format(tableName)}"
        logger.debug(f"Fetching segment metadata for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def get_segments(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.config.controller_url}/{PinotEndpoints.SEGMENTS.format(tableName)}"
        logger.debug(f"Fetching segments for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def get_index_column_detail(self, tableName: str, segmentName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        for type_suffix in ["REALTIME", "OFFLINE"]:
            url = f"{self.config.controller_url}/{PinotEndpoints.SEGMENT_DETAIL.format(tableName, type_suffix, segmentName)}"
            logger.debug(f"Trying to fetch index column details from: {url}")
            try:
                response = self.http_request(url)
                return response.json()
            except Exception as e:
                logger.error(f"Failed to fetch index column details for {tableName}_{type_suffix}/{segmentName}: {e}")
                continue
        raise ValueError("Index column detail not found")

    def get_tableconfig_schema_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLE_CONFIG.format(tableName)}"
        logger.debug(f"Fetching table config for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def list_tools(self) -> list[types.Tool]:
        return [
            types.Tool(
                name="test-connection",
                description="Test the Pinot connection and return diagnostic information",
                inputSchema={"type": "object", "properties": {}},
            ),
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
            case "test-connection":
                return [types.TextContent(type="text", text=str(self.test_connection()))]
            case "read-query":
                return [types.TextContent(type="text", text=str(self.execute_query(arguments["query"])))]
            case "list-tables":
                return [types.TextContent(type="text", text=str(self.get_tables()))]
            case "table-details":
                return [types.TextContent(type="text", text=str(self.get_table_detail(arguments["tableName"])))]
            case "segment-list":
                return [types.TextContent(type="text", text=str(self.get_segments(arguments["tableName"])))]
            case "index-column-details":
                return [types.TextContent(type="text", text=str(self.get_index_column_detail(arguments["tableName"], arguments["segmentName"])))]
            case "segment-metadata-details":
                return [types.TextContent(type="text", text=str(self.get_segment_metadata_detail(arguments["tableName"])))]
            case "tableconfig-schema-details":
                return [types.TextContent(type="text", text=str(self.get_tableconfig_schema_detail(arguments["tableName"])))]
            case _:
                raise ValueError(f"Unknown tool: {name}")
