from typing import Any
import pandas as pd
import requests
import mcp.types as types
from pinotdb import connect
import os
from dotenv import load_dotenv
from .logging_config import get_logger

# Load environment variables from .env file
load_dotenv()

logger = get_logger()

# Increase timeout configurations for authenticated clusters
REQUEST_TIMEOUT = 90  # 90 seconds for HTTP requests
CONNECTION_TIMEOUT = 60  # 60 seconds for connection establishment
QUERY_TIMEOUT = 120  # 2 minutes for query execution

# Get configuration from environment variables
PINOT_CONTROLLER_URL = os.getenv("PINOT_CONTROLLER_URL")
PINOT_BROKER_HOST = os.getenv("PINOT_BROKER_HOST")
PINOT_BROKER_PORT = int(os.getenv("PINOT_BROKER_PORT", "443"))
PINOT_BROKER_SCHEME = os.getenv("PINOT_BROKER_SCHEME", "https")
PINOT_USERNAME = os.getenv("PINOT_USERNAME")
PINOT_PASSWORD = os.getenv("PINOT_PASSWORD")
PINOT_USE_MSQE = os.getenv("PINOT_USE_MSQE", "false").lower() == "true"
PINOT_DATABASE = os.getenv("PINOT_DATABASE", "")
PINOT_TOKEN = os.getenv("PINOT_TOKEN", "")

# Setup headers for HTTP requests
HEADERS = {
    "accept": "application/json",
    "Content-Type": "application/json"
}

# Add authentication headers
if PINOT_TOKEN:
    # Use token as-is for HTTP requests
    HEADERS["Authorization"] = PINOT_TOKEN
elif PINOT_USERNAME and PINOT_PASSWORD:
    import base64
    credentials = base64.b64encode(f"{PINOT_USERNAME}:{PINOT_PASSWORD}".encode()).decode()
    HEADERS["Authorization"] = f"Basic {credentials}"

if PINOT_DATABASE:
    HEADERS["database"] = PINOT_DATABASE

# Global connection variable to reuse connections
_conn = None

def create_connection():
    """Create Pinot connection with proper authentication handling"""
    try:
        # Reload environment variables to pick up any changes
        load_dotenv(override=True)
        
        # Get fresh configuration from environment variables
        broker_host = os.getenv("PINOT_BROKER_HOST")
        broker_port = int(os.getenv("PINOT_BROKER_PORT", "443"))
        broker_scheme = os.getenv("PINOT_BROKER_SCHEME", "https")
        username = os.getenv("PINOT_USERNAME")
        password = os.getenv("PINOT_PASSWORD")
        use_msqe = os.getenv("PINOT_USE_MSQE", "false").lower() == "true"
        database = os.getenv("PINOT_DATABASE", "")
        token = os.getenv("PINOT_TOKEN", "")
        
        # For pinotdb connection, use either token directly or username/password
        auth_username = None
        auth_password = None
        
        if token:
            # For Bearer tokens, some Pinot setups expect the token as password
            # and empty or token as username
            if token.startswith("Bearer "):
                auth_password = token  # Use full Bearer token as password
                auth_username = ""  # Empty username
            else:
                auth_password = token
                auth_username = ""
        elif username and password:
            auth_username = username
            auth_password = password
        
        logger.debug(f"Creating connection to {broker_host}:{broker_port} with MSQE={use_msqe}")
        logger.debug(f"Database: {database}, Auth method: {'token' if token else 'username/password'}")
        
        # Create connection with extended timeout and additional parameters
        connection = connect(
            host=broker_host,
            port=broker_port,
            path="/query/sql",
            scheme=broker_scheme,
            username=auth_username,
            password=auth_password,
            use_multistage_engine=use_msqe,
            database=database,
            extra_conn_args={
                'timeout': QUERY_TIMEOUT,  # Use the longer query timeout
                'verify': True,  # Enable SSL verification
                'retries': 3,  # Add retry mechanism
                'backoff_factor': 1.0  # Backoff between retries
            }
        )
        
        # Test the connection with a simple query
        test_cursor = connection.cursor()
        test_cursor.execute("SELECT 1")
        test_result = test_cursor.fetchall()
        logger.debug(f"Connection test successful: {test_result}")
        
        return connection
        
    except Exception as e:
        logger.error(f"Failed to create Pinot connection: {e}")
        logger.error(f"Connection details - Host: {broker_host}, Port: {broker_port}, Scheme: {broker_scheme}")
        raise

def get_connection():
    """Get or create a reusable connection"""
    global _conn
    try:
        if _conn is None:
            _conn = create_connection()
        else:
            # Test if connection is still alive
            test_cursor = _conn.cursor()
            test_cursor.execute("SELECT 1")
            test_cursor.fetchall()
        return _conn
    except Exception as e:
        logger.warning(f"Connection test failed, creating new connection: {e}")
        _conn = create_connection()
        return _conn

# Initialize connection
try:
    conn = get_connection()
    logger.info("Initial Pinot connection established successfully")
except Exception as e:
    logger.error(f"Failed to establish initial connection: {e}")
    conn = None


class Pinot:
    def __init__(self):
        self.insights: list[str] = []

    def test_connection(self) -> dict[str, Any]:
        """Test the connection and return diagnostic information"""
        result = {
            "connection_test": False,
            "query_test": False,
            "tables_test": False,
            "error": None,
            "config": {
                "broker_host": PINOT_BROKER_HOST,
                "broker_port": PINOT_BROKER_PORT,
                "broker_scheme": PINOT_BROKER_SCHEME,
                "controller_url": PINOT_CONTROLLER_URL,
                "database": PINOT_DATABASE,
                "use_msqe": PINOT_USE_MSQE,
                "has_token": bool(PINOT_TOKEN),
                "has_username": bool(PINOT_USERNAME),
                "timeout_config": {
                    "connection": CONNECTION_TIMEOUT,
                    "request": REQUEST_TIMEOUT
                }
            }
        }
        
        try:
            # Test basic connection
            conn = get_connection()
            result["connection_test"] = True
            
            # Test simple query
            curs = conn.cursor()
            curs.execute("SELECT 1 as test_column")
            test_result = curs.fetchall()
            result["query_test"] = True
            result["query_result"] = test_result
            
            # Test tables listing
            tables = self._get_tables()
            result["tables_test"] = True
            result["tables_count"] = len(tables)
            result["sample_tables"] = tables[:5] if tables else []
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Connection test failed: {e}")
        
        return result

    def _execute_query_http(self, query: str) -> list[dict[str, Any]]:
        """Alternative query execution using HTTP requests directly to broker"""
        broker_url = f"{PINOT_BROKER_SCHEME}://{PINOT_BROKER_HOST}:{PINOT_BROKER_PORT}/query/sql"
        logger.debug(f"Executing query via HTTP: {query[:100]}...")
        
        # Prepare the request payload
        payload = {
            "sql": query,
            "queryOptions": f"timeoutMs={QUERY_TIMEOUT * 1000}"
        }
        
        try:
            response = requests.post(
                broker_url,
                headers=HEADERS,
                json=payload,
                timeout=(CONNECTION_TIMEOUT, REQUEST_TIMEOUT),
                verify=True
            )
            response.raise_for_status()
            
            result_data = response.json()
            
            # Check for query errors in response
            if 'exceptions' in result_data and result_data['exceptions']:
                raise Exception(f"Query error: {result_data['exceptions']}")
            
            # Parse the result into pandas-like format
            if 'resultTable' in result_data:
                columns = result_data['resultTable']['dataSchema']['columnNames']
                rows = result_data['resultTable']['rows']
                
                # Convert to list of dictionaries
                result = []
                for row in rows:
                    result.append(dict(zip(columns, row)))
                
                logger.debug(f"HTTP query executed successfully, returned {len(result)} rows")
                return result
            else:
                logger.warning("No resultTable in response, returning empty result")
                return []
                
        except requests.exceptions.Timeout:
            logger.error(f"HTTP query timeout after {REQUEST_TIMEOUT} seconds")
            raise
        except Exception as e:
            logger.error(f"HTTP query execution failed: {e}")
            raise

    def _execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        logger.debug(f"Executing query: {query[:100]}...")  # Log first 100 chars
        
        # Use HTTP as primary method since it works reliably with authenticated clusters
        try:
            return self._execute_query_http(query)
        except Exception as e:
            logger.warning(f"HTTP query failed: {e}, trying PinotDB fallback")
            try:
                return self._execute_query_pinotdb(query, params)
            except Exception as pinotdb_error:
                logger.error(f"Both HTTP and PinotDB queries failed. HTTP: {e}, PinotDB: {pinotdb_error}")
                raise
    
    def _execute_query_pinotdb(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Original pinotdb-based query execution"""
        logger.debug(f"Executing query via PinotDB: {query[:100]}...")
        try:
            # Use the reusable connection
            current_conn = get_connection()
            curs = current_conn.cursor()
            
            # Remove database prefix if present
            if PINOT_DATABASE and f"{PINOT_DATABASE}." in query:
                query = query.replace(f"{PINOT_DATABASE}.", "")
                logger.debug(f"Removed database prefix, query now: {query[:100]}...")
            
            # Add query timeout hint if not present
            if "SET timeoutMs" not in query.upper() and "OPTION" not in query.upper():
                # Add timeout option to the query
                timeout_ms = QUERY_TIMEOUT * 1000  # Convert to milliseconds
                if query.strip().endswith(';'):
                    query = query.rstrip(';')
                query = f"{query} OPTION(timeoutMs={timeout_ms})"
                logger.debug(f"Added timeout option: {timeout_ms}ms")
            
            logger.debug(f"Final query: {query}")
            
            # Execute with timeout
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
            global _conn
            _conn = None
            raise

    def _get_tables(self, params: dict[str, Any] | None = None) -> list[str]:
        url = f"{PINOT_CONTROLLER_URL}/tables"
        logger.debug(f"Fetching tables from: {url}")
        try:
            response = requests.get(
                url, 
                headers=HEADERS, 
                timeout=(CONNECTION_TIMEOUT, REQUEST_TIMEOUT),
                verify=True
            )
            response.raise_for_status()
            tables = response.json()["tables"]
            logger.debug(f"Successfully fetched {len(tables)} tables")
            return tables
        except requests.exceptions.Timeout:
            logger.error(f"Timeout occurred while fetching tables from {url}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch tables: {e}")
            raise

    def _get_table_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tables/{tableName}/size"
        logger.debug(f"Fetching table details for {tableName} from: {url}")
        try:
            response = requests.get(
                url, 
                headers=HEADERS, 
                timeout=(CONNECTION_TIMEOUT, REQUEST_TIMEOUT),
                verify=True
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout occurred while fetching table details for {tableName}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch table details for {tableName}: {e}")
            raise

    def _get_segment__metadata_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}/metadata"
        logger.debug(f"Fetching segment metadata for {tableName} from: {url}")
        try:
            response = requests.get(
                url, 
                headers=HEADERS, 
                timeout=(CONNECTION_TIMEOUT, REQUEST_TIMEOUT),
                verify=True
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout occurred while fetching segment metadata for {tableName}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch segment metadata for {tableName}: {e}")
            raise

    def _get_segments(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}"
        logger.debug(f"Fetching segments for {tableName} from: {url}")
        try:
            response = requests.get(
                url, 
                headers=HEADERS, 
                timeout=(CONNECTION_TIMEOUT, REQUEST_TIMEOUT),
                verify=True
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout occurred while fetching segments for {tableName}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch segments for {tableName}: {e}")
            raise

    def _get_index_column_detail(self, tableName: str, segmentName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        for type_suffix in ["REALTIME", "OFFLINE"]:
            url = f"{PINOT_CONTROLLER_URL}/segments/{tableName}_{type_suffix}/{segmentName}/metadata?columns=*"
            logger.debug(f"Trying to fetch index column details from: {url}")
            try:
                response = requests.get(
                    url, 
                    headers=HEADERS, 
                    timeout=(CONNECTION_TIMEOUT, REQUEST_TIMEOUT),
                    verify=True
                )
                if response.status_code == 200:
                    return response.json()
            except requests.exceptions.Timeout:
                logger.error(f"Timeout occurred while fetching index column details for {tableName}_{type_suffix}/{segmentName}")
                continue
            except Exception as e:
                logger.error(f"Failed to fetch index column details for {tableName}_{type_suffix}/{segmentName}: {e}")
                continue
        raise ValueError("Index column detail not found")

    def _get_tableconfig_schema_detail(self, tableName: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{PINOT_CONTROLLER_URL}/tableConfigs/{tableName}"
        logger.debug(f"Fetching table config for {tableName} from: {url}")
        try:
            response = requests.get(
                url, 
                headers=HEADERS, 
                timeout=(CONNECTION_TIMEOUT, REQUEST_TIMEOUT),
                verify=True
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Timeout occurred while fetching table config for {tableName}")
            raise
        except Exception as e:
            logger.error(f"Failed to fetch table config for {tableName}: {e}")
            raise

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
                return [types.TextContent(type="text", text=str(self._execute_query(arguments["query"])))]
            case "list-tables":
                return [types.TextContent(type="text", text=str(self._get_tables()))]
            case "table-details":
                return [types.TextContent(type="text", text=str(self._get_table_detail(arguments["tableName"])))]
            case "segment-list":
                return [types.TextContent(type="text", text=str(self._get_segments(arguments["tableName"])))]
            case "index-column-details":
                return [types.TextContent(type="text", text=str(self._get_index_column_detail(arguments["tableName"], arguments["segmentName"])))]
            case "segment-metadata-details":
                return [types.TextContent(type="text", text=str(self._get_segment__metadata_detail(arguments["tableName"])))]
            case "tableconfig-schema-details":
                return [types.TextContent(type="text", text=str(self._get_tableconfig_schema_detail(arguments["tableName"])))]
            case _:
                raise ValueError(f"Unknown tool: {name}")
