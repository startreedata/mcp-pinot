import base64
from decimal import Decimal, InvalidOperation
from fnmatch import fnmatch
import hashlib
import json
import math
import re
from threading import Lock
import time
from typing import Any, Literal
import unicodedata
from urllib.parse import quote, unquote
from uuid import uuid4

from pinotdb import connect
import requests
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .config import PinotConfig, get_logger, reload_table_filters_from_file
from .models import (
    QueryEarlyTerminationReason,
    QueryExecutionLimitName,
    QueryExecutionMetadata,
    QueryExecutionResult,
)

logger = get_logger()

MAX_QUERY_ROWS = 10_501
_QUERY_PRIMITIVE_TYPES = frozenset(
    {
        "INT",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "BOOLEAN",
        "STRING",
        "BYTES",
        "TIMESTAMP",
        "BIG_DECIMAL",
    }
)
_QUERY_STAT_FIELDS = (
    "timeUsedMs",
    "numDocsScanned",
    "totalDocs",
    "numEntriesScannedInFilter",
    "numEntriesScannedPostFilter",
    "numSegmentsQueried",
    "numSegmentsProcessed",
    "numSegmentsMatched",
    "numConsumingSegmentsQueried",
    "minConsumingFreshnessTimeMs",
    "numRowsResultSet",
    "offlineThreadCpuTimeNs",
    "realtimeThreadCpuTimeNs",
    "offlineSystemActivitiesCpuTimeNs",
    "realtimeSystemActivitiesCpuTimeNs",
    "offlineResponseSerializationCpuTimeNs",
    "realtimeResponseSerializationCpuTimeNs",
    "brokerReduceTimeMs",
)
_QUERY_LIMIT_FLAGS: tuple[QueryExecutionLimitName, ...] = (
    "numGroupsLimitReached",
    "maxRowsInJoinReached",
    "maxRowsInWindowReached",
    "mseLiteLeafStageLimitReached",
)
_EARLY_TERMINATION_REASONS: dict[str, QueryEarlyTerminationReason] = {
    "DISTINCT_MAX_ROWS": "DISTINCT_MAX_ROWS",
    "DISTINCT_MAX_ROWS_WITHOUT_CHANGE": "DISTINCT_MAX_ROWS_WITHOUT_CHANGE",
    "DISTINCT_MAX_EXECUTION_TIME": "DISTINCT_MAX_EXECUTION_TIME",
}


def _native_value_matches(value: Any, data_type: str) -> bool:
    """Reject cells that contradict the broker's declared primitive column type."""
    if value is None:
        return True
    if data_type.endswith("_ARRAY"):
        return isinstance(value, list) and all(
            _native_value_matches(item, data_type.removesuffix("_ARRAY"))
            for item in value
        )
    if data_type in {"INT", "LONG"}:
        return type(value) is int
    if data_type in {"FLOAT", "DOUBLE"}:
        return type(value) in (int, float)
    if data_type == "BOOLEAN":
        return type(value) is bool
    if data_type in {"STRING", "BYTES"}:
        return isinstance(value, str)
    if data_type == "TIMESTAMP":
        return type(value) in (str, int)
    if data_type == "BIG_DECIMAL":
        if type(value) not in (str, int, float):
            return False
        try:
            return Decimal(str(value)).is_finite()
        except InvalidOperation:
            return False
    return True  # Unrecognized types retain JSON values but cannot qualify as complete.


def get_auth_credentials(config: PinotConfig) -> tuple[str | None, str | None]:
    """Extract authentication credentials for PinotDB connection"""
    if config.token:
        if config.token.startswith("Bearer "):
            return "", config.token  # Empty username, full Bearer token as password
        else:
            return "", config.token
    elif config.username and config.password:
        return config.username, config.password
    return None, None


def test_connection_query(connection: Any) -> None:
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
    SEGMENT_DETAIL = "segments/{}/{}/metadata?columns=*"
    TABLE_CONFIG = "tableConfigs/{}"


_FORBIDDEN_PATH_COMPONENT_CHARS = frozenset("/\\?#")


def validate_pinot_path_component(value: str, component_name: str) -> str:
    """Validate one untrusted value before it is used as a Pinot URL segment.

    Validation also examines repeatedly percent-decoded forms so encoded traversal
    and delimiter payloads cannot become dangerous in an upstream proxy or client.
    The original, validated value is returned for callers that do not build URLs.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"{component_name} must be a non-empty string.")

    candidate = value
    for decoding_round in range(6):
        if candidate in {".", ".."}:
            raise ValueError(f"{component_name} must not be a traversal segment.")
        if any(char in _FORBIDDEN_PATH_COMPONENT_CHARS for char in candidate):
            raise ValueError(f"{component_name} contains a URL path delimiter.")
        if any(unicodedata.category(char) == "Cc" for char in candidate):
            raise ValueError(f"{component_name} contains a control character.")
        decoded = unquote(candidate)
        if decoded == candidate:
            return value
        if decoding_round == 5:
            raise ValueError(
                f"{component_name} contains excessive percent-encoding layers."
            )
        candidate = decoded

    raise AssertionError("unreachable percent-decoding state")


def encode_pinot_path_component(value: str, component_name: str) -> str:
    """Validate and percent-encode a value as exactly one Pinot URL segment."""
    validate_pinot_path_component(value, component_name)
    return quote(value, safe="")


_READ_QUERY_START_KEYWORDS = {"SELECT", "WITH"}
_PROHIBITED_READ_QUERY_KEYWORDS = {
    "ALTER",
    "CALL",
    "COPY",
    "CREATE",
    "DELETE",
    "DESCRIBE",
    "DROP",
    "EXEC",
    "EXECUTE",
    "EXPLAIN",
    "EXPORT",
    "GRANT",
    "IMPORT",
    "INSERT",
    "INTO",
    "LOAD",
    "MERGE",
    "REFRESH",
    "REPLACE",
    "RESET",
    "REVOKE",
    "SET",
    "SHOW",
    "TRUNCATE",
    "UPDATE",
    "UPSERT",
    "USE",
}


def _strip_sql_comments(query: str) -> str:
    """Remove SQL comments while preserving quoted strings and identifiers."""
    result: list[str] = []
    quote: str | None = None
    i = 0

    while i < len(query):
        char = query[i]
        next_char = query[i + 1] if i + 1 < len(query) else ""

        if quote:
            result.append(char)
            if char == "\\" and quote in {"'", '"'} and next_char:
                result.append(next_char)
                i += 2
                continue
            if char == quote:
                if next_char == quote:
                    result.append(next_char)
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if char in {"'", '"', "`"}:
            quote = char
            result.append(char)
            i += 1
            continue

        if char == "-" and next_char == "-":
            i += 2
            while i < len(query) and query[i] not in {"\n", "\r"}:
                i += 1
            result.append("\n")
            continue

        if char == "/" and next_char == "*":
            i += 2
            while i + 1 < len(query) and not (query[i] == "*" and query[i + 1] == "/"):
                i += 1
            i = min(i + 2, len(query))
            result.append(" ")
            continue

        result.append(char)
        i += 1

    return "".join(result)


def _split_sql_statements(query: str) -> list[str]:
    """Split SQL on semicolons outside quoted strings and identifiers."""
    query = _strip_sql_comments(query)
    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    i = 0

    while i < len(query):
        char = query[i]
        next_char = query[i + 1] if i + 1 < len(query) else ""

        if quote:
            current.append(char)
            if char == "\\" and quote in {"'", '"'} and next_char:
                current.append(next_char)
                i += 2
                continue
            if char == quote:
                if next_char == quote:
                    current.append(next_char)
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if char in {"'", '"', "`"}:
            quote = char
            current.append(char)
            i += 1
            continue

        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    statement = "".join(current).strip()
    if statement:
        statements.append(statement)

    return statements


def _sql_words(statement: str, *, top_level_only: bool = False) -> list[str]:
    """Return unquoted SQL word tokens in order."""
    words: list[str] = []
    current: list[str] = []
    quote: str | None = None
    depth = 0
    i = 0

    def append_current_word() -> None:
        if current and (not top_level_only or depth == 0):
            words.append("".join(current).upper())
        current.clear()

    while i < len(statement):
        char = statement[i]
        next_char = statement[i + 1] if i + 1 < len(statement) else ""

        if quote:
            if char == "\\" and quote in {"'", '"'} and next_char:
                i += 2
                continue
            if char == quote:
                if next_char == quote:
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if char in {"'", '"', "`"}:
            append_current_word()
            quote = char
            i += 1
            continue

        if char.isalnum() or char == "_":
            current.append(char)
        elif char == "(":
            append_current_word()
            depth += 1
        elif char == ")":
            append_current_word()
            depth = max(0, depth - 1)
        else:
            append_current_word()
        i += 1

    append_current_word()

    return words


def _strip_trailing_pinot_option(statement: str) -> str:
    """Remove a top-level trailing Pinot OPTION clause for SQL parser validation."""
    quote: str | None = None
    depth = 0
    i = 0

    def option_clause_ends_statement(index: int) -> bool:
        while index < len(statement) and statement[index].isspace():
            index += 1
        if index >= len(statement) or statement[index] != "(":
            return False

        option_quote: str | None = None
        option_depth = 0
        while index < len(statement):
            char = statement[index]
            next_char = statement[index + 1] if index + 1 < len(statement) else ""

            if option_quote:
                if char == "\\" and option_quote in {"'", '"'} and next_char:
                    index += 2
                    continue
                if char == option_quote:
                    if next_char == option_quote:
                        index += 2
                        continue
                    option_quote = None
                index += 1
                continue

            if char in {"'", '"', "`"}:
                option_quote = char
            elif char == "(":
                option_depth += 1
            elif char == ")":
                option_depth -= 1
                if option_depth == 0:
                    index += 1
                    break
            index += 1

        if option_depth != 0:
            return False

        while index < len(statement) and statement[index].isspace():
            index += 1

        return index == len(statement)

    while i < len(statement):
        char = statement[i]
        next_char = statement[i + 1] if i + 1 < len(statement) else ""

        if quote:
            if char == "\\" and quote in {"'", '"'} and next_char:
                i += 2
                continue
            if char == quote:
                if next_char == quote:
                    i += 2
                    continue
                quote = None
            i += 1
            continue

        if char in {"'", '"', "`"}:
            quote = char
            i += 1
            continue

        if char == "(":
            depth += 1
            i += 1
            continue

        if char == ")":
            depth = max(0, depth - 1)
            i += 1
            continue

        if depth == 0 and (char.isalpha() or char == "_"):
            word_start = i
            while i < len(statement) and (
                statement[i].isalnum() or statement[i] == "_"
            ):
                i += 1
            word = statement[word_start:i]
            if word.upper() == "OPTION" and option_clause_ends_statement(i):
                return statement[:word_start].rstrip()
            continue

        i += 1

    return statement


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

    def reload_table_filters(
        self,
        dry_run: bool = True,
        *,
        expected_filters: list[str] | None = None,
        require_expected: bool = False,
    ) -> dict[str, Any]:
        """Preview or reload filters from the configured file without restarting.

        This allows dynamic updates to the table access list by:
        1. Editing the YAML filter file
        2. Calling this method to reload the configuration

        Args:
            dry_run: Validate and report the candidate filters without applying them.
                Defaults to ``True`` so direct/library callers are preview-first too.
            expected_filters: Exact candidate approved by the preceding preview.
            require_expected: Reject the apply if the file no longer produces the
                expected candidate, preventing preview/apply time-of-check races.

        Returns:
            dict: Preview/application status with previous and new filters and counts.

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

        action = "Previewing" if dry_run else "Reloading"
        logger.info(f"{action} table filters from {self.config.table_filter_file}")

        # Load new filters (validates file exists and parses YAML)
        new_filters = reload_table_filters_from_file(self.config.table_filter_file)
        if require_expected and new_filters != expected_filters:
            raise ValueError(
                "Table filter file changed after preview; preview the current file "
                "again and confirm the new candidate."
            )

        # Snapshot and, unless this is a preview, atomically update the filters.
        with self._config_lock:
            old_filters = (
                list(self._included_tables)
                if self._included_tables is not None
                else None
            )
            old_count = len(old_filters) if old_filters else 0
            if not dry_run:
                self._included_tables = new_filters
            new_count = len(new_filters) if new_filters else 0

        if dry_run:
            logger.info(f"Table filter preview: {old_count} -> {new_count} patterns")
        else:
            logger.info(f"Table filters reloaded: {old_count} -> {new_count} patterns")

        return {
            "status": "preview" if dry_run else "success",
            "message": (
                "Table filters validated; no change applied"
                if dry_run
                else "Table filters reloaded successfully"
            ),
            "applied": not dry_run,
            "previous_filter_count": old_count,
            "new_filter_count": new_count,
            "previous_filters": old_filters,
            "new_filters": new_filters,
        }

    def _create_auth_headers(self, *, controller: bool = False) -> dict[str, str]:
        """Create HTTP headers with authentication based on configuration"""
        headers = {"accept": "application/json", "Content-Type": "application/json"}

        token = (
            (self.config.controller_token or self.config.token)
            if controller
            else self.config.token
        )
        username = (
            (self.config.controller_username or self.config.username)
            if controller
            else self.config.username
        )
        password = (
            (self.config.controller_password or self.config.password)
            if controller
            else self.config.password
        )

        if token:
            headers["Authorization"] = token
        elif username and password:
            creds_str = f"{username}:{password}"
            credentials = base64.b64encode(creds_str.encode()).decode()
            headers["Authorization"] = f"Basic {credentials}"

        if self.config.database:
            headers["database"] = self.config.database

        return headers

    def http_request(
        self,
        url: str,
        method: str = "GET",
        json_data: dict[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> requests.Response:
        """Make HTTP request with authentication headers and timeout handling"""
        is_controller = url.rstrip("/").startswith(
            self.config.controller_url.rstrip("/") + "/"
        )
        headers = self._create_auth_headers(controller=is_controller)
        timeout = self._request_timeouts(timeout_seconds)

        try:
            if method.upper() == "POST":
                response = requests.post(
                    url,
                    headers=headers,
                    json=json_data,
                    timeout=timeout,
                    verify=True,
                )
            else:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=timeout,
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

    def _request_timeouts(self, timeout_seconds: float | None) -> tuple[float, float]:
        """Cap connection/read thresholds without changing shared configuration.

        Requests timeouts bound network inactivity, not native cancellation or a
        wall-clock limit against a peer that continuously streams response bytes.
        """
        if timeout_seconds is None:
            return self.config.connection_timeout, self.config.request_timeout
        if (
            type(timeout_seconds) not in (int, float)
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a positive finite number.")
        connect = min(self.config.connection_timeout, timeout_seconds / 2)
        read = min(self.config.request_timeout, timeout_seconds - connect)
        return connect, read

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
        """Test connectivity the way the tools use it and return diagnostics.

        Each check runs independently and through the same code path as the tool it
        stands in for — the query check via :meth:`execute_query_with_metadata` (what
        ``read_query`` uses) and the listing check via :meth:`get_tables` (what
        ``list_tables`` uses). An earlier version probed through the pinotdb DB-API
        connection instead, which no tool uses: against a broker whose response
        pinotdb rejects (``check_sufficient_responded``) it reported every check
        failed on a deployment where every tool worked, sending operators to chase
        endpoints and credentials that were fine.

        The DB-API probe is still run, but only as an informational ``dbapi_test``
        signal: it never decides whether the deployment is healthy.
        """
        result: dict[str, Any] = {
            "connection_test": False,
            "query_test": False,
            "tables_test": False,
            "dbapi_test": False,
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

        failed: list[str] = []

        # Broker query check — the path read_query takes. A successful query also
        # proves the broker connection, so connection_test is derived from it
        # rather than from a separate client the tools never use.
        try:
            execution = self.execute_query_with_metadata(
                "SELECT 1 AS test_column", max_rows=1
            )
            if (
                execution.columns != ["test_column"]
                or execution.rows != [{"test_column": 1}]
                or type(execution.rows[0]["test_column"]) is not int
            ):
                raise ValueError("Pinot did not return the connection probe result.")
            result["query_result"] = execution.rows
            result["connection_test"] = True
            result["query_test"] = True
        except Exception as e:
            logger.error("Connection test: broker query failed: %s", e, exc_info=True)
            failed.append("broker query")

        # Controller listing check — the path list_tables takes.
        try:
            tables = self.get_tables()
            result["tables_test"] = True
            result["tables_count"] = len(tables)
            result["sample_tables"] = tables[:5] if tables else []
        except Exception as e:
            logger.error(
                "Connection test: controller table listing failed: %s", e, exc_info=True
            )
            failed.append("controller table listing")

        # Informational only: some brokers return responses the pinotdb DB-API
        # rejects even though the REST path the tools use works fine.
        try:
            cursor = self.get_connection().cursor()
            cursor.execute("SELECT 1")
            cursor.fetchall()
            result["dbapi_test"] = True
        except Exception as e:
            logger.info(
                "Connection test: optional pinotdb DB-API probe failed "
                "(does not affect any tool): %s",
                e,
            )

        if failed:
            result["error"] = (
                "Pinot connectivity check failed for: "
                + ", ".join(failed)
                + ". Verify endpoints, credentials, and network access; consult "
                "server logs for the correlation detail."
            )

        return result

    def execute_query_http(self, query: str) -> list[dict[str, Any]]:
        """Alternative query execution using HTTP requests directly to broker"""
        broker_url = f"{self.config.broker_scheme}://{self.config.broker_host}:{self.config.broker_port}/{PinotEndpoints.QUERY_SQL}"
        query_id = hashlib.sha256(query.encode()).hexdigest()[:16]
        logger.debug("Executing query via HTTP (query_id=%s)", query_id)

        query_options = f"timeoutMs={self.config.query_timeout * 1000}"
        if self.config.use_msqe:
            query_options += ";useMultiStageEngine=true"
        payload = {
            "sql": query,
            "queryOptions": query_options,
        }

        response = self.http_request(broker_url, "POST", payload)
        result_data = response.json()

        # Check for query errors in response
        if result_data.get("exceptions"):
            logger.info(
                "Pinot rejected query syntax or semantics (query_id=%s, errors=%d)",
                query_id,
                len(result_data["exceptions"]),
            )
            raise ValueError(
                "Pinot rejected the SQL query. Check table and column names, "
                "function arguments, and query syntax."
            )

        # Parse the result into pandas-like format
        if "resultTable" in result_data:
            columns = result_data["resultTable"]["dataSchema"]["columnNames"]
            rows = result_data["resultTable"]["rows"]

            # Convert to list of dictionaries
            result = [dict(zip(columns, row, strict=False)) for row in rows]
            logger.debug(f"HTTP query returned {len(result)} rows")
            return result
        else:
            logger.warning("No resultTable in response, returning empty result")
            return []

    def execute_query_with_metadata(
        self,
        query: str,
        max_rows: int = 501,
        *,
        timeout_seconds: float | None = None,
        request_id: str | None = None,
        application_name: str | None = None,
    ) -> QueryExecutionResult:
        """Execute bounded read-only SQL once, retaining native evidence.

        Missing execution metadata remains unknown, and partial execution remains
        partial even when rows exist. Complete execution is not proof that the SQL
        covered an entire dataset or that its LIMIT did not omit rows. This path
        never retries or falls back after an ambiguous submission.
        """
        self._request_timeouts(timeout_seconds)
        deadline = (
            time.monotonic() + timeout_seconds if timeout_seconds is not None else None
        )
        query = self.validate_read_query(query)
        if type(max_rows) is not int or not 1 <= max_rows <= MAX_QUERY_ROWS:
            raise ValueError(f"max_rows must be between 1 and {MAX_QUERY_ROWS}")
        query = self._bound_read_query(query, max_rows)
        self._validate_table_access(query)
        request_id = request_id if request_id is not None else uuid4().hex
        for value in (request_id, application_name):
            if value is not None and (
                not isinstance(value, str)
                or re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", value) is None
            ):
                raise ValueError(
                    "Query correlation values require 1-128 safe characters."
                )
        remaining = deadline - time.monotonic() if deadline is not None else None
        if remaining is not None and remaining < 0.002:
            raise requests.Timeout("Query budget expired before submission.")
        _, read_timeout = self._request_timeouts(remaining)
        native_timeout_ms = int(min(self.config.query_timeout, read_timeout) * 1000)
        options = [f"timeoutMs={native_timeout_ms}", f"clientQueryId={request_id}"]
        if self.config.use_msqe:
            options.append("useMultiStageEngine=true")
        if application_name is not None:
            options.append(f"applicationName={application_name}")
        broker_url = (
            f"{self.config.broker_scheme}://{self.config.broker_host}:"
            f"{self.config.broker_port}/{PinotEndpoints.QUERY_SQL}"
        )
        response = self.http_request(
            broker_url,
            "POST",
            {"sql": query, "queryOptions": ";".join(options)},
            timeout_seconds=remaining,
        )
        if deadline is not None and time.monotonic() >= deadline:
            raise requests.Timeout("Query budget expired before response validation.")
        try:
            data = response.json()
        except ValueError as exc:
            raise ValueError("Pinot returned malformed query JSON.") from exc
        bound = int(
            sqlglot.parse_one(query, read="trino").args["limit"].expression.this
        )
        result = self._query_execution_result(data, query, request_id, bound)
        if deadline is not None and time.monotonic() >= deadline:
            raise requests.Timeout("Query budget expired during response validation.")
        return result

    @staticmethod
    def _query_execution_result(
        data: Any, query: str, request_id: str, row_bound: int
    ) -> QueryExecutionResult:
        """Validate broker data without promoting missing metadata to success."""
        malformed = "Pinot returned malformed query execution evidence."
        if not isinstance(data, dict):
            raise ValueError(malformed)
        unknown = []
        exceptions = data.get("exceptions")
        if exceptions is None:
            unknown.append("exceptions_metadata_missing")
        elif not isinstance(exceptions, list):
            raise ValueError(malformed)
        elif exceptions:
            raise ValueError(
                "Pinot rejected the SQL query. Check table and column names, "
                "function arguments, and query syntax."
            )
        servers = []
        for key in ("numServersQueried", "numServersResponded"):
            value = data.get(key)
            if value is None:
                unknown.append(key + "_missing")
            elif type(value) is not int or value < 0:
                raise ValueError(malformed)
            servers.append(value)
        queried, responded = servers
        if queried is not None and responded is not None and responded > queried:
            raise ValueError(malformed)
        if queried == 0:
            unknown.append("no_server_execution_attested")
        flags: dict[str, bool | None] = {}
        for key in (*_QUERY_LIMIT_FLAGS, "partialResult", "isPartialResult"):
            value = data.get(key)
            if value is not None and type(value) is not bool:
                raise ValueError(malformed)
            flags[key] = value
        group_limit = flags["numGroupsLimitReached"]
        if group_limit is None:
            unknown.append("group_limit_metadata_missing")
        partial_values = [flags[key] for key in ("partialResult", "isPartialResult")]
        partial = any(value is True for value in partial_values)
        partial_flag = (
            partial if any(value is not None for value in partial_values) else None
        )
        limit_flags: dict[QueryExecutionLimitName, bool] = {}
        for key in _QUERY_LIMIT_FLAGS:
            value = flags[key]
            if value is not None:
                limit_flags[key] = value
        # These are enum names on the native wire, not free-form error messages.
        # Unknown future names still prove partial execution, but are not echoed.
        raw_reasons = data.get("earlyTerminationReasons", [])
        if (
            not isinstance(raw_reasons, list)
            or len(raw_reasons) > 16
            or not all(isinstance(reason, str) and reason for reason in raw_reasons)
        ):
            raise ValueError(malformed)
        termination_reasons = sorted(
            {
                _EARLY_TERMINATION_REASONS.get(reason, "UNRECOGNIZED_NATIVE_REASON")
                for reason in raw_reasons
            }
        )
        execution_limit = any(limit_flags.values()) or bool(raw_reasons)
        columns = []
        rows = []
        table = data.get("resultTable")
        if table is None:
            unknown.append("result_table_missing")
        else:
            if not isinstance(table, dict) or not isinstance(
                table.get("dataSchema"), dict
            ):
                raise ValueError(malformed)
            schema = table["dataSchema"]
            columns = schema.get("columnNames")
            raw_rows = table.get("rows")
            if (
                not isinstance(columns, list)
                or not columns
                or not all(isinstance(name, str) and name for name in columns)
                or len(set(columns)) != len(columns)
                or not isinstance(raw_rows, list)
                or len(raw_rows) > row_bound
                or any(
                    not isinstance(row, list) or len(row) != len(columns)
                    for row in raw_rows
                )
            ):
                raise ValueError(malformed)
            types = schema.get("columnDataTypes")
            if types is None:
                unknown.append("column_types_missing")
            elif (
                not isinstance(types, list)
                or len(types) != len(columns)
                or not all(isinstance(value, str) and value for value in types)
            ):
                raise ValueError(malformed)
            if types is not None and any(
                not _native_value_matches(value, data_type)
                for row in raw_rows
                for value, data_type in zip(row, types, strict=True)
            ):
                raise ValueError(malformed)
            if types is not None and any(
                data_type.removesuffix("_ARRAY") not in _QUERY_PRIMITIVE_TYPES
                for data_type in types
            ):
                unknown.append("column_type_not_validated")
            try:
                json.dumps(raw_rows, allow_nan=False)
            except (TypeError, ValueError) as exc:
                raise ValueError(malformed) from exc
            rows = [dict(zip(columns, row, strict=True)) for row in raw_rows]
        stats = {}
        for key in _QUERY_STAT_FIELDS:
            if key in data:
                value = data[key]
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError(malformed)
                stats[key] = value
        native_id = data.get("requestId")
        if native_id is not None and (
            type(native_id) not in (int, str)
            or len(str(native_id)) > 128
            or any(ord(char) < 32 for char in str(native_id))
        ):
            raise ValueError(malformed)
        incomplete = (
            partial
            or execution_limit
            or (queried is not None and responded is not None and responded < queried)
        )
        completeness: Literal["complete", "partial", "unknown"] = (
            "partial" if incomplete else "unknown" if unknown else "complete"
        )
        return QueryExecutionResult(
            columns=columns,
            rows=rows,
            metadata=QueryExecutionMetadata(
                completeness=completeness,
                request_id=request_id,
                native_query_id=native_id,
                query_sha256=hashlib.sha256(query.encode()).hexdigest(),
                servers_queried=queried,
                servers_responded=responded,
                partial_result=partial_flag,
                execution_limit_reached=(
                    execution_limit if limit_flags or raw_reasons else None
                ),
                execution_limit_flags=limit_flags,
                early_termination_reasons=termination_reasons,
                row_limit_reached=len(rows) >= row_bound,
                native_stats=stats,
                unknown_reasons=unknown,
            ),
        )

    def execute_query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        max_rows: int = 501,
    ) -> list[dict[str, Any]]:
        query = self.validate_read_query(query)
        if max_rows < 1 or max_rows > MAX_QUERY_ROWS:
            raise ValueError(f"max_rows must be between 1 and {MAX_QUERY_ROWS}")
        query = self._bound_read_query(query, max_rows)
        query_id = hashlib.sha256(query.encode()).hexdigest()[:16]
        logger.debug(
            "Executing bounded query (query_id=%s, max_rows=%d)", query_id, max_rows
        )

        # Validate table access authorization
        self._validate_table_access(query)

        # Use HTTP as primary method since it works reliably with authenticated clusters
        try:
            return self.execute_query_http(query)
        except requests.exceptions.ReadTimeout as e:
            # A read timeout can occur after Pinot accepted and executed the query.
            # Do not immediately duplicate that work through a second transport.
            logger.warning(
                "HTTP query timed out after submission (query_id=%s); not retrying "
                "through PinotDB: %s",
                query_id,
                type(e).__name__,
            )
            raise
        except requests.exceptions.ConnectionError as e:
            logger.warning(
                "HTTP connection failed before a response (query_id=%s); trying "
                "PinotDB fallback: %s",
                query_id,
                e,
            )
            try:
                return self.execute_query_pinotdb(query, params, max_rows=max_rows)
            except Exception as pinotdb_error:
                logger.error(
                    "Both query transports failed (query_id=%s): http=%s pinotdb=%s",
                    query_id,
                    type(e).__name__,
                    type(pinotdb_error).__name__,
                )
                raise

    def _bound_read_query(self, query: str, max_rows: int) -> str:
        """Apply a hard upper row bound at Pinot rather than after materialization."""
        expression = sqlglot.parse_one(query, read="trino")
        if not isinstance(expression, exp.Select):
            raise ValueError("Only read-only SELECT queries are allowed for read-query")
        existing = expression.args.get("limit")
        if existing is not None:
            literal = existing.expression
            if not isinstance(literal, exp.Literal) or not literal.is_int:
                raise ValueError("LIMIT must be a literal integer.")
            max_rows = min(max_rows, int(literal.this))
        return expression.limit(max_rows).sql(dialect="trino")

    def validate_read_query(self, query: str) -> str:
        """Validate and normalize SQL accepted by the read-query tool."""
        if not isinstance(query, str):
            raise ValueError("read-query requires a SQL string")

        statements = _split_sql_statements(query)
        if not statements:
            raise ValueError("Only read-only SELECT queries are allowed for read-query")

        if len(statements) != 1:
            raise ValueError(
                "Only a single read-only SELECT statement is allowed for read-query"
            )

        statement = statements[0]
        words = _sql_words(statement)
        top_level_words = _sql_words(statement, top_level_only=True)
        if not top_level_words or top_level_words[0] not in _READ_QUERY_START_KEYWORDS:
            raise ValueError("Only read-only SELECT queries are allowed for read-query")

        prohibited_keyword = next(
            (word for word in words if word in _PROHIBITED_READ_QUERY_KEYWORDS),
            None,
        )
        if prohibited_keyword:
            raise ValueError(
                "Only read-only SELECT queries are allowed for read-query; "
                f"found prohibited keyword {prohibited_keyword}"
            )

        if top_level_words[0] == "WITH" and "SELECT" not in top_level_words[1:]:
            raise ValueError("WITH queries must resolve to a SELECT statement")

        parse_statement = _strip_trailing_pinot_option(statement)
        if parse_statement != statement:
            raise ValueError(
                "Pinot OPTION clauses are server-controlled; remove OPTION(...) "
                "from the query."
            )
        try:
            expression = sqlglot.parse_one(parse_statement, read="trino")
        except ParseError as e:
            raise ValueError(
                "Only valid read-only SELECT queries are allowed for read-query"
            ) from e

        if not isinstance(expression, exp.Select):
            raise ValueError("Only read-only SELECT queries are allowed for read-query")

        return statement

    def preprocess_query(self, query: str) -> str:
        """Preprocess query by removing database prefix and adding timeout options"""
        # Remove database prefix if present
        if self.config.database and f"{self.config.database}." in query:
            query = query.replace(f"{self.config.database}.", "")
            logger.debug("Removed configured database prefix from query")

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
        max_rows: int = 501,
    ) -> list[dict[str, Any]]:
        """Original pinotdb-based query execution"""
        query_id = hashlib.sha256(query.encode()).hexdigest()[:16]
        logger.debug("Executing query via PinotDB (query_id=%s)", query_id)
        try:
            current_conn = self.get_connection()
            curs = current_conn.cursor()

            query = self.preprocess_query(query)
            curs.execute(query)

            # Get only the bounded page. Avoid pandas/fetchall, which materialized
            # arbitrarily large query results in the MCP process.
            columns = [item[0] for item in curs.description] if curs.description else []
            rows = curs.fetchmany(max_rows)
            result = [dict(zip(columns, row, strict=False)) for row in rows]
            logger.debug(
                "Query executed successfully (query_id=%s, rows=%d)",
                query_id,
                len(result),
            )
            return result

        except Exception as e:
            logger.error(
                "Query execution failed (query_id=%s): %s", query_id, e, exc_info=True
            )
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
        query = _strip_sql_comments(query)
        query = " ".join(query.split())

        matches = []

        # Pattern 1: Unquoted tables (after FROM/JOIN or comma-separated)
        # Matches: FROM table, JOIN table, table1, table2
        # Uses negative lookahead to exclude SQL keywords (LEFT, RIGHT, INNER, etc.)
        unquoted_pattern = (
            r"(?:\b(?:FROM|JOIN)\s+|,\s*)"
            r"(?:[\w.]+\.)?"
            r"(?!(?:LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|WHERE|GROUP|ORDER|"
            r"HAVING|LIMIT)\b)"
            r"(\w+)"
        )
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

    def _validate_table_name_access(
        self, table_name: str, component_name: str = "table name"
    ) -> None:
        """Validate that a table name is allowed by filtering rules.

        Args:
            table_name: Table name to validate

        Raises:
            ValueError: If table is not in included_tables filter
        """
        validate_pinot_path_component(table_name, component_name)
        included = self._included_tables
        if not included:
            return

        if not self._matches_patterns(table_name, included):
            allowed = ", ".join(included)
            raise ValueError(
                f"Access denied to table '{table_name}'. Allowed tables: {allowed}"
            )

    def _encode_authorized_path_component(
        self, value: str, component_name: str = "table name"
    ) -> str:
        """Apply table filtering and encode a name as one controller URL segment."""
        self._validate_table_name_access(value, component_name)
        return encode_pinot_path_component(value, component_name)

    def _extract_and_validate_name_from_json(self, json_str: str, key: str) -> None:
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
            component_name = "schema name" if key == "schemaName" else "table name"
            self._validate_table_name_access(name, component_name)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON: {e}") from e

    def _validate_table_access(self, query: str) -> None:
        """Validate that query only accesses allowed tables.

        Args:
            query: SQL query string to validate

        Raises:
            ValueError: If query references tables not in included_tables filter
        """
        included = self._included_tables
        if not included:
            return

        table_names = self._extract_sql_table_names(query)

        if not table_names:
            return

        unauthorized_tables = [
            table
            for table in table_names
            if not self._matches_patterns(table, included)
        ]

        if unauthorized_tables:
            allowed = ", ".join(included)
            unauthorized = ", ".join(unauthorized_tables)
            raise ValueError(
                f"Query references unauthorized tables: {unauthorized}. "
                f"Allowed tables: {allowed}"
            )

    def _filter_tables(self, tables: list[str]) -> list[str]:
        """Filter tables based on included_tables configuration."""
        included = self._included_tables
        if not tables or not included:
            return tables

        return [t for t in tables if self._matches_patterns(t, included)]

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
        table_path = self._encode_authorized_path_component(tableName)
        endpoint = PinotEndpoints.TABLE_SIZE.format(table_path)
        url = f"{self.config.controller_url}/{endpoint}"
        logger.debug(f"Fetching table details for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def get_segment_metadata_detail(
        self,
        tableName: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        table_path = self._encode_authorized_path_component(tableName)
        endpoint = PinotEndpoints.SEGMENT_METADATA.format(table_path)
        url = f"{self.config.controller_url}/{endpoint}"
        logger.debug(f"Fetching segment metadata for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def get_segments(
        self,
        tableName: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        table_path = self._encode_authorized_path_component(tableName)
        endpoint = PinotEndpoints.SEGMENTS.format(table_path)
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
        segment_path = encode_pinot_path_component(segmentName, "segment name")
        for type_suffix in ["REALTIME", "OFFLINE"]:
            table_path = encode_pinot_path_component(
                f"{tableName}_{type_suffix}", "physical table name"
            )
            endpoint = PinotEndpoints.SEGMENT_DETAIL.format(table_path, segment_path)
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
        table_path = self._encode_authorized_path_component(tableName)
        endpoint = PinotEndpoints.TABLE_CONFIG.format(table_path)
        url = f"{self.config.controller_url}/{endpoint}"
        logger.debug(f"Fetching table config for {tableName} from: {url}")
        response = self.http_request(url)
        return response.json()

    def create_schema(
        self,
        schemaJson: str,
        override: bool = False,
        force: bool = False,
    ) -> dict[str, Any]:
        self._extract_and_validate_name_from_json(schemaJson, "schemaName")
        url = f"{self.config.controller_url}/{PinotEndpoints.SCHEMAS}"
        params = {"override": str(override).lower(), "force": str(force).lower()}
        headers = self._create_auth_headers(controller=True)
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
        schema_path = self._encode_authorized_path_component(schemaName, "schema name")
        url = f"{self.config.controller_url}/{PinotEndpoints.SCHEMAS}/{schema_path}"
        params = {"reload": str(reload).lower(), "force": str(force).lower()}
        headers = self._create_auth_headers(controller=True)
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
        schema_path = self._encode_authorized_path_component(schemaName, "schema name")
        url = f"{self.config.controller_url}/{PinotEndpoints.SCHEMAS}/{schema_path}"
        headers = self._create_auth_headers(controller=True)
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
        headers = self._create_auth_headers(controller=True)
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

    def validate_table_config(
        self,
        table_config_json: str,
        validation_types_to_skip: list[str] | None = None,
    ) -> dict[str, Any]:
        """Ask Pinot to validate a table config without mutating cluster state."""
        self._extract_and_validate_name_from_json(table_config_json, "tableName")
        url = f"{self.config.controller_url}/tableConfigs/validate"
        params: dict[str, str] = {}
        if validation_types_to_skip:
            params["validationTypesToSkip"] = ",".join(validation_types_to_skip)
        headers = self._create_auth_headers(controller=True)
        response = requests.post(
            url,
            headers=headers,
            params=params,
            data=table_config_json,
            timeout=(self.config.connection_timeout, self.config.request_timeout),
            verify=True,
        )
        response.raise_for_status()
        return response.json()

    def update_table_config(
        self,
        tableName: str,
        tableConfigJson: str,
        validationTypesToSkip: str | None = None,
    ) -> dict[str, Any]:
        table_path = self._encode_authorized_path_component(tableName)
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLES}/{table_path}"
        params: dict[str, str] = {}
        if validationTypesToSkip:
            params["validationTypesToSkip"] = validationTypesToSkip
        headers = self._create_auth_headers(controller=True)
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
        table_path = self._encode_authorized_path_component(tableName)
        url = f"{self.config.controller_url}/{PinotEndpoints.TABLES}/{table_path}"
        params: dict[str, str] = {}
        if tableType:
            params["type"] = tableType
        headers = self._create_auth_headers(controller=True)
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
