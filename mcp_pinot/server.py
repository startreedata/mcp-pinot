# --------------------------
# File: mcp_pinot/server.py
# --------------------------
"""
FastMCP-based implementation for the Apache Pinot MCP Server.
"""

from collections.abc import Callable
from ipaddress import ip_address
import json
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field
import uvicorn

from mcp_pinot.auth import build_auth
from mcp_pinot.config import get_logger, load_pinot_config, load_server_config
from mcp_pinot.models import (
    ConnectionDiagnostics,
    FilterReloadResult,
    OperationResult,
    PinotSchema,
    QueryResult,
    SegmentIndexDetails,
    SegmentList,
    SegmentMetadataPage,
    TableConfig,
    TableList,
    TableSizeDetails,
)
from mcp_pinot.pinot_client import PinotClient
from mcp_pinot.prompts import PROMPT_TEMPLATE

logger = get_logger()

# Initialize configurations and create client
pinot_config = load_pinot_config()
server_config = load_server_config()
pinot_client = PinotClient(pinot_config)

# Build the auth provider selected by configuration (None disables auth).
_auth = build_auth(server_config)

mcp = FastMCP(
    "Pinot MCP Server",
    instructions=(
        "Query and inspect an Apache Pinot real-time OLAP cluster. Use read_query "
        "for read-only SQL (SELECT or WITH ... SELECT); queries are validated and "
        "paginated. Call list_tables to discover case-sensitive table names, and "
        "the inspection tools (table_details, get_schema, get_table_config, "
        "segment_list) before composing queries or changes. Tools annotated "
        "destructive (update_schema, update_table_config) modify cluster metadata; "
        "create_* tools add new objects. Every write tool accepts dry_run=true to "
        "validate and preview without applying the change."
    ),
    auth=_auth,
    # Internal exceptions are replaced with a generic message; only ToolError
    # messages (which we craft to be safe and actionable) reach the client.
    mask_error_details=True,
)

# Reusable hints appended to client-facing error messages (Recovery Guide).
_HINT_READ = (
    "Verify the table name (case-sensitive; see list_tables) and that the Pinot "
    "cluster is reachable."
)
_HINT_WRITE = "Verify the JSON payload is valid and the Pinot controller is reachable."

_TABLE_NAME_DESCRIPTION = (
    "Exact Pinot table name, without _OFFLINE/_REALTIME. Use letters, digits, "
    "hyphens, or underscores, optionally prefixed by one 'database.' qualifier; "
    "whitespace is invalid and '__' is reserved by Pinot. Quote hyphenated names "
    "in SQL. Names are case-sensitive when that cluster option is enabled."
)
_SCHEMA_NAME_DESCRIPTION = (
    "Exact Pinot schema name. It normally matches the table name without an "
    "_OFFLINE/_REALTIME suffix; use letters, digits, hyphens, or underscores, "
    "optionally prefixed by one 'database.' qualifier."
)
_NAME_PATTERN = r"^(?:[A-Za-z0-9_-]+\.)?[A-Za-z0-9_-]+$"


def _is_loopback_host(host: str) -> bool:
    """Return True when host is a local-only bind address."""
    if host.lower() == "localhost":
        return True

    try:
        return ip_address(host).is_loopback
    except ValueError:
        return False


def _enforce_http_auth_safety(http_enabled: bool) -> None:
    """Refuse unauthenticated HTTP on network-reachable bind addresses."""
    if http_enabled and _auth is None and not _is_loopback_host(server_config.host):
        raise SystemExit(
            "Refusing to start: HTTP transport is bound to "
            f"{server_config.host!r} without authentication. Enable an auth "
            "provider (set AUTH_PROVIDER or OAUTH_ENABLED=true) or bind "
            "local-only with MCP_HOST=127.0.0.1."
        )


def _fail(action: str, exc: Exception, hint: str = "") -> ToolError:
    """Log an internal error and return a sanitized, actionable ToolError."""
    logger.error("%s failed: %s", action, exc, exc_info=True)
    message = f"{action} failed."
    if hint:
        message = f"{message} {hint}"
    return ToolError(message)


def _call[T](
    action: str, hint: str, fn: Callable[..., T], *args: Any, **kwargs: Any
) -> T:
    """Invoke a client call, mapping failures to client-facing tool errors.

    ValueError messages (our own validation guidance) are surfaced verbatim so the
    model can self-correct; any other exception is logged and replaced with a
    generic, non-leaking message.
    """
    try:
        return fn(*args, **kwargs)
    except ToolError:
        raise
    except ValueError as e:
        raise ToolError(str(e)) from e
    except Exception as e:
        raise _fail(action, e, hint) from e


def _preview_name(json_str: str, key: str) -> str:
    """Validate a write payload's JSON and extract its required name field."""
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ToolError(f"Invalid JSON payload: {e}") from e
    name = data.get(key) if isinstance(data, dict) else None
    if not name:
        raise ToolError(f"Missing required field '{key}' in the JSON payload.")
    return str(name)


def _as_json_str(payload: dict[str, Any] | str) -> str:
    """Normalize a JSON-object-or-string write payload to a JSON string.

    Tools accept either a structured object (preferred — clients get a real input
    schema) or a raw JSON string (back-compat). Downstream client methods and the
    Pinot REST API expect a JSON string, so objects are serialized here.
    """
    if isinstance(payload, dict):
        return json.dumps(payload)
    return payload


@mcp.tool(
    annotations=ToolAnnotations(
        title="Test connection",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def test_connection() -> ConnectionDiagnostics:
    """Probe Pinot connectivity and return diagnostics.

    Runs three checks — broker connection, a trivial ``SELECT 1`` query, and a
    controller table listing — and reports which succeeded plus a small sample of
    tables. Useful for troubleshooting configuration before using other tools.

    Failure recovery:
        Individual check failures are returned in ``error``. Verify the broker and
        controller URLs, credentials, and network, then retry only failed checks.
    """
    results = _call("test_connection", _HINT_READ, pinot_client.test_connection)
    return ConnectionDiagnostics.model_validate(results)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Reload table filters",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
def reload_table_filters(
    dry_run: Annotated[
        bool,
        Field(
            description=(
                "When true (default), validate and preview the candidate allow-list "
                "without changing server state. Pass false explicitly to apply it."
            )
        ),
    ] = True,
) -> FilterReloadResult:
    """Preview or apply the configured table-filter YAML without restarting.

    Reads only the path configured by ``PINOT_TABLE_FILTER_FILE``. The YAML must be
    an object whose optional ``included_tables`` value is a list of glob strings.
    An absent or empty list means all tables. The default ``dry_run=true`` validates
    and reports the before/after patterns; pass ``false`` to apply them atomically.

    Returns:
        Preview/application status, whether it was applied, and old/new patterns.

    Failure recovery:
        A missing setting/file or malformed YAML is non-retryable until corrected;
        fix ``PINOT_TABLE_FILTER_FILE`` or its ``included_tables`` list, then retry.
    """
    results = _call(
        "reload_table_filters",
        "Verify PINOT_TABLE_FILTER_FILE points to YAML with an included_tables list.",
        pinot_client.reload_table_filters,
        dry_run=dry_run,
    )
    return FilterReloadResult.model_validate(results)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Read query",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def read_query(
    query: Annotated[
        str,
        Field(
            description=(
                "A single read-only statement in Pinot SQL: 'SELECT ...' or "
                "'WITH ... SELECT ...'. Stacked statements and DML/DDL/admin "
                "keywords (INSERT, UPDATE, DELETE, DROP, SET, ...) are rejected."
            ),
            min_length=1,
        ),
    ],
    limit: Annotated[
        int,
        Field(description="Maximum rows to return in this page.", ge=1, le=10000),
    ] = 100,
    offset: Annotated[
        int,
        Field(description="Zero-based row offset for pagination.", ge=0),
    ] = 0,
) -> QueryResult:
    """Run a read-only SQL query against Pinot and return a page of rows.

    Only a single SELECT (or WITH ... SELECT) statement is allowed; the query is
    rejected if it contains multiple statements or write/DDL/admin keywords.
    Results are paginated to keep responses small — use ``limit``/``offset`` and
    the ``has_more`` flag to page through large result sets.

    Args:
        query: The read-only SQL statement to execute.
        limit: Maximum number of rows to return in this page (1-10000).
        offset: Zero-based offset of the first row to return.

    Returns:
        QueryResult with the page of rows, the column list, total row count, and a
        ``has_more`` flag.

    Failure recovery:
        SQL/allow-list/permission failures require correcting the query or access;
        do not retry unchanged. A timeout or connection failure can be retried after
        ``test_connection`` succeeds. Zero rows is a successful result.
    """
    rows = _call("read_query", _HINT_READ, pinot_client.execute_query, query=query)
    total = len(rows)
    page = rows[offset : offset + limit]
    columns = list(page[0].keys()) if page else (list(rows[0].keys()) if rows else [])
    return QueryResult(
        columns=columns,
        rows=page,
        row_count=len(page),
        total_rows=total,
        offset=offset,
        has_more=offset + len(page) < total,
    )


@mcp.tool(
    annotations=ToolAnnotations(
        title="List tables",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def list_tables(
    limit: Annotated[
        int,
        Field(description="Maximum tables to return in this page.", ge=1, le=10000),
    ] = 100,
    offset: Annotated[
        int,
        Field(description="Zero-based offset for pagination.", ge=0),
    ] = 0,
) -> TableList:
    """List Pinot tables visible to this server (subject to table filters).

    Returns a paginated list of table names. Use ``limit``/``offset`` and the
    ``has_more`` flag to page through clusters with many tables.

    Failure recovery:
        An empty page is success. For authentication/connectivity errors, verify the
        controller with ``test_connection`` and retry after access is restored.
    """
    tables = _call("list_tables", _HINT_READ, pinot_client.get_tables)
    total = len(tables)
    page = tables[offset : offset + limit]
    return TableList(
        tables=page,
        table_count=len(page),
        total_tables=total,
        offset=offset,
        has_more=offset + len(page) < total,
    )


@mcp.tool(
    annotations=ToolAnnotations(
        title="Table size details",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def table_details(
    tableName: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            pattern=_NAME_PATTERN,
        ),
    ],
) -> TableSizeDetails:
    """Get a table's storage footprint: reported vs. estimated size in bytes.

    Use this for capacity/size questions about a whole table. It does NOT list
    segments (use ``segment_list``) or return row counts/time boundaries (use
    ``segment_metadata_details``). ``reportedSizeInBytes`` is what the servers
    currently hosting the segments report; ``estimatedSizeInBytes`` assumes every
    replica is present.

    Failure recovery:
        For not-found errors, copy an exact name from ``list_tables``. Fix permission
        errors before retrying; retry transient controller failures after a health
        check.
    """
    raw = _call(
        "table_details", _HINT_READ, pinot_client.get_table_detail, tableName=tableName
    )
    return TableSizeDetails.model_validate(raw)


@mcp.tool(
    annotations=ToolAnnotations(
        title="List segments",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def segment_list(
    tableName: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            pattern=_NAME_PATTERN,
        ),
    ],
    limit: Annotated[
        int,
        Field(
            description="Maximum segment names to return in this page.", ge=1, le=10000
        ),
    ] = 100,
    offset: Annotated[
        int,
        Field(description="Zero-based offset for pagination.", ge=0),
    ] = 0,
) -> SegmentList:
    """List a table's segment names, grouped by table type (OFFLINE/REALTIME).

    Use this to discover segment names — e.g. to get a ``segmentName`` for
    ``index_column_details``, or to see how a table is partitioned. For per-segment
    row counts / sizes / time boundaries call ``segment_metadata_details`` instead;
    for the table's total storage size call ``table_details``.

    Segment names are paginated (a busy table can have thousands) — use
    ``limit``/``offset`` and the ``has_more`` flag to page through them.

    Failure recovery:
        An empty page is success. For not-found errors, use an exact name from
        ``list_tables``; correct access errors, or retry transient controller errors.
    """
    raw = _call(
        "segment_list", _HINT_READ, pinot_client.get_segments, tableName=tableName
    )
    full = SegmentList.model_validate(raw)
    flat = [("OFFLINE", s) for s in (full.OFFLINE or [])]
    flat += [("REALTIME", s) for s in (full.REALTIME or [])]
    total = len(flat)
    page = flat[offset : offset + limit]
    page_offline = [s for kind, s in page if kind == "OFFLINE"]
    page_realtime = [s for kind, s in page if kind == "REALTIME"]
    return full.model_copy(
        update={
            "OFFLINE": page_offline if full.OFFLINE is not None else None,
            "REALTIME": page_realtime if full.REALTIME is not None else None,
            "total_segments": total,
            "returned_segments": len(page),
            "offset": offset,
            "has_more": offset + len(page) < total,
        }
    )


@mcp.tool(
    annotations=ToolAnnotations(
        title="Index/column details",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def index_column_details(
    tableName: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            pattern=_NAME_PATTERN,
        ),
    ],
    segmentName: Annotated[
        str,
        Field(
            description=(
                "Exact, case-sensitive opaque segment name returned by segment_list; "
                "do not construct, trim, or add a table-type suffix. Pinot defines "
                "the length and characters, so this client only requires non-empty."
            ),
            min_length=1,
        ),
    ],
) -> SegmentIndexDetails:
    """Get per-column index metadata for ONE segment (which indexes each column has).

    Use this to inspect how a specific segment is indexed (inverted, sorted, range,
    etc.). Requires a ``segmentName`` from ``segment_list``. For a segment's row
    count/size/time boundaries use ``segment_metadata_details``; for the table's
    declared index *configuration* (not per-segment state) use ``get_table_config``.

    Failure recovery:
        A missing segment is non-retryable with the same value; refresh
        ``segment_list`` and use an exact returned name. Retry transient controller
        errors after ``test_connection`` succeeds.
    """
    raw = _call(
        "index_column_details",
        _HINT_READ,
        pinot_client.get_index_column_detail,
        tableName=tableName,
        segmentName=segmentName,
    )
    return SegmentIndexDetails.model_validate(raw)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Segment metadata",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def segment_metadata_details(
    tableName: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            pattern=_NAME_PATTERN,
        ),
    ],
    limit: Annotated[
        int,
        Field(
            description="Maximum segment metadata objects in this page.",
            ge=1,
            le=1000,
        ),
    ] = 100,
    offset: Annotated[
        int,
        Field(description="Zero-based segment offset for pagination.", ge=0),
    ] = 0,
) -> SegmentMetadataPage:
    """Get a deterministic page of segment rows, sizes, and time boundaries.

    Pinot can return thousands of segment objects. Results are sorted by exact
    segment name, then sliced with ``limit``/``offset``; follow ``has_more`` until
    false. Use ``segment_list`` when only names are needed.

    Failure recovery:
        An empty page is success. For not-found errors, use ``list_tables``;
        correct permissions before retrying, and retry transient server failures
        only after ``test_connection`` succeeds.
    """
    raw = _call(
        "segment_metadata_details",
        _HINT_READ,
        pinot_client.get_segment_metadata_detail,
        tableName=tableName,
    )
    items = sorted(raw.items())
    total = len(items)
    page = items[offset : offset + limit]
    return SegmentMetadataPage(
        segments=dict(page),
        returned_segments=len(page),
        total_segments=total,
        offset=offset,
        has_more=offset + len(page) < total,
    )


@mcp.tool(
    annotations=ToolAnnotations(
        title="Create schema",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    )
)
def create_schema(
    schemaJson: Annotated[
        dict[str, Any] | str,
        Field(
            description="Pinot schema definition as a JSON object (preferred) or a "
            "JSON string; must include 'schemaName'.",
        ),
    ],
    override: Annotated[
        bool,
        Field(description="Replace an existing schema with the same name."),
    ] = True,
    force: Annotated[
        bool,
        Field(description="Force creation, skipping certain validations."),
    ] = False,
    dry_run: Annotated[
        bool,
        Field(description="Validate and preview without applying the change."),
    ] = False,
) -> OperationResult:
    """Create a new Pinot schema.

    Accepts the schema as a JSON object (preferred) or a JSON string. Set
    ``dry_run=true`` to validate the payload and preview the effect without
    applying it.

    Failure recovery:
        Invalid JSON, missing ``schemaName``, and controller validation failures are
        non-retryable until corrected. Permission failures require access changes;
        retry transient controller failures only after connectivity is restored.
    """
    schemaJson = _as_json_str(schemaJson)
    if dry_run:
        name = _preview_name(schemaJson, "schemaName")
        return OperationResult(
            status="dry_run",
            message=f"Would create schema '{name}' "
            f"(override={override}, force={force}). No change applied.",
        )
    results = _call(
        "create_schema",
        _HINT_WRITE,
        pinot_client.create_schema,
        schemaJson,
        override,
        force,
    )
    return OperationResult.model_validate(results)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Update schema",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def update_schema(
    schemaName: Annotated[
        str,
        Field(
            description=_SCHEMA_NAME_DESCRIPTION,
            min_length=1,
            pattern=_NAME_PATTERN,
        ),
    ],
    schemaJson: Annotated[
        dict[str, Any] | str,
        Field(
            description="Updated schema definition as a JSON object (preferred) or a "
            "JSON string.",
        ),
    ],
    reload: Annotated[
        bool,
        Field(description="Reload affected segments after updating."),
    ] = False,
    force: Annotated[
        bool,
        Field(description="Force update, skipping certain validations."),
    ] = False,
    dry_run: Annotated[
        bool,
        Field(description="Validate and preview without applying the change."),
    ] = False,
) -> OperationResult:
    """Update an existing Pinot schema.

    Accepts the schema as a JSON object (preferred) or a JSON string. This can
    change column definitions on a live table; set ``dry_run=true`` to preview
    without applying.

    Failure recovery:
        Invalid JSON/name or schema validation failures require a corrected payload;
        do not retry unchanged. Fix permission errors first, and retry transient
        controller failures only after connectivity is restored.
    """
    schemaJson = _as_json_str(schemaJson)
    if dry_run:
        return OperationResult(
            status="dry_run",
            message=f"Would update schema '{schemaName}' "
            f"(reload={reload}, force={force}). No change applied.",
        )
    results = _call(
        "update_schema",
        _HINT_WRITE,
        pinot_client.update_schema,
        schemaName,
        schemaJson,
        reload,
        force,
    )
    return OperationResult.model_validate(results)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get schema",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def get_schema(
    schemaName: Annotated[
        str,
        Field(
            description=_SCHEMA_NAME_DESCRIPTION,
            min_length=1,
            pattern=_NAME_PATTERN,
        ),
    ],
) -> PinotSchema:
    """Get one Pinot schema, including dimensions, metrics, time, and primary keys.

    This is a single-object lookup, not a list, so pagination does not apply. The
    output preserves additional fields introduced by the connected Pinot version.

    Failure recovery:
        For not-found errors, pass the table's exact schema name (normally the table
        name without a type suffix). Fix permissions before retrying; retry transient
        controller failures after ``test_connection`` succeeds.
    """
    raw = _call(
        "get_schema", _HINT_READ, pinot_client.get_schema, schemaName=schemaName
    )
    return PinotSchema.model_validate(raw)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Create table config",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    )
)
def create_table_config(
    tableConfigJson: Annotated[
        dict[str, Any] | str,
        Field(
            description="Pinot table configuration as a JSON object (preferred) or a "
            "JSON string; must include 'tableName'.",
        ),
    ],
    validationTypesToSkip: Annotated[
        str | None,
        Field(
            description="Comma-separated validation types to skip, e.g. 'TASK,UPSERT'."
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        Field(description="Validate and preview without applying the change."),
    ] = False,
) -> OperationResult:
    """Create a new Pinot table configuration.

    Accepts the config as a JSON object (preferred) or a JSON string. Set
    ``dry_run=true`` to validate the payload and preview without applying.

    Failure recovery:
        Invalid JSON, missing ``tableName``, and controller validation failures need
        a corrected payload; do not retry unchanged. Fix access failures first, and
        retry transient controller errors after connectivity is restored.
    """
    tableConfigJson = _as_json_str(tableConfigJson)
    if dry_run:
        name = _preview_name(tableConfigJson, "tableName")
        return OperationResult(
            status="dry_run",
            message=f"Would create table config '{name}'. No change applied.",
        )
    results = _call(
        "create_table_config",
        _HINT_WRITE,
        pinot_client.create_table_config,
        tableConfigJson,
        validationTypesToSkip,
    )
    return OperationResult.model_validate(results)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Update table config",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def update_table_config(
    tableName: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            pattern=_NAME_PATTERN,
        ),
    ],
    tableConfigJson: Annotated[
        dict[str, Any] | str,
        Field(
            description="Updated table configuration as a JSON object (preferred) or "
            "a JSON string.",
        ),
    ],
    validationTypesToSkip: Annotated[
        str | None,
        Field(
            description="Comma-separated validation types to skip, e.g. 'TASK,UPSERT'."
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        Field(description="Validate and preview without applying the change."),
    ] = False,
) -> OperationResult:
    """Update an existing Pinot table configuration.

    Accepts the config as a JSON object (preferred) or a JSON string. This changes
    the configuration of a live table; set ``dry_run=true`` to preview without
    applying.

    Failure recovery:
        Invalid JSON/name or controller validation failures require a corrected
        payload; do not retry unchanged. Fix access errors first, and retry transient
        controller failures only after connectivity is restored.
    """
    tableConfigJson = _as_json_str(tableConfigJson)
    if dry_run:
        return OperationResult(
            status="dry_run",
            message=f"Would update table config '{tableName}'. No change applied.",
        )
    results = _call(
        "update_table_config",
        _HINT_WRITE,
        pinot_client.update_table_config,
        tableName,
        tableConfigJson,
        validationTypesToSkip,
    )
    return OperationResult.model_validate(results)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get table config",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def get_table_config(
    tableName: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            pattern=_NAME_PATTERN,
        ),
    ],
    tableType: Annotated[
        Literal["OFFLINE", "REALTIME"] | None,
        Field(
            description="Restrict to one table type; omit to return both when present."
        ),
    ] = None,
) -> TableConfig:
    """Get one table's indexing, retention, tenant, and ingestion configuration.

    This is a single-object lookup, not a list, so pagination does not apply. Set
    ``tableType`` only when one side of a hybrid table is needed.

    Failure recovery:
        For not-found errors, use an exact name from ``list_tables`` and a valid
        table type. Fix permissions before retrying; retry transient controller
        failures after ``test_connection`` succeeds.
    """
    raw = _call(
        "get_table_config",
        _HINT_READ,
        pinot_client.get_table_config,
        tableName=tableName,
        tableType=tableType,
    )
    return TableConfig.model_validate(raw)


@mcp.prompt
def pinot_query() -> str:
    """Query Pinot database with MCP Server + Claude"""
    return PROMPT_TEMPLATE.strip()


@mcp.prompt
def explore_table(table_name: str) -> str:
    """Guide a structured exploration of a single Pinot table.

    Args:
        table_name: The Pinot table to explore (case-sensitive).
    """
    return (
        f"Help me explore the Pinot table '{table_name}'.\n"
        f"1. Call get_schema and get_table_config for '{table_name}' to learn its "
        f"columns, types, and time column.\n"
        f"2. Call table_details and segment_list for its size and segment layout.\n"
        f"3. Run read_query with a small LIMIT (e.g. 10) to sample rows from "
        f"'{table_name}'.\n"
        f"4. Summarize the table's purpose, key dimensions/metrics, and time range."
    )


@mcp.resource("pinot://tables", mime_type="application/json")
def tables_resource() -> str:
    """The Pinot tables visible to this server (honors table filters)."""
    tables = _call("tables_resource", _HINT_READ, pinot_client.get_tables)
    return json.dumps({"tables": tables})


@mcp.resource("pinot://schema/{schema_name}", mime_type="application/json")
def schema_resource(schema_name: str) -> str:
    """The Pinot schema definition for a given schema name."""
    raw = _call(
        "schema_resource", _HINT_READ, pinot_client.get_schema, schemaName=schema_name
    )
    return json.dumps(raw)


@mcp.resource("pinot://table-config/{table_name}", mime_type="application/json")
def table_config_resource(table_name: str) -> str:
    """The Pinot table configuration for a given table name."""
    raw = _call(
        "table_config_resource",
        _HINT_READ,
        pinot_client.get_table_config,
        tableName=table_name,
    )
    return json.dumps(raw)


def main():
    """Main entry point for FastMCP Pinot Server"""
    tls_enabled = server_config.ssl_keyfile and server_config.ssl_certfile
    http_enabled = bool(tls_enabled) or server_config.transport != "stdio"
    _enforce_http_auth_safety(http_enabled)

    if tls_enabled:
        app = mcp.http_app(path=server_config.path)
        uvicorn.run(
            app,
            host=server_config.host,
            port=server_config.port,
            ssl_keyfile=server_config.ssl_keyfile,
            ssl_certfile=server_config.ssl_certfile,
        )
    elif server_config.transport == "stdio":
        # stdio transport - no configuration needed
        mcp.run(transport="stdio")
    else:
        # transport is validated by FastMCP at runtime; it is a dynamic env value.
        mcp.run(
            transport=server_config.transport,  # type: ignore[arg-type]
            host=server_config.host,
            port=server_config.port,
            path=server_config.path,
        )


if __name__ == "__main__":
    main()
