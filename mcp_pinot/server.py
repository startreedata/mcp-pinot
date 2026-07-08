# --------------------------
# File: mcp_pinot/server.py
# --------------------------
"""
FastMCP-based implementation for the Apache Pinot MCP Server.
"""

from collections.abc import Callable
import inspect
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
    SegmentMetadata,
    TableConfig,
    TableConfigSchema,
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


def _host_origin_kwargs(cfg) -> dict[str, object]:
    """Streamable-HTTP DNS-rebinding (Host/Origin) protection settings.

    FastMCP defaults protection on with a localhost-only allow-list, which 421s
    requests arriving under an ingress/Service hostname. These come from config
    (MCP_HOST_ORIGIN_PROTECTION / MCP_ALLOWED_HOSTS / MCP_ALLOWED_ORIGINS).
    """
    return {
        "host_origin_protection": cfg.host_origin_protection,
        "allowed_hosts": cfg.allowed_hosts,
        "allowed_origins": cfg.allowed_origins,
    }


def _supported_kwargs(fn: Callable, kwargs: dict[str, object]) -> dict[str, object]:
    """Keep only kwargs the callable actually accepts.

    The Host/Origin params landed in FastMCP 3.4; filtering keeps this working on
    older FastMCP (where protection isn't enforced anyway) without a hard pin.
    """
    accepted = inspect.signature(fn).parameters
    return {k: v for k, v in kwargs.items() if k in accepted}


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
def reload_table_filters() -> FilterReloadResult:
    """Reload the table access filter file without restarting the server.

    Re-reads the YAML file configured via ``PINOT_TABLE_FILTER_FILE`` and applies
    the new allow-list immediately to all subsequent operations.

    Returns:
        FilterReloadResult with the reload status and the previous/new filter counts.
    """
    results = _call(
        "reload_table_filters",
        "Verify PINOT_TABLE_FILTER_FILE points to a valid YAML file.",
        pinot_client.reload_table_filters,
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
            description="Pinot table name (case-sensitive), without the "
            "_OFFLINE/_REALTIME suffix.",
            min_length=1,
        ),
    ],
) -> TableSizeDetails:
    """Get storage size details (reported and estimated bytes) for a table."""
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
        Field(description="Pinot table name (case-sensitive).", min_length=1),
    ],
) -> SegmentList:
    """List the segments for a table, grouped by table type (OFFLINE/REALTIME)."""
    raw = _call(
        "segment_list", _HINT_READ, pinot_client.get_segments, tableName=tableName
    )
    return SegmentList.model_validate(raw)


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
        Field(description="Pinot table name (case-sensitive).", min_length=1),
    ],
    segmentName: Annotated[
        str,
        Field(description="Segment name, as returned by segment_list.", min_length=1),
    ],
) -> SegmentIndexDetails:
    """Get per-column index metadata for a specific segment."""
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
        Field(description="Pinot table name (case-sensitive).", min_length=1),
    ],
) -> SegmentMetadata:
    """Get metadata for all segments of a table (rows, sizes, time boundaries)."""
    raw = _call(
        "segment_metadata_details",
        _HINT_READ,
        pinot_client.get_segment_metadata_detail,
        tableName=tableName,
    )
    return SegmentMetadata.model_validate(raw)


@mcp.tool(
    annotations=ToolAnnotations(
        title="Table config and schema",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    )
)
def tableconfig_schema_details(
    tableName: Annotated[
        str,
        Field(description="Pinot table name (case-sensitive).", min_length=1),
    ],
) -> TableConfigSchema:
    """Get the combined table configuration and schema for a table."""
    raw = _call(
        "tableconfig_schema_details",
        _HINT_READ,
        pinot_client.get_tableconfig_schema_detail,
        tableName=tableName,
    )
    return TableConfigSchema.model_validate(raw)


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
        Field(description="Name of the existing schema to update.", min_length=1),
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
        Field(description="Schema name (case-sensitive).", min_length=1),
    ],
) -> PinotSchema:
    """Fetch a Pinot schema definition by name."""
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
        Field(description="Name of the existing table to update.", min_length=1),
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
        Field(description="Pinot table name (case-sensitive).", min_length=1),
    ],
    tableType: Annotated[
        Literal["OFFLINE", "REALTIME"] | None,
        Field(
            description="Restrict to one table type; omit to return both when present."
        ),
    ] = None,
) -> TableConfig:
    """Get the configuration for a table, optionally restricted to a table type."""
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
        app = mcp.http_app(
            path=server_config.path,
            **_supported_kwargs(mcp.http_app, _host_origin_kwargs(server_config)),
        )
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
            **_supported_kwargs(mcp.run, _host_origin_kwargs(server_config)),
        )


if __name__ == "__main__":
    main()
