"""Typed output models for the Pinot MCP tools.

Returning these instead of opaque JSON strings gives every tool a documented
JSON Schema, advertised as ``outputSchema`` and returned as ``structuredContent``,
so MCP clients and LLMs can validate and parse results reliably. Models that wrap
pass-through Pinot REST payloads allow extra fields so no information is dropped
while the meaningful keys stay documented.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


class QueryResult(BaseModel):
    """A page of rows produced by a read-only SQL query."""

    columns: list[str] = Field(
        default_factory=list, description="Column names, in result order."
    )
    rows: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Rows in this page; each row maps column name to value.",
    )
    row_count: int = Field(description="Number of rows returned in this page.")
    total_rows: int = Field(
        description="Rows the query returned before this page was sliced. Pinot may "
        "have already applied its own LIMIT, so this is rows fetched, not the table "
        "total."
    )
    offset: int = Field(description="Zero-based index of the first returned row.")
    has_more: bool = Field(
        description="True when more fetched rows remain beyond this page (not "
        "necessarily more rows in the underlying table)."
    )


class TableList(BaseModel):
    """A page of Pinot table names visible to this server."""

    tables: list[str] = Field(
        default_factory=list, description="Table names in this page."
    )
    table_count: int = Field(description="Number of tables in this page.")
    total_tables: int = Field(description="Total tables visible to this server.")
    offset: int = Field(description="Zero-based index of the first returned table.")
    has_more: bool = Field(description="True when more tables remain beyond this page.")


class ConnectionDiagnostics(BaseModel):
    """Diagnostics describing Pinot connectivity and a sample of tables.

    Only the declared fields below are surfaced. The client's internal ``config``
    block (broker host/port/scheme, controller URL, database) is intentionally
    NOT passed through, so connection internals are never exposed to callers.
    """

    connection_test: bool = Field(
        default=False, description="True when a broker connection was established."
    )
    query_test: bool = Field(
        default=False, description="True when a trivial 'SELECT 1' succeeded."
    )
    tables_test: bool = Field(
        default=False, description="True when the controller table listing succeeded."
    )
    error: str | None = Field(
        default=None, description="Error message when a check failed, else null."
    )
    tables_count: int | None = Field(
        default=None, description="Number of tables discovered, when available."
    )
    sample_tables: list[str] = Field(
        default_factory=list, description="Up to five example table names."
    )


class FilterReloadResult(BaseModel):
    """Outcome of hot-reloading the table access filter file."""

    model_config = ConfigDict(extra="allow")

    status: str = Field(description="'success' or 'error'.")
    message: str = Field(description="Human-readable summary of the reload.")
    previous_filter_count: int = Field(
        default=0, description="Number of allowed tables before the reload."
    )
    new_filter_count: int = Field(
        default=0, description="Number of allowed tables after the reload."
    )


class OperationResult(BaseModel):
    """Result of a schema or table-config create/update operation."""

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _coerce_non_dict(cls, data: Any) -> Any:
        # Pinot controllers can return a bare JSON string/scalar on success
        # (e.g. "schema successfully added"). Wrap it so validation never fails
        # and never surfaces a raw ValidationError to the client.
        if not isinstance(data, dict):
            return {"status": "success", "message": str(data)}
        return data

    status: str = Field(
        default="success",
        description="Operation status, e.g. 'success', 'created', 'updated', "
        "or 'dry_run'.",
    )
    message: str | None = Field(
        default=None, description="Human-readable detail, when provided."
    )


class TableSizeDetails(BaseModel):
    """Storage size for a table (Pinot ``GET /tables/{name}/size``).

    Declared fields document the common size metrics; any additional fields Pinot
    returns (per-segment breakdowns, etc.) are preserved.
    """

    model_config = ConfigDict(extra="allow")

    tableName: str | None = Field(default=None, description="The table name.")
    reportedSizeInBytes: int | None = Field(
        default=None,
        description="Size reported by the servers currently hosting the table's "
        "segments, in bytes.",
    )
    estimatedSizeInBytes: int | None = Field(
        default=None,
        description="Estimated size assuming every replica is present, in bytes.",
    )


class SegmentList(BaseModel):
    """Segment names for a table, grouped by table type."""

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        # Some Pinot versions return a list of single-key maps, e.g.
        # ``[{"OFFLINE": [...]}, {"REALTIME": [...]}]``. Merge into one mapping so
        # the result is always an object keyed by table type.
        if isinstance(data, list):
            merged: dict[str, Any] = {}
            for item in data:
                if isinstance(item, dict):
                    merged.update(item)
            return merged
        return data

    OFFLINE: list[str] | None = Field(
        default=None, description="OFFLINE segment names in this page, when present."
    )
    REALTIME: list[str] | None = Field(
        default=None, description="REALTIME segment names in this page, when present."
    )
    total_segments: int | None = Field(
        default=None, description="Total segments across all types before paging."
    )
    returned_segments: int | None = Field(
        default=None, description="Number of segment names returned in this page."
    )
    offset: int | None = Field(
        default=None, description="Zero-based offset of the first segment in this page."
    )
    has_more: bool | None = Field(
        default=None, description="True when more segments remain beyond this page."
    )


class SegmentIndexDetails(BaseModel):
    """Per-column index metadata for a single segment."""

    model_config = ConfigDict(extra="allow")

    indexes: Any = Field(
        default=None,
        description="Per-column index metadata (index types present on each column).",
    )
    columns: Any = Field(
        default=None, description="Per-column metadata for the segment, when present."
    )


class SegmentMetadata(BaseModel):
    """Metadata for a table's segments (rows, sizes, time boundaries).

    Pinot returns a mapping keyed by segment name whose values vary per segment,
    so the contents are preserved as-is rather than enumerated here.
    """

    model_config = ConfigDict(extra="allow")


class PinotSchema(BaseModel):
    """A Pinot table schema definition (column field specs)."""

    model_config = ConfigDict(extra="allow")

    schemaName: str | None = Field(default=None, description="The schema name.")
    dimensionFieldSpecs: list[dict[str, Any]] | None = Field(
        default=None, description="Dimension (attribute) column specifications."
    )
    metricFieldSpecs: list[dict[str, Any]] | None = Field(
        default=None, description="Metric (aggregatable measure) column specifications."
    )
    dateTimeFieldSpecs: list[dict[str, Any]] | None = Field(
        default=None, description="Date/time column specifications."
    )
    primaryKeyColumns: list[str] | None = Field(
        default=None, description="Primary key columns, for upsert-enabled tables."
    )


class TableConfig(BaseModel):
    """A Pinot table configuration (``GET /tables/{name}``)."""

    model_config = ConfigDict(extra="allow")

    tableName: str | None = Field(default=None, description="The table name.")
    tableType: str | None = Field(
        default=None, description="Table type: 'OFFLINE' or 'REALTIME'."
    )
    segmentsConfig: dict[str, Any] | None = Field(
        default=None,
        description="Retention, replication, and time-column settings.",
    )
    tableIndexConfig: dict[str, Any] | None = Field(
        default=None, description="Index configuration (inverted, sorted, range, ...)."
    )
    tenants: dict[str, Any] | None = Field(
        default=None, description="Broker and server tenant assignment."
    )
    ingestionConfig: dict[str, Any] | None = Field(
        default=None, description="Ingestion / transform configuration, when present."
    )


class TableConfigSchema(BaseModel):
    """Combined table configuration and schema (``GET /tableConfigs/{name}``).

    The Pinot ``schema`` key is preserved as an extra field (rather than declared)
    to avoid shadowing Pydantic's reserved ``schema`` attribute.
    """

    model_config = ConfigDict(extra="allow")

    tableName: str | None = Field(default=None, description="The table name.")
    offline: dict[str, Any] | None = Field(
        default=None, description="OFFLINE table configuration, when present."
    )
    realtime: dict[str, Any] | None = Field(
        default=None, description="REALTIME table configuration, when present."
    )
