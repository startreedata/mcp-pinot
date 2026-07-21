"""Typed output models for the Pinot MCP tools.

Returning these instead of opaque JSON strings gives every tool a documented
JSON Schema, advertised as ``outputSchema`` and returned as ``structuredContent``,
so MCP clients and LLMs can validate and parse results reliably. Models that wrap
pass-through Pinot REST payloads allow extra fields so no information is dropped
while the meaningful keys stay documented.
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_NAME_PATTERN = r"^(?:[A-Za-z0-9_-]+\.)?[A-Za-z0-9_-]+$"


class SchemaFieldSpec(BaseModel):
    """Typed Pinot column definition used by schema write tools."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    data_type: Literal[
        "INT",
        "LONG",
        "FLOAT",
        "DOUBLE",
        "BIG_DECIMAL",
        "BOOLEAN",
        "TIMESTAMP",
        "STRING",
        "BYTES",
        "JSON",
    ] = Field(alias="dataType", serialization_alias="dataType")
    single_value_field: bool = Field(
        default=True, alias="singleValueField", serialization_alias="singleValueField"
    )
    default_null_value: Any = Field(
        default=None,
        alias="defaultNullValue",
        serialization_alias="defaultNullValue",
    )


class SchemaInput(BaseModel):
    """Structured Pinot schema accepted by create/update operations."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    schema_name: str = Field(
        alias="schemaName",
        serialization_alias="schemaName",
        min_length=1,
        max_length=128,
        pattern=_NAME_PATTERN,
    )
    dimension_field_specs: list[SchemaFieldSpec] = Field(
        default_factory=list,
        alias="dimensionFieldSpecs",
        serialization_alias="dimensionFieldSpecs",
        max_length=10000,
    )
    metric_field_specs: list[SchemaFieldSpec] = Field(
        default_factory=list,
        alias="metricFieldSpecs",
        serialization_alias="metricFieldSpecs",
        max_length=10000,
    )
    date_time_field_specs: list[SchemaFieldSpec] = Field(
        default_factory=list,
        alias="dateTimeFieldSpecs",
        serialization_alias="dateTimeFieldSpecs",
        max_length=100,
    )
    primary_key_columns: list[str] | None = Field(
        default=None,
        alias="primaryKeyColumns",
        serialization_alias="primaryKeyColumns",
        max_length=100,
    )


class TableConfigInput(BaseModel):
    """Structured Pinot table configuration accepted by write tools."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    table_name: str = Field(
        alias="tableName",
        serialization_alias="tableName",
        min_length=1,
        max_length=128,
        pattern=_NAME_PATTERN,
    )
    table_type: Literal["OFFLINE", "REALTIME"] = Field(
        alias="tableType", serialization_alias="tableType"
    )
    segments_config: dict[str, Any] = Field(
        alias="segmentsConfig", serialization_alias="segmentsConfig"
    )
    table_index_config: dict[str, Any] = Field(
        alias="tableIndexConfig", serialization_alias="tableIndexConfig"
    )
    tenants: dict[str, Any] = Field(default_factory=dict)
    ingestion_config: dict[str, Any] | None = Field(
        default=None,
        alias="ingestionConfig",
        serialization_alias="ingestionConfig",
    )


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
    truncated: bool = Field(
        default=False,
        description="True when the server-enforced fetch bound truncated the result.",
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
    """Preview or outcome of hot-reloading the table access filter file."""

    model_config = ConfigDict(extra="allow")

    status: Literal["preview", "success", "error"] = Field(
        description="Whether the candidate filters were previewed, applied, or failed."
    )
    message: str = Field(description="Human-readable summary of the reload.")
    applied: bool = Field(
        default=False,
        description="True only when the in-memory allow-list was changed.",
    )
    previous_filter_count: int = Field(
        default=0, description="Number of allowed tables before the reload."
    )
    new_filter_count: int = Field(
        default=0, description="Number of allowed tables after the reload."
    )
    previous_filters: list[str] | None = Field(
        default=None,
        description="Allow-list patterns active before the operation; null means all.",
    )
    new_filters: list[str] | None = Field(
        default=None,
        description="Validated candidate patterns; null means all tables.",
    )


class OperationResult(BaseModel):
    """Result of a schema or table-config create/update operation."""

    model_config = ConfigDict(extra="forbid")

    operation: Literal[
        "create_schema", "update_schema", "create_table", "update_table"
    ] = Field(description="Stable operation identifier.")
    resource_type: Literal["schema", "table"] = Field(
        description="Kind of Pinot resource targeted by the operation."
    )
    resource_name: str = Field(description="Exact schema or table name targeted.")
    status: Literal["preview", "success", "rejected"] = Field(
        default="success",
        description="Whether the operation was previewed, applied, or rejected.",
    )
    applied: bool = Field(
        default=False, description="True only when Pinot accepted a mutating request."
    )
    dry_run: bool = Field(
        default=False, description="True when no mutation was sent to Pinot."
    )
    message: str | None = Field(
        default=None, description="Human-readable detail, when provided."
    )
    warnings: list[str] = Field(
        default_factory=list, description="Safety or validation warnings."
    )
    verification_tool: Literal["get_schema", "get_table_config"] = Field(
        description="Read tool to call after a successful mutation."
    )
    confirmation_token: str | None = Field(
        default=None,
        description=(
            "Short-lived token bound to this exact preview; required to apply it."
        ),
    )
    response_summary: str | None = Field(
        default=None,
        description="Sanitized summary returned by the Pinot controller.",
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


class SegmentMetadataPage(BaseModel):
    """A deterministic page of segment metadata for one Pinot table."""

    segments: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Metadata keyed by exact segment name. Values can include row counts, "
            "sizes, time boundaries, and column/index details returned by Pinot."
        ),
    )
    returned_segments: int = Field(
        description="Number of segment metadata objects in this page."
    )
    total_segments: int = Field(
        description="Total segment metadata objects fetched before paging."
    )
    offset: int = Field(description="Zero-based index of the first returned segment.")
    has_more: bool = Field(
        description="True when more fetched segment metadata remains after this page."
    )


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


class TableConfigResult(BaseModel):
    """Canonical shape for one-sided and hybrid Pinot table configurations."""

    table_name: str = Field(description="Base table name requested by the caller.")
    offline: TableConfig | None = Field(
        default=None, description="OFFLINE configuration, when present/requested."
    )
    realtime: TableConfig | None = Field(
        default=None, description="REALTIME configuration, when present/requested."
    )


class TableConfigSchema(BaseModel):
    """Combined table configuration and schema (``GET /tableConfigs/{name}``).

    ``schema_data`` uses the JSON alias ``schema`` so the wire format and advertised
    output schema match Pinot without shadowing Pydantic's ``schema`` attribute.
    """

    model_config = ConfigDict(extra="allow")

    tableName: str | None = Field(default=None, description="The table name.")
    offline: dict[str, Any] | None = Field(
        default=None, description="OFFLINE table configuration, when present."
    )
    realtime: dict[str, Any] | None = Field(
        default=None, description="REALTIME table configuration, when present."
    )
    schema_data: dict[str, Any] | None = Field(
        default=None,
        alias="schema",
        serialization_alias="schema",
        description="Pinot schema definition associated with the table.",
    )
