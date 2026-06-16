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
