"""Verify queue attribution, cancellation, and correlation across worker threads."""

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastmcp import Client, FastMCP
from fastmcp.tools import ToolResult
from mcp import MCPError
from mcp.types import CallToolResult, TextContent
import pytest

from mcp_pinot.models import QueryExecutionMetadata, QueryExecutionResult
from mcp_pinot.observability import (
    AuditMiddleware,
    ConcurrencyMiddleware,
    query_request_id,
)
from mcp_pinot.server import (
    _SchemaPreservingResponseLimitMiddleware,
    _ToolRateLimitMiddleware,
    mcp,
)

META_KEY = "io.github.startreedata/mcp-pinot"


@pytest.mark.asyncio
async def test_queue_and_execution_are_attributed_separately(caplog):
    audit, concurrency = AuditMiddleware(), ConcurrencyMiddleware(1)
    context = SimpleNamespace(message=SimpleNamespace(name="read_query"))
    clock = [1.0]
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def work(_context):
        calls.append(query_request_id())
        if len(calls) == 1:
            entered.set()
            await release.wait()
        return ToolResult(content="ok")

    async def admitted(ctx):
        return await concurrency.on_call_tool(ctx, work)

    with (
        patch("mcp_pinot.observability.time.monotonic", side_effect=lambda: clock[0]),
        patch("mcp_pinot.observability.get_access_token", return_value=None),
        caplog.at_level(logging.INFO, logger="mcp-pinot"),
    ):
        first = asyncio.create_task(audit.on_call_tool(context, admitted))
        await entered.wait()
        clock[0] = 2.0
        second = asyncio.create_task(audit.on_call_tool(context, admitted))
        await asyncio.sleep(0)
        clock[0] = 5.0
        release.set()
        first_result, second_result = await first, await second
    first_meta, second_meta = first_result.meta[META_KEY], second_result.meta[META_KEY]
    assert first_meta == {
        "request_id": calls[0],
        "status": "success",
        "duration_ms": 4000,
        "queue_wait_ms": 0,
        "execution_ms": 4000,
        "admitted": True,
    }
    assert second_meta == {
        "request_id": calls[1],
        "status": "success",
        "duration_ms": 3000,
        "queue_wait_ms": 3000,
        "execution_ms": 0,
        "admitted": True,
    }
    events = [r.message for r in caplog.records if "mcp_audit" in r.message]
    assert (
        "duration_ms=4000 queue_wait_ms=0 execution_ms=4000 admitted=true" in events[0]
    )
    assert (
        "duration_ms=3000 queue_wait_ms=3000 execution_ms=0 admitted=true" in events[1]
    )
    assert len(set(calls)) == 2 and all(calls)
    assert query_request_id() is None


@pytest.mark.asyncio
async def test_cancellation_while_queued_releases_context_and_reports_no_execution(
    caplog,
):
    audit, concurrency = AuditMiddleware(), ConcurrencyMiddleware(1)
    context = SimpleNamespace(message=SimpleNamespace(name="read_query"))
    await concurrency._semaphore.acquire()
    clock = [0.0]

    async def work(_context):
        pytest.fail("queued cancellation must not execute")

    async def admitted(ctx):
        return await concurrency.on_call_tool(ctx, work)

    with (
        patch("mcp_pinot.observability.time.monotonic", side_effect=lambda: clock[0]),
        caplog.at_level(logging.INFO, logger="mcp-pinot"),
    ):
        task = asyncio.create_task(audit.on_call_tool(context, admitted))
        await asyncio.sleep(0)
        clock[0] = 2.0
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert (
        "status=cancelled duration_ms=2000 queue_wait_ms=2000 "
        "execution_ms=0 admitted=false" in caplog.text
    )
    assert query_request_id() is None
    concurrency._semaphore.release()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [RuntimeError("secret arguments"), asyncio.CancelledError()]
)
async def test_admitted_failures_release_permit_and_never_log_payload(failure, caplog):
    audit, concurrency = AuditMiddleware(), ConcurrencyMiddleware(1)
    context = SimpleNamespace(message=SimpleNamespace(name="tool\nforged"))

    async def work(_context):
        raise failure

    async def admitted(ctx):
        return await concurrency.on_call_tool(ctx, work)

    with caplog.at_level(logging.INFO, logger="mcp-pinot"):
        with pytest.raises(type(failure)):
            await audit.on_call_tool(context, admitted)
    assert concurrency._semaphore._value == 1
    assert "admitted=true" in caplog.text
    assert "secret arguments" not in caplog.text
    assert "tool=tool_forged" in caplog.text
    assert query_request_id() is None


@pytest.mark.asyncio
async def test_rate_rejection_and_error_result_are_recorded(caplog):
    context = SimpleNamespace(message=SimpleNamespace(name="read_query"))

    async def rejection(_context):
        raise ValueError("private data")

    async def error_result(_context):
        return SimpleNamespace(is_error=True)

    with caplog.at_level(logging.INFO, logger="mcp-pinot"):
        with pytest.raises(ValueError):
            await AuditMiddleware().on_call_tool(context, rejection)
        await AuditMiddleware().on_call_tool(context, error_result)
    assert "status=error" in caplog.text
    assert "queue_wait_ms=0 execution_ms=0 admitted=false" in caplog.text
    assert "private data" not in caplog.text


@pytest.mark.asyncio
async def test_fastmcp_worker_correlation_matches_native_envelope_and_audit(caplog):
    identifiers = []

    def execute(query, max_rows, application_name, request_id):
        identifiers.append(request_id)
        assert query_request_id() == request_id
        return QueryExecutionResult(
            metadata=QueryExecutionMetadata(request_id=request_id)
        )

    with (
        patch("mcp_pinot.server.pinot_client") as native,
        caplog.at_level(logging.INFO, logger="mcp-pinot"),
    ):
        native.execute_query_with_metadata.side_effect = execute
        async with Client(mcp) as client:
            result = await client.call_tool("read_query", {"query": "SELECT 1"})
    assert identifiers[0] is not None
    assert result.structured_content["metadata"]["request_id"] == identifiers[0]
    metadata = result.meta[META_KEY]
    assert metadata["request_id"] == identifiers[0]
    assert metadata["status"] == "success"
    assert metadata["admitted"] is True
    assert all(
        metadata[name] >= 0 for name in ("duration_ms", "queue_wait_ms", "execution_ms")
    )
    assert result.structured_content["metadata"]["completeness"] == "unknown"
    assert "request_id=" + identifiers[0] in caplog.text
    assert "SELECT 1" not in caplog.text


@pytest.mark.asyncio
async def test_token_lookup_failure_resets_context_and_records_error(caplog):
    async def work(_context):
        pytest.fail("token lookup failed before work")

    context = SimpleNamespace(message=SimpleNamespace(name="read_query"))
    with (
        patch(
            "mcp_pinot.observability.get_access_token",
            side_effect=TypeError("private token detail"),
        ),
        caplog.at_level(logging.INFO, logger="mcp-pinot"),
    ):
        with pytest.raises(TypeError):
            await AuditMiddleware().on_call_tool(context, work)
    assert query_request_id() is None
    assert "status=error" in caplog.text
    assert "admitted=false" in caplog.text
    assert "private token detail" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("is_error", [False, True])
async def test_protocol_metadata_preserves_raw_tool_results(is_error):
    server = FastMCP(middleware=[AuditMiddleware(), ConcurrencyMiddleware(1)])
    raw = CallToolResult(
        content=[TextContent(type="text", text="original content")],
        structured_content={"value": 42},
        is_error=is_error,
        _meta={"example.org/metadata": {"preserved": True}},
    )

    @server.tool
    def sample() -> ToolResult:
        return ToolResult.from_mcp_result(raw)

    async with Client(server) as client:
        result = await client.call_tool_mcp(
            "sample", {}, meta={META_KEY: {"request_id": "client-forged-id"}}
        )
    assert result.content == raw.content
    assert result.structured_content == raw.structured_content
    assert result.is_error is is_error
    assert result.meta["example.org/metadata"] == {"preserved": True}
    metadata = result.meta[META_KEY]
    assert metadata["status"] == ("error" if is_error else "success")
    assert metadata["admitted"] is True
    assert len(metadata["request_id"]) == 32
    assert metadata["request_id"] != "client-forged-id"
    assert raw.meta == {"example.org/metadata": {"preserved": True}}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["rate_limit", "response_limit", "internal"])
async def test_middleware_failures_return_safe_protocol_metadata(failure):
    rate = _ToolRateLimitMiddleware(10, 20)
    server = FastMCP(
        middleware=[
            AuditMiddleware(),
            rate,
            ConcurrencyMiddleware(1),
            _SchemaPreservingResponseLimitMiddleware(100),
        ],
        mask_error_details=True,
    )

    @server.tool
    def sample() -> str:
        if failure == "internal":
            raise RuntimeError("private exception detail")
        return "x" * 500

    if failure == "rate_limit":
        rate._limiter_for = lambda _principal: SimpleNamespace(
            consume=AsyncMock(return_value=False)
        )
    async with Client(server) as client:
        if failure == "rate_limit":
            with pytest.raises(MCPError) as raised:
                await client.call_tool_mcp("sample", {})
            assert raised.value.code == -32000
            assert raised.value.message == "Tool invocation rate limit exceeded"
            metadata = raised.value.error.data["_meta"][META_KEY]
            assert metadata["status"] == "error"
            assert metadata["admitted"] is False
            assert metadata["queue_wait_ms"] == metadata["execution_ms"] == 0
            assert len(metadata["request_id"]) == 32
            assert query_request_id() is None
            return
        result = await client.call_tool_mcp("sample", {})
    assert result.is_error is True
    metadata = result.meta[META_KEY]
    assert metadata["status"] == "error"
    assert metadata["admitted"] is (failure != "rate_limit")
    assert len(metadata["request_id"]) == 32
    if failure == "response_limit":
        assert "100-byte limit" in result.content[0].text
    else:
        assert result.content[0].text == "Error calling tool 'sample'"
    assert "private exception detail" not in result.model_dump_json()
    assert query_request_id() is None
