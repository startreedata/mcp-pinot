"""Payload-free tool timing and native-query correlation."""

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import re
import time
from typing import Any
from uuid import uuid4

from fastmcp.exceptions import DisabledError, FastMCPError, NotFoundError
from fastmcp.server.dependencies import get_access_token
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.server.middleware.rate_limiting import RateLimitError
from fastmcp.tools import ToolResult
from mcp.types import CallToolResult, TextContent

from mcp_pinot.config import get_logger

logger = get_logger()
_META_KEY = "io.github.startreedata/mcp-pinot"


@dataclass
class _Invocation:
    request_id: str
    started: float
    queued: float | None = None
    admitted: float | None = None
    finished: float | None = None


_invocation: ContextVar[_Invocation | None] = ContextVar("mcp_invocation", default=None)


def query_request_id() -> str | None:
    """Return the server-generated invocation ID, including in tool worker threads."""
    record = _invocation.get()
    return record.request_id if record is not None else None


class ConcurrencyMiddleware(Middleware):
    """Bound admitted work and measure time waiting for a permit."""

    def __init__(self, limit: int) -> None:
        self._semaphore = asyncio.Semaphore(limit)

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        record = _invocation.get()
        if record is not None:
            record.queued = time.monotonic()
        async with self._semaphore:
            if record is not None:
                record.admitted = time.monotonic()
            try:
                return await call_next(context)
            finally:
                if record is not None:
                    record.finished = time.monotonic()


class AuditMiddleware(Middleware):
    """Expose safe tool timings and audit the pipeline without logging payloads."""

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        record = _Invocation(request_id=uuid4().hex, started=time.monotonic())
        context_token = _invocation.set(record)
        principal_id, tool_name, status = "unknown", "unknown", "success"
        rate_error: RateLimitError | None = None
        try:
            access_token = get_access_token()
            claims = getattr(access_token, "claims", {}) or {}
            identity = str(
                claims.get("sub") or getattr(access_token, "client_id", "local")
            )
            principal_id = hashlib.sha256(identity.encode()).hexdigest()[:16]
            tool_name = re.sub(r"[^A-Za-z0-9_.-]", "_", context.message.name)[:128]
            result = await call_next(context)
            if getattr(result, "is_error", False) is True:
                status = "error"
        except asyncio.CancelledError:
            status = "cancelled"
            raise
        except RateLimitError as exc:
            status = "error"
            rate_error = exc
            raise
        except (DisabledError, NotFoundError):
            status = "error"
            result = ToolResult(
                content=[
                    TextContent(
                        type="text", text=f"Unknown tool: {context.message.name!r}"
                    )
                ],
                is_error=True,
            )
        except FastMCPError as exc:
            # FastMCP has already masked internal tool errors. Preserve the same
            # error content as its protocol adapter while adding response metadata.
            status = "error"
            result = ToolResult(
                content=[TextContent(type="text", text=str(exc))], is_error=True
            )
        except Exception:
            status = "error"
            raise
        finally:
            ended = time.monotonic()
            queue_wait = (
                (record.admitted if record.admitted is not None else ended)
                - record.queued
                if record.queued is not None
                else 0
            )
            execution = (
                (record.finished if record.finished is not None else ended)
                - record.admitted
                if record.admitted is not None
                else 0
            )
            metadata = {
                "request_id": record.request_id,
                "status": status,
                "duration_ms": int((ended - record.started) * 1000),
                "queue_wait_ms": int(queue_wait * 1000),
                "execution_ms": int(execution * 1000),
                "admitted": record.admitted is not None,
            }
            _invocation.reset(context_token)
            if rate_error is not None:
                # Preserve the JSON-RPC rate-limit error instead of changing it
                # into a tool result. The client receives the same timing envelope.
                rate_error.error = rate_error.error.model_copy(
                    update={"data": {"_meta": {_META_KEY: metadata}}}
                )
            logger.info(
                "mcp_audit request_id=%s principal_id=%s tool=%s status=%s "
                "duration_ms=%d queue_wait_ms=%d execution_ms=%d admitted=%s",
                record.request_id,
                principal_id,
                tool_name,
                status,
                metadata["duration_ms"],
                metadata["queue_wait_ms"],
                metadata["execution_ms"],
                str(record.admitted is not None).lower(),
            )
        if isinstance(result, ToolResult):
            wire_result = result.to_mcp_result()
            if isinstance(wire_result, CallToolResult):
                return ToolResult.from_mcp_result(
                    wire_result.model_copy(
                        update={
                            "meta": {**(wire_result.meta or {}), _META_KEY: metadata}
                        }
                    )
                )
            return result.model_copy(
                update={"meta": {**(result.meta or {}), _META_KEY: metadata}}
            )
        return result
