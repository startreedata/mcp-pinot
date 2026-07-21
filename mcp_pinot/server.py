# --------------------------
# File: mcp_pinot/server.py
# --------------------------
"""
FastMCP-based implementation for the Apache Pinot MCP Server.
"""

import asyncio
import base64
import binascii
from collections import Counter, OrderedDict
from collections.abc import Callable
import hashlib
import hmac
from ipaddress import ip_address
import json
import os
import secrets
from threading import Lock
import time
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.auth import require_scopes
from fastmcp.server.dependencies import get_access_token
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from fastmcp.server.middleware.rate_limiting import (
    RateLimitError,
    TokenBucketRateLimiter,
)
from fastmcp.server.middleware.response_limiting import ResponseLimitingMiddleware
from mcp.types import ToolAnnotations
from pydantic import Field
import requests
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Receive, Scope, Send
import uvicorn

from mcp_pinot import __version__
from mcp_pinot.auth import build_auth
from mcp_pinot.config import (
    get_logger,
    load_pinot_config,
    load_server_config,
    setup_logging,
)
from mcp_pinot.models import (
    ConnectionDiagnostics,
    FilterReloadResult,
    OperationResult,
    PinotSchema,
    QueryResult,
    SchemaInput,
    SegmentIndexDetails,
    SegmentList,
    SegmentMetadataPage,
    TableConfig,
    TableConfigInput,
    TableConfigResult,
    TableList,
    TableSizeDetails,
)
from mcp_pinot.pinot_client import (
    MAX_QUERY_ROWS,
    PinotClient,
    validate_pinot_path_component,
)
from mcp_pinot.prompts import PROMPT_TEMPLATE

logger = get_logger()

# Initialize configurations and create client
pinot_config = load_pinot_config()
server_config = load_server_config()
pinot_client = PinotClient(pinot_config)

# Build the auth provider selected by configuration (None disables auth).
_auth = build_auth(server_config)


def _rate_limit_client_id(context: MiddlewareContext[Any]) -> str:
    """Group quotas by authenticated principal or a non-spoofable local peer.

    Stateless HTTP creates a fresh MCP session for every request. Using that session
    ID (or client-supplied metadata) would therefore let an unauthenticated caller
    create a new bucket for every invocation. Network-reachable HTTP requires auth,
    so the direct peer fallback is used only for loopback deployments.
    """
    token = get_access_token()
    if token and token.client_id:
        return f"principal:{token.client_id}"
    if context.fastmcp_context:
        request_context = context.fastmcp_context.request_context
        request = request_context.request if request_context else None
        peer = getattr(request, "client", None)
        peer_host = getattr(peer, "host", None)
        if peer_host:
            return f"peer:{peer_host}"
        try:
            return f"session:{context.fastmcp_context.session_id}"
        except RuntimeError:
            pass
    return "anonymous"


class _ToolRateLimitMiddleware(Middleware):
    """Rate-limit tool invocations per principal/session, not protocol setup."""

    def __init__(
        self,
        requests_per_second: float,
        burst_capacity: int,
        *,
        max_clients: int = 10_000,
        idle_ttl_seconds: float = 600,
    ) -> None:
        self._requests_per_second = requests_per_second
        self._burst_capacity = burst_capacity
        self._max_clients = max_clients
        self._idle_ttl_seconds = idle_ttl_seconds
        self._limiters: OrderedDict[str, tuple[TokenBucketRateLimiter, float]] = (
            OrderedDict()
        )

    def _limiter_for(self, principal: str) -> TokenBucketRateLimiter:
        """Return a bucket while bounding and expiring the per-client cache."""
        now = time.monotonic()
        existing = self._limiters.pop(principal, None)
        if existing is not None:
            limiter, _last_seen = existing
            self._limiters[principal] = (limiter, now)
            return limiter

        while self._limiters:
            _oldest_principal, (_limiter, last_seen) = next(
                iter(self._limiters.items())
            )
            if now - last_seen <= self._idle_ttl_seconds:
                break
            self._limiters.popitem(last=False)

        if len(self._limiters) >= self._max_clients:
            self._limiters.popitem(last=False)

        limiter = TokenBucketRateLimiter(
            self._burst_capacity, self._requests_per_second
        )
        self._limiters[principal] = (limiter, now)
        return limiter

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        principal = _rate_limit_client_id(context)
        if not await self._limiter_for(principal).consume():
            raise RateLimitError("Tool invocation rate limit exceeded")
        return await call_next(context)


class _ConcurrencyMiddleware(Middleware):
    """Bound concurrent tool work so slow Pinot calls cannot exhaust the process."""

    def __init__(self, limit: int) -> None:
        self._semaphore = asyncio.Semaphore(limit)

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        async with self._semaphore:
            return await call_next(context)


class _AuditMiddleware(Middleware):
    """Emit payload-free structured audit events for every tool invocation."""

    async def on_call_tool(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        started = time.monotonic()
        token = get_access_token()
        principal = token.client_id if token and token.client_id else "local"
        tool_name = context.message.name
        status = "success"
        try:
            return await call_next(context)
        except Exception:
            status = "error"
            raise
        finally:
            logger.info(
                "mcp_audit principal=%s tool=%s status=%s duration_ms=%d",
                principal,
                tool_name,
                status,
                int((time.monotonic() - started) * 1000),
            )


_RATE_LIMIT_RPS = float(os.getenv("MCP_RATE_LIMIT_RPS", "10"))
_RATE_LIMIT_BURST = int(os.getenv("MCP_RATE_LIMIT_BURST", "20"))
_RATE_LIMIT_MAX_CLIENTS = int(os.getenv("MCP_RATE_LIMIT_MAX_CLIENTS", "10000"))
_RATE_LIMIT_IDLE_TTL_SECONDS = float(
    os.getenv("MCP_RATE_LIMIT_IDLE_TTL_SECONDS", "600")
)
_MAX_CONCURRENCY = int(os.getenv("MCP_MAX_CONCURRENCY", "8"))
_MAX_RESPONSE_BYTES = int(os.getenv("MCP_MAX_RESPONSE_BYTES", "1000000"))
if (
    min(
        _RATE_LIMIT_RPS,
        _RATE_LIMIT_BURST,
        _RATE_LIMIT_MAX_CLIENTS,
        _RATE_LIMIT_IDLE_TTL_SECONDS,
        _MAX_CONCURRENCY,
        _MAX_RESPONSE_BYTES,
    )
    <= 0
):
    raise ValueError("MCP rate, concurrency, and response limits must be positive.")

mcp = FastMCP(
    "Pinot MCP Server",
    version=__version__,
    instructions=(
        "Query and inspect an Apache Pinot real-time OLAP cluster. Use read_query "
        "for read-only SQL (SELECT or WITH ... SELECT); queries are validated and "
        "paginated. Call list_tables to discover case-sensitive table names, and "
        "the inspection tools (get_table_size, get_schema, get_table_config, "
        "list_segments) before composing queries or changes. Every write defaults "
        "to preview and requires the returned short-lived confirmation token before "
        "the exact reviewed payload can be applied."
    ),
    auth=_auth,
    middleware=[
        _ToolRateLimitMiddleware(
            _RATE_LIMIT_RPS,
            _RATE_LIMIT_BURST,
            max_clients=_RATE_LIMIT_MAX_CLIENTS,
            idle_ttl_seconds=_RATE_LIMIT_IDLE_TTL_SECONDS,
        ),
        _ConcurrencyMiddleware(_MAX_CONCURRENCY),
        ResponseLimitingMiddleware(max_size=_MAX_RESPONSE_BYTES),
        _AuditMiddleware(),
    ],
    # Internal exceptions are replaced with a generic message; only ToolError
    # messages (which we craft to be safe and actionable) reach the client.
    mask_error_details=True,
)


@mcp.custom_route("/livez", methods=["GET"], include_in_schema=False)
async def livez(_request: Request) -> PlainTextResponse:
    """Report that the MCP process and ASGI event loop are alive."""
    return PlainTextResponse("ok")


@mcp.custom_route("/readyz", methods=["GET"], include_in_schema=False)
async def readyz(_request: Request) -> PlainTextResponse:
    """Report that the MCP application completed startup and can accept work."""
    return PlainTextResponse("ready")


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

_MAX_QUERY_PAGE_SIZE = 500
# Keep the public offset bound derived from the client's single upstream fetch cap.
_MAX_QUERY_OFFSET = MAX_QUERY_ROWS - _MAX_QUERY_PAGE_SIZE - 1
_NAME_PATTERN = r"^(?:[A-Za-z0-9_-]+\.)?[A-Za-z0-9_-]+$"


def _canonical_host_authority(value: str) -> str:
    """Validate and canonicalize one exact HTTP Host authority."""
    if not value or value != value.strip():
        raise ValueError("Host authorities must be non-empty and have no whitespace.")
    try:
        value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError(
            "Host authorities must contain only ASCII characters."
        ) from exc
    if any(char.isspace() or ord(char) < 32 for char in value):
        raise ValueError("Host authorities must not contain whitespace or controls.")
    if any(char in value for char in "*/\\?#,@%"):
        raise ValueError("Host authorities must be exact host[:port] values.")

    parsed = urlsplit(f"//{value}")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"Invalid Host authority {value!r}.") from exc
    if (
        parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or value.endswith(":")
    ):
        raise ValueError(f"Invalid Host authority {value!r}.")

    hostname = parsed.hostname.casefold()
    authority = f"[{hostname}]" if ":" in hostname else hostname
    return f"{authority}:{port}" if port is not None else authority


def _canonical_origin(value: str) -> str:
    """Validate and canonicalize one exact HTTP(S) browser Origin."""
    if not value or value != value.strip() or value == "null":
        raise ValueError("Origins must be explicit HTTP(S) origins.")
    parsed = urlsplit(value)
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.netloc
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"Invalid Origin {value!r}; use scheme://host[:port].")
    authority = _canonical_host_authority(parsed.netloc)
    return f"{parsed.scheme.casefold()}://{authority}"


class MCPHostOriginMiddleware:
    """Enforce exact Host and Origin allowlists on the MCP HTTP endpoint."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        mcp_path: str,
        allowed_hosts: tuple[str, ...],
        allowed_origins: tuple[str, ...],
    ) -> None:
        self.app = app
        self.mcp_path = mcp_path.rstrip("/") or "/"
        self.allowed_hosts = frozenset(
            _canonical_host_authority(host) for host in allowed_hosts
        )
        self.allowed_origins = frozenset(
            _canonical_origin(origin) for origin in allowed_origins
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        protected_path = path == self.mcp_path or (
            self.mcp_path != "/" and path.startswith(f"{self.mcp_path}/")
        )
        if scope["type"] != "http" or not protected_path:
            await self.app(scope, receive, send)
            return

        headers = scope.get("headers", [])
        host_headers = [
            value.decode("latin-1")
            for name, value in headers
            if name.lower() == b"host"
        ]
        origin_headers = [
            value.decode("latin-1")
            for name, value in headers
            if name.lower() == b"origin"
        ]

        try:
            host_allowed = len(host_headers) == 1 and (
                _canonical_host_authority(host_headers[0]) in self.allowed_hosts
            )
            origin_allowed = not origin_headers or (
                len(origin_headers) == 1
                and _canonical_origin(origin_headers[0]) in self.allowed_origins
            )
        except ValueError:
            host_allowed = False
            origin_allowed = False

        if not host_allowed or not origin_allowed:
            response = PlainTextResponse("Forbidden", status_code=403)
            await response(scope, receive, send)
            return

        await self.app(scope, receive, send)


_READ_AUTH = require_scopes("pinot:read") if _auth is not None else None
_WRITE_AUTH = require_scopes("pinot:write") if _auth is not None else None
_ADMIN_AUTH = require_scopes("pinot:admin") if _auth is not None else None

_confirmation_secret_env = os.getenv("MCP_CONFIRMATION_SECRET")
_CONFIRMATION_MASTER_SECRET = (
    _confirmation_secret_env.encode()
    if _confirmation_secret_env
    else secrets.token_bytes(32)
)
# Even with an operator-supplied master secret, bind tokens to this process. This
# makes restarts and accidental multi-process routing fail closed instead of
# allowing a consumed nonce to be replayed against another in-memory nonce store.
_CONFIRMATION_SECRET = hmac.new(
    _CONFIRMATION_MASTER_SECRET,
    b"mcp-pinot-process:" + secrets.token_bytes(32),
    hashlib.sha256,
).digest()
_CONFIRMATION_TTL_SECONDS = int(os.getenv("MCP_CONFIRMATION_TTL_SECONDS", "300"))
if not 30 <= _CONFIRMATION_TTL_SECONDS <= 3600:
    raise ValueError("MCP_CONFIRMATION_TTL_SECONDS must be between 30 and 3600.")
_used_confirmation_tokens: dict[str, int] = {}
_confirmation_lock = Lock()


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
    """Log internals and return a stable, sanitized error classification."""
    logger.error("%s failed: %s", action, exc, exc_info=True)
    code = "PINOT_INTERNAL_ERROR"
    category = "server"
    retryable = False
    retry_after: int | None = None
    message = f"{action} failed."
    recovery_steps = [hint] if hint else ["Check server logs and configuration."]

    if isinstance(exc, requests.exceptions.Timeout):
        code = "PINOT_TIMEOUT"
        category = "transient"
        retryable = True
        message = f"{action} timed out."
        recovery_steps = [
            "Call test_connection before retrying.",
            "Reduce query or page complexity if connectivity is healthy.",
        ]
    elif isinstance(exc, requests.exceptions.ConnectionError):
        code = "PINOT_UNAVAILABLE"
        category = "transient"
        retryable = True
        message = "The Pinot endpoint is unavailable."
        recovery_steps = ["Call test_connection and retry after connectivity returns."]
    elif isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        if status == 401:
            code, category = "PINOT_AUTHENTICATION_REQUIRED", "authentication"
            message = "Pinot rejected the configured service credential."
            recovery_steps = ["Rotate or correct the Pinot service credential."]
        elif status == 403:
            code, category = "PINOT_PERMISSION_DENIED", "authorization"
            message = "The Pinot service credential lacks permission for this action."
            recovery_steps = ["Grant the minimum required Pinot permission."]
        elif status == 404:
            code, category = "PINOT_RESOURCE_NOT_FOUND", "permanent"
            message = "The requested Pinot resource was not found."
            recovery_steps = [hint or "Use a discovery tool and copy an exact name."]
        elif status == 408:
            code, category, retryable = "PINOT_TIMEOUT", "transient", True
            message = f"{action} timed out."
            recovery_steps = ["Call test_connection before retrying."]
        elif status == 429:
            code, category, retryable = "PINOT_RATE_LIMITED", "transient", True
            message = "Pinot rate-limited the request."
            raw_retry_after = exc.response.headers.get("Retry-After")
            if raw_retry_after and raw_retry_after.isdigit():
                retry_after = int(raw_retry_after)
            recovery_steps = ["Wait for the retry interval, then retry once."]
        elif 500 <= status < 600:
            code, category, retryable = "PINOT_SERVER_ERROR", "transient", True
            message = "Pinot returned a server error."
            recovery_steps = ["Call test_connection before retrying."]
        else:
            code, category = "PINOT_REQUEST_REJECTED", "permanent"
            message = "Pinot rejected the request."
            recovery_steps = [hint or "Correct the request before retrying."]

    return ToolError(
        json.dumps(
            {
                "code": code,
                "category": category,
                "retryable": retryable,
                "message": message,
                "recovery_steps": recovery_steps,
                "retry_after_seconds": retry_after,
            },
            separators=(",", ":"),
        )
    )


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
        raise ToolError(
            json.dumps(
                {
                    "code": "INVALID_INPUT",
                    "category": "permanent",
                    "retryable": False,
                    "message": str(e),
                    "recovery_steps": ["Correct the named input and retry."],
                    "retry_after_seconds": None,
                },
                separators=(",", ":"),
            )
        ) from e
    except Exception as e:
        raise _fail(action, e, hint) from e


def _validate_base_name(name: str, kind: str) -> str:
    """Enforce identifier constraints that JSON Schema patterns cannot express."""
    if len(name) > 128 or "__" in name or name.endswith(("_OFFLINE", "_REALTIME")):
        raise ToolError(
            f"Invalid {kind} name. Use a base name of at most 128 characters; "
            "double underscores and _OFFLINE/_REALTIME suffixes are not allowed."
        )
    return name


def _schema_payload(
    schema: SchemaInput, expected_name: str | None = None
) -> tuple[str, str]:
    name = _validate_base_name(schema.schema_name, "schema")
    if expected_name is not None and name != expected_name:
        raise ToolError(
            f"schema.schemaName must exactly match schema_name ({expected_name!r})."
        )
    payload = json.dumps(
        schema.model_dump(by_alias=True, exclude_none=True),
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(payload.encode()) > 256_000:
        raise ToolError("Schema payload exceeds the 256000-byte safety limit.")

    fields = [
        *schema.dimension_field_specs,
        *schema.metric_field_specs,
        *schema.date_time_field_specs,
    ]
    names = [field.name for field in fields]
    duplicates = sorted(name for name, count in Counter(names).items() if count > 1)
    if duplicates:
        raise ToolError(f"Schema column names must be unique: {', '.join(duplicates)}")
    if schema.primary_key_columns:
        missing = sorted(set(schema.primary_key_columns) - set(names))
        if missing:
            raise ToolError(
                "primaryKeyColumns reference undefined fields: " + ", ".join(missing)
            )
    return name, payload


def _table_payload(
    table_config: TableConfigInput, expected_name: str | None = None
) -> tuple[str, str]:
    name = _validate_base_name(table_config.table_name, "table")
    if expected_name is not None and name != expected_name:
        raise ToolError(
            f"table_config.tableName must exactly match table_name ({expected_name!r})."
        )
    payload = json.dumps(
        table_config.model_dump(by_alias=True, exclude_none=True),
        sort_keys=True,
        separators=(",", ":"),
    )
    if len(payload.encode()) > 512_000:
        raise ToolError("Table-config payload exceeds the 512000-byte safety limit.")
    return name, payload


def _token_subject(
    operation: str, resource_name: str, payload: str, options: dict[str, Any]
) -> dict[str, Any]:
    return {
        "operation": operation,
        "resource": resource_name,
        "payload_sha256": hashlib.sha256(payload.encode()).hexdigest(),
        "options": options,
    }


def _issue_confirmation_token(
    operation: str, resource_name: str, payload: str, options: dict[str, Any]
) -> str:
    claims = {
        **_token_subject(operation, resource_name, payload, options),
        "expires_at": int(time.time()) + _CONFIRMATION_TTL_SECONDS,
        "nonce": secrets.token_urlsafe(12),
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(claims, sort_keys=True, separators=(",", ":")).encode()
    ).rstrip(b"=")
    signature = hmac.new(_CONFIRMATION_SECRET, encoded, hashlib.sha256).digest()
    return (
        encoded.decode()
        + "."
        + base64.urlsafe_b64encode(signature).rstrip(b"=").decode()
    )


def _decode_base64url_unpadded(value: str) -> bytes:
    """Decode the canonical unpadded base64url form used by confirmation tokens."""
    if not value or "=" in value:
        raise ValueError("base64url value must be non-empty and unpadded")
    try:
        decoded = base64.b64decode(
            value + ("=" * (-len(value) % 4)),
            altchars=b"-_",
            validate=True,
        )
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid base64url value") from exc
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode()
    if not hmac.compare_digest(canonical, value):
        raise ValueError("non-canonical base64url value")
    return decoded


def _consume_confirmation_token(
    token: str | None,
    operation: str,
    resource_name: str,
    payload: str,
    options: dict[str, Any],
) -> None:
    if not token:
        raise ToolError(
            "Applying a write requires confirmation_token from a dry_run preview "
            "of this exact payload."
        )
    if len(token) > 4096 or "." not in token:
        raise ToolError("Invalid confirmation_token.")
    encoded_text, signature_text = token.split(".", 1)
    encoded = encoded_text.encode()
    try:
        signature = _decode_base64url_unpadded(signature_text)
        expected_signature = hmac.new(
            _CONFIRMATION_SECRET, encoded, hashlib.sha256
        ).digest()
        claims = json.loads(_decode_base64url_unpadded(encoded_text))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise ToolError("Invalid confirmation_token.") from exc
    if not hmac.compare_digest(signature, expected_signature):
        raise ToolError("Invalid confirmation_token signature.")
    if not isinstance(claims, dict):
        raise ToolError("Invalid confirmation_token claims.")
    expected = _token_subject(operation, resource_name, payload, options)
    if any(claims.get(key) != value for key, value in expected.items()):
        raise ToolError("confirmation_token does not match this exact operation.")
    expires_at = claims.get("expires_at")
    if not isinstance(expires_at, int) or expires_at <= int(time.time()):
        raise ToolError("confirmation_token expired; preview the operation again.")
    nonce = claims.get("nonce")
    if (
        not isinstance(nonce, str)
        or not 12 <= len(nonce) <= 64
        or any(
            char
            not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
            for char in nonce
        )
    ):
        raise ToolError("Invalid confirmation_token nonce.")
    with _confirmation_lock:
        now = int(time.time())
        expired = [
            key for key, expiry in _used_confirmation_tokens.items() if expiry <= now
        ]
        for key in expired:
            del _used_confirmation_tokens[key]
        if nonce in _used_confirmation_tokens:
            raise ToolError("confirmation_token has already been used.")
        _used_confirmation_tokens[nonce] = expires_at


def _controller_summary(result: Any) -> str:
    """Return only a bounded, non-secret summary from a controller response."""
    if isinstance(result, dict):
        value = result.get("message") or result.get("status") or "Request accepted."
    else:
        value = result
    return str(value)[:500]


_SECRET_KEY_MARKERS = (
    "password",
    "secret",
    "token",
    "credential",
    "sasl.jaas.config",
    "api_key",
    "apikey",
)


def _redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: (
                "[REDACTED]"
                if any(marker in key.lower() for marker in _SECRET_KEY_MARKERS)
                else _redact_sensitive(child)
            )
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [_redact_sensitive(child) for child in value]
    return value


@mcp.tool(
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="Test connection",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
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
    auth=_ADMIN_AUTH,
    annotations=ToolAnnotations(
        title="Reload table filters",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=True,
        openWorldHint=False,
    ),
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
    confirmation_token: Annotated[
        str | None,
        Field(
            description=(
                "One-time token returned by a dry-run preview of the exact current "
                "filter-file contents. Required when dry_run is false."
            ),
            max_length=4096,
        ),
    ] = None,
) -> FilterReloadResult:
    """Preview or apply the configured table-filter YAML without restarting.

    Reads only the path configured by ``PINOT_TABLE_FILTER_FILE``. The YAML must be
    an object whose ``included_tables`` value is a non-empty list of glob strings.
    Allowing every table requires an explicit ``allow_all: true``. The default
    ``dry_run=true`` validates and reports the before/after patterns and returns a
    short-lived confirmation token. Pass ``dry_run=false`` with that token to apply
    the exact candidate atomically. Editing the file after preview invalidates the
    confirmation and requires another preview.

    Returns:
        Preview/application status, whether it was applied, old/new patterns, and
        a confirmation token on previews.

    Failure recovery:
        A missing setting/file or malformed YAML is non-retryable until corrected;
        fix ``PINOT_TABLE_FILTER_FILE`` or its ``included_tables`` list, then retry.
    """
    preview = _call(
        "reload_table_filters",
        "Verify PINOT_TABLE_FILTER_FILE points to YAML with an included_tables list.",
        pinot_client.reload_table_filters,
        dry_run=True,
    )
    candidate_filters = preview.get("new_filters")
    payload = json.dumps(
        {"new_filters": candidate_filters}, sort_keys=True, separators=(",", ":")
    )
    if dry_run:
        preview["confirmation_token"] = _issue_confirmation_token(
            "reload_table_filters", "table_filters", payload, {}
        )
        return FilterReloadResult.model_validate(preview)

    _consume_confirmation_token(
        confirmation_token,
        "reload_table_filters",
        "table_filters",
        payload,
        {},
    )
    results = _call(
        "reload_table_filters",
        "Preview the current filter file again and confirm the unchanged candidate.",
        pinot_client.reload_table_filters,
        dry_run=False,
        expected_filters=candidate_filters,
        require_expected=True,
    )
    return FilterReloadResult.model_validate(results)


@mcp.tool(
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="Read query",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
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
            max_length=20_000,
        ),
    ],
    limit: Annotated[
        int,
        Field(
            description="Maximum rows to return in this page.",
            ge=1,
            le=_MAX_QUERY_PAGE_SIZE,
        ),
    ] = 100,
    offset: Annotated[
        int,
        Field(
            description="Zero-based row offset for pagination.",
            ge=0,
            le=_MAX_QUERY_OFFSET,
        ),
    ] = 0,
) -> QueryResult:
    """Run a read-only SQL query against Pinot and return a page of rows.

    Only a single SELECT (or WITH ... SELECT) statement is allowed; the query is
    rejected if it contains multiple statements or write/DDL/admin keywords.
    Results are paginated to keep responses small — use ``limit``/``offset`` and
    the ``has_more`` flag to page through large result sets.

    Returns ``QueryResult`` with the page of rows, the column list, fetched row
    count, and a ``has_more`` flag.

    Failure recovery:
        SQL/allow-list/permission failures require correcting the query or access;
        do not retry unchanged. A timeout or connection failure can be retried after
        ``test_connection`` succeeds. Zero rows is a successful result.
    """
    fetch_bound = offset + limit + 1
    rows = _call(
        "read_query",
        _HINT_READ,
        pinot_client.execute_query,
        query=query,
        max_rows=fetch_bound,
    )
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
        truncated=total >= fetch_bound,
    )


@mcp.tool(
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="List tables",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def list_tables(
    limit: Annotated[
        int,
        Field(description="Maximum tables to return in this page.", ge=1, le=500),
    ] = 100,
    offset: Annotated[
        int,
        Field(description="Zero-based offset for pagination.", ge=0, le=10000),
    ] = 0,
) -> TableList:
    """List Pinot tables visible to this server (subject to table filters).

    Returns a paginated list of table names. Use ``limit``/``offset`` and the
    ``has_more`` flag to page through clusters with many tables.

    Failure recovery:
        An empty page is success. For authentication/connectivity errors, verify the
        controller with ``test_connection`` and retry after access is restored.
    """
    tables = sorted(_call("list_tables", _HINT_READ, pinot_client.get_tables))
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
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="Table size details",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def get_table_size(
    table_name: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
            pattern=_NAME_PATTERN,
        ),
    ],
) -> TableSizeDetails:
    """Get a table's storage footprint: reported vs. estimated size in bytes.

    Use this for capacity/size questions about a whole table. It does NOT list
    segments (use ``list_segments``) or return row counts/time boundaries (use
    ``list_segment_metadata``). ``reportedSizeInBytes`` is what the servers
    currently hosting the segments report; ``estimatedSizeInBytes`` assumes every
    replica is present.

    Failure recovery:
        For not-found errors, copy an exact name from ``list_tables``. Fix permission
        errors before retrying; retry transient controller failures after a health
        check.
    """
    _validate_base_name(table_name, "table")
    raw = _call(
        "get_table_size",
        _HINT_READ,
        pinot_client.get_table_detail,
        tableName=table_name,
    )
    return TableSizeDetails.model_validate(raw)


@mcp.tool(
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="List segments",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def list_segments(
    table_name: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
            pattern=_NAME_PATTERN,
        ),
    ],
    limit: Annotated[
        int,
        Field(
            description="Maximum segment names to return in this page.", ge=1, le=500
        ),
    ] = 100,
    offset: Annotated[
        int,
        Field(description="Zero-based offset for pagination.", ge=0, le=10000),
    ] = 0,
) -> SegmentList:
    """List a table's segment names, grouped by table type (OFFLINE/REALTIME).

    Use this to discover segment names — e.g. to get a ``segment_name`` for
    ``get_segment_index_metadata``, or to see how a table is partitioned. For
    per-segment row counts / sizes / time boundaries call
    ``list_segment_metadata`` instead; for total storage call ``get_table_size``.

    Segment names are paginated (a busy table can have thousands) — use
    ``limit``/``offset`` and the ``has_more`` flag to page through them.

    Failure recovery:
        An empty page is success. For not-found errors, use an exact name from
        ``list_tables``; correct access errors, or retry transient controller errors.
    """
    _validate_base_name(table_name, "table")
    raw = _call(
        "list_segments", _HINT_READ, pinot_client.get_segments, tableName=table_name
    )
    full = SegmentList.model_validate(raw)
    flat = sorted(("OFFLINE", s) for s in (full.OFFLINE or []))
    flat += sorted(("REALTIME", s) for s in (full.REALTIME or []))
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
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="Index/column details",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def get_segment_index_metadata(
    table_name: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
            pattern=_NAME_PATTERN,
        ),
    ],
    segment_name: Annotated[
        str,
        Field(
            description=(
                "Exact, case-sensitive opaque segment name returned by list_segments; "
                "do not construct, trim, or add a table-type suffix. Pinot defines "
                "the length and characters, so this client only requires non-empty."
            ),
            min_length=1,
            max_length=1024,
        ),
    ],
) -> SegmentIndexDetails:
    """Get per-column index metadata for ONE segment (which indexes each column has).

    Use this to inspect how a specific segment is indexed (inverted, sorted, range,
    etc.). Requires a ``segment_name`` from ``list_segments``. For a segment's row
    count/size/time boundaries use ``list_segment_metadata``; for the table's
    declared index *configuration* (not per-segment state) use ``get_table_config``.

    Failure recovery:
        A missing segment is non-retryable with the same value; refresh
        ``list_segments`` and use an exact returned name. Retry transient controller
        errors after ``test_connection`` succeeds.
    """
    _validate_base_name(table_name, "table")
    raw = _call(
        "get_segment_index_metadata",
        _HINT_READ,
        pinot_client.get_index_column_detail,
        tableName=table_name,
        segmentName=segment_name,
    )
    return SegmentIndexDetails.model_validate(raw)


@mcp.tool(
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="Segment metadata",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def list_segment_metadata(
    table_name: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
            pattern=_NAME_PATTERN,
        ),
    ],
    limit: Annotated[
        int,
        Field(
            description="Maximum segment metadata objects in this page.",
            ge=1,
            le=500,
        ),
    ] = 100,
    offset: Annotated[
        int,
        Field(description="Zero-based segment offset for pagination.", ge=0, le=10000),
    ] = 0,
) -> SegmentMetadataPage:
    """Get a deterministic page of segment rows, sizes, and time boundaries.

    Pinot can return thousands of segment objects. Results are sorted by exact
    segment name, then sliced with ``limit``/``offset``; follow ``has_more`` until
    false. Use ``list_segments`` when only names are needed.

    Failure recovery:
        An empty page is success. For not-found errors, use ``list_tables``;
        correct permissions before retrying, and retry transient server failures
        only after ``test_connection`` succeeds.
    """
    _validate_base_name(table_name, "table")
    raw = _call(
        "list_segment_metadata",
        _HINT_READ,
        pinot_client.get_segment_metadata_detail,
        tableName=table_name,
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
    auth=_WRITE_AUTH,
    annotations=ToolAnnotations(
        title="Create schema",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    ),
)
def create_schema(
    schema: Annotated[
        SchemaInput,
        Field(description="Structured Pinot schema. schemaName is required."),
    ],
    dry_run: Annotated[
        bool,
        Field(description="Preview safely (default); false requests application."),
    ] = True,
    confirmation_token: Annotated[
        str | None,
        Field(
            description="Token returned by a preview of this exact payload.",
            max_length=4096,
        ),
    ] = None,
) -> OperationResult:
    """Preview or create a new Pinot schema without replacing an existing schema.

    The default preview performs strict local structural validation and returns a
    short-lived confirmation token bound to the exact normalized schema. Pass that
    token with ``dry_run=false`` to apply. Replacement belongs in ``update_schema``.

    Failure recovery:
        Invalid JSON, missing ``schemaName``, and controller validation failures are
        non-retryable until corrected. Permission failures require access changes;
        retry transient controller failures only after connectivity is restored.
    """
    name, payload = _schema_payload(schema)
    options = {"override": False, "force": False}
    if dry_run:
        return OperationResult(
            operation="create_schema",
            resource_type="schema",
            resource_name=name,
            status="preview",
            applied=False,
            dry_run=True,
            message=(
                "Schema structure validated locally. No controller mutation was sent."
            ),
            warnings=["Apply will fail safely if this schema already exists."],
            verification_tool="get_schema",
            confirmation_token=_issue_confirmation_token(
                "create_schema", name, payload, options
            ),
        )
    _consume_confirmation_token(
        confirmation_token, "create_schema", name, payload, options
    )
    results = _call(
        "create_schema",
        _HINT_WRITE,
        pinot_client.create_schema,
        payload,
        False,
        False,
    )
    return OperationResult(
        operation="create_schema",
        resource_type="schema",
        resource_name=name,
        status="success",
        applied=True,
        dry_run=False,
        message=f"Schema '{name}' was created.",
        verification_tool="get_schema",
        response_summary=_controller_summary(results),
    )


@mcp.tool(
    auth=_WRITE_AUTH,
    annotations=ToolAnnotations(
        title="Update schema",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=False,
        openWorldHint=True,
    ),
)
def update_schema(
    schema_name: Annotated[
        str,
        Field(
            description=_SCHEMA_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
            pattern=_NAME_PATTERN,
        ),
    ],
    schema: Annotated[
        SchemaInput,
        Field(description="Complete replacement schema; schemaName must match."),
    ],
    reload: Annotated[
        bool,
        Field(description="Reload affected segments after updating."),
    ] = False,
    dry_run: Annotated[
        bool,
        Field(description="Preview safely (default); false requests application."),
    ] = True,
    confirmation_token: Annotated[
        str | None,
        Field(
            description="Token returned by a preview of this exact payload.",
            max_length=4096,
        ),
    ] = None,
) -> OperationResult:
    """Update an existing Pinot schema.

    Accepts a typed schema object. This can change column definitions on a live
    table; the default ``dry_run=true`` previews without applying and returns a
    confirmation token bound to the exact replacement.

    Failure recovery:
        Invalid JSON/name or schema validation failures require a corrected payload;
        do not retry unchanged. Fix permission errors first, and retry transient
        controller failures only after connectivity is restored.
    """
    _validate_base_name(schema_name, "schema")
    name, payload = _schema_payload(schema, expected_name=schema_name)
    options = {"reload": reload, "force": False}
    if dry_run:
        current = _call(
            "update_schema preview",
            _HINT_READ,
            pinot_client.get_schema,
            schemaName=name,
        )
        proposed = json.loads(payload)
        changed = sorted(
            key
            for key in set(current) | set(proposed)
            if current.get(key) != proposed.get(key)
        )
        return OperationResult(
            operation="update_schema",
            resource_type="schema",
            resource_name=name,
            status="preview",
            applied=False,
            dry_run=True,
            message="Schema validated locally and compared with current state.",
            warnings=[
                "Changed top-level fields: " + (", ".join(changed) or "none"),
                *(["Applying will reload affected segments."] if reload else []),
            ],
            verification_tool="get_schema",
            confirmation_token=_issue_confirmation_token(
                "update_schema", name, payload, options
            ),
        )
    _consume_confirmation_token(
        confirmation_token, "update_schema", name, payload, options
    )
    results = _call(
        "update_schema",
        _HINT_WRITE,
        pinot_client.update_schema,
        name,
        payload,
        reload,
        False,
    )
    return OperationResult(
        operation="update_schema",
        resource_type="schema",
        resource_name=name,
        status="success",
        applied=True,
        dry_run=False,
        message=f"Schema '{name}' was updated.",
        verification_tool="get_schema",
        response_summary=_controller_summary(results),
    )


@mcp.tool(
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="Get schema",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def get_schema(
    schema_name: Annotated[
        str,
        Field(
            description=_SCHEMA_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
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
    _validate_base_name(schema_name, "schema")
    raw = _call(
        "get_schema", _HINT_READ, pinot_client.get_schema, schemaName=schema_name
    )
    return PinotSchema.model_validate(raw)


@mcp.tool(
    auth=_WRITE_AUTH,
    annotations=ToolAnnotations(
        title="Create table config",
        readOnlyHint=False,
        destructiveHint=False,
        idempotentHint=False,
        openWorldHint=True,
    ),
)
def create_table_config(
    table_config: Annotated[
        TableConfigInput,
        Field(description="Complete structured Pinot table configuration."),
    ],
    dry_run: Annotated[
        bool,
        Field(description="Preview safely (default); false requests application."),
    ] = True,
    confirmation_token: Annotated[
        str | None,
        Field(
            description="Token returned by a preview of this exact payload.",
            max_length=4096,
        ),
    ] = None,
) -> OperationResult:
    """Create a new Pinot table configuration.

    Accepts a typed table-config object. The default ``dry_run=true`` asks Pinot
    to validate the payload and returns a confirmation token without applying.

    Failure recovery:
        Invalid JSON, missing ``tableName``, and controller validation failures need
        a corrected payload; do not retry unchanged. Fix access failures first, and
        retry transient controller errors after connectivity is restored.
    """
    name, payload = _table_payload(table_config)
    options: dict[str, Any] = {"validation_types_to_skip": []}
    validation = _call(
        "validate table config",
        _HINT_WRITE,
        pinot_client.validate_table_config,
        payload,
        [],
    )
    if dry_run:
        return OperationResult(
            operation="create_table",
            resource_type="table",
            resource_name=name,
            status="preview",
            applied=False,
            dry_run=True,
            message="Pinot validated the table config. No mutation was sent.",
            warnings=(
                ["Pinot reported unrecognized properties."]
                if validation.get("unrecognizedProperties")
                else []
            ),
            verification_tool="get_table_config",
            confirmation_token=_issue_confirmation_token(
                "create_table", name, payload, options
            ),
        )
    _consume_confirmation_token(
        confirmation_token, "create_table", name, payload, options
    )
    results = _call(
        "create_table_config",
        _HINT_WRITE,
        pinot_client.create_table_config,
        payload,
        None,
    )
    return OperationResult(
        operation="create_table",
        resource_type="table",
        resource_name=name,
        status="success",
        applied=True,
        dry_run=False,
        message=f"Table config '{name}' was created.",
        verification_tool="get_table_config",
        response_summary=_controller_summary(results),
    )


@mcp.tool(
    auth=_WRITE_AUTH,
    annotations=ToolAnnotations(
        title="Update table config",
        readOnlyHint=False,
        destructiveHint=True,
        idempotentHint=False,
        openWorldHint=True,
    ),
)
def update_table_config(
    table_name: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
            pattern=_NAME_PATTERN,
        ),
    ],
    table_config: Annotated[
        TableConfigInput,
        Field(description="Complete replacement table configuration."),
    ],
    dry_run: Annotated[
        bool,
        Field(description="Preview safely (default); false requests application."),
    ] = True,
    confirmation_token: Annotated[
        str | None,
        Field(
            description="Token returned by a preview of this exact payload.",
            max_length=4096,
        ),
    ] = None,
) -> OperationResult:
    """Update an existing Pinot table configuration.

    Accepts a typed replacement table-config object. This changes a live table;
    the default ``dry_run=true`` asks Pinot to validate it, compares it with the
    current configuration, and returns a token without applying.

    Failure recovery:
        Invalid JSON/name or controller validation failures require a corrected
        payload; do not retry unchanged. Fix access errors first, and retry transient
        controller failures only after connectivity is restored.
    """
    _validate_base_name(table_name, "table")
    name, payload = _table_payload(table_config, expected_name=table_name)
    options: dict[str, Any] = {"validation_types_to_skip": []}
    validation = _call(
        "validate table config",
        _HINT_WRITE,
        pinot_client.validate_table_config,
        payload,
        [],
    )
    if dry_run:
        current = _call(
            "update table config preview",
            _HINT_READ,
            pinot_client.get_table_config,
            tableName=name,
            tableType=table_config.table_type,
        )
        proposed = json.loads(payload)
        changed = sorted(
            key
            for key in set(current) | set(proposed)
            if current.get(key) != proposed.get(key)
        )
        return OperationResult(
            operation="update_table",
            resource_type="table",
            resource_name=name,
            status="preview",
            applied=False,
            dry_run=True,
            message=(
                "Pinot validated the config and it was compared with current state."
            ),
            warnings=[
                "Changed top-level fields: " + (", ".join(changed) or "none"),
                *(
                    ["Pinot reported unrecognized properties."]
                    if validation.get("unrecognizedProperties")
                    else []
                ),
            ],
            verification_tool="get_table_config",
            confirmation_token=_issue_confirmation_token(
                "update_table", name, payload, options
            ),
        )
    _consume_confirmation_token(
        confirmation_token, "update_table", name, payload, options
    )
    results = _call(
        "update_table_config",
        _HINT_WRITE,
        pinot_client.update_table_config,
        name,
        payload,
        None,
    )
    return OperationResult(
        operation="update_table",
        resource_type="table",
        resource_name=name,
        status="success",
        applied=True,
        dry_run=False,
        message=f"Table config '{name}' was updated.",
        verification_tool="get_table_config",
        response_summary=_controller_summary(results),
    )


@mcp.tool(
    auth=_READ_AUTH,
    annotations=ToolAnnotations(
        title="Get table config",
        readOnlyHint=True,
        idempotentHint=True,
        openWorldHint=True,
    ),
)
def get_table_config(
    table_name: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
            pattern=_NAME_PATTERN,
        ),
    ],
    table_type: Annotated[
        Literal["OFFLINE", "REALTIME"] | None,
        Field(
            description="Restrict to one table type; omit to return both when present."
        ),
    ] = None,
) -> TableConfigResult:
    """Get one table's indexing, retention, tenant, and ingestion configuration.

    This is a single-object lookup, not a list, so pagination does not apply. Set
    ``table_type`` only when one side of a hybrid table is needed.

    Failure recovery:
        For not-found errors, use an exact name from ``list_tables`` and a valid
        table type. Fix permissions before retrying; retry transient controller
        failures after ``test_connection`` succeeds.
    """
    _validate_base_name(table_name, "table")
    raw = _call(
        "get_table_config",
        _HINT_READ,
        pinot_client.get_table_config,
        tableName=table_name,
        tableType=table_type,
    )
    redacted = _redact_sensitive(raw)
    if table_type == "OFFLINE":
        return TableConfigResult(
            table_name=table_name, offline=TableConfig.model_validate(redacted)
        )
    if table_type == "REALTIME":
        return TableConfigResult(
            table_name=table_name, realtime=TableConfig.model_validate(redacted)
        )
    return TableConfigResult(
        table_name=table_name,
        offline=(
            TableConfig.model_validate(redacted["OFFLINE"])
            if isinstance(redacted, dict) and redacted.get("OFFLINE")
            else None
        ),
        realtime=(
            TableConfig.model_validate(redacted["REALTIME"])
            if isinstance(redacted, dict) and redacted.get("REALTIME")
            else None
        ),
    )


@mcp.prompt(
    name="pinot_query",
    title="Plan a safe Pinot query",
    description="Build and run a bounded read-only Pinot SQL workflow.",
    auth=_READ_AUTH,
)
def pinot_query() -> str:
    """Query Pinot through the MCP server with any compatible client."""
    return PROMPT_TEMPLATE.strip()


@mcp.prompt(
    name="explore_table",
    title="Explore a Pinot table",
    description="Inspect schema, configuration, size, segments, and sample rows.",
    auth=_READ_AUTH,
)
def explore_table(
    table_name: Annotated[
        str,
        Field(
            description=_TABLE_NAME_DESCRIPTION,
            min_length=1,
            max_length=128,
            pattern=_NAME_PATTERN,
        ),
    ],
) -> str:
    """Guide a structured exploration of a single Pinot table.

    Args:
        table_name: The Pinot table to explore (case-sensitive).
    """
    return (
        f"Help me explore the Pinot table '{table_name}'.\n"
        f"1. Call get_schema and get_table_config for '{table_name}' to learn its "
        f"columns, types, and time column.\n"
        f"2. Call get_table_size and list_segments for size and segment layout.\n"
        f"3. Run read_query with a small LIMIT (e.g. 10) to sample rows from "
        f"'{table_name}'.\n"
        f"4. Summarize the table's purpose, key dimensions/metrics, and time range."
    )


@mcp.resource(
    "pinot://tables",
    name="pinot_tables",
    title="Visible Pinot tables",
    description="A bounded, sorted catalog of tables visible to this server.",
    mime_type="application/json",
    auth=_READ_AUTH,
)
def tables_resource() -> str:
    """The Pinot tables visible to this server (honors table filters)."""
    tables = sorted(_call("tables_resource", _HINT_READ, pinot_client.get_tables))
    bounded = tables[:500]
    return json.dumps(
        {
            "tables": bounded,
            "returned_tables": len(bounded),
            "total_tables": len(tables),
            "truncated": len(tables) > len(bounded),
            "next_action": "Use list_tables with offset=500 when truncated.",
        }
    )


@mcp.resource(
    "pinot://schema/{schema_name}",
    name="pinot_schema",
    title="Pinot schema",
    description="A validated Pinot schema resource by exact schema name.",
    mime_type="application/json",
    auth=_READ_AUTH,
)
def schema_resource(schema_name: str) -> str:
    """The Pinot schema definition for a given schema name."""
    validate_pinot_path_component(schema_name, "schema resource name")
    raw = _call(
        "schema_resource", _HINT_READ, pinot_client.get_schema, schemaName=schema_name
    )
    return json.dumps(raw)


@mcp.resource(
    "pinot://table-config/{table_name}",
    name="pinot_table_config",
    title="Redacted Pinot table configuration",
    description="A secret-redacted table configuration by exact table name.",
    mime_type="application/json",
    auth=_READ_AUTH,
)
def table_config_resource(table_name: str) -> str:
    """The Pinot table configuration for a given table name."""
    validate_pinot_path_component(table_name, "table-config resource name")
    raw = _call(
        "table_config_resource",
        _HINT_READ,
        pinot_client.get_table_config,
        tableName=table_name,
    )
    return json.dumps(_redact_sensitive(raw))


def _create_http_app() -> ASGIApp:
    """Build the same stateless, guarded ASGI app for HTTP and HTTPS."""
    if not server_config.allowed_hosts:
        raise SystemExit(
            "Refusing to start HTTP transport without an exact Host allowlist. "
            "Set MCP_ALLOWED_HOSTS to comma-separated host[:port] authorities."
        )
    if server_config.transport not in {"http", "streamable-http", "stdio"}:
        raise SystemExit("MCP_TRANSPORT must be 'stdio', 'http', or 'streamable-http'.")

    http_transport: Literal["http", "streamable-http"] = (
        "streamable-http" if server_config.transport == "streamable-http" else "http"
    )
    try:
        app = mcp.http_app(
            path=server_config.path,
            stateless_http=True,
            transport=http_transport,
        )
        return MCPHostOriginMiddleware(
            app,
            mcp_path=server_config.path,
            allowed_hosts=server_config.allowed_hosts,
            allowed_origins=server_config.allowed_origins,
        )
    except ValueError as exc:
        raise SystemExit(f"Invalid MCP HTTP allowlist: {exc}") from exc


def main():
    """Main entry point for FastMCP Pinot Server"""
    setup_logging()
    key_configured = bool(server_config.ssl_keyfile)
    cert_configured = bool(server_config.ssl_certfile)
    if key_configured != cert_configured:
        raise SystemExit(
            "Refusing to start with partial TLS configuration: set both "
            "MCP_SSL_KEYFILE and MCP_SSL_CERTFILE, or neither."
        )

    tls_enabled = key_configured and cert_configured
    if server_config.transport == "stdio":
        if tls_enabled:
            raise SystemExit(
                "MCP_SSL_KEYFILE and MCP_SSL_CERTFILE are only valid with an "
                "HTTP transport; STDIO never opens a listener."
            )
        mcp.run(transport="stdio")
        return

    _enforce_http_auth_safety(http_enabled=True)
    app = _create_http_app()
    if tls_enabled:
        uvicorn.run(
            app,
            host=server_config.host,
            port=server_config.port,
            ssl_keyfile=server_config.ssl_keyfile,
            ssl_certfile=server_config.ssl_certfile,
        )
    else:
        uvicorn.run(
            app,
            host=server_config.host,
            port=server_config.port,
        )


if __name__ == "__main__":
    main()
