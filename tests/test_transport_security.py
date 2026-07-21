import os
from unittest.mock import patch

import httpx
import pytest

from mcp_pinot.config import load_server_config
from mcp_pinot.server import MCPHostOriginMiddleware, _create_http_app, main, mcp

_INITIALIZE_REQUEST = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-11-25",
        "capabilities": {},
        "clientInfo": {"name": "transport-security-test", "version": "1"},
    },
}
_MCP_HEADERS = {"accept": "application/json, text/event-stream"}


def _guarded_app():
    raw_app = mcp.http_app(path="/mcp", stateless_http=True, json_response=True)
    guarded_app = MCPHostOriginMiddleware(
        raw_app,
        mcp_path="/mcp",
        allowed_hosts=("mcp.example:8443",),
        allowed_origins=("https://client.example",),
    )
    return raw_app, guarded_app


def test_local_server_config_has_safe_exact_host_defaults():
    with (
        patch("mcp_pinot.config.load_dotenv"),
        patch.dict(os.environ, {}, clear=True),
    ):
        config = load_server_config()

    assert config.allowed_hosts == ("127.0.0.1", "127.0.0.1:8080")
    assert config.allowed_origins == ()


def test_wildcard_bind_requires_explicit_hosts_and_parses_exact_allowlists():
    env = {
        "MCP_HOST": "0.0.0.0",
        "MCP_ALLOWED_HOSTS": "mcp.example,mcp.example:443",
        "MCP_ALLOWED_ORIGINS": "https://app.example,https://admin.example:8443",
    }
    with (
        patch("mcp_pinot.config.load_dotenv"),
        patch.dict(os.environ, env, clear=True),
    ):
        config = load_server_config()

    assert config.allowed_hosts == ("mcp.example", "mcp.example:443")
    assert config.allowed_origins == (
        "https://app.example",
        "https://admin.example:8443",
    )


@pytest.mark.parametrize("transport_name", ["http", "streamable-http"])
def test_http_app_is_stateless_for_every_http_transport(transport_name):
    raw_app = object()
    with (
        patch("mcp_pinot.server.server_config") as config,
        patch("mcp_pinot.server.mcp.http_app", return_value=raw_app) as http_app,
    ):
        config.transport = transport_name
        config.path = "/mcp"
        config.allowed_hosts = ("mcp.example",)
        config.allowed_origins = ()
        app = _create_http_app()

    assert isinstance(app, MCPHostOriginMiddleware)
    http_app.assert_called_once_with(
        path="/mcp",
        stateless_http=True,
        transport=transport_name,
    )


def test_http_app_refuses_empty_host_allowlist():
    with patch("mcp_pinot.server.server_config") as config:
        config.transport = "http"
        config.path = "/mcp"
        config.allowed_hosts = ()
        config.allowed_origins = ()
        with pytest.raises(SystemExit, match="MCP_ALLOWED_HOSTS"):
            _create_http_app()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("extra_headers", "expected_status"),
    [
        ({}, 200),
        ({"origin": "https://client.example"}, 200),
        ({"origin": "https://evil.example"}, 403),
        ({"host": "evil.example"}, 403),
        ({"host": "mcp.example"}, 403),
    ],
)
async def test_initialize_enforces_exact_host_and_origin_allowlists(
    extra_headers, expected_status
):
    """Raw MCP initialize is accepted only for exact allowed authorities."""
    raw_app, guarded_app = _guarded_app()
    headers = {**_MCP_HEADERS, **extra_headers}
    async with raw_app.lifespan(raw_app):
        transport = httpx.ASGITransport(app=guarded_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://mcp.example:8443"
        ) as client:
            response = await client.post(
                "/mcp", json=_INITIALIZE_REQUEST, headers=headers
            )

    assert response.status_code == expected_status
    if expected_status == 403:
        assert response.text == "Forbidden"


@pytest.mark.asyncio
async def test_duplicate_host_and_origin_headers_are_rejected():
    """Ambiguous security headers fail closed before MCP protocol handling."""
    raw_app, guarded_app = _guarded_app()
    headers = [
        ("accept", "application/json, text/event-stream"),
        ("host", "mcp.example:8443"),
        ("host", "evil.example"),
        ("origin", "https://client.example"),
        ("origin", "https://client.example"),
    ]
    async with raw_app.lifespan(raw_app):
        transport = httpx.ASGITransport(app=guarded_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://mcp.example:8443"
        ) as client:
            response = await client.post(
                "/mcp", json=_INITIALIZE_REQUEST, headers=headers
            )

    assert response.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize(("path", "body"), [("/livez", "ok"), ("/readyz", "ready")])
async def test_health_routes_are_available_outside_mcp_header_guard(path, body):
    raw_app, guarded_app = _guarded_app()
    async with raw_app.lifespan(raw_app):
        transport = httpx.ASGITransport(app=guarded_app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://evil.example"
        ) as client:
            response = await client.get(
                path, headers={"origin": "https://evil.example"}
            )

    assert response.status_code == 200
    assert response.text == body


@pytest.mark.parametrize(
    ("keyfile", "certfile"),
    [("/tls/key.pem", None), (None, "/tls/cert.pem")],
)
def test_main_rejects_partial_tls_configuration(keyfile, certfile):
    with patch("mcp_pinot.server.server_config") as config:
        config.transport = "http"
        config.ssl_keyfile = keyfile
        config.ssl_certfile = certfile
        with (
            patch("mcp_pinot.server.mcp.run") as run,
            patch("mcp_pinot.server.uvicorn.run") as uvicorn_run,
            pytest.raises(SystemExit, match="partial TLS configuration"),
        ):
            main()

    run.assert_not_called()
    uvicorn_run.assert_not_called()
