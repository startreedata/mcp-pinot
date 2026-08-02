import os
from typing import ClassVar
from unittest.mock import patch

import pytest

from mcp_pinot.auth import (
    available_providers,
    build_auth,
    register_auth_provider,
)
from mcp_pinot.config import ServerConfig, load_server_config

_OAUTH_ENV = {
    "OAUTH_ISSUER": "https://issuer.example.com",
    "OAUTH_JWKS_URI": "https://issuer.example.com/.well-known/jwks.json",
    "OAUTH_AUTHORIZATION_ENDPOINT": "https://issuer.example.com/authorize",
    "OAUTH_TOKEN_ENDPOINT": "https://issuer.example.com/token",
    "OAUTH_CLIENT_ID": "id",
    "OAUTH_CLIENT_SECRET": "secret",
    "OAUTH_BASE_URL": "http://localhost:8080",
    "OAUTH_AUDIENCE": "http://localhost:8080/mcp",
}


def _cfg(**kwargs) -> ServerConfig:
    return ServerConfig(**kwargs)


class TestBuildAuth:
    """Test the pluggable auth provider dispatch."""

    def test_returns_none_when_unset(self):
        assert build_auth(_cfg(auth_provider=None)) is None

    def test_returns_none_for_none_provider(self):
        assert build_auth(_cfg(auth_provider="none")) is None

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown auth provider"):
            build_auth(_cfg(auth_provider="does-not-exist"))

    def test_oauth_provider_builds_oauthproxy(self):
        from fastmcp.server.auth import OAuthProxy

        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, _OAUTH_ENV, clear=True):
                auth = build_auth(_cfg(auth_provider="oauth"))
        assert isinstance(auth, OAuthProxy)

    def test_custom_provider_registration(self):
        """A downstream provider (e.g. the StarTree fork) can register itself."""
        sentinel = object()
        register_auth_provider("startree-test", lambda cfg: sentinel)
        assert build_auth(_cfg(auth_provider="startree-test")) is sentinel
        assert "startree-test" in available_providers()

    def test_builtin_providers_available(self):
        providers = available_providers()
        assert "oauth" in providers
        assert "none" in providers
        assert "static" in providers


class TestStaticAuth:
    """Test the static shared-secret (service-to-service) auth provider."""

    def test_builds_static_token_verifier(self):
        from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, {"MCP_STATIC_TOKEN": "s3cret"}, clear=True):
                auth = build_auth(_cfg(auth_provider="static"))
        assert isinstance(auth, StaticTokenVerifier)

    def test_missing_token_raises(self):
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, {}, clear=True):
                with pytest.raises(ValueError, match="MCP_STATIC_TOKEN"):
                    build_auth(_cfg(auth_provider="static"))

    def test_blank_token_raises(self):
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, {"MCP_STATIC_TOKEN": "   "}, clear=True):
                with pytest.raises(ValueError, match="MCP_STATIC_TOKEN"):
                    build_auth(_cfg(auth_provider="static"))

    @pytest.mark.asyncio
    async def test_static_token_gates_the_http_endpoint(self):
        """End-to-end: only the configured bearer token reaches the MCP endpoint."""
        from fastmcp import FastMCP
        import httpx

        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, {"MCP_STATIC_TOKEN": "s3cret"}, clear=True):
                auth = build_auth(_cfg(auth_provider="static"))

        app = FastMCP("test", auth=auth).http_app(path="/mcp")
        initialize = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"},
            },
        }
        headers = {"Accept": "application/json, text/event-stream"}

        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://127.0.0.1:8080"
            ) as client:

                async def post(**extra):
                    response = await client.post(
                        "/mcp", json=initialize, headers={**headers, **extra}
                    )
                    return response.status_code

                assert await post() == 401
                assert await post(Authorization="Bearer wrong") == 401
                assert await post(Authorization="Bearer s3cret") == 200

    @pytest.mark.asyncio
    async def test_static_read_scope_is_enforced_at_runtime(self):
        """A real auth provider permits read tools and denies write tools."""
        from fastmcp import FastMCP
        from fastmcp.server.auth import require_scopes
        import httpx

        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(
                os.environ,
                {
                    "MCP_STATIC_TOKEN": "read-only-token",
                    "MCP_STATIC_SCOPES": "pinot:read",
                },
                clear=True,
            ):
                auth = build_auth(_cfg(auth_provider="static"))

        server = FastMCP("scope-test", auth=auth)

        @server.tool(auth=require_scopes("pinot:read"))
        def read_tool() -> str:
            return "read-ok"

        @server.tool(auth=require_scopes("pinot:write"))
        def write_tool() -> str:
            return "write-should-not-run"

        app = server.http_app(path="/mcp", stateless_http=True, json_response=True)
        headers = {
            "Accept": "application/json, text/event-stream",
            "Authorization": "Bearer read-only-token",
            "MCP-Protocol-Version": "2025-06-18",
        }
        initialize = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1"},
            },
        }

        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://127.0.0.1:8080"
            ) as client:
                assert (
                    await client.post("/mcp", json=initialize, headers=headers)
                ).status_code == 200

                async def call_tool(name: str) -> httpx.Response:
                    return await client.post(
                        "/mcp",
                        json={
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "tools/call",
                            "params": {"name": name, "arguments": {}},
                        },
                        headers=headers,
                    )

                read_response = await call_tool("read_tool")
                write_response = await call_tool("write_tool")

        assert read_response.status_code == 200
        assert "read-ok" in read_response.text
        assert "write-should-not-run" not in write_response.text
        # FastMCP hides components the principal lacks scope to invoke.
        assert "unknown tool" in write_response.text.lower()


class TestAuthProviderResolution:
    """Test how the active provider name is resolved from the environment."""

    def test_defaults_to_none(self):
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, {}, clear=True):
                assert load_server_config().auth_provider is None

    def test_oauth_enabled_backward_compat(self):
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, {"OAUTH_ENABLED": "true"}, clear=True):
                assert load_server_config().auth_provider == "oauth"

    def test_explicit_auth_provider_overrides_legacy_flag(self):
        env = {"AUTH_PROVIDER": "StarTree", "OAUTH_ENABLED": "true"}
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env, clear=True):
                # Normalized to lower-case.
                assert load_server_config().auth_provider == "startree"


class TestOAuthAudience:
    """OAUTH_AUDIENCE is optional and defaults to the canonical resource URI."""

    @staticmethod
    def _verifier(env):
        from mcp_pinot.auth.oauth import build_token_verifier
        from mcp_pinot.config import load_oauth_config

        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env, clear=True):
                return build_token_verifier(load_oauth_config(), "/mcp")

    def test_defaults_to_canonical_resource_uri(self):
        env = {k: v for k, v in _OAUTH_ENV.items() if k != "OAUTH_AUDIENCE"}
        assert self._verifier(env).audience == "http://localhost:8080/mcp"

    def test_missing_audience_no_longer_blocks_startup(self):
        """It used to raise, which made every Dex-backed deployment unstartable."""
        env = {k: v for k, v in _OAUTH_ENV.items() if k != "OAUTH_AUDIENCE"}
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env, clear=True):
                build_auth(_cfg(auth_provider="oauth"))

    def test_explicit_non_canonical_audience_is_honoured(self):
        """Dex and friends set `aud` to the client ID; that must not be fatal."""
        env = {**_OAUTH_ENV, "OAUTH_AUDIENCE": "startree-mcp-server"}
        assert self._verifier(env).audience == "startree-mcp-server"

    def test_non_canonical_audience_warns(self, caplog):
        env = {**_OAUTH_ENV, "OAUTH_AUDIENCE": "startree-mcp-server"}
        with caplog.at_level("WARNING", logger="mcp_pinot.auth.oauth"):
            with patch("mcp_pinot.config.load_dotenv"):
                with patch.dict(os.environ, env, clear=True):
                    build_auth(_cfg(auth_provider="oauth"))
        assert "canonical MCP resource URI" in caplog.text


class TestOAuthGrantedScopes:
    """Pinot scopes granted to principals an OIDC provider authenticates."""

    @staticmethod
    def _token(scopes):
        from mcp.server.auth.provider import AccessToken

        return AccessToken(token="t", client_id="c", scopes=list(scopes))

    async def _verify(self, granted, token_scopes):
        from mcp_pinot.auth.oauth import PinotScopeGrantingVerifier

        verifier = PinotScopeGrantingVerifier(
            jwks_uri=_OAUTH_ENV["OAUTH_JWKS_URI"],
            issuer=_OAUTH_ENV["OAUTH_ISSUER"],
            audience="aud",
            granted_scopes=granted,
        )
        upstream = self._token(token_scopes)
        with patch(
            "fastmcp.server.auth.providers.jwt.JWTVerifier.verify_token",
            return_value=upstream,
        ):
            return await verifier.verify_token("raw-token"), upstream

    @pytest.mark.asyncio
    async def test_grants_are_unioned_onto_token_scopes(self):
        """Dex issues `openid`; the Pinot scopes have to come from configuration."""
        result, _ = await self._verify(
            ["pinot:read", "pinot:write", "pinot:admin"], ["openid"]
        )
        assert result.scopes == ["openid", "pinot:read", "pinot:write", "pinot:admin"]

    @pytest.mark.asyncio
    async def test_read_only_grant_withholds_write_and_admin(self):
        result, _ = await self._verify(["pinot:read"], ["openid"])
        assert result.scopes == ["openid", "pinot:read"]

    @pytest.mark.asyncio
    async def test_token_scopes_are_never_revoked(self):
        """A provider that can mint Pinot scopes keeps them under a narrow grant."""
        result, _ = await self._verify(["pinot:read"], ["pinot:read", "pinot:write"])
        assert set(result.scopes) == {"pinot:read", "pinot:write"}

    @pytest.mark.asyncio
    async def test_no_grant_returns_token_untouched(self):
        result, upstream = await self._verify([], ["openid"])
        assert result is upstream

    @pytest.mark.asyncio
    async def test_invalid_token_stays_rejected(self):
        from mcp_pinot.auth.oauth import PinotScopeGrantingVerifier

        verifier = PinotScopeGrantingVerifier(
            jwks_uri=_OAUTH_ENV["OAUTH_JWKS_URI"],
            issuer=_OAUTH_ENV["OAUTH_ISSUER"],
            audience="aud",
            granted_scopes=["pinot:read"],
        )
        with patch(
            "fastmcp.server.auth.providers.jwt.JWTVerifier.verify_token",
            return_value=None,
        ):
            assert await verifier.verify_token("bad") is None

    @pytest.mark.asyncio
    async def test_default_grant_is_the_full_pinot_scope_set(self):
        from mcp_pinot.auth.oauth import build_token_verifier
        from mcp_pinot.config import load_oauth_config

        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, _OAUTH_ENV, clear=True):
                verifier = build_token_verifier(load_oauth_config(), "/mcp")
        with patch(
            "fastmcp.server.auth.providers.jwt.JWTVerifier.verify_token",
            return_value=self._token(["openid"]),
        ):
            token = await verifier.verify_token("raw")
        assert token.scopes == ["openid", "pinot:read", "pinot:write", "pinot:admin"]

    def test_unsupported_grant_scope_raises(self):
        env = {**_OAUTH_ENV, "OAUTH_GRANTED_SCOPES": "pinot:read superuser"}
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env, clear=True):
                with pytest.raises(ValueError, match="OAUTH_GRANTED_SCOPES"):
                    build_auth(_cfg(auth_provider="oauth"))


class TestOAuthPlusStatic:
    """One deployment serving interactive users and one trusted backend."""

    _ENV: ClassVar[dict[str, str]] = {
        **_OAUTH_ENV,
        "MCP_STATIC_TOKEN": "backend-secret",
    }

    def _build(self, **extra):
        provider = extra.pop("_provider", "oauth+static")
        env = {**self._ENV, **extra}
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env, clear=True):
                return build_auth(_cfg(auth_provider=provider))

    def test_builds_an_oauth_proxy_so_the_login_routes_exist(self):
        from fastmcp.server.auth import OAuthProxy

        assert isinstance(self._build(), OAuthProxy)

    def test_provider_name_order_is_irrelevant(self):
        """The name describes a set of accepted credentials, not an order."""
        from mcp_pinot.auth.multi import OAuthStaticProxy

        for name in ("oauth+static", "static+oauth", " oauth + static "):
            auth = self._build(_provider=name)
            assert isinstance(auth, OAuthStaticProxy)

    def test_composite_provider_is_discoverable(self):
        assert "oauth+static" in available_providers()

    def test_missing_static_token_fails_startup(self):
        """Asking for both and getting one silently would be worse than failing."""
        env = {k: v for k, v in self._ENV.items() if k != "MCP_STATIC_TOKEN"}
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env, clear=True):
                with pytest.raises(ValueError, match="MCP_STATIC_TOKEN"):
                    build_auth(_cfg(auth_provider="oauth+static"))

    def test_incomplete_oauth_fails_startup(self):
        env = {k: v for k, v in self._ENV.items() if k != "OAUTH_JWKS_URI"}
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env, clear=True):
                with pytest.raises(ValueError, match="OAUTH_JWKS_URI"):
                    build_auth(_cfg(auth_provider="oauth+static"))

    @pytest.mark.asyncio
    async def test_the_shared_token_authenticates_with_its_own_scopes(self):
        auth = self._build(MCP_STATIC_SCOPES="pinot:read")
        token = await auth.load_access_token("backend-secret")
        assert token is not None
        assert token.scopes == ["pinot:read"]
        assert token.client_id == "mcp-static-client"

    @pytest.mark.asyncio
    async def test_oauth_routes_and_static_token_work_on_the_same_http_app(self):
        """Regression: OAuthProxy must not reject the raw shared bearer token."""
        from fastmcp import FastMCP
        import httpx

        auth = self._build(MCP_STATIC_SCOPES="pinot:read")
        app = FastMCP("hybrid-test", auth=auth).http_app(path="/mcp")
        initialize = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "hybrid-test", "version": "1"},
            },
        }
        headers = {
            "Accept": "application/json, text/event-stream",
            "Authorization": "Bearer backend-secret",
        }

        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://localhost:8080"
            ) as client:
                discovery = await client.get(
                    "/.well-known/oauth-protected-resource/mcp"
                )
                initialize_response = await client.post(
                    "/mcp", json=initialize, headers=headers
                )

        assert discovery.status_code == 200
        assert initialize_response.status_code == 200

    @pytest.mark.asyncio
    async def test_an_oidc_token_authenticates_with_the_granted_scopes(self):
        """Reached only after the shared-token check misses."""
        from mcp.server.auth.provider import AccessToken as UpstreamToken

        auth = self._build(
            MCP_STATIC_SCOPES="pinot:read",
            OAUTH_GRANTED_SCOPES="pinot:read pinot:write",
        )
        upstream = UpstreamToken(token="jwt", client_id="alice", scopes=["openid"])
        with patch(
            "fastmcp.server.auth.providers.jwt.JWTVerifier.verify_token",
            return_value=upstream,
        ):
            token = await auth._token_validator.verify_token("a-dex-jwt")
        assert token.client_id == "alice"
        assert token.scopes == ["openid", "pinot:read", "pinot:write"]

    @pytest.mark.asyncio
    async def test_an_unrecognised_token_is_rejected(self):
        auth = self._build()
        with patch(
            "fastmcp.server.auth.providers.jwt.JWTVerifier.verify_token",
            return_value=None,
        ):
            assert await auth.load_access_token("neither") is None

    @pytest.mark.asyncio
    async def test_the_shared_token_short_circuits_the_oidc_check(self):
        """The backend calls on every request; it must not pay for a JWKS fetch."""
        from unittest.mock import AsyncMock

        from fastmcp.server.auth import OAuthProxy

        auth = self._build()
        with patch.object(
            OAuthProxy, "load_access_token", new_callable=AsyncMock
        ) as oauth_load:
            token = await auth.load_access_token("backend-secret")
        assert token is not None
        oauth_load.assert_not_awaited()
