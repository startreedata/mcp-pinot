import os
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

    def test_configured_token_authenticates(self):
        """The configured secret is accepted; anything else is rejected."""
        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, {"MCP_STATIC_TOKEN": "s3cret"}, clear=True):
                auth = build_auth(_cfg(auth_provider="static"))
        # StaticTokenVerifier stores accepted tokens keyed by the secret string.
        assert "s3cret" in auth.tokens
        assert "wrong" not in auth.tokens


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
