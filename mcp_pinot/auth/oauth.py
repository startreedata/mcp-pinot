"""OAuth (OIDC) auth provider for the Pinot MCP server.

This is the general, open-source OAuth path: an :class:`OAuthProxy` in front of an
upstream OIDC provider, validating JWT access tokens via :class:`JWTVerifier`. It
is registered as the ``oauth`` auth provider (see :mod:`mcp_pinot.auth`).
"""

from fastmcp.server.auth import OAuthProxy
from fastmcp.server.auth.providers.jwt import JWTVerifier

from mcp_pinot.config import ServerConfig, load_oauth_config


def build_oauth_auth(server_config: ServerConfig) -> OAuthProxy:
    """Build the OAuthProxy auth provider from environment configuration."""
    oauth_config = load_oauth_config()

    token_verifier = JWTVerifier(
        jwks_uri=oauth_config.jwks_uri,
        issuer=oauth_config.issuer,
        audience=oauth_config.audience,
        # Enforced on the access token. Default None (do not enforce): OIDC scopes
        # such as profile/email are rarely present on access tokens, so requiring
        # them would 401 valid users. Set OAUTH_REQUIRED_SCOPES to opt in. This is
        # distinct from valid_scopes below (which is only advertised).
        required_scopes=oauth_config.required_scopes,
    )

    return OAuthProxy(
        upstream_authorization_endpoint=oauth_config.upstream_authorization_endpoint,
        upstream_token_endpoint=oauth_config.upstream_token_endpoint,
        upstream_client_id=oauth_config.client_id,
        upstream_client_secret=oauth_config.client_secret,
        token_verifier=token_verifier,
        extra_authorize_params=oauth_config.extra_authorize_params,
        base_url=oauth_config.base_url,
        # Advertised as scopes_supported in the OAuth discovery metadata so
        # clients (e.g. mcp-remote / Claude Desktop) will request them instead
        # of refusing the flow on an empty scopes_supported. See fastmcp#1716.
        valid_scopes=oauth_config.scopes,
    )
