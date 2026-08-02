"""OAuth (OIDC) auth provider for the Pinot MCP server.

This is the general, open-source OAuth path: an :class:`OAuthProxy` in front of an
upstream OIDC provider, validating JWT access tokens via :class:`JWTVerifier`. It
is registered as the ``oauth`` auth provider (see :mod:`mcp_pinot.auth`).
"""

import logging
from typing import Any

from fastmcp.server.auth import OAuthProxy
from fastmcp.server.auth.auth import AccessToken
from fastmcp.server.auth.providers.jwt import JWTVerifier

from mcp_pinot.config import OAuthConfig, ServerConfig, load_oauth_config

logger = logging.getLogger(__name__)


class PinotScopeGrantingVerifier(JWTVerifier):
    """JWT verifier that grants configured Pinot scopes to valid principals.

    Every tool, resource, and prompt is gated on ``pinot:read``/``pinot:write``/
    ``pinot:admin``. Those are resource-specific authorization scopes, and a
    general-purpose OIDC provider issues a fixed catalog it cannot extend — Dex,
    for instance, accepts only ``openid``, ``profile``, ``email``, ``groups``,
    ``offline_access`` and its cross-client audience scope, rejecting anything
    else. Without a grant, a correctly authenticated user's token would carry no
    Pinot scope at all and every tool call would be denied.

    So the deployment decides what an authenticated principal may do:
    ``OAUTH_GRANTED_SCOPES`` (default: all three) is unioned onto whatever the
    token already carries. Set it to ``pinot:read`` for a read-only deployment —
    the mutating tools are then rejected for every OIDC caller. Scopes a token
    genuinely carries are always preserved, so a provider that *can* mint Pinot
    scopes keeps working and a narrower grant never revokes them.
    """

    def __init__(
        self,
        *args: Any,
        granted_scopes: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._granted_scopes = list(granted_scopes or [])

    async def verify_token(self, token: str) -> AccessToken | None:
        access_token = await super().verify_token(token)
        if access_token is None or not self._granted_scopes:
            return access_token
        merged = list(dict.fromkeys([*access_token.scopes, *self._granted_scopes]))
        if merged == list(access_token.scopes):
            return access_token
        return access_token.model_copy(update={"scopes": merged})


def build_token_verifier(
    oauth_config: OAuthConfig, mcp_path: str
) -> PinotScopeGrantingVerifier:
    """Build the access-token verifier for the OAuth provider.

    Split out from :func:`build_oauth_auth` because the audience resolution and the
    scope grant are the parts worth exercising directly — ``OAuthProxy`` keeps its
    verifier private.
    """
    canonical_resource = f"{oauth_config.base_url.rstrip('/')}{mcp_path}"
    audience = oauth_config.audience or canonical_resource
    if audience != canonical_resource:
        # Allowed, but worth saying out loud: RFC 9728 metadata advertises the
        # canonical resource URI while tokens are validated against a different
        # audience. Providers that set `aud` to the client ID (Dex among them)
        # require exactly this, so it is a warning rather than a hard failure.
        logger.warning(
            "OAUTH_AUDIENCE (%r) differs from the canonical MCP resource URI (%r). "
            "Tokens are validated against OAUTH_AUDIENCE, while the resource URI is "
            "what RFC 9728 metadata advertises. Point OAUTH_AUDIENCE at the resource "
            "URI if your provider can issue that audience.",
            audience,
            canonical_resource,
        )

    return PinotScopeGrantingVerifier(
        jwks_uri=oauth_config.jwks_uri,
        issuer=oauth_config.issuer,
        audience=audience,
        # Optional baseline scopes are enforced here; component-level Pinot scopes
        # are independently enforced for every tool/resource/prompt.
        required_scopes=oauth_config.required_scopes,
        # Pinot scopes granted to any principal this provider authenticates.
        granted_scopes=oauth_config.granted_scopes,
    )


def build_oauth_auth(server_config: ServerConfig) -> OAuthProxy:
    """Build the OAuthProxy auth provider from environment configuration."""
    oauth_config = load_oauth_config()
    token_verifier = build_token_verifier(oauth_config, server_config.path)

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
