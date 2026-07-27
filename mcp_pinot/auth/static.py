"""Static bearer-token auth provider for the Pinot MCP server.

Service-to-service authentication via a single shared secret presented as a
Bearer token. A trusted backend caller (e.g. an agent service) holds the token
and sends ``Authorization: Bearer <token>`` on every request.

Use this when the caller is a backend service, not an interactive user: a full
OIDC/browser flow (the ``oauth`` provider) is unnecessary, and the caller can be
issued a long-lived secret out of band. The MCP server still queries Pinot with
its own credentials, so this gates *access to the MCP server*; it does not scope
Pinot data per end user.

Registered as the ``static`` auth provider (see :mod:`mcp_pinot.auth`). Enable
with ``AUTH_PROVIDER=static`` and set ``MCP_STATIC_TOKEN`` to the shared secret.
"""

from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

from mcp_pinot.config import ServerConfig, load_static_scopes, load_static_token

# Identity attributed to any request bearing the shared token. The MCP server
# has no per-user notion under this provider — every authenticated call is this
# single service principal.
_STATIC_CLIENT_ID = "mcp-static-client"


def build_static_auth(server_config: ServerConfig) -> StaticTokenVerifier:
    """Build a StaticTokenVerifier from the configured shared secret.

    Raises ``ValueError`` when ``MCP_STATIC_TOKEN`` is unset so a misconfigured
    deployment fails loudly at startup rather than booting unauthenticated.
    """
    token = load_static_token()
    scopes = load_static_scopes()
    return StaticTokenVerifier(
        # Static authentication is intended for a trusted service principal. Give
        # that principal explicit scopes so the same component-level authorization
        # checks used for OAuth remain active instead of being silently bypassed.
        tokens={token: {"client_id": _STATIC_CLIENT_ID, "scopes": scopes}},
    )
