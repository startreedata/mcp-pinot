"""Accept more than one kind of credential on a single MCP deployment.

A hosted deployment usually has two kinds of caller at once:

* **people**, whose MCP client completes the OIDC flow and arrives with a token
  from the environment's identity provider, and
* **one trusted backend** (an agent service, or a gateway calling as itself),
  which cannot run a browser flow and holds a shared secret instead.

Selecting a single provider forces a choice between them, and there is no clean
workaround: a second deployment means a second hostname and certificate, and many
identity providers cannot issue the client-credentials grant a backend would need.

So ``AUTH_PROVIDER=oauth+static`` builds the OAuth provider — which owns the
discovery, registration and authorization routes people need — and wraps its
client-facing token validation so it recognises the shared secret before trying a
FastMCP-issued OAuth token. Both spellings (``static+oauth``) select the same thing;
the shared secret is always tried first because that check is a constant-time
comparison, while the OAuth path may validate a signed token and fetch signing
keys for its upstream token.

Each credential keeps its own authorization: the shared secret carries
``MCP_STATIC_SCOPES`` and an OIDC user carries ``OAUTH_GRANTED_SCOPES``, so a
backend can be read-only while people retain write access, or the reverse. The
principals stay distinguishable in logs by ``client_id``.
"""

from typing import Any, cast

from fastmcp.server.auth import OAuthProxy
from fastmcp.server.auth.auth import AccessToken, TokenVerifier

from mcp_pinot.auth.oauth import build_token_verifier
from mcp_pinot.auth.static import build_static_auth
from mcp_pinot.config import ServerConfig, get_logger, load_oauth_config

logger = get_logger()


class OAuthStaticProxy(OAuthProxy):
    """OAuth proxy that also accepts a raw static bearer token at the MCP route.

    ``OAuthProxy`` does not pass the bearer token received by the MCP endpoint to
    its configured ``token_verifier``. It first requires a FastMCP-issued JWT,
    swaps that JWT for the stored upstream token, and only then invokes the
    verifier. Consequently, putting the static verifier inside
    ``OAuthProxy(token_verifier=...)`` cannot authenticate a raw shared secret.

    This subclass checks the static credential at the client-facing boundary and
    delegates every other token to the unmodified OAuth proxy flow. The upstream
    OAuth verifier remains dedicated to validating the swapped OIDC token.
    """

    def __init__(
        self,
        *,
        static_token_verifier: TokenVerifier,
        **kwargs: Any,
    ) -> None:
        self._static_token_verifier = static_token_verifier
        super().__init__(**kwargs)

    async def load_access_token(self, token: str) -> AccessToken | None:
        static_access = await self._static_token_verifier.verify_token(token)
        if static_access is not None:
            return static_access
        # OAuthProxy's public annotation comes from the base MCP package, while
        # FastMCP's override contract and configured verifier use its AccessToken
        # subclass. The value returned here originates from that verifier.
        return cast(AccessToken | None, await super().load_access_token(token))


def build_oauth_static_auth(server_config: ServerConfig) -> OAuthProxy:
    """Build the OAuth provider, additionally accepting the static shared token.

    Fails at startup if either half is misconfigured — a deployment that asked for
    both should not silently come up supporting one.
    """
    static_verifier = build_static_auth(server_config)
    oauth_config = load_oauth_config()
    oidc_verifier = build_token_verifier(oauth_config, server_config.path)

    logger.info(
        "Auth accepts two credential types: the static shared token (scopes: %s) "
        "and %s-issued tokens (granted scopes: %s).",
        " ".join(static_verifier.tokens[next(iter(static_verifier.tokens))]["scopes"]),
        oauth_config.issuer,
        " ".join(oauth_config.granted_scopes or []),
    )

    return OAuthStaticProxy(
        upstream_authorization_endpoint=oauth_config.upstream_authorization_endpoint,
        upstream_token_endpoint=oauth_config.upstream_token_endpoint,
        upstream_client_id=oauth_config.client_id,
        upstream_client_secret=oauth_config.client_secret,
        # OAuthProxy invokes this only after swapping its own client-facing JWT for
        # the upstream OIDC token.
        token_verifier=oidc_verifier,
        # The raw static bearer token is checked at the client-facing boundary.
        static_token_verifier=static_verifier,
        extra_authorize_params=oauth_config.extra_authorize_params,
        base_url=oauth_config.base_url,
        valid_scopes=oauth_config.scopes,
    )
