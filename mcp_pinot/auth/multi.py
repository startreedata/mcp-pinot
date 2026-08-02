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
discovery, registration and authorization routes people need — and gives it a
:class:`ChainedTokenVerifier` that recognises the shared secret as well as an OIDC
token. Both spellings (``static+oauth``) select the same thing; the shared secret is
always tried first because that check is a constant-time comparison, while the OIDC
path may fetch signing keys.

Each credential keeps its own authorization: the shared secret carries
``MCP_STATIC_SCOPES`` and an OIDC user carries ``OAUTH_GRANTED_SCOPES``, so a
backend can be read-only while people retain write access, or the reverse. The
principals stay distinguishable in logs by ``client_id``.
"""

from collections.abc import Sequence

from fastmcp.server.auth import OAuthProxy
from fastmcp.server.auth.auth import AccessToken, TokenVerifier

from mcp_pinot.auth.oauth import build_token_verifier
from mcp_pinot.auth.static import build_static_auth
from mcp_pinot.config import ServerConfig, get_logger, load_oauth_config

logger = get_logger()


class ChainedTokenVerifier(TokenVerifier):
    """Verify a bearer token against several verifiers, in order.

    The first verifier that recognises the token decides the request, including
    the scopes the resulting principal holds. A token no verifier recognises is
    rejected exactly as a single verifier would reject it.
    """

    def __init__(self, verifiers: Sequence[TokenVerifier]) -> None:
        if not verifiers:
            raise ValueError("ChainedTokenVerifier requires at least one verifier.")
        # Advertise the union of nothing: per-tool Pinot scopes are enforced on the
        # component, and a baseline shared by unrelated credential types would
        # reject one of them. Individual verifiers keep their own required_scopes.
        super().__init__(required_scopes=None)
        self._verifiers = list(verifiers)

    async def verify_token(self, token: str) -> AccessToken | None:
        for verifier in self._verifiers:
            access_token = await verifier.verify_token(token)
            if access_token is not None:
                return access_token
        return None


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

    return OAuthProxy(
        upstream_authorization_endpoint=oauth_config.upstream_authorization_endpoint,
        upstream_token_endpoint=oauth_config.upstream_token_endpoint,
        upstream_client_id=oauth_config.client_id,
        upstream_client_secret=oauth_config.client_secret,
        # Shared secret first: a constant-time comparison, and it avoids a signing-key
        # fetch for the backend caller that sends it on every request.
        token_verifier=ChainedTokenVerifier([static_verifier, oidc_verifier]),
        extra_authorize_params=oauth_config.extra_authorize_params,
        base_url=oauth_config.base_url,
        valid_scopes=oauth_config.scopes,
    )
