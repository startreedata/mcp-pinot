"""Pluggable authentication for the Pinot MCP server.

The server obtains its auth provider through :func:`build_auth`, which selects a
provider by name (``ServerConfig.auth_provider``) from a registry. Built-in
providers are ``none``, ``oauth`` and ``static``. Additional providers can be
contributed
**without modifying this package** by registering a Python entry point in the
``mcp_pinot.auth_providers`` group — for example a private StarTree token
provider in a downstream fork::

    [project.entry-points."mcp_pinot.auth_providers"]
    startree = "startree_mcp_pinot.auth:build_startree_auth"

A provider builder is any callable taking the :class:`ServerConfig` and returning
a FastMCP auth object (e.g. ``OAuthProxy``, ``RemoteAuthProvider`` or a
``TokenVerifier``) or ``None``. Keeping the seam additive lets a private fork stay
in sync with upstream by merging cleanly.
"""

from collections.abc import Callable
from importlib.metadata import entry_points
from typing import Any

from mcp_pinot.auth.oauth import build_oauth_auth
from mcp_pinot.auth.static import build_static_auth
from mcp_pinot.config import ServerConfig, get_logger

logger = get_logger()

# A FastMCP auth object accepted by ``FastMCP(auth=...)`` (e.g. OAuthProxy,
# RemoteAuthProvider, or a TokenVerifier), or None for no authentication.
AuthProvider = Any
AuthBuilder = Callable[[ServerConfig], "AuthProvider | None"]

_ENTRY_POINT_GROUP = "mcp_pinot.auth_providers"

_PROVIDERS: dict[str, AuthBuilder] = {}
_entry_points_loaded = False


def register_auth_provider(name: str, builder: AuthBuilder) -> None:
    """Register an auth provider builder under a case-insensitive name."""
    _PROVIDERS[name.lower()] = builder


def _build_none(server_config: ServerConfig) -> None:
    """The no-authentication provider."""
    return None


def _load_entry_point_providers() -> None:
    """Discover and register auth providers exposed via entry points (once)."""
    global _entry_points_loaded
    if _entry_points_loaded:
        return
    _entry_points_loaded = True
    try:
        discovered = entry_points(group=_ENTRY_POINT_GROUP)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Failed to enumerate auth provider entry points: %s", exc)
        return
    for entry_point in discovered:
        try:
            register_auth_provider(entry_point.name, entry_point.load())
            logger.debug(
                "Registered auth provider %r from entry point", entry_point.name
            )
        except Exception:  # pragma: no cover - defensive
            logger.warning(
                "Failed to load auth provider %r from entry point",
                entry_point.name,
                exc_info=True,
            )


def available_providers() -> list[str]:
    """Return the sorted names of all registered providers (incl. entry points)."""
    _load_entry_point_providers()
    return sorted(_PROVIDERS)


def build_auth(server_config: ServerConfig) -> "AuthProvider | None":
    """Build the auth provider selected by ``server_config.auth_provider``.

    Returns ``None`` (no authentication) when no provider is selected or the
    selected provider is ``none``. Raises ``ValueError`` for an unknown provider.
    """
    name = (server_config.auth_provider or "none").lower()
    if name == "none":
        return None

    _load_entry_point_providers()
    builder = _PROVIDERS.get(name)
    if builder is None:
        available = ", ".join(sorted(_PROVIDERS))
        raise ValueError(
            f"Unknown auth provider {name!r}. Set AUTH_PROVIDER to one of: {available}."
        )
    return builder(server_config)


# Register built-in providers.
register_auth_provider("none", _build_none)
register_auth_provider("oauth", build_oauth_auth)
register_auth_provider("static", build_static_auth)
