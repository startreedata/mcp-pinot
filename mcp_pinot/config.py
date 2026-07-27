from dataclasses import dataclass
import json
import logging
import os
import sys
from urllib.parse import urlparse

from dotenv import find_dotenv, load_dotenv
import yaml


def setup_logging() -> None:
    """Configure the application logger without replacing host/framework handlers."""
    level_name = os.getenv("MCP_LOG_LEVEL", "INFO").strip().upper()
    level = logging.getLevelNamesMapping().get(level_name)
    if not isinstance(level, int):
        raise ValueError(
            "MCP_LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL."
        )

    app_logger = logging.getLogger("mcp-pinot")
    handler = next(
        (
            existing
            for existing in app_logger.handlers
            if getattr(existing, "_mcp_pinot_handler", False)
        ),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler(sys.stderr)
        handler._mcp_pinot_handler = True  # type: ignore[attr-defined]
        app_logger.addHandler(handler)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    app_logger.setLevel(level)
    app_logger.propagate = False


def get_logger(name: str = "mcp-pinot") -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)


# Create a default logger for this module
logger = get_logger()


@dataclass
class PinotConfig:
    """Configuration container for Pinot connection settings"""

    controller_url: str
    broker_host: str
    broker_port: int
    broker_scheme: str
    username: str | None
    password: str | None
    token: str | None
    database: str
    use_msqe: bool
    request_timeout: int = 60
    connection_timeout: int = 60
    query_timeout: int = 60
    included_tables: list[str] | None = None
    table_filter_file: str | None = None
    controller_username: str | None = None
    controller_password: str | None = None
    controller_token: str | None = None


@dataclass
class ServerConfig:
    """Configuration container for MCP server transport settings"""

    transport: str = "stdio"
    host: str = "127.0.0.1"
    port: int = 8080
    ssl_keyfile: str | None = None
    ssl_certfile: str | None = None
    oauth_enabled: bool = False
    path: str = "/mcp"
    # Name of the active auth provider (see mcp_pinot.auth). None disables auth.
    auth_provider: str | None = None
    # Exact HTTP Host authorities accepted at the MCP endpoint. Wildcards are not
    # supported; include both hostname and hostname:port when clients use both.
    allowed_hosts: tuple[str, ...] = ("127.0.0.1", "127.0.0.1:8080")
    # Exact browser Origin values accepted at the MCP endpoint. An empty tuple
    # rejects requests that send Origin while allowing clients that omit it.
    allowed_origins: tuple[str, ...] = ()


# Default OAuth scopes advertised in the server's discovery metadata
# (scopes_supported) and requested by clients. Without a non-empty
# scopes_supported, the mcp-remote bridge (Claude Desktop) treats every scope as
# invalid and refuses to start the OAuth flow. See fastmcp#1716.
DEFAULT_OAUTH_SCOPES = [
    "openid",
    "profile",
    "email",
    "pinot:read",
    "pinot:write",
    "pinot:admin",
]
PINOT_AUTHORIZATION_SCOPES = frozenset({"pinot:read", "pinot:write", "pinot:admin"})


@dataclass
class OAuthConfig:
    """Configuration container for OAuth authentication settings"""

    client_id: str
    client_secret: str
    base_url: str
    upstream_authorization_endpoint: str
    upstream_token_endpoint: str
    jwks_uri: str
    issuer: str
    audience: str
    extra_authorize_params: dict[str, str] | None = None
    scopes: list[str] | None = None  # advertised OAuth scope catalog
    required_scopes: list[str] | None = None  # optional global baseline scopes


def _parse_broker_url(broker_url: str) -> tuple[str, int, str]:
    """Parse broker URL and return (host, port, scheme)"""
    try:
        parsed = urlparse(broker_url)

        # Check if we got valid components
        if not parsed.scheme and not parsed.netloc:
            raise ValueError(f"Invalid URL format: {broker_url}")

        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        scheme = parsed.scheme or "http"
        return host, port, scheme
    except Exception as e:
        logger.warning(
            f"Failed to parse PINOT_BROKER_URL '{broker_url}': {e}. Using defaults."
        )
        return "localhost", 80, "http"


def _read_token_from_file(token_filename: str) -> str | None:
    """Read token from file and return it, handling errors gracefully"""
    try:
        if not os.path.exists(token_filename):
            logger.error(f"Token file not found: {token_filename}")
            return None

        if not os.path.isfile(token_filename):
            logger.error(f"Token path is not a file: {token_filename}")
            return None

        with open(token_filename, encoding="utf-8") as f:
            token = f.read().strip()

        if not token:
            logger.warning(f"Token file is empty: {token_filename}")
            return None

        # Add Bearer prefix if not already present
        if not token.startswith("Bearer "):
            token = f"Bearer {token}"

        logger.debug(f"Successfully read token from file: {token_filename}")
        return token

    except PermissionError:
        logger.error(f"Permission denied reading token file: {token_filename}")
        return None
    except Exception as e:
        logger.error(f"Failed to read token from file {token_filename}: {e}")
        return None


def _validate_filter_file_path(filter_file_path: str | None) -> bool:
    """Validate that the filter file path exists and is accessible.

    Args:
        filter_file_path: Path to the filter file

    Returns:
        bool: True if valid, False if no path specified

    Raises:
        FileNotFoundError: If filter file path is configured but file doesn't exist
    """
    if not filter_file_path:
        logger.debug("No table filter file specified")
        return False

    if not os.path.exists(filter_file_path):
        raise FileNotFoundError(
            f"Table filter file not found: {filter_file_path}. "
            f"Please check PINOT_TABLE_FILTER_FILE configuration."
        )

    return True


def _parse_table_filter_config(filter_file_path: str) -> dict:
    """Parse YAML configuration from filter file.

    Args:
        filter_file_path: Path to the YAML filter file

    Returns:
        dict: Parsed and structurally valid configuration.
    """
    try:
        with open(filter_file_path, encoding="utf-8") as file:
            config = yaml.safe_load(file)

        if not isinstance(config, dict):
            raise ValueError("Table filter YAML must contain an object at the root.")

        return config

    except yaml.YAMLError as e:
        raise ValueError(f"Invalid table filter YAML in {filter_file_path}.") from e
    except OSError as e:
        raise OSError(f"Failed to read table filter file {filter_file_path}.") from e


def _load_table_filters(filter_file_path: str | None) -> list[str] | None:
    """Load table filters from YAML configuration file.

    Args:
        filter_file_path: Path to YAML file containing table filters

    Returns:
        list[str] | None: List of included table names, or None if not configured.
                         Returns None (no filtering) if the list is empty.
    """
    if not filter_file_path or not _validate_filter_file_path(filter_file_path):
        return None

    config = _parse_table_filter_config(filter_file_path)

    included_tables = config.get("included_tables")
    allow_all = config.get("allow_all", False)
    if not isinstance(allow_all, bool):
        raise ValueError("'allow_all' must be true or false when provided.")

    if included_tables is None or included_tables == []:
        if not allow_all:
            raise ValueError(
                "Table filter configuration would allow every table. Set "
                "'allow_all: true' explicitly, or provide a non-empty "
                "'included_tables' list."
            )
        logger.warning("Table filter explicitly configured to allow all tables.")
        return None

    if not isinstance(included_tables, list) or not all(
        isinstance(pattern, str) for pattern in included_tables
    ):
        raise ValueError("'included_tables' must be a list of strings.")
    if allow_all:
        logger.warning(
            "Both allow_all=true and included_tables are configured; the explicit "
            "included_tables allow-list takes precedence."
        )
    if len(included_tables) > 1000:
        raise ValueError("'included_tables' may contain at most 1000 patterns.")
    for pattern in included_tables:
        if not pattern or len(pattern) > 256 or any(ord(char) < 32 for char in pattern):
            raise ValueError(
                "Each table-filter pattern must contain 1-256 printable characters."
            )

    table_count = len(included_tables)
    logger.info(f"{table_count} table(s) available after filtering.")

    return included_tables


def reload_table_filters_from_file(file_path: str) -> list[str] | None:
    """Public API for reloading table filters from a YAML file.

    This function validates the file exists, parses the YAML configuration,
    and returns the updated filter list. Designed for hot-reloading filters
    without restarting the server.

    Args:
        file_path: Path to YAML filter file

    Returns:
        list[str] | None: New filter list, or None if empty/not configured

    Raises:
        FileNotFoundError: If the filter file doesn't exist
        yaml.YAMLError: If the YAML is invalid
    """
    return _load_table_filters(file_path)


def load_pinot_config() -> PinotConfig:
    """Load and return Pinot configuration from environment variables"""
    load_dotenv(dotenv_path=find_dotenv(usecwd=True), override=False)

    # Get the broker URL if provided
    broker_url = os.getenv("PINOT_BROKER_URL")

    # Parse defaults from URL if provided
    if broker_url:
        url_host, url_port, url_scheme = _parse_broker_url(broker_url)
    else:
        # Default to Pinot quickstart values
        url_host, url_port, url_scheme = "localhost", 8000, "http"

    # Get individual broker configs with URL as fallback
    broker_host = os.getenv("PINOT_BROKER_HOST", url_host)
    broker_port = int(os.getenv("PINOT_BROKER_PORT", str(url_port)))
    broker_scheme = os.getenv("PINOT_BROKER_SCHEME", url_scheme)

    # Issue warnings if individual configs override URL values
    if broker_url:
        if (
            os.getenv("PINOT_BROKER_HOST")
            and os.getenv("PINOT_BROKER_HOST") != url_host
        ):
            logger.warning(
                f"PINOT_BROKER_HOST='{broker_host}' overrides host "
                f"'{url_host}' from PINOT_BROKER_URL"
            )
        broker_port_env = os.getenv("PINOT_BROKER_PORT")
        if broker_port_env and int(broker_port_env) != url_port:
            logger.warning(
                f"PINOT_BROKER_PORT='{broker_port}' overrides port "
                f"'{url_port}' from PINOT_BROKER_URL"
            )
        if (
            os.getenv("PINOT_BROKER_SCHEME")
            and os.getenv("PINOT_BROKER_SCHEME") != url_scheme
        ):
            logger.warning(
                f"PINOT_BROKER_SCHEME='{broker_scheme}' overrides scheme "
                f"'{url_scheme}' from PINOT_BROKER_URL"
            )

    # Load token, prioritizing direct token over token file
    token = os.getenv("PINOT_TOKEN")
    token_filename = os.getenv("PINOT_TOKEN_FILENAME")

    # If no direct token but token filename is provided, read from file
    if not token and token_filename:
        token = _read_token_from_file(token_filename)
        if token is None:
            logger.warning(
                f"Failed to read token from {token_filename}, continuing without token"
            )

    # Load table filters from YAML file if configured
    filter_file_path = os.getenv("PINOT_TABLE_FILTER_FILE")
    included_tables = _load_table_filters(filter_file_path)

    return PinotConfig(
        controller_url=os.getenv("PINOT_CONTROLLER_URL", "http://localhost:9000"),
        broker_host=broker_host,
        broker_port=broker_port,
        broker_scheme=broker_scheme,
        username=os.getenv("PINOT_USERNAME"),
        password=os.getenv("PINOT_PASSWORD"),
        token=token,
        database=os.getenv("PINOT_DATABASE", ""),
        use_msqe=os.getenv("PINOT_USE_MSQE", "false").lower() == "true",
        request_timeout=int(os.getenv("PINOT_REQUEST_TIMEOUT", "60")),
        connection_timeout=int(os.getenv("PINOT_CONNECTION_TIMEOUT", "60")),
        query_timeout=int(os.getenv("PINOT_QUERY_TIMEOUT", "60")),
        included_tables=included_tables,
        table_filter_file=filter_file_path,
        controller_username=os.getenv("PINOT_CONTROLLER_USERNAME"),
        controller_password=os.getenv("PINOT_CONTROLLER_PASSWORD"),
        controller_token=os.getenv("PINOT_CONTROLLER_TOKEN"),
    )


def _resolve_auth_provider() -> str | None:
    """Resolve the active auth provider name from the environment.

    Honors ``AUTH_PROVIDER`` when set; otherwise falls back to the legacy
    ``OAUTH_ENABLED`` flag ('true' -> 'oauth') for backward compatibility.
    """
    explicit = os.getenv("AUTH_PROVIDER")
    if explicit and explicit.strip():
        return explicit.strip().lower()
    if os.getenv("OAUTH_ENABLED", "false").lower() == "true":
        return "oauth"
    return None


def _parse_http_allowlist(raw: str | None) -> tuple[str, ...]:
    """Parse a comma-separated exact-match HTTP allowlist."""
    if raw is None:
        return ()
    return tuple(value.strip() for value in raw.split(",") if value.strip())


def _default_allowed_hosts(host: str, port: int) -> tuple[str, ...]:
    """Return safe exact Host defaults for a concrete bind address.

    Wildcard bind addresses do not identify a public authority, so deployments
    using them must explicitly set MCP_ALLOWED_HOSTS.
    """
    normalized_host = host.strip()
    if normalized_host in {"0.0.0.0", "::", "[::]"}:
        return ()
    authority_host = (
        f"[{normalized_host}]"
        if ":" in normalized_host and not normalized_host.startswith("[")
        else normalized_host
    )
    return (authority_host, f"{authority_host}:{port}")


def load_server_config() -> ServerConfig:
    """Load and return MCP server configuration from environment variables"""
    load_dotenv(dotenv_path=find_dotenv(usecwd=True), override=False)

    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "8080"))
    allowed_hosts_raw = os.getenv("MCP_ALLOWED_HOSTS")
    allowed_hosts = (
        _default_allowed_hosts(host, port)
        if allowed_hosts_raw is None
        else _parse_http_allowlist(allowed_hosts_raw)
    )

    return ServerConfig(
        transport=os.getenv("MCP_TRANSPORT", "stdio").lower(),
        host=host,
        port=port,
        ssl_keyfile=os.getenv("MCP_SSL_KEYFILE"),
        ssl_certfile=os.getenv("MCP_SSL_CERTFILE"),
        oauth_enabled=os.getenv("OAUTH_ENABLED", "false").lower() == "true",
        path=os.getenv("MCP_PATH", "/mcp"),
        auth_provider=_resolve_auth_provider(),
        allowed_hosts=allowed_hosts,
        allowed_origins=_parse_http_allowlist(os.getenv("MCP_ALLOWED_ORIGINS")),
    )


def _parse_oauth_scopes(raw: str | None) -> list[str]:
    """Parse OAUTH_SCOPES (space- or comma-separated) into a list of scopes.

    Falls back to DEFAULT_OAUTH_SCOPES when unset/empty so the server always
    advertises a non-empty scopes_supported in its OAuth discovery metadata.
    """
    if not raw or not raw.strip():
        return list(DEFAULT_OAUTH_SCOPES)
    scopes = [scope.strip() for scope in raw.replace(",", " ").split()]
    return [scope for scope in scopes if scope] or list(DEFAULT_OAUTH_SCOPES)


def _parse_optional_scopes(raw: str | None) -> list[str] | None:
    """Parse OAUTH_REQUIRED_SCOPES (space/comma-separated) into a list, or None.

    Returns None when unset/empty so the JWT verifier does NOT enforce scopes by
    default — OIDC scopes like 'profile'/'email' rarely appear on access tokens, so
    enforcing them would reject otherwise-valid tokens. This is distinct from
    OAUTH_SCOPES, which is only *advertised* (scopes_supported), never enforced.
    """
    if not raw or not raw.strip():
        return None
    scopes = [scope.strip() for scope in raw.replace(",", " ").split()]
    return [scope for scope in scopes if scope] or None


def load_static_token() -> str:
    """Return the shared bearer secret for the ``static`` auth provider.

    Raises ``ValueError`` when ``MCP_STATIC_TOKEN`` is unset/empty so a
    misconfigured ``AUTH_PROVIDER=static`` deployment fails at startup rather
    than accepting requests it cannot authenticate.
    """
    load_dotenv(dotenv_path=find_dotenv(usecwd=True), override=False)
    token = os.getenv("MCP_STATIC_TOKEN", "").strip()
    if not token:
        raise ValueError(
            "AUTH_PROVIDER=static requires MCP_STATIC_TOKEN to be set to a "
            "non-empty shared secret."
        )
    return token


def load_static_scopes() -> list[str]:
    """Return explicit scopes granted to the shared static service principal."""
    raw = os.getenv("MCP_STATIC_SCOPES")
    if not raw or not raw.strip():
        return ["pinot:read", "pinot:write", "pinot:admin"]
    scopes = list(dict.fromkeys(raw.replace(",", " ").split()))
    invalid = sorted(set(scopes) - PINOT_AUTHORIZATION_SCOPES)
    if invalid:
        raise ValueError(
            "MCP_STATIC_SCOPES contains unsupported scopes: " + ", ".join(invalid)
        )
    if not scopes:
        raise ValueError("MCP_STATIC_SCOPES must grant at least one Pinot scope.")
    return scopes


def load_oauth_config() -> OAuthConfig:
    """Load and return OAuth configuration from environment variables"""
    load_dotenv(dotenv_path=find_dotenv(usecwd=True), override=False)

    # Parse extra authorization parameters from environment variables
    # Format: OAUTH_EXTRA_AUTH_PARAMS='{"param1": "value1", "param2": "value2"}'
    extra_authorize_params = None
    extra_params_str = os.getenv("OAUTH_EXTRA_AUTH_PARAMS")
    if extra_params_str:
        try:
            extra_authorize_params = json.loads(extra_params_str)
            if not isinstance(extra_authorize_params, dict):
                logger.warning(
                    "OAUTH_EXTRA_AUTH_PARAMS must be a JSON object. Ignoring."
                )
                extra_authorize_params = None
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Invalid OAUTH_EXTRA_AUTH_PARAMS JSON: {e}. Ignoring.")
            extra_authorize_params = None

    values = {
        "client_id": os.getenv("OAUTH_CLIENT_ID", "").strip(),
        "client_secret": os.getenv("OAUTH_CLIENT_SECRET", "").strip(),
        "base_url": os.getenv("OAUTH_BASE_URL", "").strip(),
        "upstream_authorization_endpoint": os.getenv(
            "OAUTH_AUTHORIZATION_ENDPOINT", ""
        ).strip(),
        "upstream_token_endpoint": os.getenv("OAUTH_TOKEN_ENDPOINT", "").strip(),
        "jwks_uri": os.getenv("OAUTH_JWKS_URI", "").strip(),
        "issuer": os.getenv("OAUTH_ISSUER", "").strip(),
        "audience": os.getenv("OAUTH_AUDIENCE", "").strip(),
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        env_by_field = {
            "client_id": "OAUTH_CLIENT_ID",
            "client_secret": "OAUTH_CLIENT_SECRET",
            "base_url": "OAUTH_BASE_URL",
            "upstream_authorization_endpoint": "OAUTH_AUTHORIZATION_ENDPOINT",
            "upstream_token_endpoint": "OAUTH_TOKEN_ENDPOINT",
            "jwks_uri": "OAUTH_JWKS_URI",
            "issuer": "OAUTH_ISSUER",
            "audience": "OAUTH_AUDIENCE",
        }
        env_names = ", ".join(env_by_field[name] for name in missing)
        raise ValueError(f"OAuth configuration is incomplete; missing: {env_names}.")

    for name in (
        "base_url",
        "upstream_authorization_endpoint",
        "upstream_token_endpoint",
        "jwks_uri",
        "issuer",
    ):
        parsed = urlparse(values[name])
        loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"OAuth {name} must be an absolute URL.")
        if parsed.scheme != "https" and not (parsed.scheme == "http" and loopback):
            raise ValueError(
                f"OAuth {name} must use HTTPS (HTTP is allowed only for loopback)."
            )

    return OAuthConfig(
        **values,
        extra_authorize_params=extra_authorize_params,
        scopes=_parse_oauth_scopes(os.getenv("OAUTH_SCOPES")),
        required_scopes=_parse_optional_scopes(os.getenv("OAUTH_REQUIRED_SCOPES")),
    )
