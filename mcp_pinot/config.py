from dataclasses import dataclass
import json
import logging
import os
import sys
from urllib.parse import urlparse

from dotenv import load_dotenv
import yaml


def setup_logging():
    """Set up basic logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )


def get_logger(name: str = "mcp-pinot") -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)


# Initialize logging when module is imported
setup_logging()

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


@dataclass
class ServerConfig:
    """Configuration container for MCP server transport settings"""

    transport: str = "http"
    host: str = "0.0.0.0"
    port: int = 8080
    ssl_keyfile: str | None = None
    ssl_certfile: str | None = None
    oauth_enabled: bool = False
    path: str = "/mcp"


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
    audience: str | None = None
    extra_authorize_params: dict[str, str] | None = None


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

        with open(token_filename, "r", encoding="utf-8") as f:
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


def _parse_table_filter_config(filter_file_path: str) -> dict | None:
    """Parse YAML configuration from filter file.

    Args:
        filter_file_path: Path to the YAML filter file

    Returns:
        dict | None: Parsed configuration or None on error
    """
    try:
        with open(filter_file_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)

        if not config:
            logger.warning("Empty table filter configuration file")
            return None

        return config

    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML syntax in {filter_file_path}: {e}")
        return None
    except OSError as e:
        logger.error(f"Failed to read filter file {filter_file_path}: {e}")
        return None


def _load_table_filters(filter_file_path: str | None) -> list[str] | None:
    """Load table filters from YAML configuration file.

    Args:
        filter_file_path: Path to YAML file containing table filters

    Returns:
        list[str] | None: List of included table names, or None if not configured.
                         Returns None (no filtering) if the list is empty.
    """
    if not _validate_filter_file_path(filter_file_path):
        return None

    config = _parse_table_filter_config(filter_file_path)
    if not config:
        return None

    included_tables = config.get("included_tables")

    # Treat empty lists the same as None - no filtering
    if not included_tables:
        logger.info(
            "Table filter set but no tables listed — including all tables."
        )
        return None

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
    load_dotenv(override=True)

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
        if (
            os.getenv("PINOT_BROKER_PORT")
            and int(os.getenv("PINOT_BROKER_PORT")) != url_port
        ):
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
    )


def load_server_config() -> ServerConfig:
    """Load and return MCP server configuration from environment variables"""
    load_dotenv(override=True)

    return ServerConfig(
        transport=os.getenv("MCP_TRANSPORT", "http").lower(),
        host=os.getenv("MCP_HOST", "0.0.0.0"),
        port=int(os.getenv("MCP_PORT", "8080")),
        ssl_keyfile=os.getenv("MCP_SSL_KEYFILE"),
        ssl_certfile=os.getenv("MCP_SSL_CERTFILE"),
        oauth_enabled=os.getenv("OAUTH_ENABLED", "false").lower() == "true",
        path=os.getenv("MCP_PATH", "/mcp"),
    )


def load_oauth_config() -> OAuthConfig:
    """Load and return OAuth configuration from environment variables"""
    load_dotenv(override=True)

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

    return OAuthConfig(
        client_id=os.getenv("OAUTH_CLIENT_ID", ""),
        client_secret=os.getenv("OAUTH_CLIENT_SECRET", ""),
        base_url=os.getenv("OAUTH_BASE_URL", "http://localhost:8080"),
        upstream_authorization_endpoint=os.getenv("OAUTH_AUTHORIZATION_ENDPOINT", ""),
        upstream_token_endpoint=os.getenv("OAUTH_TOKEN_ENDPOINT", ""),
        jwks_uri=os.getenv("OAUTH_JWKS_URI", ""),
        issuer=os.getenv("OAUTH_ISSUER", ""),
        audience=os.getenv("OAUTH_AUDIENCE"),
        extra_authorize_params=extra_authorize_params,
    )
