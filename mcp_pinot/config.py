from dataclasses import dataclass
import logging
import os
from urllib.parse import urlparse

from dotenv import load_dotenv


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


@dataclass
class ServerConfig:
    """Configuration container for MCP server transport settings"""

    transport: str = "both"  # "stdio", "http", or "both"
    host: str = "0.0.0.0"
    port: int = 8080
    ssl_keyfile: str | None = None
    ssl_certfile: str | None = None
    endpoint: str = "/sse"


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
        logging.warning(
            f"Failed to parse PINOT_BROKER_URL '{broker_url}': {e}. Using defaults."
        )
        return "localhost", 80, "http"


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
            logging.warning(
                f"PINOT_BROKER_HOST='{broker_host}' overrides host "
                f"'{url_host}' from PINOT_BROKER_URL"
            )
        if (
            os.getenv("PINOT_BROKER_PORT")
            and int(os.getenv("PINOT_BROKER_PORT")) != url_port
        ):
            logging.warning(
                f"PINOT_BROKER_PORT='{broker_port}' overrides port "
                f"'{url_port}' from PINOT_BROKER_URL"
            )
        if (
            os.getenv("PINOT_BROKER_SCHEME")
            and os.getenv("PINOT_BROKER_SCHEME") != url_scheme
        ):
            logging.warning(
                f"PINOT_BROKER_SCHEME='{broker_scheme}' overrides scheme "
                f"'{url_scheme}' from PINOT_BROKER_URL"
            )

    return PinotConfig(
        controller_url=os.getenv("PINOT_CONTROLLER_URL", "http://localhost:9000"),
        broker_host=broker_host,
        broker_port=broker_port,
        broker_scheme=broker_scheme,
        username=os.getenv("PINOT_USERNAME"),
        password=os.getenv("PINOT_PASSWORD"),
        token=os.getenv("PINOT_TOKEN"),
        database=os.getenv("PINOT_DATABASE", ""),
        use_msqe=os.getenv("PINOT_USE_MSQE", "false").lower() == "true",
        request_timeout=int(os.getenv("PINOT_REQUEST_TIMEOUT", "60")),
        connection_timeout=int(os.getenv("PINOT_CONNECTION_TIMEOUT", "60")),
        query_timeout=int(os.getenv("PINOT_QUERY_TIMEOUT", "60")),
    )


def load_server_config() -> ServerConfig:
    """Load and return MCP server configuration from environment variables"""
    load_dotenv(override=True)

    return ServerConfig(
        transport=os.getenv("MCP_TRANSPORT", "both").lower(),
        host=os.getenv("MCP_HOST", "0.0.0.0"),
        port=int(os.getenv("MCP_PORT", "8080")),
        ssl_keyfile=os.getenv("MCP_SSL_KEYFILE"),
        ssl_certfile=os.getenv("MCP_SSL_CERTFILE"),
        endpoint=os.getenv("MCP_ENDPOINT", "/sse"),
    )
