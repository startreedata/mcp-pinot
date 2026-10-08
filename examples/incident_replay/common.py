"""Shared restrictions for local replay endpoints."""

import os
from pathlib import Path
import shutil
from urllib.parse import urlsplit


def loopback_url(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in ("http", "https")
        or parsed.hostname not in ("127.0.0.1", "localhost", "::1")
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "Replay endpoints must be explicit loopback broker/controller URLs."
        )
    return value.rstrip("/")


def executable_path(value: str, *, program: str) -> str:
    """Locate the intended program without accepting arbitrary command names."""
    requested = Path(value).expanduser()
    allowed = {"java": ("java", "java.exe"), "codex": ("codex", "codex.exe")}[program]
    if requested.name not in allowed:
        raise ValueError(f"Executable must be named {allowed[0]} or {allowed[1]}.")
    directory = str(requested.parent) if os.path.dirname(value) else None
    name = allowed[1] if requested.name.endswith(".exe") else allowed[0]
    # Explicit paths bypass PATH splitting (notably ';' in Windows directories).
    located = shutil.which(str(requested.absolute()) if directory else name)
    if located is None or not Path(located).is_file():
        raise ValueError(f"An executable {program} program is required.")
    if directory is not None and Path(located).resolve() != requested.resolve():
        raise ValueError(f"Requested executable does not match the {program} lookup.")
    return located


def replay_env(broker: str, controller: str) -> dict[str, str]:
    """Pin the local child configuration despite environment or checkout dotenv."""
    broker, controller = loopback_url(broker), loopback_url(controller)
    parsed = urlsplit(broker)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("PINOT_", "MCP_", "OAUTH_")) and key != "AUTH_PROVIDER"
    }
    env.update(
        {
            "PINOT_BROKER_URL": broker,
            "PINOT_BROKER_HOST": parsed.hostname,
            "PINOT_BROKER_PORT": str(
                parsed.port or (443 if parsed.scheme == "https" else 80)
            ),
            "PINOT_BROKER_SCHEME": parsed.scheme,
            "PINOT_CONTROLLER_URL": controller,
            "PINOT_USE_MSQE": "false",
            "PINOT_REQUEST_TIMEOUT": "60",
            "PINOT_CONNECTION_TIMEOUT": "60",
            "PINOT_QUERY_TIMEOUT": "60",
            "PYTHON_DOTENV_DISABLED": "1",
            "MCP_TRANSPORT": "stdio",
            "MCP_HOST": "127.0.0.1",
            "MCP_PORT": "8080",
            "MCP_PATH": "/mcp",
            "AUTH_PROVIDER": "none",
            "OAUTH_ENABLED": "false",
            "MCP_LOG_LEVEL": "INFO",
            "MCP_RATE_LIMIT_RPS": "10",
            "MCP_RATE_LIMIT_BURST": "20",
            "MCP_RATE_LIMIT_MAX_CLIENTS": "10000",
            "MCP_RATE_LIMIT_IDLE_TTL_SECONDS": "600",
            "MCP_MAX_CONCURRENCY": "8",
            "MCP_MAX_RESPONSE_BYTES": "1000000",
            "MCP_CONFIRMATION_TTL_SECONDS": "300",
        }
    )
    # Explicit empties also prevent load_dotenv(override=False) from refilling
    # absent credentials, database selection, or filters from a checkout .env.
    for name in (
        "PINOT_USERNAME",
        "PINOT_PASSWORD",
        "PINOT_TOKEN",
        "PINOT_TOKEN_FILENAME",
        "PINOT_CONTROLLER_USERNAME",
        "PINOT_CONTROLLER_PASSWORD",
        "PINOT_CONTROLLER_TOKEN",
        "PINOT_DATABASE",
        "PINOT_TABLE_FILTER_FILE",
        "MCP_SSL_KEYFILE",
        "MCP_SSL_CERTFILE",
        "MCP_ALLOWED_HOSTS",
        "MCP_ALLOWED_ORIGINS",
        "MCP_CONFIRMATION_SECRET",
        "MCP_STATIC_TOKEN",
    ):
        env[name] = ""
    return env
