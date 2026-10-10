"""Exercise the disk storage used by the Helm OAuth persistence mount."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("provider", ["oauth", "oauth+static"])
def test_registration_survives_process_restart(tmp_path: Path, provider: str) -> None:
    env = {
        **os.environ,
        "AUTH_PROVIDER": provider,
        "MCP_STATIC_TOKEN": "test-static-token",
        "OAUTH_ISSUER": "https://issuer.example.com",
        "OAUTH_JWKS_URI": "https://issuer.example.com/jwks",
        "OAUTH_AUTHORIZATION_ENDPOINT": "https://issuer.example.com/authorize",
        "OAUTH_TOKEN_ENDPOINT": "https://issuer.example.com/token",
        "OAUTH_CLIENT_ID": "test-upstream-client",
        "OAUTH_CLIENT_SECRET": "test-stable-upstream-secret",
        "OAUTH_BASE_URL": "http://localhost:8080",
        "OAUTH_AUDIENCE": "http://localhost:8080/mcp",
        "FASTMCP_HOME": str(tmp_path / "persistent-home"),
    }
    script = """
import asyncio
import sys
from mcp.shared.auth import OAuthClientInformationFull
from pydantic import AnyUrl
from mcp_pinot.auth import build_auth
from mcp_pinot.config import load_server_config

async def main():
    auth = build_auth(load_server_config())
    if sys.argv[1] == "register":
        await auth.register_client(OAuthClientInformationFull(
            client_id="test-registered-client",
            redirect_uris=[AnyUrl("http://localhost:4567/callback")],
        ))
    else:
        client = await auth.get_client("test-registered-client")
        if sys.argv[1] == "missing":
            assert client is None
        else:
            assert client is not None
            assert str(client.redirect_uris[0]) == "http://localhost:4567/callback"

asyncio.run(main())
"""
    for operation in ("register", "read"):
        subprocess.run(  # noqa: S603 - fixed test script, no external input
            [sys.executable, "-c", script, operation],
            env=env,
            check=True,
            text=True,
            timeout=30,
        )
    # A replacement pod without the same volume cannot recover the registration.
    env["FASTMCP_HOME"] = str(tmp_path / "replacement-ephemeral-home")
    subprocess.run(  # noqa: S603 - fixed test script, no external input
        [sys.executable, "-c", script, "missing"],
        env=env,
        check=True,
        text=True,
        timeout=30,
    )
