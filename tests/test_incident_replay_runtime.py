"""Keep replay receipts sensitive to changes in the actual imported backend."""

import json
import os
from pathlib import Path
import subprocess
import sys

from examples.incident_replay.live import source_hashes


def test_runtime_identity_changes_with_backend_or_dependency_lock(tmp_path):
    replay = tmp_path / "examples" / "incident_replay"
    replay.mkdir(parents=True)
    (replay / "runner.py").write_text("# harness\n")
    backend = tmp_path / "mcp_pinot" / "auth"
    backend.mkdir(parents=True)
    production = backend / "provider.py"
    production.write_text("# backend before\n")
    lock = tmp_path / "uv.lock"
    lock.write_text("# dependency lock before\n")
    before = source_hashes(tmp_path)
    production.write_text("# backend after\n")
    after = source_hashes(tmp_path)
    harness_key = str((replay / "runner.py").relative_to(tmp_path))
    production_key = str(production.relative_to(tmp_path))
    assert before != after
    assert before[harness_key] == after[harness_key]
    lock.write_text("# dependency lock after\n")
    assert source_hashes(tmp_path)["uv.lock"] != after["uv.lock"]
    production.unlink()
    assert production_key not in source_hashes(tmp_path)


def test_probe_help_does_not_initialize_ambient_auth(tmp_path):
    root = Path(__file__).resolve().parents[1]
    env = dict(os.environ, AUTH_PROVIDER="static", MCP_STATIC_TOKEN="")
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(root / "examples/incident_replay/probes.py"), "--help"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "--broker" in result.stdout


def test_probe_import_pins_local_config_and_restores_environment(tmp_path):
    root = Path(__file__).resolve().parents[1]
    (tmp_path / ".env").write_text(
        "PINOT_PASSWORD=checkout-test-secret\n"
        "PINOT_CONTROLLER_URL=https://remote.invalid\n"
        "PINOT_DATABASE=remote_database\n"
    )
    env = dict(
        os.environ,
        AUTH_PROVIDER="static",
        MCP_STATIC_TOKEN="",
        PINOT_BROKER_HOST="remote.invalid",
        PINOT_BROKER_PORT="9999",
        PINOT_TOKEN="inherited-test-token",
        PYTHONPATH=str(root),
    )
    for name in (
        "PYTHON_DOTENV_DISABLED",
        "PINOT_PASSWORD",
        "PINOT_DATABASE",
        "PINOT_CONTROLLER_URL",
    ):
        env.pop(name, None)
    script = (
        "import json, os, sys; from examples.incident_replay import probes; "
        "assert 'mcp_pinot.server' not in sys.modules; before=dict(os.environ); "
        "server=probes._load_local_server('http://127.0.0.1:18000',"
        "'http://127.0.0.1:19090'); p=server.pinot_config; "
        "print(json.dumps([p.broker_host,p.broker_port,p.controller_url,p.password,"
        "p.token,p.database,server._auth is None,dict(os.environ)==before]))"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert json.loads(result.stdout) == [
        "127.0.0.1",
        18000,
        "http://127.0.0.1:19090",
        "",
        "",
        "",
        True,
        True,
    ]
