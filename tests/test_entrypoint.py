"""Exercise argument forwarding through the actual container entrypoint."""

import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

import pytest


@pytest.mark.skipif(shutil.which("sh") is None, reason="POSIX shell unavailable")
@pytest.mark.parametrize("arguments", [[], ["--incident-profiles", "policy file.json"]])
def test_container_entrypoint_forwards_arguments(
    tmp_path: Path, arguments: list[str]
) -> None:
    (tmp_path / "dotenv.py").write_text("def load_dotenv(*args, **kwargs): pass\n")
    package = tmp_path / "mcp_pinot"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "server.py").write_text(
        "import json, sys\ndef main(): print(json.dumps(sys.argv[1:]))\n"
    )
    python = tmp_path / "python"
    python.write_text(
        f'#!/bin/sh\nexec {shlex.quote(Path(sys.executable).as_posix())} "$@"\n',
        newline="\n",
    )
    python.chmod(0o755)
    result = subprocess.run(  # noqa: S603
        [
            shutil.which("sh"),
            (Path(__file__).resolve().parents[1] / "run.sh").as_posix(),
            *arguments,
        ],
        cwd=tmp_path,
        env={**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"]},
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert json.loads(result.stdout) == arguments
