"""Run trusted validation commands inside the workflow's isolated container."""

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared", type=Path, required=True)
    args = parser.parse_args()
    prepared = json.loads(args.prepared.read_text())
    commands = prepared["setup_commands"] + prepared["validation_commands"]
    if not prepared["validation_commands"] or not all(
        isinstance(command, str) and command.strip() for command in commands
    ):
        raise ValueError("Validation requires nonempty trusted commands")
    env = dict(os.environ)
    env.update(prepared["validation_env"])
    for command in commands:
        argv = shlex.split(command)
        print(f"Running: {shlex.join(argv)}", flush=True)
        subprocess.run(argv, check=True, env=env, timeout=600)  # noqa: S603


if __name__ == "__main__":
    main()
