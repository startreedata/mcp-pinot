"""Run trusted validation commands inside the workflow's isolated container."""

import os
from pathlib import Path
import shlex
import subprocess

from config import load_policy


def main() -> None:
    policy = load_policy(Path(__file__).with_name("policy.toml"))
    commands = policy["setup_commands"] + policy["validation_commands"]
    if not policy["validation_commands"] or not all(
        isinstance(command, str) and command.strip() for command in commands
    ):
        raise ValueError("Validation requires nonempty trusted commands")
    env = dict(os.environ)
    env.update(policy["validation_env"])
    for command in commands:
        argv = shlex.split(command)
        print(f"Running: {shlex.join(argv)}", flush=True)
        subprocess.run(argv, check=True, env=env, timeout=600)  # noqa: S603


if __name__ == "__main__":
    main()
