"""Install the same controller locally into one or more Git repositories."""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import re
import shutil
import subprocess
import tomllib

from config import DEFAULT_POLICY

SOURCE = Path(__file__).resolve().parents[2]
GIT = shutil.which("git")
if not GIT:
    raise RuntimeError("Installation requires Git")
WORKFLOWS = (
    "maintainer-controller.yml",
    "maintainer-review-signal.yml",
    "maintainer-reviewer.yml",
    "maintainer-worker.yml",
    "maintainer-checks.yml",
)


def _default_branch(target: Path, explicit: str | None) -> str:
    if explicit:
        branch = explicit
    else:
        result = subprocess.run(  # noqa: S603
            [
                GIT,
                "-C",
                str(target),
                "symbolic-ref",
                "--quiet",
                "refs/remotes/origin/HEAD",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            raise ValueError(
                f"{target}: origin/HEAD is unknown; supply --default-branch"
            )
        branch = result.stdout.strip().removeprefix("refs/remotes/origin/")
    subprocess.run(  # noqa: S603
        [GIT, "check-ref-format", "--branch", branch],
        capture_output=True,
        check=True,
    )
    return branch


def _generic_policy(branch: str, ci: list[str]) -> bytes:
    policy = deepcopy(DEFAULT_POLICY)
    policy["default_branch"] = branch
    policy["ci_workflows"] = ci
    # Render the default schema, never clone an activated source policy.
    lines = ["# Configure repository paths and commands before leaving observe mode."]
    for key, value in policy.items():
        if not isinstance(value, dict):
            lines.append(f"{key} = {json.dumps(value, ensure_ascii=False)}")
    for key, table in policy.items():
        if isinstance(table, dict):
            lines.extend(("", f"[{key}]"))
            for name, value in table.items():
                lines.append(f"{name} = {json.dumps(value, ensure_ascii=False)}")
    text = "\n".join(lines) + "\n"
    tomllib.loads(text)
    return text.encode()


def _plan(target: Path, branch: str, ci: list[str]) -> dict[Path, bytes]:
    source_dir = SOURCE / ".github/maintainer"
    required = (
        "controller.py",
        "config.py",
        "github_api.py",
        "state.py",
        "policy.py",
        "worker.py",
        "publish.py",
        "reviewer.py",
    )
    for name in required:
        if not (source_dir / name).is_file():
            raise ValueError(f"Incomplete installer source: {name}")
    files = [
        path
        for path in source_dir.rglob("*")
        if path.is_file() and path.suffix in {".py", ".md", ".Dockerfile"}
    ]
    files += [SOURCE / ".github/workflows" / name for name in WORKFLOWS]
    files += [
        SOURCE / "docs/repo-maintainer.md",
        SOURCE / ".github/ISSUE_TEMPLATE/ai_task.yml",
    ]
    plan = {}
    for source in sorted(files):
        content = source.read_bytes()
        if source.name == "maintainer-checks.yml":
            replacement = f"    branches: [{json.dumps(branch)}]".encode()
            content = re.sub(
                rb"(?m)^    branches: \[.*\]$",
                lambda _, replacement=replacement: replacement,
                content,
            )
        if source.name == "maintainer-controller.yml":
            names = [
                *ci,
                "Repo Maintainer Review Signal",
                "Repo Maintainer Worker",
                "Repo Maintainer Reviewer",
            ]
            replacement = f"    workflows: {json.dumps(names)}".encode()
            content = re.sub(
                rb"(?m)^    workflows: .*?$",
                lambda _, replacement=replacement: replacement,
                content,
            )
        if source.name == "maintainer-reviewer.yml":
            # Omit completion wakeups if this target has no Actions CI names.
            if not ci:
                content = re.sub(
                    rb"(?m)^  workflow_run:\n    workflows: .*?\n"
                    rb"    types: \[completed\]\n",
                    b"",
                    content,
                )
            names = ci
            replacement = f"    workflows: {json.dumps(names)}".encode()
            content = re.sub(
                rb"(?m)^    workflows: .*?$",
                lambda _, replacement=replacement: replacement,
                content,
            )
        plan[target / source.relative_to(SOURCE)] = content
    policy_path = target / ".github/maintainer/policy.toml"
    if not policy_path.exists():
        plan[policy_path] = _generic_policy(branch, ci)
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=Path, action="append", required=True)
    parser.add_argument(
        "--default-branch", help="Fallback when local origin/HEAD is absent"
    )
    parser.add_argument("--ci-workflow", action="append", default=[])
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace differing runtime files; never replace existing policy",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if any(
        not name.strip() or any(ord(char) < 32 for char in name)
        for name in args.ci_workflow
    ):
        parser.error(
            "--ci-workflow requires a nonempty workflow name without control characters"
        )
    plan = {}
    for raw_target in args.target:
        target = raw_target.expanduser().resolve()
        repository = subprocess.run(  # noqa: S603
            [GIT, "-C", str(target), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )
        if Path(repository.stdout.strip()).resolve() != target:
            raise ValueError(f"--target must be a repository root: {target}")
        existing = target / ".github/maintainer/policy.toml"
        if existing.exists():
            policy = tomllib.loads(existing.read_text())
            branch = _default_branch(target, policy.get("default_branch"))
            ci = policy.get("ci_workflows", [])
            if not isinstance(ci, list) or not all(
                isinstance(name, str) for name in ci
            ):
                raise ValueError(
                    f"{target}: existing ci_workflows must be a string list"
                )
            if args.default_branch and args.default_branch != branch:
                raise ValueError(
                    f"{target}: --default-branch conflicts with preserved policy"
                )
            if args.ci_workflow and args.ci_workflow != ci:
                raise ValueError(
                    f"{target}: --ci-workflow conflicts with preserved policy"
                )
        else:
            branch = _default_branch(target, args.default_branch)
            ci = args.ci_workflow
        for path, content in _plan(target, branch, ci).items():
            if path.is_symlink() or any(
                parent.is_symlink() for parent in path.parents if parent != target
            ):
                raise ValueError(f"Refusing symlink destination: {path}")
            if path.exists() and path.read_bytes() != content and not args.overwrite:
                raise ValueError(
                    f"Refusing different existing file: {path}; "
                    "inspect then use --overwrite"
                )
            if not path.exists() or path.read_bytes() != content:
                plan[path] = content
    # Preflight every target before writing any file in the batch.
    for path, content in plan.items():
        print(f"{'Would write' if args.dry_run else 'Write'} {path}")
        if not args.dry_run:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
    print(
        f"{'Planned' if args.dry_run else 'Installed'} {len(plan)} files; "
        "no GitHub changes made"
    )


if __name__ == "__main__":
    main()
