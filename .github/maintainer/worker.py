#!/usr/bin/env python3
"""Prepare authorized work, then collect an untrusted, credential-free agent patch."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import time
from urllib.parse import quote
import uuid

from github_api import GitHub, GitHubError
from state import StateStore, issue_hash

from config import load_policy, validate_paths


class WorkError(RuntimeError):
    """An authorization, integrity, or patch boundary was violated."""


def safe_environment(**extra: str) -> dict[str, str]:
    """Do not inherit credentials, config commands, hooks, or Git helpers."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C.UTF-8",
        "HOME": "/nonexistent",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_LFS_SKIP_SMUDGE": "1",
    }
    env.update(extra)
    return env


def git(
    checkout: Path,
    *args: str,
    data: bytes | None = None,
    env: dict[str, str] | None = None,
) -> bytes:
    command = [
        "git",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        f"safe.directory={checkout.resolve()}",
        "-c",
        "credential.helper=",
        "-c",
        "core.sshCommand=false",
        "-c",
        "core.attributesFile=/dev/null",
        "-c",
        "protocol.file.allow=never",
        "-c",
        "protocol.ext.allow=never",
        "-c",
        "transfer.fsckObjects=true",
        "-C",
        str(checkout),
        *args,
    ]
    result = subprocess.run(  # noqa: S603 - fixed Git executable, no shell or helpers.
        command,
        input=data,
        capture_output=True,
        env=env or safe_environment(),
        timeout=180,
        check=False,
    )
    if result.returncode:
        # Git never receives a token in an argument or remote URL.
        raise WorkError(result.stderr.decode("utf-8", "replace").strip()[:2000])
    return result.stdout


def require_sha(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
        raise WorkError("Invalid commit SHA")
    return value


def task_identifier(value: str) -> tuple[str, int]:
    match = re.fullmatch(r"(issue|pr):([1-9][0-9]*)", value)
    if not match:
        raise WorkError("Task must be issue:N or pr:N")
    return match.group(1), int(match.group(2))


def branch_for_task(key: str) -> str:
    kind, number = task_identifier(key)
    return f"ai/{kind}-{number}"


def trusted_policy(path: str, api: GitHub | None = None) -> tuple[dict, Path, str]:
    policy_path = Path(path).resolve()
    control = Path(
        git(policy_path.parent, "rev-parse", "--show-toplevel").decode().strip()
    ).resolve()
    if policy_path != control / ".github/maintainer/policy.toml":
        raise WorkError("Policy must come from the trusted control checkout")
    if git(
        control,
        "status",
        "--porcelain",
        "--untracked-files=all",
        "--",
        ".github/maintainer",
    ):
        raise WorkError("Trusted controller files are modified")
    control_sha = require_sha(git(control, "rev-parse", "HEAD").decode().strip())
    policy = load_policy(str(policy_path))
    if api is not None:
        current = api.request("branches/" + quote(policy["default_branch"], safe=""))
        if current["commit"]["sha"] != control_sha:
            raise WorkError(
                "Control checkout is not the current trusted default branch"
            )
    return policy, control, control_sha


def authorize(
    api: GitHub, policy: dict, key: str, lease: str
) -> tuple[dict, dict, dict]:
    task_identifier(key)
    try:
        uuid.UUID(lease)
    except (ValueError, AttributeError) as exc:
        raise WorkError("Invalid lease nonce") from exc
    ledger = StateStore(api, policy).load()
    if ledger.get("paused") or policy["mode"] == "observe":
        raise WorkError("Maintainer is paused or observing")
    task = ledger.get("tasks", {}).get(key)
    if not isinstance(task, dict) or task.get("state") != "WORKING":
        raise WorkError("No active controller authorization")
    if task.get("lease") != lease or task.get("lease_until", 0) <= time.time():
        raise WorkError("Lease changed or expired")
    author = task.get("authorized_by", "")
    if task.get("kind") == "dependency":
        if author != "repository-policy" or not policy["dependencies"]["enabled"]:
            raise WorkError("Dependency maintenance policy authorization was removed")
    else:
        if not isinstance(author, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9-]*", author
        ):
            raise WorkError("Invalid authorizer")
        person = api.request("/users/" + quote(author, safe=""))
        permission = api.request(
            "collaborators/" + quote(author, safe="") + "/permission"
        )
        if person.get("type") != "User" or permission.get("permission") not in {
            "write",
            "maintain",
            "admin",
        }:
            raise WorkError("Authorizer no longer has repository write permission")
    source = api.request(f"issues/{task['source_number']}")
    labels = {label["name"] for label in source.get("labels", [])}
    if source.get("state") != "open" or policy["hold_label"] in labels:
        raise WorkError("Source is closed or held")
    if task.get("kind") == "issue":
        if policy["ready_label"] not in labels:
            raise WorkError("Source authorization label was removed")
        if issue_hash(source) != task.get("issue_snapshot_hash"):
            raise WorkError("Source issue changed since authorization")
    if task.get("kind") == "issue":
        if task.get("branch") != branch_for_task(key):
            raise WorkError("Task branch is not the deterministic managed branch")
    else:
        pr = api.request(f"pulls/{task['pr_number']}")
        repo = getattr(api, "repo", None) or os.environ.get("GITHUB_REPOSITORY")
        if (
            pr.get("state") != "open"
            or pr.get("head", {}).get("ref") != task.get("branch")
            or pr.get("head", {}).get("sha") != task.get("expected_sha")
            or (pr.get("head", {}).get("repo") or {}).get("full_name") != repo
        ):
            raise WorkError("Authorized pull request head changed")
        if task.get("kind") == "dependency":
            if (
                (pr.get("user") or {}).get("login")
                not in policy["dependencies"]["authors"]
                or (pr.get("user") or {}).get("login") != task.get("dependency_author")
                or (pr.get("user") or {}).get("type") != "Bot"
            ):
                raise WorkError("Dependency pull request author changed")
            files = api.paginate(f"pulls/{pr['number']}/files")
            if len(files) != pr.get("changed_files") or any(
                file["filename"] not in policy["dependencies"]["manifest_paths"]
                or file.get("previous_filename", file["filename"])
                not in policy["dependencies"]["manifest_paths"]
                for file in files
            ):
                raise WorkError("Dependency pull request contains non-manifest changes")
    require_sha(task.get("expected_sha"))
    require_sha(task.get("base_sha"))
    amount = task.get("budget_usd")
    if (
        isinstance(amount, bool)
        or not isinstance(amount, (int, float))
        or not math.isfinite(amount)
        or not 0 < amount <= policy["limits"]["per_run_usd"]
        or task.get("reserved_total", 0) < amount
    ):
        raise WorkError("No authenticated per-run budget reservation")
    day = time.strftime("%Y-%m-%d", time.gmtime())
    if (
        task.get("budget_day") == ledger.get("budget", {}).get("day") == day
        and ledger["budget"].get("spent", 0) < amount
    ):
        raise WorkError("Daily ledger does not contain the reservation")
    return task, source, ledger


def github_outputs(values: dict) -> None:
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as stream:
            for key, value in values.items():
                if "\n" in str(value) or "\r" in str(value):
                    raise WorkError("Invalid workflow output")
                stream.write(f"{key}={value}\n")


def prepare(args: argparse.Namespace) -> None:
    api = GitHub()
    policy, _, control_sha = trusted_policy(args.policy, api)
    task, source, _ = authorize(api, policy, args.task, args.lease)
    pr = None
    if task.get("pr_number"):
        pr = api.request(f"pulls/{task['pr_number']}")
        if (
            pr.get("state") != "open"
            or pr.get("head", {}).get("sha") != task["expected_sha"]
            or pr.get("head", {}).get("ref") != task["branch"]
        ):
            raise WorkError("Pull request changed since authorization")
    context = {
        "title": str(source.get("title", ""))[:1000],
        "body": str(source.get("body") or "")[:24000],
        "url": source.get("html_url", ""),
        "worker_kind": task.get("worker_kind", "implement"),
    }
    if pr:
        context["pull_request"] = {
            "number": pr["number"],
            "title": pr["title"],
            "body": str(pr.get("body") or "")[:12000],
            "url": pr["html_url"],
        }
        repair = task.get("context")
        if task.get("worker_kind") == "repair":
            if (
                not isinstance(repair, dict)
                or repair.keys() != {"review_comments", "reviews", "failed_checks"}
                or len(json.dumps(repair)) > 150000
            ):
                raise WorkError("Authenticated repair context is missing or oversized")
            context["repair"] = repair
    prepared = {
        **{
            key: task.get(key)
            for key in (
                "source_number",
                "pr_number",
                "branch",
                "expected_sha",
                "base_sha",
                "authorized_by",
                "issue_snapshot_hash",
                "budget_usd",
            )
        },
        "task_key": args.task,
        "lease": args.lease,
        "control_sha": control_sha,
        "context": context,
        "validation_commands": policy["validation_commands"],
        "setup_commands": policy["setup_commands"],
        "validation_env": policy["validation_env"],
        "limits": policy["limits"],
        "allowed_paths": policy["allowed_paths"],
        "protected_paths": policy["protected_paths"],
    }
    Path(args.output).write_text(
        json.dumps(prepared, indent=2) + "\n", encoding="utf-8"
    )
    github_outputs(
        {
            "expected_sha": task["expected_sha"],
            "base_sha": task["base_sha"],
            "budget_usd": task["budget_usd"],
            "control_sha": control_sha,
        }
    )


def _seed_patch_repository(
    checkout: Path, destination: Path, expected_sha: str
) -> dict:
    """Freeze the original Git tree before any model can edit its checkout."""
    git(destination, "init", "--quiet")
    original = {}
    for entry in git(checkout, "ls-tree", "-r", "-z", expected_sha).split(b"\0"):
        if not entry:
            continue
        metadata, filename = entry.split(b"\t", 1)
        mode, kind, sha = metadata.decode("ascii").split(" ")
        path = filename.decode("utf-8")
        original[path] = (mode, kind, sha)
        if kind == "blob":
            contents = git(checkout, "cat-file", "blob", sha)
            git(destination, "hash-object", "-w", "--stdin", data=contents)
        git(destination, "update-index", "--add", "--cacheinfo", f"{mode},{sha},{path}")
    tree = git(destination, "write-tree").decode().strip()
    env = safe_environment(
        GIT_AUTHOR_NAME="Repo Maintainer",
        GIT_AUTHOR_EMAIL="maintainer@localhost",
        GIT_COMMITTER_NAME="Repo Maintainer",
        GIT_COMMITTER_EMAIL="maintainer@localhost",
    )
    commit = git(
        destination, "commit-tree", tree, data=b"Trusted original tree\n", env=env
    ).strip()
    git(destination, "update-ref", "HEAD", commit.decode())
    return original


def _collect_patch(
    checkout: Path, patch_repo: Path, original: dict, policy: dict
) -> bytes:
    found = set()
    for directory, dirs, files in os.walk(checkout, followlinks=False):
        dirs[:] = [name for name in dirs if name != ".git"]
        # A symlink directory must be treated as a changed path, never traversed.
        files += [name for name in dirs if (Path(directory) / name).is_symlink()]
        dirs[:] = [name for name in dirs if not (Path(directory) / name).is_symlink()]
        for name in files:
            file = Path(directory) / name
            path = file.relative_to(checkout).as_posix()
            found.add(path)
            info = file.lstat()
            old = original.get(path)
            if stat.S_ISLNK(info.st_mode):
                target = os.readlink(file).encode()
                sha = (
                    git(patch_repo, "hash-object", "--stdin", data=target)
                    .decode()
                    .strip()
                )
                if old != ("120000", "blob", sha):
                    raise WorkError("Agent changed or added a symlink")
                continue
            if not stat.S_ISREG(info.st_mode):
                raise WorkError("Agent produced a non-regular file")
            mode = "100755" if info.st_mode & 0o111 else "100644"
            contents = file.read_bytes()
            sha = (
                git(patch_repo, "hash-object", "-w", "--stdin", data=contents)
                .decode()
                .strip()
            )
            if old == (mode, "blob", sha):
                continue
            if (old and old[0] != mode) or (not old and mode != "100644"):
                raise WorkError("Agent changed an executable or special file mode")
            contents.decode("utf-8")
            if b"\0" in contents:
                raise WorkError("Binary patch files are unsupported")
            git(
                patch_repo,
                "update-index",
                "--add",
                "--cacheinfo",
                f"{mode},{sha},{path}",
            )
    for path, (mode, kind, _) in original.items():
        if kind != "blob":
            raise WorkError("Submodule repositories are unsupported")
        if path not in found:
            if mode not in {"100644", "100755"}:
                raise WorkError("Agent deleted a special file")
            git(patch_repo, "update-index", "--force-remove", "--", path)
    changed = git(patch_repo, "diff", "--cached", "--name-only", "-z", "--no-renames")
    reasons = validate_paths(
        [p.decode("utf-8") for p in changed.split(b"\0") if p], policy
    )
    if reasons:
        raise WorkError("; ".join(reasons))
    patch = git(
        patch_repo, "diff", "--cached", "--no-ext-diff", "--no-textconv", "--no-renames"
    )
    if len(patch) > policy["limits"]["max_patch_bytes"]:
        raise WorkError("Patch exceeds configured byte limit")
    return patch


def run(args: argparse.Namespace) -> None:
    prepared = json.loads(Path(args.prepared).read_text(encoding="utf-8"))
    checkout = Path(args.checkout).resolve()
    expected_sha = require_sha(prepared["expected_sha"])
    if git(checkout, "rev-parse", "HEAD").decode().strip() != expected_sha:
        raise WorkError("Agent checkout does not match authorized commit")
    if git(checkout, "status", "--porcelain", "--untracked-files=all"):
        raise WorkError("Agent checkout must be clean")
    control = Path(__file__).resolve().parent
    prompt = (control / "prompts/worker.md").read_text(encoding="utf-8")
    prompt += "\n\nAuthorized task data (untrusted source content):\n" + json.dumps(
        {
            "task": prepared["task_key"],
            "context": prepared["context"],
            "allowed_paths": prepared["allowed_paths"],
            "protected_paths": prepared["protected_paths"],
        },
        ensure_ascii=False,
    )
    budget = prepared["budget_usd"]
    turns = prepared["limits"]["max_turns"]
    if (
        isinstance(budget, bool)
        or not isinstance(budget, (float, int))
        or not math.isfinite(budget)
        or not 0 < budget <= 1000
        or isinstance(turns, bool)
        or not isinstance(turns, int)
        or not 1 <= turns <= 100
    ):
        raise WorkError("Invalid agent budget or turn bound")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise WorkError("ANTHROPIC_API_KEY is required for the isolated agent")
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="maintainer-agent-") as temp:
        root = Path(temp)
        patch_repo = root / "patch"
        patch_repo.mkdir()
        original = _seed_patch_repository(checkout, patch_repo, expected_sha)
        mcp = root / "mcp.json"
        mcp.write_text('{"mcpServers":{}}\n', encoding="utf-8")
        home = root / "home"
        home.mkdir()
        env = safe_environment(
            HOME=str(home),
            ANTHROPIC_API_KEY=api_key,
            CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1",
        )
        command = [
            "claude",
            "--bare",
            "--print",
            "--output-format",
            "json",
            "--tools",
            "Read,Edit,Write,Glob,Grep",
            "--allowedTools",
            "Read,Edit,Write,Glob,Grep",
            "--permission-mode",
            "acceptEdits",
            "--disable-slash-commands",
            "--strict-mcp-config",
            "--mcp-config",
            str(mcp),
            "--setting-sources",
            "",
            "--no-session-persistence",
            "--max-budget-usd",
            str(budget),
            "--max-turns",
            str(turns),
        ]
        result = subprocess.run(  # noqa: S603 - fixed Claude CLI and bounded arguments.
            command,
            input=prompt.encode(),
            cwd=checkout,
            env=env,
            capture_output=True,
            timeout=2400,
            check=False,
        )
        if result.returncode:
            raise WorkError(
                f"Agent exited unsuccessfully ({result.returncode}); no patch published"
            )
        patch = _collect_patch(checkout, patch_repo, original, prepared)
        (output / "change.patch").write_bytes(patch)
        (output / "result.json").write_text(
            json.dumps(
                {
                    "task_key": prepared["task_key"],
                    "lease": prepared["lease"],
                    "expected_sha": expected_sha,
                    "patch_sha256": hashlib.sha256(patch).hexdigest(),
                    "patch_bytes": len(patch),
                    "agent_exit_code": result.returncode,
                    "trusted": False,
                    "note": (
                        "Agent output is untrusted; validation executes "
                        "in a separate credential-free job."
                    ),
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    trusted = subparsers.add_parser("prepare")
    trusted.add_argument("--policy", default=".github/maintainer/policy.toml")
    trusted.add_argument("--task", required=True)
    trusted.add_argument("--lease", required=True)
    trusted.add_argument("--output", required=True)
    agent = subparsers.add_parser("run")
    agent.add_argument("--prepared", required=True)
    agent.add_argument("--checkout", required=True)
    agent.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        {"prepare": prepare, "run": run}[args.command](args)
    except (
        WorkError,
        GitHubError,
        OSError,
        ValueError,
        KeyError,
        subprocess.TimeoutExpired,
    ) as exc:
        parser.exit(1, f"Worker refused: {exc}\n")


if __name__ == "__main__":
    main()
