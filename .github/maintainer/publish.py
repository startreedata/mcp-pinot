#!/usr/bin/env python3
"""Validate untrusted patches and publish from rebuilt, authenticated state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import quote, urlparse

from github_api import GitHub, GitHubError
from worker import (
    WorkError,
    authorize,
    git,
    github_outputs,
    require_sha,
    safe_environment,
    trusted_policy,
    writer_identity,
)

from config import load_policy, validate_paths


def validate_patch(
    checkout: Path,
    patch_file: Path,
    policy: dict,
    expected_sha: str,
    *,
    materialize: bool = False,
) -> tuple[str, list[str], str]:
    require_sha(expected_sha)
    if git(checkout, "rev-parse", "HEAD").decode().strip() != expected_sha:
        raise WorkError("Patch checkout does not match authorized commit")
    if git(checkout, "status", "--porcelain", "--untracked-files=all"):
        raise WorkError("Patch checkout must be pristine")
    patch = patch_file.read_bytes()
    if not patch or len(patch) > policy["limits"]["max_patch_bytes"]:
        raise WorkError("Empty patch or patch exceeds byte limit")
    patch.decode("utf-8")
    if b"\0" in patch or b"GIT binary patch" in patch or b"Binary files " in patch:
        raise WorkError("Binary patches are unsupported")
    # Check the index before unsafe paths or special modes reach the filesystem.
    git(
        checkout,
        "apply",
        "--cached",
        "--check",
        "--whitespace=error-all",
        "-",
        data=patch,
    )
    git(checkout, "apply", "--cached", "--whitespace=error-all", "-", data=patch)
    raw = git(checkout, "diff", "--cached", "--raw", "-z", "--no-renames").split(b"\0")
    paths = []
    for index in range(0, len(raw) - 1, 2):
        fields = raw[index].decode("ascii").split()
        old, new = fields[0][1:], fields[1]
        if old not in {"000000", "100644", "100755"} or new not in {
            "000000",
            "100644",
            "100755",
        }:
            raise WorkError(
                "Symlink, submodule, and special file patches are prohibited"
            )
        if (old != "000000" and new != "000000" and old != new) or (
            old == "000000" and new != "100644"
        ):
            raise WorkError("Executable and file mode changes are prohibited")
        paths.append(raw[index + 1].decode("utf-8"))
    if not paths:
        raise WorkError("Patch makes no tracked changes")
    reasons = validate_paths(paths, policy)
    if reasons:
        raise WorkError("; ".join(reasons))
    tree = git(checkout, "write-tree").decode().strip()
    if materialize:
        git(checkout, "checkout-index", "--all", "--force")
        # checkout-index does not remove files deleted in the new index.
        tracked = {
            p.decode() for p in git(checkout, "ls-files", "-z").split(b"\0") if p
        }
        for path in paths:
            if path not in tracked:
                (checkout / path).unlink()
    return tree, paths, hashlib.sha256(patch).hexdigest()


def validate(args: argparse.Namespace) -> None:
    # This entry point runs with no API or model credentials in a test container.
    policy = load_policy(args.policy)
    prepared = json.loads(Path(args.prepared).read_text(encoding="utf-8"))
    tree, paths, digest = validate_patch(
        Path(args.checkout).resolve(),
        Path(args.patch),
        policy,
        prepared["expected_sha"],
        materialize=True,
    )
    print(json.dumps({"tree": tree, "paths": paths, "patch_sha256": digest}))


def validation_receipt(
    api: GitHub, policy: dict, task: str, lease: str, run_id: str, control_sha: str
) -> str:
    if not re.fullmatch(r"[1-9][0-9]*", run_id):
        raise WorkError("Invalid validation run id")
    if run_id != os.environ.get("GITHUB_RUN_ID"):
        raise WorkError("Validation must belong to this publisher workflow run")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "")
    if not re.fullmatch(r"[1-9][0-9]*", attempt):
        raise WorkError("Missing current workflow attempt")
    run = api.request(f"actions/runs/{run_id}")
    title = f"Repo Maintainer Worker | {task} | {lease}"
    workflow = policy.get("worker_workflow", "maintainer-worker.yml")
    if (
        run.get("path", "").split("@", 1)[0] != f".github/workflows/{workflow}"
        or run.get("event") != "workflow_dispatch"
        or run.get("display_title") != title
        or run.get("head_branch") != policy["default_branch"]
        or run.get("head_sha") != control_sha
        or run.get("run_attempt") != int(attempt)
        or (run.get("actor") or {}).get("login") != policy["app_login"]
    ):
        raise WorkError("Validation workflow provenance does not match authorization")
    jobs = api.paginate(f"actions/runs/{run_id}/attempts/{attempt}/jobs")
    matching = [job for job in jobs if job.get("name") == "validate"]
    if (
        len(matching) != 1
        or matching[0].get("status") != "completed"
        or matching[0].get("conclusion") != "success"
    ):
        raise WorkError("Current validation job has not completed successfully")
    return str(matching[0].get("html_url") or run["html_url"])


def _remote_url(api: GitHub) -> str:
    repo = getattr(api, "repo", None) or os.environ.get("GITHUB_REPOSITORY", "")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise WorkError("Invalid repository identity")
    server = urlparse(os.environ.get("GITHUB_SERVER_URL", "https://github.com"))
    if (
        server.scheme != "https"
        or not server.hostname
        or server.username
        or server.password
        or server.path not in {"", "/"}
    ):
        raise WorkError("Publisher requires a trusted HTTPS GitHub server")
    return f"https://{server.netloc}/{repo}.git"


def _branch(api: GitHub, branch: str) -> str | None:
    try:
        return require_sha(
            api.request("git/ref/heads/" + quote(branch, safe=""))["object"]["sha"]
        )
    except GitHubError as exc:
        if exc.status == 404:
            return None
        raise


def _managed_pr(api: GitHub, policy: dict, task: dict) -> dict | None:
    repo = getattr(api, "repo", None) or os.environ["GITHUB_REPOSITORY"]
    owner = repo.split("/", 1)[0]
    pulls = api.paginate(
        "pulls?state=all&head=" + quote(owner + ":" + task["branch"], safe="")
    )
    matching = [
        pr
        for pr in pulls
        if pr.get("head", {}).get("ref") == task["branch"]
        and pr.get("head", {}).get("repo", {}).get("full_name") == repo
    ]
    if len(matching) > 1:
        raise WorkError("Managed branch has multiple pull requests")
    if matching and (
        matching[0].get("state") != "open"
        or matching[0].get("base", {}).get("ref") != policy["default_branch"]
    ):
        raise WorkError("Managed branch pull request is closed or targets another base")
    if task.get("pr_number") and (
        not matching or matching[0]["number"] != task["pr_number"]
    ):
        raise WorkError("Managed pull request changed")
    if matching and task.get("kind") == "issue":
        marker = f"<!-- repo-maintainer-task:issue:{task['source_number']} -->"
        if (matching[0].get("user") or {}).get("login") != policy[
            "app_login"
        ] or marker not in str(matching[0].get("body") or ""):
            raise WorkError(
                "Issue pull request is not owned by the configured writer task"
            )
    return matching[0] if matching else None


def publish(args: argparse.Namespace) -> None:
    api = GitHub()
    policy, _, control_sha = trusted_policy(args.policy, api)
    writer_identity(api, policy)
    task, source, _ = authorize(api, policy, args.task, args.lease)
    receipt = validation_receipt(
        api, policy, args.task, args.lease, args.validation_run, control_sha
    )
    expected_sha = task["expected_sha"]
    default = api.request("branches/" + quote(policy["default_branch"], safe=""))[
        "commit"
    ]["sha"]
    if default != task["base_sha"]:
        raise WorkError("Default branch advanced after controller authorization")
    existing = _managed_pr(api, policy, task)
    remote_sha = _branch(api, task["branch"])
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        raise WorkError("An isolated writer credential is required")
    remote = _remote_url(api)
    # Ignore worker-prepared metadata and the supplied candidate checkout entirely.
    with tempfile.TemporaryDirectory(prefix="maintainer-publish-") as temp:
        root = Path(temp)
        checkout = root / "repo"
        checkout.mkdir()
        git(checkout, "init", "--quiet")
        askpass = root / "askpass"
        askpass.write_text(
            '#!/bin/sh\ncase "$1" in\n'
            '*Username*) printf "%s\\n" "x-access-token";;\n'
            '*) printf "%s\\n" "$MAINTAINER_GIT_TOKEN";;\nesac\n',
            encoding="utf-8",
        )
        askpass.chmod(0o700)
        env = safe_environment(GIT_ASKPASS=str(askpass), MAINTAINER_GIT_TOKEN=token)
        git(
            checkout,
            "fetch",
            "--quiet",
            "--no-tags",
            "--depth=1",
            remote,
            expected_sha,
            env=env,
        )
        git(checkout, "checkout", "--quiet", "--detach", expected_sha)
        tree, paths, digest = validate_patch(
            checkout, Path(args.patch), policy, expected_sha
        )
        if task.get("kind") == "dependency" and any(
            path not in policy["dependencies"]["manifest_paths"] for path in paths
        ):
            raise WorkError("Dependency repair patch contains non-manifest changes")
        message = (
            f"Repo maintainer: {args.task}\n\nMaintainer-Lease: {args.lease}\n"
            f"Patch-SHA256: {digest}\n"
        )
        if remote_sha and remote_sha != expected_sha:
            # Recover our exact commit after a push succeeded but PR creation failed.
            remote_commit = api.request(f"git/commits/{remote_sha}")
            if (
                remote_commit.get("tree", {}).get("sha") != tree
                or [p.get("sha") for p in remote_commit.get("parents", [])]
                != [expected_sha]
                or remote_commit.get("message", "").rstrip() != message.rstrip()
            ):
                raise WorkError("Managed branch changed since authorization")
            published_sha = remote_sha
        else:
            if task.get("pr_number") and remote_sha is None:
                raise WorkError("Managed pull request branch disappeared")
            commit_env = safe_environment(
                GIT_AUTHOR_NAME=policy["app_login"],
                GIT_AUTHOR_EMAIL=f"{policy['app_login']}@users.noreply.github.com",
                GIT_COMMITTER_NAME=policy["app_login"],
                GIT_COMMITTER_EMAIL=f"{policy['app_login']}@users.noreply.github.com",
            )
            published_sha = (
                git(
                    checkout,
                    "commit-tree",
                    tree,
                    "-p",
                    expected_sha,
                    data=message.encode(),
                    env=commit_env,
                )
                .decode()
                .strip()
            )
            # Recheck lease, source, write permission, and head immediately before push.
            latest, _, _ = authorize(api, policy, args.task, args.lease)
            if (
                latest["expected_sha"] != expected_sha
                or _branch(api, task["branch"]) != remote_sha
            ):
                raise WorkError(
                    "Authorization or remote head changed before publication"
                )
            git(
                checkout,
                "push",
                "--porcelain",
                remote,
                f"{published_sha}:refs/heads/{task['branch']}",
                env=env,
            )
        if _branch(api, task["branch"]) != published_sha:
            raise WorkError("Published branch could not be confirmed")
    if not existing:
        title = ("[Repo Maintainer] " + str(source.get("title", args.task)))[:240]
        body = (
            f"<!-- repo-maintainer-task:{args.task} -->\n"
            f"Fixes #{task['source_number']}.\n\n"
            f"Managed task `{args.task}`; authorization lease `{args.lease}`.\n\n"
            f"Validation evidence: [credential-free validation job]({receipt}).\n"
            f"Published commit: `{published_sha}`.\n\n"
            "Independent review and configured merge gates are still required."
        )
        existing = api.request(
            "pulls",
            method="POST",
            data={
                "title": title,
                "body": body,
                "head": task["branch"],
                "base": policy["default_branch"],
                "draft": False,
            },
        )
    api.request(
        f"issues/{existing['number']}/labels",
        method="POST",
        data={"labels": [policy["managed_label"]]},
    )
    marker = f"<!-- repo-maintainer-published:{args.lease} -->"
    comments = api.paginate(f"issues/{existing['number']}/comments")
    if not any(
        comment.get("user", {}).get("login") == policy["app_login"]
        and marker in str(comment.get("body", ""))
        for comment in comments
    ):
        api.request(
            f"issues/{existing['number']}/comments",
            method="POST",
            data={
                "body": f"{marker}\nPublished `{published_sha}` for `{args.task}`. "
                f"[Validation job]({receipt}) completed successfully. "
                "Reviewers must independently assess the change "
                "and any remaining feedback.",
            },
        )
    github_outputs({"published_sha": published_sha, "pr_number": existing["number"]})
    print(
        json.dumps(
            {
                "published_sha": published_sha,
                "pr_number": existing["number"],
                "paths": paths,
                "validation_url": receipt,
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    verify = subparsers.add_parser("validate")
    verify.add_argument("--policy", default=".github/maintainer/policy.toml")
    verify.add_argument("--prepared", required=True)
    verify.add_argument("--patch", required=True)
    verify.add_argument("--checkout", required=True)
    trusted = subparsers.add_parser("publish")
    trusted.add_argument("--policy", default=".github/maintainer/policy.toml")
    trusted.add_argument("--task", required=True)
    trusted.add_argument("--lease", required=True)
    trusted.add_argument("--patch", required=True)
    trusted.add_argument("--checkout", required=True)
    trusted.add_argument("--validation-run", required=True)
    args = parser.parse_args()
    try:
        {"validate": validate, "publish": publish}[args.command](args)
    except (WorkError, GitHubError, OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"Publisher refused: {exc}\n")


if __name__ == "__main__":
    main()
