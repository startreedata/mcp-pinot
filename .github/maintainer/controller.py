"""Reconcile repository maintenance from trusted GitHub metadata, never PR code."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
import uuid

from github_api import GitHub, GitHubError
from policy import approver_logins, evaluate, merge_rule_blockers
from state import StateStore, initial_state, issue_hash
from worker import writer_identity

from config import load_policy, validate_paths

TERMINAL = {"DONE", "NEEDS_HUMAN"}
THREAD_QUERY = """
query($owner:String!, $name:String!, $number:Int!, $cursor:String) {
  repository(owner:$owner, name:$name) {
    pullRequest(number:$number) {
      reviewThreads(first:100, after:$cursor) {
        nodes { id isResolved isOutdated path line
          comments(first:100) { nodes { body author { login } }
            pageInfo { hasNextPage } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"""


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class Controller:
    def __init__(self, api, policy, event=None, *, dry_run=False, now=None):
        self.api = api
        self.policy = policy
        self.event = event or {}
        self.dry_run = dry_run or policy["mode"] == "observe"
        self.now = time.time() if now is None else now
        self.store = StateStore(api, policy)
        self.state = self.store.load() if policy["state_issue"] else initial_state()
        self.actions = []
        self.permissions = {}
        self.ci_ids = None
        if not self.dry_run:
            writer_identity(api, policy)

    def record(self, action, **fields):
        self.actions.append({"action": action, **fields})

    def save(self):
        if not self.dry_run:
            self.store.save(self.state)

    def write_permission(self, login):
        if login not in self.permissions:
            try:
                permission = self.api.request(
                    f"collaborators/{quote(login, safe='')}/permission"
                )["permission"]
                self.permissions[login] = permission in {"write", "maintain", "admin"}
            except GitHubError as exc:
                if exc.status != 404:
                    raise
                self.permissions[login] = False
        return self.permissions[login]

    def transition(self, key, task, state, reason=""):
        changed = task.get("state") != state or task.get("reason", "") != reason
        task.update(state=state, reason=reason)
        if changed:
            self.record("state", task=key, state=state, reason=reason)
            if state in {
                "AUTHORIZED",
                "WAITING_REVIEW",
                "NEEDS_HUMAN",
                "MERGED_PENDING_CI",
                "DONE",
            }:
                task["notice_pending"] = state
        self.save()
        if state == "NEEDS_HUMAN" and not self.dry_run:
            self.api.request(
                f"issues/{task.get('pr_number') or task['source_number']}/labels",
                "POST",
                {"labels": [self.policy["needs_human_label"]]},
            )

    def operator_command(self):
        if self.event.get("action") != "created" or "comment" not in self.event:
            return
        comment = self.event["comment"]
        command = str(comment.get("body") or "").strip()
        if command not in {
            "/maintainer resume",
            "/maintainer pause-repo",
            "/maintainer resume-repo",
        }:
            return
        login = comment.get("user", {}).get("login", "")
        if not login or not self.write_permission(login):
            self.record("unauthorized-command")
            return
        live = self.api.request(f"issues/comments/{comment['id']}")
        if (
            live.get("body") != comment.get("body")
            or live.get("updated_at") != comment.get("updated_at")
            or live.get("user", {}).get("login") != login
        ):
            raise ValueError("Operator comment changed since its event")
        number = self.event["issue"]["number"]
        if command.endswith("-repo"):
            if number != self.policy["state_issue"]:
                self.record(
                    "ignored-command",
                    reason="Repository commands belong on the ledger issue",
                )
                return
            if command == "/maintainer resume-repo":
                tip = self.api.request(
                    f"git/ref/heads/{self.policy['default_branch']}"
                )["object"]["sha"]
                status, _ = self.main_ci({"merge_sha": tip})
                if status != "success":
                    self.record(
                        "resume-blocked",
                        reason="Every current main workflow must succeed",
                    )
                    return
                self.state.update(
                    paused=False,
                    pause_reason="",
                    repo_notice_pending="Repository resumed",
                )
            else:
                self.state.update(
                    paused=True,
                    pause_reason=f"Paused by {login}",
                    repo_notice_pending="Repository paused",
                )
            self.save()
            return
        for key, task in self.state["tasks"].items():
            if number not in {task["source_number"], task.get("pr_number")}:
                continue
            if task["state"] != "NEEDS_HUMAN":
                self.record(
                    "resume-blocked",
                    task=key,
                    reason="Only an escalated task can resume",
                )
                continue
            source = self.api.request(f"issues/{task['source_number']}")
            pr = self.recover_pr(task)
            if pr and pr.get("merged"):
                task["merge_sha"] = pr["merge_commit_sha"]
                self.transition(key, task, "MERGED_PENDING_CI")
                continue
            if task["kind"] == "issue":
                if number != task["source_number"] or issue_hash(
                    self.event["issue"]
                ) != issue_hash(source):
                    self.record(
                        "resume-blocked",
                        task=key,
                        reason="Resume on the current source issue snapshot",
                    )
                    continue
                task.update(authorized_by=login, issue_snapshot_hash=issue_hash(source))
            task["base_sha"] = self.api.request(
                f"git/ref/heads/{self.policy['default_branch']}"
            )["object"]["sha"]
            task["expected_sha"] = pr["head"]["sha"] if pr else task["base_sha"]
            # Preserve all counters and charged reservations across human resumes.
            task.pop("last_signal", None)
            self.transition(key, task, "WAITING_REVIEW" if pr else "AUTHORIZED")

    def authorize_issues(self, issues):
        for issue in issues:
            if "pull_request" in issue or issue["number"] == self.policy["state_issue"]:
                continue
            labels = {label["name"] for label in issue["labels"]}
            if self.policy["ready_label"] not in labels:
                continue
            key = f"issue:{issue['number']}"
            fingerprint = issue_hash(issue)
            old = self.state["tasks"].get(key)
            if old and old.get("issue_snapshot_hash") == fingerprint:
                continue
            author = issue["user"]["login"]
            authorized = author if self.write_permission(author) else None
            # External issues are authorized only by a fresh maintainer label event
            # containing exactly the currently observed issue content.
            event_issue = self.event.get("issue", {})
            if (
                self.event.get("action") == "labeled"
                and self.event.get("label", {}).get("name")
                == self.policy["ready_label"]
                and event_issue.get("number") == issue["number"]
                and issue_hash(event_issue) == fingerprint
            ):
                sender = self.event.get("sender", {}).get("login", "")
                if sender and self.write_permission(sender):
                    authorized = sender
            if not authorized:
                self.record("unauthorized", task=key)
                if old and old.get("state") not in {"DONE", "MERGED_PENDING_CI"}:
                    self.transition(
                        key,
                        old,
                        "NEEDS_HUMAN",
                        "Issue content changed; reapply the ready label to authorize "
                        "this snapshot",
                    )
                continue
            if old:
                if old.get("issue_snapshot_hash") != fingerprint:
                    # Never replace a running reservation or reset its budget.
                    self.transition(
                        key,
                        old,
                        "NEEDS_HUMAN",
                        "Issue content changed; finish or cancel the existing task "
                        "before resuming",
                    )
                continue
            base = self.api.request(f"git/ref/heads/{self.policy['default_branch']}")[
                "object"
            ]["sha"]
            task = {
                "kind": "issue",
                "source_number": issue["number"],
                "authorized_by": authorized,
                "issue_snapshot_hash": fingerprint,
                "base_sha": base,
                "expected_sha": base,
                "branch": f"ai/issue-{issue['number']}",
                "repair_round": 0,
                "attempts": 0,
                "reserved_total": 0.0,
            }
            self.state["tasks"][key] = task
            self.transition(key, task, "AUTHORIZED")

    def adopt_dependency(self, pr):
        if not self.policy["dependencies"]["enabled"]:
            return
        labels = {label["name"] for label in pr["labels"]}
        user = pr["user"]
        if (
            user.get("type") != "Bot"
            or user["login"] not in self.policy["dependencies"]["authors"]
            or not pr["head"].get("repo")
            or pr["head"]["repo"]["full_name"] != self.api.repo
            or pr["base"]["ref"] != self.policy["default_branch"]
            or self.policy["hold_label"] in labels
        ):
            return
        key = f"pr:{pr['number']}"
        if key in self.state["tasks"]:
            return
        files = self.api.paginate(f"pulls/{pr['number']}/files")
        paths = {file["filename"] for file in files}
        expected = set(self.policy["dependencies"]["manifest_paths"])
        if (
            len(files) != pr["changed_files"]
            or not paths
            or not paths <= expected
            or validate_paths(files, self.policy)
        ):
            self.record(
                "dependency-manual",
                pr=pr["number"],
                reason="Change exceeds dependency repair scope",
            )
            return
        task = {
            "kind": "dependency",
            "source_number": pr["number"],
            "pr_number": pr["number"],
            "authorized_by": "repository-policy",
            "dependency_author": user["login"],
            "base_sha": pr["base"]["sha"],
            "expected_sha": pr["head"]["sha"],
            "branch": pr["head"]["ref"],
            "repair_round": 0,
            "attempts": 0,
            "reserved_total": 0.0,
        }
        self.state["tasks"][key] = task
        self.transition(key, task, "WAITING_REVIEW")
        if not self.dry_run and self.policy["managed_label"] not in labels:
            self.api.request(
                f"issues/{pr['number']}/labels",
                "POST",
                {"labels": [self.policy["managed_label"]]},
            )

    def review_context(self, pr):
        owner, name = self.api.repo.split("/", 1)
        cursor = None
        comments = []
        while True:
            data = self.api.graphql(
                THREAD_QUERY,
                {
                    "owner": owner,
                    "name": name,
                    "number": pr["number"],
                    "cursor": cursor,
                },
            )
            threads = data["repository"]["pullRequest"]["reviewThreads"]
            for thread in threads["nodes"]:
                if thread["isResolved"] or thread["isOutdated"]:
                    continue
                if thread["comments"]["pageInfo"]["hasNextPage"]:
                    raise ValueError(
                        "Review discussion too large; requires human review"
                    )
                for comment in thread["comments"]["nodes"]:
                    login = (comment.get("author") or {}).get("login", "")
                    if login and (
                        login.casefold()
                        in {actor.casefold() for actor in approver_logins(self.policy)}
                        or self.write_permission(login)
                    ):
                        comments.append(
                            {
                                "thread": thread["id"],
                                "path": thread["path"],
                                "line": thread["line"],
                                "author": login,
                                "body": comment["body"][:8000],
                            }
                        )
            page = threads["pageInfo"]
            if not page["hasNextPage"]:
                break
            next_cursor = page["endCursor"]
            if not next_cursor or next_cursor == cursor:
                raise ValueError("Incomplete review thread pagination")
            cursor = next_cursor
        checks = self.api.paginate(f"commits/{pr['head']['sha']}/check-runs")
        failures = []
        expected = {
            (check["name"], check["app_id"])
            for check in self.policy["merge"]["required_checks"]
        }
        for check in checks:
            if (
                (check["name"], check["app"]["id"]) in expected
                and check["status"] == "completed"
                and check["conclusion"]
                in {
                    "failure",
                    "timed_out",
                    "cancelled",
                    "action_required",
                    "startup_failure",
                }
            ):
                failures.append(
                    {
                        "name": check["name"],
                        "id": check["id"],
                        "url": check.get("details_url", ""),
                        "output": check.get("output", {}),
                    }
                )
        reviews = self.api.paginate(f"pulls/{pr['number']}/reviews")
        formal = []
        for review in reviews:
            login = review["user"]["login"]
            if (
                review["state"] == "CHANGES_REQUESTED"
                and review.get("commit_id") == pr["head"]["sha"]
                and self.write_permission(login)
            ):
                formal.append(
                    {
                        "id": review["id"],
                        "author": login,
                        "body": (review.get("body") or "")[:8000],
                    }
                )
        context = {
            "review_comments": comments,
            "reviews": formal,
            "failed_checks": failures,
        }
        if len(json.dumps(context)) > 24000:
            raise ValueError("Repair context exceeds bounded input limit")
        return context

    def reserve(self, key, task, kind, context=None):
        limits = self.policy["limits"]
        if self.state["paused"]:
            self.record("paused", task=key)
            return
        active = sum(
            item.get("state") == "WORKING" for item in self.state["tasks"].values()
        )
        if active >= limits["max_workers"]:
            self.record("queued", task=key)
            return
        if task["attempts"] >= limits["max_attempts"]:
            self.transition(key, task, "NEEDS_HUMAN", "Task attempt limit reached")
            return
        if kind == "repair" and task["repair_round"] >= limits["max_repair_rounds"]:
            self.transition(key, task, "NEEDS_HUMAN", "Repair round limit reached")
            return
        budget = self.state["budget"]
        cap = limits["per_run_usd"]
        if budget["spent"] + cap > limits["daily_usd"]:
            self.record("budget-wait", task=key)
            return
        task.update(
            lease=str(uuid.uuid4()),
            lease_until=self.now + limits["lease_seconds"],
            worker_kind=kind,
            budget_usd=cap,
            budget_day=budget["day"],
            attempts=task["attempts"] + 1,
            reserved_total=task["reserved_total"] + cap,
        )
        if kind == "repair":
            task["repair_round"] += 1
            task["context"] = context
            task["last_signal"] = digest(context)
        budget["spent"] += cap
        # Persist authorization and reservation before any dispatch. Failures never
        # refund a cap; a lost dispatch is escalated instead of replayed blindly.
        self.transition(key, task, "WORKING")
        self.record(
            "dispatch", task=key, kind=kind, lease=task["lease"], budget_usd=cap
        )
        if not self.dry_run:
            try:
                self.api.request(
                    f"actions/workflows/{self.policy['worker_workflow']}/dispatches",
                    "POST",
                    {
                        "ref": self.policy["default_branch"],
                        "inputs": {"task": key, "lease": task["lease"]},
                    },
                )
            except (GitHubError, OSError) as exc:
                self.transition(
                    key,
                    task,
                    "NEEDS_HUMAN",
                    f"Worker dispatch outcome unknown ({type(exc).__name__}); "
                    "reservation retained",
                )

    def worker_finished(self, task):
        run = self.event.get("workflow_run", {})
        title = run.get("display_title", "")
        return (
            run.get("path", "").split("@", 1)[0]
            == f".github/workflows/{self.policy['worker_workflow']}"
            and task.get("lease", "!missing!") in title
            and run.get("event") == "workflow_dispatch"
            and run.get("status") == "completed"
            and run.get("head_branch") == self.policy["default_branch"]
            and run.get("actor", {}).get("login") == self.policy["app_login"]
        )

    def recover_pr(self, task):
        if task.get("pr_number"):
            return self.api.request(f"pulls/{task['pr_number']}")
        owner = self.api.repo.split("/", 1)[0]
        prs = self.api.paginate(
            f"pulls?state=all&head={quote(owner + ':' + task['branch'], safe='')}"
        )
        matches = [
            pr
            for pr in prs
            if pr["head"]["ref"] == task["branch"]
            and pr["base"]["ref"] == self.policy["default_branch"]
            and pr["head"]["repo"]
            and pr["head"]["repo"]["full_name"] == self.api.repo
            and pr["user"]["login"] == self.policy["app_login"]
            and f"<!-- repo-maintainer-task:issue:{task['source_number']} -->"
            in (pr.get("body") or "")
        ]
        if len(matches) > 1:
            raise ValueError("Multiple PRs for one task branch")
        if matches:
            task["pr_number"] = matches[0]["number"]
            self.save()
            return self.api.request(f"pulls/{task['pr_number']}")
        return None

    def main_ci(self, task):
        if self.ci_ids is None:
            workflows = self.api.paginate("actions/workflows")
            self.ci_ids = {}
            for name in self.policy["ci_workflows"]:
                matches = [
                    workflow
                    for workflow in workflows
                    if workflow.get("name") == name
                    and workflow.get("state") == "active"
                    and str(workflow.get("path", "")).startswith(".github/workflows/")
                ]
                if len(matches) != 1 or type(matches[0].get("id")) is not int:
                    raise ValueError(f"Main CI workflow identity is unknown: {name}")
                self.ci_ids[name] = matches[0]["id"]
        sha = task["merge_sha"]
        runs = self.api.paginate(f"actions/runs?head_sha={sha}&event=push")
        relevant = [
            run
            for run in runs
            if run["head_sha"] == sha
            and run["head_branch"] == self.policy["default_branch"]
            and run.get("event") == "push"
            and run.get("workflow_id") == self.ci_ids.get(run.get("name"))
        ]
        names = self.policy["ci_workflows"]
        latest = {}
        for run in relevant:
            if run["name"] in names and (
                run["name"] not in latest or run["id"] > latest[run["name"]]["id"]
            ):
                latest[run["name"]] = run
        failed = [
            run["name"]
            for run in latest.values()
            if run["status"] == "completed" and run["conclusion"] != "success"
        ]
        if failed:
            self.state["paused"] = True
            self.state["repo_notice_pending"] = "Main CI failed; repository paused"
            self.state["pause_reason"] = f"Main CI failed at {sha}: {', '.join(failed)}"
            self.save()
            return "failed", self.state["pause_reason"]
        if names and all(
            name in latest
            and latest[name]["status"] == "completed"
            and latest[name]["conclusion"] == "success"
            for name in names
        ):
            return "success", ""
        return "pending", ""

    def policy_check(self, pr, blockers):
        app_id = self.policy["merge"]["policy_check_app_id"]
        self.record("policy", pr=pr["number"], sha=pr["head"]["sha"], blockers=blockers)
        if self.dry_run or not app_id:
            return
        conclusion = "action_required" if blockers else "success"
        summary = (
            "\n".join(f"- {reason}" for reason in blockers)
            or "Current head satisfies controller policy. "
            "GitHub branch rules still apply."
        )
        policy_token = os.environ.get("MAINTAINER_POLICY_TOKEN")
        check_api = (
            GitHub(repo=self.api.repo, token=policy_token) if policy_token else self.api
        )
        check = check_api.request(
            "check-runs",
            "POST",
            {
                "name": "maintainer/policy",
                "head_sha": pr["head"]["sha"],
                "status": "completed",
                "conclusion": conclusion,
                "output": {
                    "title": "Repository maintenance policy",
                    "summary": summary[:60000],
                },
            },
        )
        if check.get("app", {}).get("id") != app_id:
            raise ValueError("Policy check was emitted by an unexpected GitHub App")

    def reconcile_task(self, key, task, issues_by_number):
        if task["state"] in TERMINAL:
            return
        if task["state"] == "MERGED_PENDING_CI":
            status, reason = self.main_ci(task)
            if status == "success":
                self.transition(
                    key, task, "DONE", f"Main CI passed at {task['merge_sha']}"
                )
            elif status == "failed":
                self.transition(key, task, "NEEDS_HUMAN", reason)
            return
        pr = self.recover_pr(task)
        if pr and pr.get("merged"):
            task["merge_sha"] = pr["merge_commit_sha"]
            self.transition(key, task, "MERGED_PENDING_CI")
            return
        if task["kind"] == "issue":
            issue = issues_by_number.get(task["source_number"])
            if issue is None:
                issue = self.api.request(f"issues/{task['source_number']}")
            if issue_hash(issue) != task["issue_snapshot_hash"]:
                self.transition(
                    key, task, "NEEDS_HUMAN", "Authorized issue content changed"
                )
                return
            if not self.write_permission(task["authorized_by"]):
                self.transition(
                    key, task, "NEEDS_HUMAN", "Maintainer authorization was revoked"
                )
                return
            if (
                issue["state"] != "open"
                or self.policy["hold_label"]
                in {label["name"] for label in issue["labels"]}
                or self.policy["ready_label"]
                not in {label["name"] for label in issue["labels"]}
            ):
                self.transition(
                    key,
                    task,
                    "NEEDS_HUMAN",
                    "Issue closed, held, or authorization label removed",
                )
                return
        if task["state"] == "WORKING":
            finished = self.worker_finished(task)
            if finished or self.now > task["lease_until"]:
                run = self.event.get("workflow_run", {})
                if not finished or run.get("conclusion") != "success":
                    self.transition(
                        key,
                        task,
                        "NEEDS_HUMAN",
                        "Worker failed or lease expired; inspect its run "
                        "before resuming",
                    )
                    return
                if not pr or pr["head"]["sha"] == task["expected_sha"]:
                    self.transition(
                        key,
                        task,
                        "NEEDS_HUMAN",
                        "Worker completed without a published change",
                    )
                    return
                head = pr["head"]["sha"]
                comments = self.api.paginate(f"issues/{pr['number']}/comments")
                marker = f"<!-- repo-maintainer-published:{task['lease']} -->"
                published = f"Published `{head}` for `{key}`."
                receipts = [
                    comment
                    for comment in comments
                    if comment.get("user", {}).get("login") == self.policy["app_login"]
                    and comment.get("user", {}).get("type")
                    == self.policy.get("writer_type", "Bot")
                    and marker in (comment.get("body") or "")
                    and published in (comment.get("body") or "")
                ]
                commit = self.api.request(f"git/commits/{head}")
                parents = [parent["sha"] for parent in commit.get("parents", [])]
                if (
                    len(receipts) != 1
                    or parents != [task["expected_sha"]]
                    or f"Maintainer-Lease: {task['lease']}"
                    not in commit.get("message", "")
                ):
                    self.transition(
                        key,
                        task,
                        "NEEDS_HUMAN",
                        "Current head differs from authenticated worker publication",
                    )
                    return
                task["expected_sha"] = head
                task.pop("context", None)
                self.transition(key, task, "WAITING_REVIEW")
            else:
                # Periodic recovery also finds successful runs whose event was lost.
                runs = self.api.paginate(
                    f"actions/workflows/{self.policy['worker_workflow']}/runs?event=workflow_dispatch"
                )
                for run in runs:
                    if (
                        task["lease"] in run.get("display_title", "")
                        and run.get("actor", {}).get("login")
                        == self.policy["app_login"]
                        and run.get("head_branch") == self.policy["default_branch"]
                        and run.get("status") == "completed"
                    ):
                        self.event = {"workflow_run": run}
                        self.reconcile_task(key, task, issues_by_number)
                        return
                return
        if not pr:
            self.reserve(key, task, "implement")
            return
        if pr.get("merged"):
            task["merge_sha"] = pr["merge_commit_sha"]
            self.transition(key, task, "MERGED_PENDING_CI")
            return
        if pr["state"] != "open":
            self.transition(key, task, "NEEDS_HUMAN", "PR closed without merging")
            return
        labels = {label["name"] for label in pr["labels"]}
        if self.policy["hold_label"] in labels:
            self.transition(key, task, "NEEDS_HUMAN", "Maintainer hold")
            return
        if pr["head"]["sha"] != task["expected_sha"]:
            self.transition(
                key,
                task,
                "NEEDS_HUMAN",
                "PR head changed outside the reserved worker; "
                "reauthorize before editing",
            )
            return
        blockers = evaluate(self.api, self.policy, pr, task, require_mergeable=False)
        self.policy_check(pr, blockers)
        path_blockers = validate_paths(
            self.api.paginate(f"pulls/{pr['number']}/files"), self.policy
        )
        if path_blockers:
            self.transition(key, task, "NEEDS_HUMAN", "; ".join(path_blockers))
            return
        if pr.get("draft"):
            self.record("waiting", task=key, blockers=["Draft PR"])
            return
        try:
            context = self.review_context(pr)
        except ValueError as exc:
            self.transition(key, task, "NEEDS_HUMAN", str(exc))
            return
        if any(context.values()) and digest(context) != task.get("last_signal"):
            self.reserve(key, task, "repair", context)
            return
        if blockers:
            self.record("waiting", task=key, blockers=blockers)
            return
        if self.policy["mode"] != "autopilot" or not self.policy["merge"]["enabled"]:
            self.record("ready-for-human-merge", task=key)
            return
        if self.state["paused"]:
            self.record("paused", task=key)
            return
        rule_blockers = merge_rule_blockers(self.api, self.policy)
        if rule_blockers:
            self.record("merge-rules-blocked", task=key, blockers=rule_blockers)
            return
        # Re-fetch and reevaluate immediately before SHA-bound merge; a changed
        # head or base invalidates the earlier decision.
        current = self.api.request(f"pulls/{pr['number']}")
        blockers = evaluate(self.api, self.policy, current, task)
        if current["head"]["sha"] != pr["head"]["sha"] or blockers:
            self.record("merge-race", task=key, blockers=blockers)
            return
        if task["kind"] == "issue":
            source = self.api.request(f"issues/{task['source_number']}")
            login = task["authorized_by"]
            self.permissions.pop(login, None)
            labels = {label["name"] for label in source["labels"]}
            if (
                issue_hash(source) != task["issue_snapshot_hash"]
                or source["state"] != "open"
                or self.policy["ready_label"] not in labels
                or self.policy["hold_label"] in labels
                or not self.write_permission(login)
            ):
                self.transition(
                    key,
                    task,
                    "NEEDS_HUMAN",
                    "Source authorization changed before merge",
                )
                return
        self.record("merge", task=key, sha=current["head"]["sha"])
        if not self.dry_run:
            response = self.api.request(
                f"pulls/{pr['number']}/merge",
                "PUT",
                {
                    "sha": current["head"]["sha"],
                    "merge_method": "squash",
                },
            )
            if response.get("merged"):
                task["merge_sha"] = response["sha"]
                self.transition(key, task, "MERGED_PENDING_CI")

    def notify(self):
        channel = self.policy["slack"]["channel"]
        token = os.environ.get("SLACK_BOT_TOKEN")
        notices = list(self.state["tasks"].items())
        if self.state.get("repo_notice_pending"):
            notices.append(("repository", self.state))
        for key, task in notices:
            notice_field = (
                "repo_notice_pending" if key == "repository" else "notice_pending"
            )
            notice = task.get(notice_field)
            if not notice:
                continue
            if not channel or not token or self.dry_run:
                self.record(
                    "notification",
                    task=key,
                    state=notice,
                    delivery="not-configured"
                    if not channel or not token
                    else "dry-run",
                )
                continue
            text = f"{self.api.repo} / {key}\nState: {notice}"
            reason = (
                task.get("pause_reason") if key == "repository" else task.get("reason")
            )
            if reason:
                text += f"\n{reason}"
            number = task.get("pr_number") or task.get(
                "source_number", self.policy["state_issue"]
            )
            route = "pull" if task.get("pr_number") else "issues"
            text += f"\nhttps://github.com/{self.api.repo}/{route}/{number}"
            payload = {
                "channel": channel,
                "text": text,
                "unfurl_links": False,
                "unfurl_media": False,
                "client_msg_id": str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        digest(
                            {
                                "repo": self.api.repo,
                                "key": key,
                                "notice": notice,
                                "lease": task.get("lease"),
                                "head": task.get("expected_sha"),
                                "reason": reason,
                            }
                        ),
                    )
                ),
            }
            if task.get("slack_thread_ts"):
                payload["thread_ts"] = task["slack_thread_ts"]
            request = Request(
                "https://slack.com/api/chat.postMessage",
                data=json.dumps(payload).encode(),
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            try:
                with urlopen(request, timeout=20) as response:  # noqa: S310
                    result = json.load(response)
                if not result.get("ok"):
                    raise ValueError(result.get("error", "Slack delivery failed"))
                task.setdefault("slack_thread_ts", result["ts"])
                task.pop(notice_field, None)
                self.save()
            except (HTTPError, OSError, ValueError):
                self.record("notification-retry", task=key)

    def run(self):
        day = datetime.fromtimestamp(self.now, UTC).date().isoformat()
        if self.state["budget"]["day"] != day:
            self.state["budget"] = {"day": day, "spent": 0.0}
            self.save()
        issues = self.api.paginate("issues?state=open")
        prs = self.api.paginate("pulls?state=open")
        by_number = {issue["number"]: issue for issue in issues}
        # A completed push failure pauses all future work before evaluating PRs.
        run = self.event.get("workflow_run", {})
        if (
            run.get("name") in self.policy["ci_workflows"]
            and run.get("head_branch") == self.policy["default_branch"]
            and run.get("event") == "push"
            and run.get("status") == "completed"
            and run.get("conclusion") != "success"
        ):
            self.state.update(
                paused=True,
                pause_reason=f"Main CI failed: {run['name']} at {run['head_sha']}",
                repo_notice_pending="Main CI failed; repository paused",
            )
            self.save()
            self.record("repository-paused", reason=self.state["pause_reason"])
        self.operator_command()
        self.authorize_issues(issues)
        for summary in prs:
            pr = self.api.request(f"pulls/{summary['number']}")
            self.adopt_dependency(pr)
        # First check every post-merge task. Never merge another PR ahead of a
        # failure already present on a previous merge commit.
        tasks = list(self.state["tasks"].items())
        tasks.sort(key=lambda item: item[1].get("state") != "MERGED_PENDING_CI")
        for key, task in tasks:
            self.reconcile_task(key, task, by_number)
        managed = {task.get("pr_number") for task in self.state["tasks"].values()}
        for pr in prs:
            if pr["number"] not in managed:
                self.policy_check(pr, [])
        self.notify()
        return {
            "repository": self.api.repo,
            "mode": self.policy["mode"],
            "dry_run": self.dry_run,
            "paused": self.state["paused"],
            "actions": self.actions,
            "budget": self.state["budget"],
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", default=".github/maintainer/policy.toml")
    parser.add_argument("--event", default=os.environ.get("GITHUB_EVENT_PATH"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    event = json.loads(Path(args.event).read_text()) if args.event else {}
    result = Controller(
        GitHub(), load_policy(args.policy), event, dry_run=args.dry_run
    ).run()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
