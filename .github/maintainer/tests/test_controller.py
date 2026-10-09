"""Offline reconciliation regressions: authorization, leases, budget and main CI."""

from copy import deepcopy
from datetime import UTC, datetime
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from controller import Controller
from state import initial_state, issue_hash

from config import DEFAULT_POLICY

NOW = 1791514800
BASE = "a" * 40
HEAD = "b" * 40
MERGED = "c" * 40


def policy():
    p = deepcopy(DEFAULT_POLICY)
    p.update(
        mode="maintain",
        app_login="maintainer[bot]",
        state_issue=99,
        allowed_paths=["src/**", "tests/**", "pyproject.toml", "uv.lock"],
        validation_commands=["python -m unittest discover"],
        ci_workflows=["Build", "Lint"],
    )
    p["merge"]["independent_approvers"] = ["reviewer", "copilot[bot]"]
    p["merge"]["required_checks"] = [{"name": "unit", "app_id": 15368}]
    p["dependencies"]["manifest_paths"] = ["pyproject.toml", "uv.lock"]
    return p


def issue(author="owner"):
    return {
        "number": 1,
        "title": "Fix input",
        "body": "Acceptance: handles empty input",
        "user": {"login": author, "type": "User"},
        "state": "open",
        "labels": [{"name": "ai:ready"}],
    }


def pr(number=2):
    return {
        "number": number,
        "state": "open",
        "merged": False,
        "draft": False,
        "mergeable": True,
        "mergeable_state": "clean",
        "changed_files": 1,
        "user": {"login": "maintainer[bot]", "type": "Bot"},
        "labels": [{"name": "ai:managed"}],
        "body": "<!-- repo-maintainer-task:issue:1 -->",
        "head": {
            "sha": HEAD,
            "ref": "ai/issue-1",
            "repo": {"full_name": "example/repo"},
        },
        "base": {"sha": BASE, "ref": "main"},
    }


def task(state="WAITING_REVIEW"):
    return {
        "kind": "issue",
        "source_number": 1,
        "authorized_by": "owner",
        "issue_snapshot_hash": issue_hash(issue()),
        "base_sha": BASE,
        "expected_sha": HEAD if state != "AUTHORIZED" else BASE,
        "branch": "ai/issue-1",
        "pr_number": 2,
        "state": state,
        "attempts": 1,
        "repair_round": 0,
        "reserved_total": 5.0,
        "lease": "abcd-lease",
        "lease_until": NOW + 5400,
        "budget_usd": 5.0,
    }


class MemoryStore:
    def __init__(self, api, _policy):
        self.api = api

    def load(self):
        return deepcopy(self.api.ledger)

    def save(self, state):
        self.api.ledger = deepcopy(state)
        self.api.saves += 1


class FakeGitHub:
    repo = "example/repo"

    def __init__(self):
        self.ledger = initial_state()
        self.ledger["budget"] = {
            "day": datetime.fromtimestamp(NOW, UTC).date().isoformat(),
            "spent": 5.0,
        }
        self.issues = []
        self.prs = {}
        self.runs = []
        self.checks = []
        self.reviews = []
        self.permissions = {"owner": "write", "reviewer": "write"}
        self.calls = []
        self.saves = 0
        self.threads = []
        self.comments = {}

    def request(self, path, method="GET", data=None):
        self.calls.append((path, method, data))
        if method != "GET":
            if path == "check-runs":
                return {"app": {"id": 12345}, **data}
            return {"merged": True, "sha": MERGED}
        if path.startswith("collaborators/"):
            return {
                "permission": self.permissions.get(path.split("/")[1], "read"),
                "user": {"type": "User"},
            }
        if path.startswith("git/commits/"):
            working = self.ledger["tasks"]["issue:1"]
            return {
                "parents": [{"sha": working["expected_sha"]}],
                "message": f"Maintainer-Lease: {working['lease']}",
            }
        if path.startswith("rules/branches/"):
            return [
                {
                    "type": "required_status_checks",
                    "parameters": {
                        "strict_required_status_checks_policy": True,
                        "required_status_checks": [
                            {"context": "unit", "integration_id": 15368},
                            {"context": "maintainer/policy", "integration_id": 12345},
                        ],
                    },
                },
                {
                    "type": "pull_request",
                    "parameters": {
                        "required_approving_review_count": 1,
                        "dismiss_stale_reviews_on_push": True,
                        "required_review_thread_resolution": True,
                    },
                },
            ]
        if path.startswith("git/ref/"):
            return {"object": {"sha": BASE}}
        if path.startswith("compare/"):
            return {
                "merge_base_commit": {"sha": BASE},
                "status": "ahead",
                "files": [{"filename": "src/fix.py"}],
            }
        if path.startswith("pulls/"):
            return deepcopy(self.prs[int(path.split("/")[1])])
        if path.startswith("issues/comments/"):
            return deepcopy(self.comments[int(path.split("/")[-1])])
        if path.startswith("issues/"):
            return deepcopy(
                next(
                    item
                    for item in self.issues
                    if item["number"] == int(path.split("/")[1])
                )
            )
        raise AssertionError(path)

    def paginate(self, path):
        if path.startswith("rules/branches/"):
            return self.request(path)
        if path == "issues?state=open":
            return deepcopy([item for item in self.issues if item["state"] == "open"])
        if path.startswith("pulls?"):
            return deepcopy(list(self.prs.values()))
        if path.startswith("issues/") and path.endswith("/comments"):
            working = self.ledger["tasks"].get("issue:1", {})
            return [
                {
                    "user": {"login": "maintainer[bot]", "type": "Bot"},
                    "body": (
                        f"<!-- repo-maintainer-published:{working.get('lease')} -->\n"
                        f"Published `{HEAD}` for `issue:1`."
                    ),
                }
            ]
        if path.endswith("/files"):
            return [{"filename": "src/fix.py", "status": "modified"}]
        if "check-runs" in path:
            return deepcopy(self.checks)
        if path.endswith("/reviews"):
            return deepcopy(self.reviews)
        if path == "actions/workflows":
            return [
                {
                    "id": n,
                    "name": name,
                    "path": f".github/workflows/{name}.yml",
                    "state": "active",
                }
                for n, name in enumerate(("Build", "Lint"), 1)
            ]
        if "actions/" in path:
            return deepcopy(self.runs)
        raise AssertionError(path)

    def graphql(self, query, variables):
        return {
            "repository": {
                "pullRequest": {
                    "headRefOid": HEAD,
                    "reviewThreads": {
                        "totalCount": len(self.threads),
                        "nodes": deepcopy(self.threads),
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                }
            }
        }


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.api = FakeGitHub()
        self.policy = policy()
        self.actor_patch = patch.dict(
            os.environ, {"MAINTAINER_WRITER_ACTOR": "maintainer[bot]"}
        )
        self.actor_patch.start()
        self.addCleanup(self.actor_patch.stop)
        self.store_patch = patch("controller.StateStore", MemoryStore)
        self.store_patch.start()
        self.addCleanup(self.store_patch.stop)

    def controller(self, event=None, dry_run=False):
        return Controller(self.api, self.policy, event, dry_run=dry_run, now=NOW)

    def test_external_ready_label_does_not_dispatch(self):
        self.api.issues = [issue("outsider")]
        report = self.controller().run()
        self.assertFalse(self.api.ledger["tasks"])
        self.assertFalse(
            any(action["action"] == "dispatch" for action in report["actions"])
        )

    def test_fresh_label_by_writer_authorizes_exact_external_snapshot(self):
        external = issue("outsider")
        self.api.issues = [external]
        event = {
            "action": "labeled",
            "label": {"name": "ai:ready"},
            "issue": deepcopy(external),
            "sender": {"login": "owner"},
        }
        self.controller(event).run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "WORKING")
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["authorized_by"], "owner")

    def test_stale_label_event_cannot_authorize_changed_content(self):
        external = issue("outsider")
        event = {
            "action": "labeled",
            "label": {"name": "ai:ready"},
            "issue": deepcopy(external),
            "sender": {"login": "owner"},
        }
        external["body"] += "\nNew instruction"
        self.api.issues = [external]
        self.controller(event).run()
        self.assertFalse(self.api.ledger["tasks"])

    def test_duplicate_event_does_not_repeat_worker_or_charge_budget(self):
        self.api.issues = [issue()]
        self.controller().run()
        reserved = self.api.ledger["budget"]["spent"]
        self.controller().run()
        dispatches = [
            call for call in self.api.calls if call[0].endswith("/dispatches")
        ]
        self.assertEqual(len(dispatches), 1)
        self.assertEqual(self.api.ledger["budget"]["spent"], reserved)

    def test_observe_does_not_write_or_persist(self):
        self.api.issues = [issue()]
        self.controller(dry_run=True).run()
        self.assertEqual(self.api.saves, 0)
        self.assertFalse(any(method != "GET" for _, method, _ in self.api.calls))

    def test_issue_edit_keeps_budget_and_stops_old_lease(self):
        self.api.issues = [issue()]
        self.controller().run()
        reserved = self.api.ledger["budget"]["spent"]
        self.api.issues[0]["body"] += "changed"
        self.controller().run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "NEEDS_HUMAN")
        self.assertEqual(self.api.ledger["budget"]["spent"], reserved)

    def test_expired_lease_escalates_without_new_reservation(self):
        working = task("WORKING")
        working.pop("pr_number")
        working["lease_until"] = NOW - 1
        self.api.ledger["tasks"] = {"issue:1": working}
        self.api.issues = [issue()]
        self.controller().run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "NEEDS_HUMAN")
        self.assertEqual(self.api.ledger["budget"]["spent"], 5.0)

    def test_daily_cap_queues_without_dispatch(self):
        self.api.ledger["budget"]["spent"] = self.policy["limits"]["daily_usd"]
        self.api.issues = [issue()]
        self.controller().run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "AUTHORIZED")
        self.assertFalse(
            any(path.endswith("/dispatches") for path, _, _ in self.api.calls)
        )

    def test_independent_app_change_requests_reach_the_writer_repair_context(self):
        for mode in ("review", "approve"):
            with self.subTest(mode=mode):
                self.api = FakeGitHub()
                self.policy["review"].update(
                    {"mode": mode, "login": "independent[bot]", "identity_type": "Bot"}
                )
                self.api.ledger["tasks"] = {"issue:1": task()}
                self.api.issues = [issue()]
                self.api.prs[2] = pr()
                self.api.reviews = [
                    {
                        "id": 1,
                        "state": "CHANGES_REQUESTED",
                        "commit_id": HEAD,
                        "user": {"login": "independent[bot]", "type": "Bot"},
                        "body": "Null input still loses its guard.",
                    }
                ]
                self.controller().run()
                repaired = self.api.ledger["tasks"]["issue:1"]
                self.assertEqual(repaired["state"], "WORKING")
                self.assertEqual(repaired["worker_kind"], "repair")
                self.assertEqual(
                    repaired["context"]["reviews"][0]["author"], "independent[bot]"
                )

    def test_withdrawn_app_change_request_does_not_start_a_repair(self):
        self.policy["review"].update(
            {"mode": "approve", "login": "independent[bot]", "identity_type": "Bot"}
        )
        self.api.ledger["tasks"] = {"issue:1": task()}
        self.api.issues = [issue()]
        self.api.prs[2] = pr()
        self.api.checks = [
            {
                "id": 1,
                "name": "unit",
                "app": {"id": 15368},
                "head_sha": HEAD,
                "status": "completed",
                "conclusion": "success",
            }
        ]
        user = {"login": "independent[bot]", "type": "Bot"}
        self.api.reviews = [
            {
                "id": 1,
                "state": "CHANGES_REQUESTED",
                "commit_id": HEAD,
                "user": user,
                "body": "Old request",
            },
            {"id": 2, "state": "APPROVED", "commit_id": HEAD, "user": user},
        ]
        self.controller().run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "WAITING_REVIEW")
        self.assertFalse(
            any(path.endswith("/dispatches") for path, _, _ in self.api.calls)
        )

    def test_human_commit_prevents_repair_and_merge(self):
        self.api.ledger["tasks"] = {"issue:1": task()}
        self.api.issues = [issue()]
        self.api.prs[2] = pr()
        self.api.prs[2]["head"]["sha"] = "d" * 40
        self.controller().run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "NEEDS_HUMAN")
        self.assertFalse(
            any(
                path.endswith(("/dispatches", "/merge"))
                for path, _, _ in self.api.calls
            )
        )

    def test_post_merge_ci_checks_closed_issue_and_requires_every_workflow(self):
        merged = task("MERGED_PENDING_CI")
        merged["merge_sha"] = MERGED
        self.api.ledger["tasks"] = {"issue:1": merged}
        self.api.issues = [issue()]
        self.api.issues[0]["state"] = "closed"
        self.api.runs = [
            {
                "id": 1,
                "name": "Build",
                "workflow_id": 1,
                "event": "push",
                "head_sha": MERGED,
                "head_branch": "main",
                "status": "completed",
                "conclusion": "success",
            }
        ]
        self.controller().run()
        self.assertEqual(
            self.api.ledger["tasks"]["issue:1"]["state"], "MERGED_PENDING_CI"
        )
        self.api.runs.append(
            {**self.api.runs[0], "id": 2, "name": "Lint", "workflow_id": 2}
        )
        self.controller().run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "DONE")

    def test_main_failure_pauses_before_other_dispatch(self):
        self.api.issues = [issue()]
        event = {
            "workflow_run": {
                "name": "Build",
                "head_branch": "main",
                "event": "push",
                "status": "completed",
                "conclusion": "failure",
                "head_sha": BASE,
            }
        }
        self.controller(event).run()
        self.assertTrue(self.api.ledger["paused"])
        self.assertFalse(
            any(path.endswith("/dispatches") for path, _, _ in self.api.calls)
        )

    def test_issue_to_verified_main_completion(self):
        self.policy.update(mode="autopilot")
        self.policy["merge"].update(enabled=True, policy_check_app_id=12345)
        self.api.issues = [issue()]
        self.controller().run()
        working = deepcopy(self.api.ledger["tasks"]["issue:1"])
        self.api.prs[2] = pr()
        self.api.checks = [
            {
                "id": 1,
                "name": "unit",
                "app": {"id": 15368},
                "status": "completed",
                "conclusion": "success",
                "head_sha": HEAD,
            }
        ]
        self.api.reviews = [
            {
                "id": 1,
                "state": "APPROVED",
                "commit_id": HEAD,
                "user": {"login": "copilot[bot]", "type": "Bot"},
            }
        ]
        event = {
            "workflow_run": {
                "path": ".github/workflows/maintainer-worker.yml",
                "display_title": (
                    f"Repo Maintainer Worker | issue:1 | {working['lease']}"
                ),
                "event": "workflow_dispatch",
                "status": "completed",
                "conclusion": "success",
                "head_branch": "main",
                "actor": {"login": "maintainer[bot]"},
            }
        }
        self.controller(event).run()
        self.assertEqual(
            self.api.ledger["tasks"]["issue:1"]["state"], "MERGED_PENDING_CI"
        )
        merges = [call for call in self.api.calls if call[0].endswith("/merge")]
        self.assertEqual(merges[0][2], {"sha": HEAD, "merge_method": "squash"})
        self.api.issues[0]["state"] = "closed"
        self.api.runs = [
            {
                "id": n,
                "name": name,
                "workflow_id": n,
                "event": "push",
                "head_sha": MERGED,
                "head_branch": "main",
                "status": "completed",
                "conclusion": "success",
            }
            for n, name in enumerate(("Build", "Lint"), 1)
        ]
        self.controller().run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "DONE")

    def test_human_resume_preserves_all_limits_and_spend(self):
        old = task("NEEDS_HUMAN")
        old.update(attempts=2, repair_round=1, reserved_total=10.0)
        self.api.ledger["tasks"] = {"issue:1": old}
        source = issue()
        self.api.issues = [source]
        self.api.prs[2] = pr()
        comment = {
            "id": 7,
            "body": "/maintainer resume",
            "updated_at": "2026-10-08T01:00:00Z",
            "user": {"login": "owner", "type": "User"},
        }
        self.api.comments[7] = comment
        controller = self.controller(
            {"action": "created", "comment": comment, "issue": source}
        )
        controller.operator_command()
        resumed = self.api.ledger["tasks"]["issue:1"]
        self.assertEqual(resumed["state"], "WAITING_REVIEW")
        self.assertEqual(
            (resumed["attempts"], resumed["repair_round"], resumed["reserved_total"]),
            (2, 1, 10.0),
        )
        self.assertEqual(self.api.ledger["budget"]["spent"], 5.0)

    def test_name_spoofed_main_workflow_cannot_complete_task(self):
        merged = task("MERGED_PENDING_CI")
        merged["merge_sha"] = MERGED
        self.api.ledger["tasks"] = {"issue:1": merged}
        self.api.runs = [
            {
                "id": n,
                "name": name,
                "workflow_id": 999,
                "event": "push",
                "head_sha": MERGED,
                "head_branch": "main",
                "status": "completed",
                "conclusion": "success",
            }
            for n, name in enumerate(("Build", "Lint"), 1)
        ]
        self.controller().run()
        self.assertEqual(
            self.api.ledger["tasks"]["issue:1"]["state"], "MERGED_PENDING_CI"
        )

    def test_human_push_after_publisher_receipt_is_not_adopted(self):
        working = task("WORKING")
        working["expected_sha"] = BASE
        self.api.ledger["tasks"] = {"issue:1": working}
        self.api.issues = [issue()]
        self.api.prs[2] = pr()
        self.api.prs[2]["head"]["sha"] = "d" * 40
        event = {
            "workflow_run": {
                "path": ".github/workflows/maintainer-worker.yml",
                "display_title": (
                    f"Repo Maintainer Worker | issue:1 | {working['lease']}"
                ),
                "event": "workflow_dispatch",
                "status": "completed",
                "conclusion": "success",
                "head_branch": "main",
                "actor": {"login": "maintainer[bot]"},
            }
        }
        self.controller(event).run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "NEEDS_HUMAN")
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["expected_sha"], BASE)

    def test_issue_edit_during_final_merge_evidence_blocks_merge(self):
        self.policy.update(mode="autopilot")
        self.policy["merge"].update(enabled=True, policy_check_app_id=12345)
        self.api.issues = [issue()]
        self.api.ledger["tasks"] = {"issue:1": task()}
        self.api.prs[2] = pr()
        self.api.checks = [
            {
                "id": 1,
                "name": "unit",
                "app": {"id": 15368},
                "status": "completed",
                "conclusion": "success",
                "head_sha": HEAD,
            }
        ]
        self.api.reviews = [
            {
                "id": 1,
                "state": "APPROVED",
                "commit_id": HEAD,
                "user": {"login": "copilot[bot]", "type": "Bot"},
            }
        ]
        original = self.api.graphql
        count = 0

        def changed_during_evidence(query, variables):
            nonlocal count
            count += 1
            if count == 3:
                self.api.issues[0]["body"] += "Changed instruction"
            return original(query, variables)

        self.api.graphql = changed_during_evidence
        self.controller().run()
        self.assertEqual(self.api.ledger["tasks"]["issue:1"]["state"], "NEEDS_HUMAN")
        self.assertFalse(any(path.endswith("/merge") for path, _, _ in self.api.calls))


if __name__ == "__main__":
    unittest.main()
