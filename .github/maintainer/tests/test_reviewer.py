"""Independent review must not inherit the writer's identity or stale evidence."""

from copy import deepcopy
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from policy import approval_blockers
from reviewer import (
    collect_snapshot,
    prepare_review,
    publish_review,
    reviewer_identity,
    validate_result,
)
from state import StateStore, initial_state
from test_controller import HEAD, FakeGitHub, policy, pr
from worker import WorkError, writer_identity

from config import load_policy

CONTROL = "c" * 40
CLEAN = {
    "verdict": "approve",
    "summary": "Checked the complete input handling diff.",
    "findings": [],
}
FILES = [
    {
        "filename": "src/fix.py",
        "status": "modified",
        "additions": 1,
        "deletions": 1,
        "patch": "@@ -1 +1 @@\n-old\n+new",
    }
]


class ReviewGitHub(FakeGitHub):
    def __init__(self):
        super().__init__()
        self.prs[2] = pr()
        self.prs[2].update(
            user={"login": "other-user", "type": "User"},
            title="Fix input",
            html_url="https://github.com/example/repo/pull/2",
            body="Regression fix",
            labels=[],
        )
        self.prs[2]["base"]["repo"] = {"full_name": self.repo}
        self.files = deepcopy(FILES)
        self.permissions["reviewer-user"] = "write"
        self.checks = [
            {
                "id": 1,
                "name": "unit",
                "app": {"id": 15368},
                "head_sha": HEAD,
                "status": "completed",
                "conclusion": "success",
            }
        ]
        self.role_ledgers = {}
        self.review_posts = []
        self.issue_comments = []
        self.auth_user = {"login": "reviewer-user", "type": "User"}
        self.native_actor = {"login": "independent[bot]", "type": "Bot"}

    def request(self, path, method="GET", data=None):
        if path == "/user":
            return deepcopy(self.auth_user)
        if path == "pulls/2/reviews" and method == "POST":
            review = {
                "id": len(self.reviews) + 1,
                "user": deepcopy(self.native_actor),
                "body": data["body"],
                "commit_id": data["commit_id"],
                "state": {
                    "APPROVE": "APPROVED",
                    "COMMENT": "COMMENTED",
                    "REQUEST_CHANGES": "CHANGES_REQUESTED",
                }[data["event"]],
            }
            self.review_posts.append(deepcopy(data))
            self.reviews.append(review)
            return deepcopy(review)
        return super().request(path, method, data)

    def paginate(self, path):
        if path.endswith("/files"):
            return deepcopy(self.files)
        if path == "pulls/2/comments":
            return []
        if path == "issues/2/comments":
            return deepcopy(self.issue_comments)
        return super().paginate(path)


class RoleStore:
    def __init__(self, api, _policy, *, marker):
        self.api = api
        self.marker = marker

    def load(self):
        return deepcopy(self.api.role_ledgers.get(self.marker, initial_state()))

    def save(self, state):
        self.api.role_ledgers[self.marker] = deepcopy(state)


class ReviewerTests(unittest.TestCase):
    def setUp(self):
        self.api = ReviewGitHub()
        self.policy = policy()
        self.policy["review"].update(
            mode="approve", login=self.api.native_actor["login"], identity_type="Bot"
        )
        self.env = patch.dict(
            os.environ, {"MAINTAINER_REVIEWER_ACTOR": "independent[bot]"}
        )
        self.env.start()
        self.addCleanup(self.env.stop)
        self.store = patch("reviewer.StateStore", RoleStore)
        self.store.start()
        self.addCleanup(self.store.stop)

    def prepare(self):
        return prepare_review(self.api, self.policy, CONTROL, 2)

    def test_independent_app_can_approve_other_user_pr_without_managed_label(self):
        prepared = self.prepare()
        published = publish_review(self.api, self.policy, prepared, CLEAN)
        self.assertEqual(published["event"], "APPROVE")
        self.assertEqual(self.api.review_posts[0]["commit_id"], HEAD)
        self.assertEqual(self.api.reviews[0]["user"]["login"], "independent[bot]")
        self.assertFalse(self.prepare()["ready"])
        self.assertEqual(len(self.api.review_posts), 1)

    def test_two_users_can_submit_an_independent_native_approval(self):
        self.policy.update(app_login="writer-user", writer_type="User")
        self.policy["review"].update(
            {"login": self.api.auth_user["login"], "identity_type": "User"}
        )
        self.api.native_actor = deepcopy(self.api.auth_user)
        prepared = self.prepare()
        published = publish_review(self.api, self.policy, prepared, CLEAN)
        self.assertEqual(published["event"], "APPROVE")
        self.assertEqual(self.api.reviews[0]["user"]["login"], "reviewer-user")

    def test_same_writer_identity_and_self_authored_pr_are_refused(self):
        self.policy["review"]["login"] = self.policy["app_login"].upper()
        with self.assertRaises(WorkError):
            self.prepare()
        self.policy["review"]["login"] = "independent[bot]"
        self.api.prs[2]["user"]["login"] = "independent[bot]"
        with self.assertRaises(WorkError):
            self.prepare()

    def test_red_ci_produces_comment_then_reuses_analysis_for_native_approval(self):
        self.api.checks[0]["conclusion"] = "failure"
        prepared = self.prepare()
        self.assertEqual(
            publish_review(self.api, self.policy, prepared, CLEAN)["event"], "COMMENT"
        )
        ledger = next(iter(self.api.role_ledgers.values()))
        spent = ledger["budget"]["spent"]
        self.api.checks[0]["conclusion"] = "success"
        reused = self.prepare()
        self.assertTrue(reused["skip_model"])
        self.assertEqual(reused["budget_usd"], 0.0)
        self.assertEqual(
            publish_review(self.api, self.policy, reused, CLEAN)["event"], "APPROVE"
        )
        self.assertEqual(
            next(iter(self.api.role_ledgers.values()))["budget"]["spent"], spent
        )

    def test_another_reviewers_change_request_prevents_native_approval(self):
        self.api.reviews = [
            {
                "id": 7,
                "user": {"login": "reviewer-user", "type": "User"},
                "state": "CHANGES_REQUESTED",
                "commit_id": BASE,
                "body": "The old blocking request remains unresolved.",
            }
        ]
        prepared = self.prepare()
        published = publish_review(self.api, self.policy, prepared, CLEAN)
        self.assertEqual(published["event"], "COMMENT")
        self.assertTrue(published["approval_blockers"])

    def test_changed_head_and_new_human_feedback_invalidate_analysis(self):
        prepared = self.prepare()
        self.api.prs[2]["head"]["sha"] = "d" * 40
        with self.assertRaises(WorkError):
            publish_review(self.api, self.policy, prepared, CLEAN)
        self.api.prs[2]["head"]["sha"] = HEAD
        self.api.issue_comments.append(
            {
                "id": 77,
                "user": {"login": "reviewer-user"},
                "body": "This overlooks a null input case",
            }
        )
        with self.assertRaises(WorkError):
            publish_review(self.api, self.policy, prepared, CLEAN)
        self.assertFalse(self.api.review_posts)

    def test_findings_and_protected_or_truncated_diffs_cannot_be_approved(self):
        finding = {
            "path": "src/fix.py",
            "line": 1,
            "body": "New path loses null handling.",
        }
        with self.assertRaises(WorkError):
            validate_result({**CLEAN, "findings": [finding]}, FILES)
        prepared = self.prepare()
        result = {
            "verdict": "request_changes",
            "summary": "Null input remains unsafe.",
            "findings": [finding],
        }
        self.assertEqual(
            publish_review(self.api, self.policy, prepared, result)["event"],
            "REQUEST_CHANGES",
        )
        self.api.files[0]["filename"] = ".github/maintainer/policy.toml"
        self.assertTrue(
            approval_blockers(
                self.api, self.policy, self.api.prs[2], "independent[bot]"
            )
        )
        self.api.files = deepcopy(FILES)
        self.api.files[0]["patch"] = "@@ -1 +1 @@\n-old"
        with self.assertRaises(WorkError):
            collect_snapshot(self.api, self.policy, self.api.prs[2])

    def test_budget_duplicate_and_expired_lease_never_launch_another_paid_review(self):
        self.policy["review"]["daily_usd"] = 5.0
        prepared = self.prepare()
        self.assertFalse(self.prepare()["ready"])
        self.assertEqual(
            next(iter(self.api.role_ledgers.values()))["budget"]["spent"], 5.0
        )
        ledger = next(iter(self.api.role_ledgers.values()))
        ledger["tasks"]["pr:2"]["lease_until"] = 1
        self.assertFalse(self.prepare()["ready"])
        self.assertEqual(
            next(iter(self.api.role_ledgers.values()))["tasks"]["pr:2"]["state"],
            "NEEDS_HUMAN",
        )
        with self.assertRaises(WorkError):
            publish_review(self.api, self.policy, prepared, CLEAN)

    def test_bot_writer_requires_the_actual_minted_identity_hint(self):
        with patch.dict(os.environ, {"MAINTAINER_WRITER_ACTOR": ""}):
            with self.assertRaises(WorkError):
                writer_identity(self.api, self.policy)
        with patch.dict(os.environ, {"MAINTAINER_WRITER_ACTOR": "independent[bot]"}):
            with self.assertRaises(WorkError):
                writer_identity(self.api, self.policy)

    def test_user_tokens_are_bound_to_distinct_actual_accounts(self):
        self.policy["review"].update(
            {"login": self.api.auth_user["login"], "identity_type": "User"}
        )
        self.assertEqual(
            reviewer_identity(self.api, self.policy)["login"], "reviewer-user"
        )
        self.policy.update(app_login="writer-user", writer_type="User")
        self.api.auth_user = {"login": "writer-user", "type": "User"}
        writer_identity(self.api, self.policy)
        with self.assertRaises(WorkError):
            reviewer_identity(self.api, self.policy)
        self.api.auth_user = {"login": "reviewer-user", "type": "User"}
        with self.assertRaises(WorkError):
            writer_identity(self.api, self.policy)
        self.api.permissions["reviewer-user"] = "read"
        self.assertTrue(
            approval_blockers(self.api, self.policy, self.api.prs[2], "reviewer-user")
        )


class IdentityConfigTests(unittest.TestCase):
    def test_two_users_are_valid_but_same_identity_and_writer_approver_list_are_not(
        self,
    ):
        body = """version=1
mode="maintain"
app_login="writer-user"
writer_type="User"
state_issue=99
allowed_paths=["src/**"]
validation_commands=["python -m unittest"]
[review]
mode="approve"
login="reviewer-user"
identity_type="User"
"""
        with tempfile.TemporaryDirectory() as temp:
            file = Path(temp) / "policy.toml"
            file.write_text(body)
            self.assertEqual(load_policy(file)["writer_type"], "User")
            file.write_text(
                body.replace('login="reviewer-user"', 'login="WRITER-USER"')
            )
            with self.assertRaises(ValueError):
                load_policy(file)
            file.write_text(body + '\n[merge]\nindependent_approvers=["WRITER-USER"]\n')
            with self.assertRaises(ValueError):
                load_policy(file)

    def test_writer_and_reviewer_ledgers_coexist_with_their_own_author_types(self):
        import json

        source = initial_state()
        comments = [
            {
                "id": 1,
                "body": "<!-- repo-maintainer-state:v1 -->\n```json\n"
                + json.dumps(source)
                + "\n```",
                "user": {"login": "writer-user", "type": "User"},
            },
            {
                "id": 2,
                "body": "<!-- repo-reviewer-state:v1 -->\n```json\n"
                + json.dumps(source)
                + "\n```",
                "user": {"login": "independent[bot]", "type": "Bot"},
            },
        ]
        api = ReviewGitHub()
        api.paginate = lambda _: comments
        p = policy()
        p.update(app_login="writer-user", writer_type="User")
        self.assertEqual(StateStore(api, p).load()["tasks"], {})
        p.update(app_login="independent[bot]", writer_type="Bot")
        self.assertEqual(
            StateStore(api, p, marker="<!-- repo-reviewer-state:v1 -->").load()[
                "tasks"
            ],
            {},
        )


if __name__ == "__main__":
    unittest.main()
