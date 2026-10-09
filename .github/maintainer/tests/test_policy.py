"""Merge-policy evidence must belong to the current head and trusted identities."""

import unittest

from policy import evaluate
from state import STATE_MARKER, StateStore
from test_controller import BASE, HEAD, FakeGitHub, policy, pr, task

from config import path_allowed, validate_paths


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.api = FakeGitHub()
        self.policy = policy()
        self.pr = pr()
        self.task = task()
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

    def blockers(self):
        return evaluate(self.api, self.policy, self.pr, self.task)

    def test_current_native_independent_bot_approval_and_ci_pass(self):
        self.assertEqual(self.blockers(), [])

    def test_required_own_policy_does_not_create_a_circular_admission_gate(self):
        self.pr["mergeable_state"] = "blocked"
        self.assertEqual(
            evaluate(
                self.api, self.policy, self.pr, self.task, require_mergeable=False
            ),
            [],
        )
        self.assertTrue(self.blockers())

    def test_old_sha_approval_does_not_count(self):
        self.api.reviews[0]["commit_id"] = BASE
        self.assertTrue(self.blockers())

    def test_commented_lgtm_is_not_native_approval(self):
        self.api.reviews[0]["state"] = "COMMENTED"
        self.assertTrue(self.blockers())

    def test_untrusted_app_cannot_spoof_green_required_check(self):
        self.api.checks[0]["app"]["id"] = 42
        self.assertTrue(self.blockers())

    def test_newest_failed_or_pending_run_overrides_older_green(self):
        for status, conclusion in [("completed", "failure"), ("in_progress", None)]:
            with self.subTest(status=status):
                self.api.checks = [
                    self.api.checks[0],
                    {
                        **self.api.checks[0],
                        "id": 2,
                        "status": status,
                        "conclusion": conclusion,
                    },
                ]
                self.assertTrue(self.blockers())

    def test_missing_or_skipped_checks_do_not_pass(self):
        self.api.checks[0]["conclusion"] = "skipped"
        self.assertTrue(self.blockers())
        self.api.checks = []
        self.assertTrue(self.blockers())

    def test_old_human_changes_request_remains_blocking(self):
        self.api.reviews.append(
            {
                "id": 2,
                "state": "CHANGES_REQUESTED",
                "commit_id": BASE,
                "user": {"login": "reviewer", "type": "User"},
            }
        )
        self.api.reviews.append(
            {
                "id": 3,
                "state": "COMMENTED",
                "commit_id": HEAD,
                "user": {"login": "reviewer", "type": "User"},
            }
        )
        self.assertTrue(self.blockers())

    def test_unresolved_or_truncated_threads_block(self):
        self.api.threads = [{"isResolved": False}]
        self.assertTrue(self.blockers())
        original = self.api.graphql

        def truncated(query, variables):
            data = original(query, variables)
            data["repository"]["pullRequest"]["reviewThreads"]["totalCount"] = 2
            return data

        self.api.graphql = truncated
        self.api.threads = [{"isResolved": True}]
        self.assertTrue(self.blockers())

    def test_sensitive_rename_source_cannot_hide_in_allowed_target(self):
        self.assertTrue(
            validate_paths(
                [
                    {
                        "filename": "src/new.py",
                        "previous_filename": ".github/workflows/ci.yml",
                    }
                ],
                self.policy,
            )
        )
        for path in (
            "../src/x",
            "src/../x",
            ".github/maintainer/policy.toml",
            "src/auth/key.py",
            ".claude/settings.json",
            "AGENTS.md",
        ):
            with self.subTest(path=path):
                self.assertFalse(
                    path_allowed(path, {**self.policy, "allowed_paths": ["**"]})
                )

    def test_base_move_blocks(self):
        self.task["base_sha"] = "d" * 40
        self.assertTrue(self.blockers())

    def test_dependency_cannot_use_bot_approval(self):
        self.task.update(
            kind="dependency",
            authorized_by="repository-policy",
            dependency_author="dependabot[bot]",
        )
        self.pr["user"] = {"login": "dependabot[bot]", "type": "Bot"}
        self.pr["head"]["ref"] = self.task["branch"] = "dependabot/uv/dependency"
        self.assertTrue(self.blockers())


class LedgerTests(unittest.TestCase):
    def test_forged_ledger_comment_is_rejected(self):
        api = FakeGitHub()
        api.paginate = lambda _: [
            {
                "id": 1,
                "body": STATE_MARKER + "\n```json\n{}\n```",
                "user": {"login": "outsider", "type": "User"},
            }
        ]
        with self.assertRaises(ValueError):
            StateStore(api, policy()).load()

    def test_multiple_ledger_markers_are_not_merged(self):
        api = FakeGitHub()
        entry = {
            "id": 1,
            "body": STATE_MARKER,
            "user": {"login": "maintainer[bot]", "type": "Bot"},
        }
        api.paginate = lambda _: [entry, {**entry, "id": 2}]
        with self.assertRaises(ValueError):
            StateStore(api, policy()).load()


if __name__ == "__main__":
    unittest.main()
