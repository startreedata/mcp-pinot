"""Offline trust-boundary regressions using real temporary Git repositories."""

from copy import deepcopy
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from publish import validate_patch
from state import initial_state, issue_hash
from worker import WorkError, authorize, git, safe_environment

from config import DEFAULT_POLICY


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "--quiet")
        self.stage("docs/edit.txt", b"old\n")
        self.stage("docs/delete.txt", b"delete me\n")
        tree = git(self.repo, "write-tree").decode().strip()
        identity = safe_environment(
            GIT_AUTHOR_NAME="test",
            GIT_AUTHOR_EMAIL="test@localhost",
            GIT_COMMITTER_NAME="test",
            GIT_COMMITTER_EMAIL="test@localhost",
        )
        self.base = (
            git(self.repo, "commit-tree", tree, data=b"base\n", env=identity)
            .decode()
            .strip()
        )
        git(self.repo, "update-ref", "HEAD", self.base)
        git(self.repo, "checkout-index", "--all")
        self.policy = deepcopy(DEFAULT_POLICY)
        self.policy["allowed_paths"] = ["**"]
        self.patch = self.root / "change.patch"

    def stage(self, path, contents, mode="100644"):
        sha = (
            git(self.repo, "hash-object", "-w", "--stdin", data=contents)
            .decode()
            .strip()
        )
        git(self.repo, "update-index", "--add", "--cacheinfo", f"{mode},{sha},{path}")

    def capture_patch(self):
        self.patch.write_bytes(
            git(self.repo, "diff", "--cached", "--no-ext-diff", "--no-textconv")
        )
        git(self.repo, "read-tree", self.base)

    def test_real_patch_materializes_added_edited_and_deleted_files(self):
        self.stage("docs/edit.txt", b"new\n")
        self.stage("docs/add.txt", b"added\n")
        git(self.repo, "update-index", "--force-remove", "docs/delete.txt")
        self.capture_patch()
        _, paths, digest = validate_patch(
            self.repo,
            self.patch,
            self.policy,
            self.base,
            materialize=True,
        )
        self.assertEqual(paths, ["docs/add.txt", "docs/delete.txt", "docs/edit.txt"])
        self.assertEqual((self.repo / "docs/edit.txt").read_bytes(), b"new\n")
        self.assertEqual((self.repo / "docs/add.txt").read_bytes(), b"added\n")
        self.assertFalse((self.repo / "docs/delete.txt").exists())
        self.assertEqual(len(digest), 64)

    def test_patch_rejects_sensitive_paths_and_special_modes_even_when_allowed(self):
        cases = [
            ("protected", ".github/workflows/attack.yml", b"attack\n", "100644"),
            ("symlink", "docs/link", b"../../outside", "120000"),
            ("new executable", "docs/attack.sh", b"exit 0\n", "100755"),
            ("changed mode", "docs/edit.txt", b"old\n", "100755"),
            ("binary", "docs/binary", b"\0bad", "100644"),
        ]
        for name, filename, contents, mode in cases:
            with self.subTest(name=name):
                git(self.repo, "read-tree", self.base)
                self.stage(filename, contents, mode)
                self.capture_patch()
                with self.assertRaises(WorkError):
                    validate_patch(self.repo, self.patch, self.policy, self.base)
        git(self.repo, "read-tree", self.base)
        self.stage("docs/edit.txt", b"new\n")
        self.capture_patch()
        with self.assertRaises(WorkError):
            validate_patch(self.repo, self.patch, self.policy, "a" * 40)
        self.policy["limits"]["max_patch_bytes"] = 1
        with self.assertRaises(WorkError):
            validate_patch(self.repo, self.patch, self.policy, self.base)

    def test_authenticated_budget_survives_midnight_and_revoked_writer_is_rejected(
        self,
    ):
        source = {
            "number": 1,
            "title": "Task",
            "body": "Authorized content",
            "state": "open",
            "labels": [{"name": "ai:ready"}],
        }
        lease = str(uuid.uuid4())
        today = time.strftime("%Y-%m-%d", time.gmtime())
        ledger = initial_state()
        ledger["budget"] = {"day": today, "spent": 0.0}
        task = {
            "kind": "issue",
            "state": "WORKING",
            "source_number": 1,
            "authorized_by": "writer",
            "lease": lease,
            "lease_until": time.time() + 60,
            "branch": "ai/issue-1",
            "base_sha": self.base,
            "expected_sha": self.base,
            "issue_snapshot_hash": issue_hash(source),
            "budget_usd": 5.0,
            "reserved_total": 5.0,
            "budget_day": "2020-01-01",
            "attempts": 1,
            "repair_round": 0,
            "worker_kind": "implement",
        }
        ledger["tasks"]["issue:1"] = task
        self.policy.update(mode="maintain", app_login="maintainer[bot]", state_issue=99)

        class API:
            repo = "example/repo"
            permission = "write"

            def request(self, path):
                if path.startswith("/users/"):
                    return {"type": "User"}
                if path.startswith("collaborators/"):
                    return {"permission": self.permission}
                if path == "issues/1":
                    return source
                raise AssertionError(path)

        api = API()
        with patch("worker.StateStore") as store:
            store.return_value.load.return_value = ledger
            authorize(api, self.policy, "issue:1", lease)
            task["budget_day"] = today
            with self.assertRaisesRegex(WorkError, "reservation"):
                authorize(api, self.policy, "issue:1", lease)
            ledger["budget"]["spent"] = 5.0
            api.permission = "read"
            with self.assertRaisesRegex(WorkError, "write permission"):
                authorize(api, self.policy, "issue:1", lease)

    def test_process_environment_strips_credentials_and_ignores_foreign_git_config(
        self,
    ):
        credentials = dict.fromkeys(
            ("GH_TOKEN", "GITHUB_TOKEN", "ANTHROPIC_API_KEY", "AWS_SECRET_ACCESS_KEY"),
            "test-only-value",
        )
        with patch.dict(os.environ, credentials):
            environment = safe_environment(GIT_TEST_ASSUME_DIFFERENT_OWNER="true")
            self.assertTrue(credentials.keys().isdisjoint(environment))
            self.assertEqual(
                git(self.repo, "rev-parse", "HEAD", env=environment).decode().strip(),
                self.base,
            )


if __name__ == "__main__":
    unittest.main()
