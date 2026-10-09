"""One App-authored issue comment is the durable controller ledger."""

from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
import math
import re
import uuid

STATE_MARKER = "<!-- repo-maintainer-state:v1 -->"
STATE_PREFIX = "<!-- repo-maintainer-state:"
TASK_STATES = {
    "AUTHORIZED",
    "WORKING",
    "WAITING_REVIEW",
    "NEEDS_HUMAN",
    "MERGED_PENDING_CI",
    "DONE",
}


def initial_state():
    return {
        "version": 1,
        "paused": False,
        "tasks": {},
        "budget": {"day": datetime.now(UTC).date().isoformat(), "spent": 0.0},
    }


def issue_hash(issue):
    """Bind authorization to the issue title and body, never to mutable labels."""
    title, body = issue.get("title"), issue.get("body")
    if not isinstance(title, str) or (body is not None and not isinstance(body, str)):
        raise ValueError("Issue snapshot needs a title and a string or null body")
    snapshot = json.dumps(
        {"title": title, "body": body or ""},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(snapshot.encode("utf-8")).hexdigest()


def _nonnegative(value):
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
        and value >= 0
    )


def _validate(state):
    required = {"version", "paused", "tasks", "budget"}
    if (
        not isinstance(state, dict)
        or not required <= state.keys()
        or not (state.keys() - required)
        <= {"pause_reason", "repo_notice_pending", "slack_thread_ts"}
    ):
        raise ValueError("Malformed controller ledger")
    if type(state["version"]) is not int or state["version"] != 1:
        raise ValueError("Unsupported controller ledger version")
    if type(state["paused"]) is not bool or not isinstance(state["tasks"], dict):
        raise ValueError("Malformed controller paused flag or tasks")
    if "pause_reason" in state and (
        not isinstance(state["pause_reason"], str) or len(state["pause_reason"]) > 4096
    ):
        raise ValueError("Malformed controller pause reason")
    for field, limit in (("repo_notice_pending", 256), ("slack_thread_ts", 64)):
        if field in state and (
            not isinstance(state[field], str) or len(state[field]) > limit
        ):
            raise ValueError(f"Malformed controller {field}")
    budget = state["budget"]
    if not isinstance(budget, dict) or budget.keys() != {"day", "spent"}:
        raise ValueError("Malformed controller budget")
    try:
        day = datetime.strptime(budget["day"], "%Y-%m-%d").date().isoformat()
    except (TypeError, ValueError):
        raise ValueError("Malformed controller budget day") from None
    if day != budget["day"]:
        raise ValueError("Controller budget day must use YYYY-MM-DD")
    if not _nonnegative(budget["spent"]):
        raise ValueError("Malformed controller spent budget")
    if len(state["tasks"]) > 1000:
        raise ValueError("Controller ledger has too many tasks")
    for key, task in state["tasks"].items():
        if not re.fullmatch(r"(?:issue|pr):[1-9][0-9]*", key):
            raise ValueError("Malformed controller task key")
        if not isinstance(task, dict):
            raise ValueError(f"Malformed controller task: {key}")
        required = {
            "source_number",
            "kind",
            "state",
            "authorized_by",
            "base_sha",
            "expected_sha",
            "branch",
            "repair_round",
            "attempts",
            "reserved_total",
        }
        if not required <= task.keys():
            raise ValueError(f"Controller task is incomplete: {key}")
        if (
            type(task["source_number"]) is not int
            or task["source_number"] < 1
            or task["source_number"] != int(key.split(":")[1])
            or task["kind"] not in {"issue", "dependency", "adopted"}
            or task["state"] not in TASK_STATES
        ):
            raise ValueError(f"Malformed controller task identity/state: {key}")
        for field in ("authorized_by", "base_sha", "expected_sha", "branch"):
            if not isinstance(task[field], str) or not task[field]:
                raise ValueError(f"Controller task {key} needs {field}")
        if any(
            not re.fullmatch(r"[0-9a-f]{40}", task[field])
            for field in ("base_sha", "expected_sha")
        ):
            raise ValueError(f"Malformed controller task SHA: {key}")
        for field in ("repair_round", "attempts"):
            if type(task[field]) is not int or task[field] < 0:
                raise ValueError(f"Malformed controller task counter: {key}")
        if not _nonnegative(task["reserved_total"]):
            raise ValueError(f"Malformed controller task reservation: {key}")
        if task["kind"] == "issue" and not re.fullmatch(
            r"[0-9a-f]{64}",
            task.get("issue_snapshot_hash", ""),
        ):
            raise ValueError(f"Controller issue snapshot is missing: {key}")
        if "pr_number" in task and (
            type(task["pr_number"]) is not int or task["pr_number"] < 1
        ):
            raise ValueError(f"Malformed controller task PR number: {key}")
        if "lease" in task:
            try:
                uuid.UUID(task["lease"])
            except (ValueError, TypeError, AttributeError):
                raise ValueError(f"Malformed controller task lease: {key}") from None
        if "lease_until" in task and not _nonnegative(task["lease_until"]):
            raise ValueError(f"Malformed controller task lease expiration: {key}")
        if task["state"] == "WORKING" and (
            not task.get("lease")
            or not task.get("lease_until")
            or task.get("worker_kind") not in {"implement", "repair"}
        ):
            raise ValueError(f"Working task has no valid lease/worker: {key}")


def _serialized(state):
    return json.dumps(
        state,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


class StateStore:
    def __init__(self, api, policy):
        self.api = api
        self.policy = policy
        self._comment_id = None
        self._revision = None
        self._loaded = False

    def _read(self):
        comments = self.api.paginate(f"issues/{self.policy['state_issue']}/comments")
        if any(not isinstance(comment, dict) for comment in comments):
            raise ValueError("Controller ledger comments are incomplete")
        ledgers = [
            comment
            for comment in comments
            if isinstance(comment.get("body"), str) and STATE_PREFIX in comment["body"]
        ]
        if len(ledgers) > 1:
            raise ValueError("Multiple controller ledgers found; human repair required")
        if not ledgers:
            return None, None
        comment = ledgers[0]
        user = comment.get("user", {})
        if user.get("login") != self.policy["app_login"] or user.get("type") != "Bot":
            raise ValueError(
                "Controller ledger is not authored by the configured App bot"
            )
        match = re.fullmatch(
            re.escape(STATE_MARKER) + r"\n```json\n(.+)\n```\n?",
            comment["body"],
            flags=re.DOTALL,
        )
        if not match:
            raise ValueError("Malformed controller ledger comment")
        try:
            state = json.loads(match.group(1))
        except json.JSONDecodeError:
            raise ValueError("Controller ledger contains invalid JSON") from None
        _validate(state)
        if type(comment.get("id")) is not int or comment["id"] < 1:
            raise ValueError("Controller ledger comment has no valid ID")
        return comment["id"], state

    def load(self):
        if not self.policy["state_issue"]:
            if self.policy["mode"] != "observe":
                raise ValueError("Active controller needs a state issue")
            self._loaded = True
            return initial_state()
        self._comment_id, state = self._read()
        self._revision = _serialized(state) if state is not None else None
        self._loaded = True
        return deepcopy(state) if state is not None else initial_state()

    def save(self, state):
        if self.policy["mode"] == "observe":
            raise ValueError("Observe mode cannot write the controller ledger")
        if not self._loaded:
            raise ValueError("Controller ledger must be loaded before saving")
        _validate(state)
        current_id, current = self._read()
        revision = _serialized(current) if current is not None else None
        if current_id != self._comment_id or revision != self._revision:
            raise ValueError(
                "Controller ledger changed since load; refusing stale write"
            )
        encoded = _serialized(state)
        if encoded == self._revision:
            return
        body = f"{STATE_MARKER}\n```json\n{encoded}\n```"
        if len(body.encode("utf-8")) > 60000:
            raise ValueError("Controller ledger exceeds the safe comment size")
        if self._comment_id is None:
            comment = self.api.request(
                f"issues/{self.policy['state_issue']}/comments",
                method="POST",
                data={"body": body},
            )
        else:
            comment = self.api.request(
                f"issues/comments/{self._comment_id}",
                method="PATCH",
                data={"body": body},
            )
        user = comment.get("user", {}) if isinstance(comment, dict) else {}
        if (
            user.get("login") != self.policy["app_login"]
            or user.get("type") != "Bot"
            or comment.get("body") != body
            or type(comment.get("id")) is not int
        ):
            raise ValueError(
                "GitHub did not confirm the authenticated App ledger write"
            )
        self._comment_id, self._revision = comment["id"], encoded
