"""Read allowlisted diagnostics from one fresh, owned local Codex rollout."""

from datetime import UTC, datetime, timedelta
import hashlib
from itertools import pairwise
import json
import os
from pathlib import Path
import re
import stat

_UUID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
_TOKENS = frozenset(
    (
        "input_tokens",
        "cached_input_tokens",
        "cache_write_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
)


def _uuid(value: object) -> str:
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise ValueError("Invalid owned rollout or turn ID.")
    return value


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Missing rollout timestamp.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Invalid rollout timestamp.") from exc
    if parsed.tzinfo is None:
        raise ValueError("Rollout timestamps must have a timezone.")
    return parsed


def _record(raw: bytes) -> dict:
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Malformed telemetry record.") from exc
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise ValueError("Malformed telemetry record.")
    return value


def _tokens(value: object, *, snapshot: bool) -> dict:
    if not isinstance(value, dict) or not value:
        raise ValueError("Missing telemetry token counts.")
    if snapshot:
        if not _TOKENS.issubset(value):
            raise ValueError("Incomplete telemetry token counts.")
    elif not set(value).issubset(_TOKENS):
        raise ValueError("Unknown completed usage field.")
    counts = {key: value[key] for key in _TOKENS if key in value}
    if any(type(count) is not int or count < 0 for count in counts.values()):
        raise ValueError("Telemetry token counts must be nonnegative integers.")
    if "input_tokens" not in counts or "output_tokens" not in counts:
        raise ValueError("Missing input or output token counts.")
    if (
        counts.get("cached_input_tokens", 0) > counts["input_tokens"]
        or counts.get("cache_write_input_tokens", 0) > counts["input_tokens"]
        or counts.get("reasoning_output_tokens", 0) > counts["output_tokens"]
        or (
            "total_tokens" in counts
            and counts["total_tokens"]
            != counts["input_tokens"] + counts["output_tokens"]
        )
    ):
        raise ValueError("Inconsistent telemetry token counts.")
    return counts


def _regular_bytes(path: Path, root: Path) -> bytes:
    """Reject symlinks and changes during reading before retaining any metadata."""
    try:
        relative = path.relative_to(root)
        current = root
        for component in relative.parts:
            current /= component
            if current.is_symlink():
                raise ValueError("Telemetry paths must not contain symlinks.")
        path.resolve().relative_to(root.resolve())
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ValueError("Telemetry source must be a regular file.")
            raw = source.read()
            after = os.fstat(source.fileno())
        if (before.st_size, before.st_mtime_ns) != (
            after.st_size,
            after.st_mtime_ns,
        ) or len(raw) != after.st_size:
            raise ValueError("Telemetry source changed during capture.")
        return raw
    except OSError as exc:
        raise ValueError("Telemetry source cannot be read safely.") from exc


def _source(home: Path, thread_id: str, started: datetime, finished: datetime) -> Path:
    dates = {
        endpoint.astimezone(zone).date()
        for endpoint in (started, finished)
        for zone in (UTC, None)
    }
    # CLI versions have used both UTC and local date directories. The adjacent
    # dates cover a midnight boundary without searching unrelated session history.
    dates |= {day + timedelta(days=offset) for day in dates for offset in (-1, 1)}
    matches = set()
    for name in ("sessions", "archived_sessions"):
        root = home / name
        if not root.exists():
            continue
        if root.is_symlink() or not root.is_dir():
            raise ValueError("Invalid telemetry session root.")
        directories = [root] if name == "archived_sessions" else []
        directories += [root / day.strftime("%Y/%m/%d") for day in sorted(dates)]
        for directory in directories:
            for path in directory.glob(f"rollout-*-{thread_id}.jsonl"):
                matches.add(path)
    if len(matches) != 1:
        raise ValueError("Expected exactly one owned rollout source.")
    return matches.pop()


def _optional_text(payload: dict, key: str) -> str:
    value = payload.get(key)
    if value is None:
        return "UNKNOWN"
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Invalid selected model metadata.")
    return value


def capture_host_telemetry(
    events_path: Path,
    workspace: Path,
    started_at: datetime,
    finished_at: datetime,
    cli_usage: dict | str,
) -> dict:
    """Bind local diagnostics to the CLI UUID, cwd, fresh turn, and actual usage.

    Local selection and token snapshots never attest a provider model or billing.
    Missing or inconsistent ownership raises ``ValueError``; callers retain their
    independently verified MCP receipts. This helper never mutates the rollout.
    """
    if (
        started_at.tzinfo is None
        or finished_at.tzinfo is None
        or finished_at < started_at
        or finished_at - started_at > timedelta(days=1)
    ):
        raise ValueError("Invalid telemetry capture window.")
    thread_ids, completed = [], []
    try:
        for raw in events_path.read_bytes().splitlines():
            event = _record(raw)
            if event["type"] == "thread.started":
                thread_ids.append(_uuid(event.get("thread_id")))
            elif event["type"] == "turn.completed":
                completed.append(_tokens(event.get("usage"), snapshot=False))
    except OSError as exc:
        raise ValueError("CLI telemetry events are unavailable.") from exc
    if len(thread_ids) != 1 or len(completed) > 1:
        raise ValueError("Expected one CLI thread and at most one completed turn.")
    if isinstance(cli_usage, dict):
        cli_usage = _tokens(cli_usage, snapshot=False)
        if completed != [cli_usage]:
            raise ValueError("Completed CLI usage does not match its actual event.")
    elif cli_usage != "UNKNOWN" or completed:
        raise ValueError("Inconsistent completed CLI usage.")

    home = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    if home.is_symlink() or not home.is_dir():
        raise ValueError("An existing nonsymlink CODEX_HOME is required.")
    thread_id = thread_ids[0]
    source = _source(home, thread_id, started_at, finished_at)
    raw_source = _regular_bytes(source, home)
    metadata: list[dict] = []
    contexts: list[dict] = []
    starts: list[dict] = []
    finishes: list[dict] = []
    snapshots: list[dict] = []
    selected: list[dict] = []
    for line_number, raw in enumerate(raw_source.splitlines(keepends=True), 1):
        record = _record(raw)
        kind = record["type"]
        # Do not access payloads for response_item, reasoning, agent messages,
        # authentication data, or any other nonallowlisted record type.
        if kind not in ("session_meta", "turn_context", "event_msg"):
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("Malformed allowlisted telemetry record.")
        if kind == "event_msg":
            kind = payload.get("type")
            if kind not in ("task_started", "task_complete", "token_count"):
                continue
        timestamp = _timestamp(record.get("timestamp"))
        if not started_at <= timestamp <= finished_at:
            raise ValueError("Telemetry record is outside the owned capture window.")
        filtered: dict = {}
        if kind == "session_meta":
            if _uuid(payload.get("id")) != thread_id:
                raise ValueError("Foreign telemetry session ID.")
            cwd = payload.get("cwd")
            if (
                not isinstance(cwd, str)
                or not Path(cwd).is_absolute()
                or Path(cwd).resolve() != workspace.resolve()
            ):
                raise ValueError("Foreign telemetry working directory.")
            filtered = {"id": thread_id, "cwd": cwd}
            if "model_provider" in payload:
                filtered["model_provider"] = _optional_text(payload, "model_provider")
            metadata.append(filtered)
        elif kind in ("turn_context", "task_started", "task_complete"):
            filtered = {"turn_id": _uuid(payload.get("turn_id"))}
            if kind == "turn_context":
                for key in ("model", "effort"):
                    if key in payload:
                        filtered[key] = _optional_text(payload, key)
                contexts.append(filtered)
            else:
                (starts if kind == "task_started" else finishes).append(filtered)
        else:
            info = payload.get("info")
            if not isinstance(info, dict):
                raise ValueError("Missing token snapshot metadata.")
            filtered = {
                "total_token_usage": _tokens(
                    info.get("total_token_usage"), snapshot=True
                ),
                "last_token_usage": _tokens(
                    info.get("last_token_usage"), snapshot=True
                ),
            }
            total, last = filtered["total_token_usage"], filtered["last_token_usage"]
            if any(last[key] > total[key] for key in _TOKENS) or (
                snapshots
                and any(
                    total[key] < snapshots[-1]["total_token_usage"][key]
                    for key in _TOKENS
                )
            ):
                raise ValueError("Inconsistent cumulative token snapshots.")
            snapshots.append(filtered)
        selected.append(
            {
                "line": line_number,
                "type": kind,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "timestamp": record["timestamp"],
                "payload": filtered,
            }
        )

    if (
        len(metadata) != 1
        or len(contexts) != 1
        or len(starts) != 1
        or len(finishes) > 1
    ):
        raise ValueError("Expected one owned session and one fresh turn.")
    turn_id = contexts[0]["turn_id"]
    if any(item["turn_id"] != turn_id for item in starts + finishes):
        raise ValueError("Foreign telemetry turn ID.")
    types = [item["type"] for item in selected]
    if types != [
        "session_meta",
        "task_started",
        "turn_context",
        *(["token_count"] * len(snapshots)),
        *(["task_complete"] if finishes else []),
    ] or any(
        _timestamp(before["timestamp"]) > _timestamp(after["timestamp"])
        for before, after in pairwise(selected)
    ):
        raise ValueError("Telemetry snapshots are outside the bound turn sequence.")
    latest = snapshots[-1] if snapshots else {}
    if isinstance(cli_usage, dict) and (
        not finishes
        or not latest
        or any(latest["total_token_usage"].get(k) != v for k, v in cli_usage.items())
    ):
        raise ValueError("Local totals do not match completed CLI usage.")
    return {
        "source": "Owned local Codex rollout diagnostics; not a provider receipt.",
        "thread_id": thread_id,
        "turn_id": turn_id,
        "cwd_binding_verified": True,
        "host_selected_model": contexts[0].get("model", "UNKNOWN"),
        "host_selected_effort": contexts[0].get("effort", "UNKNOWN"),
        "host_selected_provider": metadata[0].get("model_provider", "UNKNOWN"),
        "provider_attested_model": "UNKNOWN",
        "billing": "UNKNOWN",
        "token_usage": {
            "complete": False,
            "latest_total": latest.get("total_token_usage", "UNKNOWN"),
            "latest_response": latest.get("last_token_usage", "UNKNOWN"),
            "snapshot_count": len(snapshots),
            "cli_completed_usage_matches": bool(completed),
        },
        "completion": {
            "task_complete_observed": bool(finishes),
            "cli_completed_turn_observed": bool(completed),
        },
        "lineage": {
            "source_path": str(source),
            "source_sha256": hashlib.sha256(raw_source).hexdigest(),
            "allowed_records": selected,
        },
    }
