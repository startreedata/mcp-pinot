"""Keep owned host diagnostics separate from actual completed CLI receipts."""

from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from examples.incident_replay.telemetry import capture_host_telemetry
import pytest

THREAD = "01a11eb4-1fea-7620-9941-54e465980a26"
TURN = "01a11eb4-262d-7f90-8d8b-f4ef7cc5ae42"
OTHER = "01a11eb4-262d-7f90-8d8b-f4ef7cc5ae43"
START = datetime(2026, 10, 9, 3, 28, 2, tzinfo=UTC)
END = datetime(2026, 10, 9, 3, 28, 28, tzinfo=UTC)
TOKENS = {
    "input_tokens": 12081,
    "cached_input_tokens": 0,
    "cache_write_input_tokens": 0,
    "output_tokens": 60,
    "reasoning_output_tokens": 0,
    "total_tokens": 12141,
}
USAGE = {key: value for key, value in TOKENS.items() if key != "total_tokens"}


def write_jsonl(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(record) + "\n" for record in records))


@pytest.fixture
def owned(tmp_path, monkeypatch):
    home = tmp_path / "codex"
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    events = tmp_path / "events.jsonl"
    source = home / "sessions/2026/10/09" / f"rollout-owned-{THREAD}.jsonl"
    records = [
        {
            "timestamp": "2026-10-09T03:28:03Z",
            "type": "session_meta",
            "payload": {
                "id": THREAD,
                "cwd": str(workspace),
                "model_provider": "openai",
                "instructions": "private-instructions",
            },
        },
        {
            "timestamp": "2026-10-09T03:28:05Z",
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": TURN},
        },
        {
            "timestamp": "2026-10-09T03:28:06Z",
            "type": "turn_context",
            "payload": {"turn_id": TURN, "model": "selected-model"},
        },
        {
            "timestamp": "2026-10-09T03:28:10Z",
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {
                    "total_token_usage": deepcopy(TOKENS),
                    "last_token_usage": deepcopy(TOKENS),
                    "auth": "private-auth",
                },
            },
        },
        {
            "timestamp": "2026-10-09T03:28:27Z",
            "type": "event_msg",
            "payload": {"type": "task_complete", "turn_id": TURN},
        },
        {
            "type": "response_item",
            "payload": {"type": "reasoning", "content": "private-reasoning"},
        },
        {
            "type": "event_msg",
            "payload": {"type": "item_completed", "content": "private-message"},
        },
    ]
    cli = [
        {"type": "thread.started", "thread_id": THREAD},
        {"type": "turn.completed", "usage": deepcopy(USAGE)},
    ]
    write_jsonl(source, records)
    write_jsonl(events, cli)
    monkeypatch.setenv("CODEX_HOME", str(home))
    return home, workspace, events, source, records, cli


def capture(owned, usage=USAGE):
    _, workspace, events, _, _, _ = owned
    return capture_host_telemetry(events, workspace, START, END, usage)


def test_only_allowlisted_owned_metadata_is_exported_with_source_lineage(owned):
    result = capture(owned)
    assert result["host_selected_model"] == "selected-model"
    assert result["host_selected_effort"] == "UNKNOWN"
    assert result["host_selected_provider"] == "openai"
    assert result["provider_attested_model"] == result["billing"] == "UNKNOWN"
    assert result["token_usage"]["complete"] is False
    assert result["token_usage"]["cli_completed_usage_matches"] is True
    assert "private-" not in json.dumps(result)
    lineage = result["lineage"]
    assert lineage["source_sha256"] == hashlib.sha256(owned[3].read_bytes()).hexdigest()
    original_lines = owned[3].read_bytes().splitlines(keepends=True)
    for record in lineage["allowed_records"]:
        assert (
            record["sha256"]
            == hashlib.sha256(original_lines[record["line"] - 1]).hexdigest()
        )
    assert [record["type"] for record in lineage["allowed_records"]] == [
        "session_meta",
        "task_started",
        "turn_context",
        "token_count",
        "task_complete",
    ]


def test_timeout_snapshots_remain_partial_and_never_become_completed_usage(owned):
    records = [
        r for r in owned[4] if r.get("payload", {}).get("type") != "task_complete"
    ]
    write_jsonl(owned[3], records)
    write_jsonl(owned[2], owned[5][:1])
    result = capture(owned, "UNKNOWN")
    assert result["token_usage"]["latest_total"] == TOKENS
    assert result["token_usage"]["complete"] is False
    assert result["token_usage"]["cli_completed_usage_matches"] is False
    assert result["completion"]["task_complete_observed"] is False
    assert result["provider_attested_model"] == "UNKNOWN"


@pytest.mark.parametrize("foreign", ["session", "cwd", "turn", "completion", "stale"])
def test_foreign_or_stale_metadata_is_rejected(owned, foreign):
    records = owned[4]
    if foreign == "session":
        records[0]["payload"]["id"] = OTHER
    elif foreign == "cwd":
        records[0]["payload"]["cwd"] = str(owned[1].parent)
    elif foreign == "turn":
        records[2]["payload"]["turn_id"] = OTHER
    elif foreign == "completion":
        records[4]["payload"]["turn_id"] = OTHER
    else:
        records[2]["timestamp"] = "2026-10-09T03:28:29Z"
    write_jsonl(owned[3], records)
    with pytest.raises(ValueError):
        capture(owned)


@pytest.mark.parametrize("count", [-1, True, "12081"])
def test_invalid_or_boolean_token_counts_are_rejected(owned, count):
    owned[4][3]["payload"]["info"]["total_token_usage"]["input_tokens"] = count
    write_jsonl(owned[3], owned[4])
    with pytest.raises(ValueError):
        capture(owned)


def test_altered_completed_usage_cannot_be_reconciled_with_local_totals(owned):
    usage = {**USAGE, "output_tokens": 61}
    owned[5][1]["usage"] = usage
    write_jsonl(owned[2], owned[5])
    with pytest.raises(ValueError, match="Local totals"):
        capture(owned, usage)
    with pytest.raises(ValueError, match="actual event"):
        capture(owned)


@pytest.mark.parametrize("position", [1, 5])
def test_token_snapshots_before_start_or_after_completion_are_rejected(owned, position):
    records = owned[4]
    snapshot = records.pop(3)
    records.insert(position, snapshot)
    write_jsonl(owned[3], records)
    with pytest.raises(ValueError, match="bound turn sequence"):
        capture(owned)


@pytest.mark.parametrize(
    "kind", ["thread", "source", "context", "symlink", "malformed"]
)
def test_ambiguous_or_unsafe_sources_are_rejected(owned, kind):
    if kind == "thread":
        write_jsonl(owned[2], [*owned[5], owned[5][0]])
    elif kind == "source":
        write_jsonl(owned[0] / "archived_sessions" / owned[3].name, owned[4])
    elif kind == "context":
        write_jsonl(owned[3], [*owned[4], owned[4][2]])
    elif kind == "symlink":
        original = owned[3].with_suffix(".original")
        owned[3].rename(original)
        owned[3].symlink_to(original)
    else:
        with owned[3].open("a") as source:
            source.write("{malformed}\n")
    with pytest.raises(ValueError):
        capture(owned)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO requires POSIX")
def test_fifo_source_is_rejected_without_blocking(owned):
    source = owned[3]
    source.unlink()
    os.mkfifo(source)
    script = """
from pathlib import Path
import sys
from examples.incident_replay.telemetry import _regular_bytes
_regular_bytes(Path(sys.argv[1]), Path(sys.argv[2]))
"""
    result = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-c",
            script,
            str(source),
            str(owned[0]),
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=3,
        check=False,
    )
    assert result.returncode != 0
    assert "Telemetry source must be a regular file." in result.stderr


def test_bounded_local_date_lookup_and_flat_archive_are_supported(owned, monkeypatch):
    previous = os.environ.get("TZ")
    try:
        monkeypatch.setenv("TZ", "America/Los_Angeles")
        if hasattr(time, "tzset"):
            time.tzset()
        local_path = owned[0] / "sessions/2026/10/08" / owned[3].name
        local_path.parent.mkdir(parents=True)
        owned[3].rename(local_path)
        assert Path(capture(owned)["lineage"]["source_path"]) == local_path
        archive = owned[0] / "archived_sessions" / local_path.name
        archive.parent.mkdir()
        local_path.rename(archive)
        assert Path(capture(owned)["lineage"]["source_path"]) == archive
        stale = owned[0] / "sessions/2026/09/01" / archive.name
        stale.parent.mkdir(parents=True)
        archive.rename(stale)
        with pytest.raises(ValueError, match="exactly one"):
            capture(owned)
    finally:
        if previous is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", previous)
        if hasattr(time, "tzset"):
            time.tzset()
