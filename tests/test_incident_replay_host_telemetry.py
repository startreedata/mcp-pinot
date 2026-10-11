"""Keep optional host diagnostics and archive failures outside finish verification."""

import argparse
from copy import deepcopy
import hashlib
import json
import subprocess
from unittest.mock import Mock

from examples.incident_replay import runner
import pytest
from test_incident_replay_runner import CASE, calls
from test_incident_replay_telemetry import (
    END,
    OTHER,
    START,
    THREAD,
    USAGE,
    write_jsonl,
)
from test_incident_replay_telemetry import (
    owned as existing_owned_fixture,
)


@pytest.fixture
def owned_rollout(tmp_path, monkeypatch):
    return existing_owned_fixture.__wrapped__(tmp_path, monkeypatch)


@pytest.fixture
def replay_model(owned_rollout, monkeypatch):
    home, workspace, events, source, records, _ = owned_rollout
    recorded = calls()
    hypothesis = {
        "kind": "deployment",
        "service": "payments",
        "version": "v2",
        "zone": None,
    }
    recorded[-1]["args"]["hypothesis"] = hypothesis
    recorded[-1]["response"]["result"]["structuredContent"]["hypothesis"] = deepcopy(
        hypothesis
    )
    state = {"calls": recorded, "home": home, "source": source}

    def model(args, _case, directory):
        workspace.rename(directory / "workspace")
        events.rename(directory / "events.jsonl")
        records[0]["payload"]["cwd"] = str(directory / "workspace")
        write_jsonl(source, records)
        state["original_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
        runtime = {
            "exit_code": 0,
            "timed_out": False,
            "cli_issues": [],
            "model_usage": deepcopy(USAGE),
        }
        if args.host_telemetry:
            runtime["host_telemetry_window"] = {
                "started_at": START.isoformat(),
                "finished_at": END.isoformat(),
            }
        return recorded, runtime

    state["model"] = model
    monkeypatch.setattr(runner, "model", model)
    monkeypatch.setattr(runner, "executable_path", lambda *_args, **_kwargs: "codex")
    return state


def arguments(*, host_telemetry=True):
    return argparse.Namespace(
        mode="model", timeout=60, codex="codex", host_telemetry=host_telemetry
    )


def assert_original_finish(prediction, replay_model):
    assert prediction["verified_finish"] is True
    assert prediction["qualification"]["qualified"] is True
    assert prediction["status"] == "proposed"
    assert prediction["calls"] == replay_model["calls"]
    assert (
        prediction["raw_finish"]
        == replay_model["calls"][-1]["response"]["result"]["structuredContent"]
    )
    assert "error" not in prediction


@pytest.mark.asyncio
async def test_host_telemetry_opt_out_never_captures_or_archives(
    replay_model, monkeypatch, tmp_path
):
    capture = Mock(side_effect=AssertionError("Unexpected metadata capture"))
    archive = Mock(side_effect=AssertionError("Unexpected archive command"))
    monkeypatch.setattr(runner, "capture_host_telemetry", capture)
    monkeypatch.setattr(runner.subprocess, "run", archive)

    directory = tmp_path / "prediction"
    prediction = await runner._prediction(
        arguments(host_telemetry=False), CASE, directory
    )

    assert_original_finish(prediction, replay_model)
    capture.assert_not_called()
    archive.assert_not_called()
    assert "host_telemetry" not in prediction
    assert "full_elapsed_ms" not in prediction
    assert not (directory / "host-telemetry.json").exists()


@pytest.mark.asyncio
async def test_foreign_metadata_never_archives_or_invalidates_verified_finish(
    owned_rollout, replay_model, monkeypatch, tmp_path
):
    owned_rollout[4][0]["payload"]["id"] = OTHER
    archive = Mock(side_effect=AssertionError("Foreign session must not be archived"))
    monkeypatch.setattr(runner.subprocess, "run", archive)

    directory = tmp_path / "prediction"
    prediction = await runner._prediction(arguments(), CASE, directory)

    assert_original_finish(prediction, replay_model)
    archive.assert_not_called()
    assert (
        prediction["host_telemetry_error"]
        == "ValueError: Foreign telemetry session ID."
    )
    assert "host_telemetry" not in prediction
    assert "host_session_archive_ms" not in prediction
    assert not (directory / "host-telemetry.json").exists()
    assert replay_model["source"].exists()


@pytest.mark.asyncio
async def test_archive_is_owned_and_byte_preserving_with_separate_elapsed_times(
    replay_model, monkeypatch, tmp_path
):
    source = replay_model["source"]
    archived_source = replay_model["home"] / "archived_sessions" / source.name
    unrelated = source.with_name(f"rollout-unrelated-{OTHER}.jsonl")
    unrelated.write_bytes(b"unrelated session must remain untouched\n")
    clock = [100.0]
    monkeypatch.setattr(runner.time, "monotonic", lambda: clock[0])

    def model(*args):
        clock[0] += 2
        return replay_model["model"](*args)

    capture_actual = runner.capture_host_telemetry
    capture_count = [0]

    def capture(*args):
        capture_count[0] += 1
        clock[0] += 0.5 if capture_count[0] == 1 else 0.25
        return capture_actual(*args)

    archive_commands = []

    def archive(argv, **kwargs):
        archive_commands.append(argv)
        assert kwargs == {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
            "timeout": 10,
            "shell": False,
        }
        clock[0] += 3
        archived_source.parent.mkdir()
        source.rename(archived_source)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(runner, "model", model)
    monkeypatch.setattr(runner, "capture_host_telemetry", capture)
    monkeypatch.setattr(runner.subprocess, "run", archive)

    directory = tmp_path / "prediction"
    prediction = await runner._prediction(arguments(), CASE, directory)

    assert_original_finish(prediction, replay_model)
    assert archive_commands == [["codex", "archive", THREAD]]
    assert capture_count[0] == 2
    assert prediction["host_session_archived"] is True
    assert prediction["host_session_bytes_preserved"] is True
    telemetry = prediction["host_telemetry"]
    assert telemetry["lineage"]["archived_source_path"] == str(archived_source)
    assert telemetry["lineage"]["source_sha256"] == replay_model["original_sha256"]
    assert (
        hashlib.sha256(archived_source.read_bytes()).hexdigest()
        == replay_model["original_sha256"]
    )
    assert telemetry["provider_attested_model"] == telemetry["billing"] == "UNKNOWN"
    assert json.loads((directory / "host-telemetry.json").read_text()) == telemetry
    assert unrelated.read_bytes() == b"unrelated session must remain untouched\n"
    assert prediction["elapsed_ms"] == 2000
    assert prediction["host_telemetry_capture_ms"] == 500
    assert prediction["host_session_archive_ms"] == 3250
    assert prediction["telemetry_elapsed_ms"] == 3750
    assert prediction["full_elapsed_ms"] == 5750


@pytest.mark.asyncio
async def test_archive_timeout_is_diagnostic_and_preserves_verified_finish(
    replay_model, monkeypatch, tmp_path
):
    def timeout(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"])

    archive = Mock(side_effect=timeout)
    monkeypatch.setattr(runner.subprocess, "run", archive)

    directory = tmp_path / "prediction"
    prediction = await runner._prediction(arguments(), CASE, directory)

    assert_original_finish(prediction, replay_model)
    assert archive.call_args.args == (["codex", "archive", THREAD],)
    assert prediction["host_telemetry_error"].startswith("TimeoutExpired: ")
    assert "host_session_bytes_preserved" not in prediction
    assert prediction["host_telemetry"]["thread_id"] == THREAD
    assert (directory / "host-telemetry.json").exists()
    assert replay_model["source"].exists()


@pytest.mark.asyncio
async def test_changed_archive_bytes_are_reported_without_rewriting_finish(
    replay_model, monkeypatch, tmp_path
):
    source = replay_model["source"]

    def archive(argv, **_kwargs):
        archived_source = replay_model["home"] / "archived_sessions" / source.name
        archived_source.parent.mkdir()
        source.rename(archived_source)
        with archived_source.open("ab") as destination:
            destination.write(b'{"type":"response_item","payload":{}}\n')
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(runner.subprocess, "run", archive)
    prediction = await runner._prediction(arguments(), CASE, tmp_path / "prediction")

    assert_original_finish(prediction, replay_model)
    assert prediction["host_session_archived"] is False
    assert "host_session_bytes_preserved" not in prediction
    assert prediction["host_telemetry_error"] == (
        "ValueError: Archived host session changed its original bytes."
    )


@pytest.mark.asyncio
async def test_diagnostic_write_failure_still_persists_verified_prediction(
    replay_model, monkeypatch, tmp_path
):
    args = arguments()
    args.dataset = tmp_path / "fixture"
    args.dataset.mkdir()
    (args.dataset / "public.json").write_text(
        json.dumps({"schema_version": 1, "cases": [CASE]})
    )
    args.output = tmp_path / "predictions"
    diagnostic_path = args.output / "0" / "host-telemetry.json"
    original_write = runner.write

    def write(path, value):
        if path == diagnostic_path:
            raise OSError("Cannot persist host diagnostics")
        return original_write(path, value)

    source = replay_model["source"]

    def archive(argv, **_kwargs):
        archived_source = replay_model["home"] / "archived_sessions" / source.name
        archived_source.parent.mkdir()
        source.rename(archived_source)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(runner, "write", write)
    monkeypatch.setattr(runner.subprocess, "run", archive)
    predictions = await runner.run(args)
    prediction = predictions["cases"][0]

    assert_original_finish(prediction, replay_model)
    assert prediction["host_telemetry_error"] == (
        "OSError: Cannot persist host diagnostics"
    )
    assert prediction["host_session_archived"] is True
    assert prediction["host_session_bytes_preserved"] is True
    assert not diagnostic_path.exists()
    assert json.loads((args.output / "predictions.json").read_text()) == predictions


@pytest.mark.asyncio
async def test_successful_archive_command_must_actually_move_owned_session(
    replay_model, monkeypatch, tmp_path
):
    archive = Mock(
        side_effect=lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0)
    )
    monkeypatch.setattr(runner.subprocess, "run", archive)
    prediction = await runner._prediction(arguments(), CASE, tmp_path / "prediction")

    assert_original_finish(prediction, replay_model)
    assert archive.call_args.args == (["codex", "archive", THREAD],)
    assert prediction["host_session_archived"] is False
    assert "host_session_bytes_preserved" not in prediction
    assert prediction["host_telemetry_error"] == (
        "ValueError: Owned host session remains outside the archive."
    )
    assert replay_model["source"].exists()
