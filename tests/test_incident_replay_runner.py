"""Check host qualification against counterexamples and preserve raw model output."""

import argparse
from copy import deepcopy
import hashlib
import json
import subprocess
import sys

from examples.incident_replay import runner
from examples.incident_replay.common import replay_env
import pytest

from mcp_pinot.incidents import IncidentEvidence, IncidentFinish
from mcp_pinot.models import QueryExecutionMetadata


def evidence():
    cohorts = [
        {
            "service": service,
            "version": version,
            "zone": zone,
            "errorClass": "ok",
            "count": 40,
        }
        for zone in ("east", "west")
        for service, version in (
            ("checkout", "v1"),
            ("payments", "v1"),
            ("payments", "v2"),
        )
    ]
    incident = []
    for row in cohorts:
        if row["version"] == "v2" or row["service"] == "checkout":
            incident += [
                {**row, "count": 20},
                {
                    **row,
                    "count": 20,
                    "errorClass": (
                        "timeout"
                        if row["service"] == "payments"
                        else "dependency_timeout"
                    ),
                },
            ]
        else:
            incident.append(row)
    hypothesis = {
        "kind": "deployment",
        "service": "payments",
        "version": "v2",
        "zone": None,
    }
    rows = {
        "baseline": cohorts,
        "incident": incident,
        "watermark": [
            {"service": s, "watermark_ms": 9999} for s in ("checkout", "payments")
        ],
        "changes": [
            {
                "eventType": "deploy",
                "service": "payments",
                "version": "v2",
                "zone": "all",
                "eventTs": 5100,
            }
        ],
        "trace": [
            {
                "spanId": "root",
                "parentSpanId": "",
                "service": "checkout",
                "errorClass": "dependency_timeout",
            },
            {
                "spanId": "child",
                "parentSpanId": "root",
                "service": "payments",
                "errorClass": "timeout",
            },
        ],
        "onset": [{"count": 40, "min_eventTs": 6000}],
    }
    return {
        kind: {"record": {"rows": value}, "args": {"candidate": hypothesis}}
        for kind, value in rows.items()
    }


CASE = {
    "case_id": "public-case",
    "profile_id": "public-profile",
    "service": "checkout",
    "baseline_start_ms": 1000,
    "start_ms": 5000,
    "end_ms": 10000,
    "trace_id": "trace-1",
}


@pytest.mark.parametrize(
    "counterexample,status",
    [
        (None, "proposed"),
        ("controls", "abstained"),
        ("baseline", "incomplete"),
        ("parent", "incomplete"),
        ("cycle", "incomplete"),
        ("onset", "abstained"),
    ],
)
def test_qualification_requires_controls_actual_parent_and_preceding_change(
    counterexample, status
):
    records = evidence()
    if counterexample == "controls":
        records["incident"]["record"]["rows"] = [
            row
            for row in records["incident"]["record"]["rows"]
            if row["service"] != "payments" or row["version"] == "v2"
        ]
    elif counterexample == "baseline":
        records["baseline"]["record"]["rows"] = [
            row
            for row in records["baseline"]["record"]["rows"]
            if row["service"] != "checkout"
        ]
    elif counterexample == "parent":
        records["trace"]["record"]["rows"][1]["parentSpanId"] = "missing"
    elif counterexample == "cycle":
        records["trace"]["record"]["rows"][0]["parentSpanId"] = "child"
    elif counterexample == "onset":
        records["changes"]["record"]["rows"][0]["eventTs"] = 7000
    assert runner.policy(records, CASE)["status"] == status


def calls():
    opened = {
        key: CASE[key]
        for key in ("profile_id", "service", "baseline_start_ms", "start_ms", "end_ms")
    }
    records = [
        {
            "tool": "begin_investigation",
            "args": opened,
            "response": {"result": {"structuredContent": {"run_id": "run"}}},
        }
    ]
    citations = {}
    for kind, item in evidence().items():
        sql = "SELECT '" + kind + "'"
        record = IncidentEvidence(
            run_id="run",
            evidence_id="ev_" + kind,
            kind=kind,
            sql=sql,
            complete=True,
            rows=item["record"]["rows"],
            metadata=QueryExecutionMetadata(
                completeness="complete",
                request_id=kind,
                query_sha256=hashlib.sha256(sql.encode()).hexdigest(),
            ),
        ).model_dump()
        record["sha256"] = hashlib.sha256(
            json.dumps(
                {k: v for k, v in record.items() if k != "sha256"},
                sort_keys=True,
            ).encode()
        ).hexdigest()
        citations[record["evidence_id"]] = record["sha256"]
        records.append(
            {
                "tool": "get_trace" if kind == "trace" else "query_incident",
                "args": (
                    {"run_id": "run", "trace_id": CASE["trace_id"]}
                    if kind == "trace"
                    else {
                        "run_id": "run",
                        "kind": kind,
                        **(item["args"] if kind == "onset" else {}),
                    }
                ),
                "response": {
                    "result": {
                        "structuredContent": record,
                        "_meta": {runner.NAMESPACE: {"request_id": kind}},
                    }
                },
            }
        )
    # A server can legitimately record this unsupported caller hypothesis.
    hypothesis = {
        "kind": "deployment",
        "service": "inventory",
        "version": "v2",
        "zone": None,
    }
    finished = IncidentFinish(
        run_id="run",
        status="proposed",
        hypothesis=hypothesis,
        citations=citations,
        reason="Model assertion",
        query_count=6,
        failed_evidence_ids=[],
    ).model_dump()
    records.append(
        {
            "tool": "finish_investigation",
            "args": {
                "run_id": "run",
                "status": "proposed",
                "hypothesis": hypothesis,
                "citations": list(citations),
            },
            "response": {"result": {"structuredContent": finished}},
        }
    )
    return records


@pytest.mark.asyncio
async def test_unqualified_model_proposal_remains_visible(monkeypatch, tmp_path):
    dataset = tmp_path / "fixture"
    dataset.mkdir()
    (dataset / "public.json").write_text(
        json.dumps({"schema_version": 1, "cases": [CASE]})
    )
    monkeypatch.setattr(runner, "model", lambda *args: (calls(), {"exit_code": 0}))
    predictions = await runner.run(
        argparse.Namespace(
            dataset=dataset,
            output=tmp_path / "predictions",
            mode="model",
            timeout=60,
        )
    )
    result = predictions["cases"][0]
    assert result["verified_finish"]
    assert (
        result["status"] == "proposed"
        and result["hypothesis"]["service"] == "inventory"
    )
    assert not result["qualification"]["qualified"]
    assert result["qualification"]["hypothesis"]["service"] == "payments"


def test_replay_child_config_ignores_remote_environment_and_checkout_dotenv(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("PINOT_BROKER_HOST", "remote.invalid")
    monkeypatch.setenv("PINOT_BROKER_PORT", "9999")
    monkeypatch.setenv("PINOT_TOKEN", "inherited-test-token")
    (tmp_path / ".env").write_text(
        "PINOT_PASSWORD=dotenv-test-secret\nPINOT_TOKEN_FILENAME=private-token\n"
        "PINOT_TABLE_FILTER_FILE=missing-filter\nPINOT_CONTROLLER_TOKEN=private\n"
        "PINOT_DATABASE=remote_database\nPINOT_USE_MSQE=true\n"
    )
    script = (
        "import json; from mcp_pinot.config import load_pinot_config; "
        "p=load_pinot_config(); print(json.dumps([p.broker_host,p.broker_port,"
        "p.broker_scheme,p.controller_url,p.username,p.password,p.token,"
        "p.controller_username,p.controller_password,p.controller_token,"
        "p.database,p.use_msqe,p.table_filter_file]))"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        env=replay_env("http://127.0.0.1:18000", "http://127.0.0.1:19090"),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert json.loads(result.stdout) == [
        "127.0.0.1",
        18000,
        "http",
        "http://127.0.0.1:19090",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        False,
        "",
    ]


def test_replay_helper_preserves_cli_auth_but_never_loads_checkout_secrets(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("OPENAI_API_KEY", "cli-auth-test-value")
    monkeypatch.setenv("OAUTH_CLIENT_SECRET", "parent-oauth-test-value")
    (tmp_path / ".env").write_text("OAUTH_CLIENT_SECRET=checkout-oauth-test-value\n")
    script = (
        "import json, os; from mcp_pinot.config import load_pinot_config; "
        "load_pinot_config(); print(json.dumps(["
        "os.environ.get('OPENAI_API_KEY') == 'cli-auth-test-value',"
        "os.environ.get('OAUTH_CLIENT_SECRET') is None]))"
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        env=replay_env("http://127.0.0.1:18000", "http://127.0.0.1:19090"),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert json.loads(result.stdout) == [True, True]


@pytest.mark.parametrize(
    "changed", [None, "structured", "meta", "content", "failed", "native_error"]
)
def test_cli_receipt_matches_full_received_finish_and_rejects_errors(changed):
    audited = deepcopy(calls()[-1])
    wire = audited["response"]["result"]
    wire.update(
        content=[{"type": "text", "text": json.dumps(wire["structuredContent"])}],
        _meta={runner.NAMESPACE: {"request_id": "finish"}},
        isError=False,
    )
    received = deepcopy(wire)
    received["structured_content"] = received.pop("structuredContent")
    received.pop("isError")  # The real CLI omits this default-false field.
    item = {
        "type": "mcp_tool_call",
        "server": "incident_reader",
        "tool": audited["tool"],
        "arguments": audited["args"],
        "status": "completed",
        "error": None,
        "result": received,
    }
    if changed == "structured":
        received["structured_content"]["hypothesis"]["service"] = "payments"
    elif changed == "meta":
        received["_meta"][runner.NAMESPACE]["request_id"] = "different"
    elif changed == "content":
        received["content"][0]["text"] = "Different actual result"
    elif changed == "failed":
        item["status"], item["error"] = "failed", "Native failure"
    elif changed == "native_error":
        wire["isError"] = True
    result = runner.cli_receipts(
        [json.dumps({"type": "item.completed", "item": item})], [audited]
    )
    assert bool(result["cli_issues"]) is (changed is not None)


def test_known_startup_warning_is_preserved_without_hiding_other_errors():
    message = (
        "Under-development features enabled: skip_host_skill_discovery. "
        "Under-development features are incomplete and may behave unpredictably. "
        "To suppress this warning, set "
        "`suppress_unstable_features_warning = true` in /tmp/config.toml."
    )
    warning = {"type": "error", "message": message}
    line = json.dumps({"type": "item.completed", "item": warning})
    result = runner.cli_receipts([line], [])
    assert result["cli_warnings"] == [warning] and not result["cli_issues"]
    unknown = json.dumps(
        {
            "type": "item.completed",
            "item": {"type": "error", "message": "Real CLI error"},
        }
    )
    assert runner.cli_receipts([unknown], [])["cli_issues"]


@pytest.mark.asyncio
async def test_failed_prior_evidence_keeps_actual_proposal_visible_but_unverified(
    monkeypatch, tmp_path
):
    recorded = calls()
    recorded.insert(
        1,
        {
            "tool": "query_incident",
            "args": {"run_id": "run", "kind": "incident"},
            "response": {
                "result": {
                    "isError": True,
                    "content": [{"type": "text", "text": "Admission budget exhausted"}],
                }
            },
        },
    )
    dataset = tmp_path / "fixture"
    dataset.mkdir()
    (dataset / "public.json").write_text(
        json.dumps({"schema_version": 1, "cases": [CASE]})
    )
    monkeypatch.setattr(runner, "model", lambda *args: (recorded, {"exit_code": 0}))
    predictions = await runner.run(
        argparse.Namespace(
            dataset=dataset, output=tmp_path / "predictions", mode="model", timeout=60
        )
    )
    result = predictions["cases"][0]
    assert (
        result["status"] == "proposed" and result["raw_finish"]["status"] == "proposed"
    )
    assert (
        result["hypothesis"]
        == recorded[-1]["response"]["result"]["structuredContent"]["hypothesis"]
    )
    assert not result["verified_finish"] and not result["qualification"]["qualified"]
    assert result["error"]
