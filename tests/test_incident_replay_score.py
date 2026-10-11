"""Guard independent scoring against case omission and unsupported proposals."""

from copy import deepcopy
import hashlib
import json

from examples.incident_replay.runner import qualify
from examples.incident_replay.score import score
import pytest
from test_incident_replay_runner import CASE, calls


def documents():
    hypothesis = {"kind": "deployment", "service": "payments", "version": "v2"}
    truth = {
        "schema_version": 1,
        "cases": [
            {"case_id": "a", "status": "proposed", "hypothesis": hypothesis},
            {"case_id": "b", "status": "abstained", "hypothesis": None},
        ],
    }
    for case in truth["cases"]:
        case["public_scope"] = {
            "case_id": case["case_id"],
            "profile_id": "public-profile",
            "service": "checkout",
            "baseline_start_ms": 100,
            "start_ms": 200,
            "end_ms": 300,
            "trace_id": "public-trace",
        }
    predictions = {
        "schema_version": 1,
        "mode": "test",
        "cases": [
            dict(case, elapsed_ms=10, calls=[{"tool": "query_incident"}])
            for case in deepcopy(truth["cases"])
        ],
    }
    return predictions, truth


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unexpected"])
def test_incomplete_or_duplicate_case_sets_cannot_be_scored(mutation):
    predictions, truth = documents()
    if mutation == "missing":
        predictions["cases"].pop()
    elif mutation == "duplicate":
        predictions["cases"].append(deepcopy(predictions["cases"][0]))
    else:
        predictions["cases"][0]["case_id"] = "foreign"
    with pytest.raises(ValueError):
        score(predictions, truth)


def test_wrong_hypothesis_and_unsupported_proposal_count_as_false_proposals():
    predictions, truth = documents()
    predictions["cases"][0]["hypothesis"]["version"] = "v999"
    predictions["cases"][1].update(
        status="proposed",
        hypothesis={"kind": "deployment", "service": "checkout"},
        correct=True,
    )
    report = score(predictions, truth)
    assert report["correct_count"] == 0
    assert report["false_proposals"] == 2
    assert report["proposal_precision"] == 0
    assert report["token_usage"] == report["cost_usd"] == "UNKNOWN"


def test_qualification_and_finish_receipts_do_not_override_raw_prediction_score():
    predictions, truth = documents()
    predictions["cases"][0]["hypothesis"]["version"] = "v999"
    predictions["cases"][0]["verified_finish"] = True
    predictions["cases"][0]["qualification"] = dict(truth["cases"][0], qualified=True)
    report = score(predictions, truth)
    assert report["correct_count"] == 1
    assert report["completed_count"] == 0
    assert report["qualified_count"] == 0
    assert report["qualified_state_matches"] == 0
    assert report["cases"][0]["unsupported_verification_claim"] is True
    assert report["tool_counts"] == {"query_incident": 2}
    assert len(report["sha256"]) == 64


@pytest.mark.parametrize(
    "corrupt", [None, "digest", "foreign", "outcome", "tool_error", "scope"]
)
def test_completion_is_bound_to_real_same_run_citations_and_finish(corrupt):
    predictions, truth = documents()
    case = predictions["cases"][0]
    run_id = "inc_" + "1" * 32
    evidence_id = "ev_" + "2" * 32
    sql = "SELECT 'baseline'"
    evidence = {
        "evidence_id": evidence_id,
        "run_id": run_id,
        "kind": "baseline",
        "complete": True,
        "sql": sql,
        "metadata": {
            "request_id": "baseline",
            "query_sha256": hashlib.sha256(sql.encode()).hexdigest(),
        },
    }
    digest = hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest()
    evidence["sha256"] = digest
    finish = {
        **deepcopy(truth["cases"][0]),
        "run_id": run_id,
        "citations": {evidence_id: digest},
        "hypothesis_validated": False,
        "confirmed_cause": False,
        "dataset_coverage_attested": False,
    }
    arguments = {
        "run_id": run_id,
        "citations": [evidence_id],
        "status": case["status"],
        "hypothesis": deepcopy(case["hypothesis"]),
    }
    case["calls"] = [
        {
            "tool": "begin_investigation",
            "args": {
                key: truth["cases"][0]["public_scope"][key]
                for key in (
                    "profile_id",
                    "service",
                    "baseline_start_ms",
                    "start_ms",
                    "end_ms",
                )
            },
            "response": {"result": {"structuredContent": {"run_id": run_id}}},
        },
        {
            "tool": "query_incident",
            "args": {"run_id": run_id, "kind": "baseline"},
            "response": {
                "result": {
                    "structuredContent": evidence,
                    "_meta": {
                        "io.github.startreedata/mcp-pinot": {"request_id": "baseline"}
                    },
                }
            },
        },
        {
            "tool": "finish_investigation",
            "args": arguments,
            "response": {"result": {"structuredContent": finish}},
        },
    ]
    case["qualification"] = dict(truth["cases"][0], qualified=True)
    if corrupt == "digest":
        evidence["sha256"] = "0" * 64
    elif corrupt == "foreign":
        case["calls"][1]["args"]["run_id"] = "inc_" + "3" * 32
    elif corrupt == "outcome":
        finish["hypothesis"]["version"] = "v999"
    elif corrupt == "tool_error":
        case["calls"][-1]["response"]["result"]["isError"] = True
    elif corrupt == "scope":
        case["calls"][0]["args"]["profile_id"] = "foreign-profile"
    report = score(predictions, truth)
    assert report["completed_count"] == int(corrupt is None)
    # Receipt binding does not turn missing cohorts/watermarks/trace into evidence.
    assert report["qualified_count"] == 0


@pytest.mark.parametrize("coverage", ["all", "baseline", "duplicate", "incomplete"])
def test_qualification_requires_finish_to_cite_selected_evidence(coverage):
    recorded = calls()
    run_id = "inc_" + "1" * 32
    citations = {}
    for index, call in enumerate(recorded):
        record = call["response"]["result"]["structuredContent"]
        record["run_id"] = run_id
        if "run_id" in call["args"]:
            call["args"]["run_id"] = run_id
        if call["tool"] in ("query_incident", "get_trace"):
            record["evidence_id"] = "ev_" + f"{index:032x}"
            record["sha256"] = hashlib.sha256(
                json.dumps(
                    {key: value for key, value in record.items() if key != "sha256"},
                    sort_keys=True,
                ).encode()
            ).hexdigest()
            citations[record["evidence_id"]] = record["sha256"]
    outcome = qualify(recorded, CASE)
    finish = recorded[-1]
    receipt = finish["response"]["result"]["structuredContent"]
    if coverage == "duplicate":
        duplicate = deepcopy(recorded[1])
        record = duplicate["response"]["result"]["structuredContent"]
        record["evidence_id"] = "ev_" + "9" * 32
        record["sha256"] = hashlib.sha256(
            json.dumps(
                {key: value for key, value in record.items() if key != "sha256"},
                sort_keys=True,
            ).encode()
        ).hexdigest()
        recorded.insert(1, duplicate)
        receipt["query_count"] += 1
    if coverage in ("baseline", "incomplete"):
        citations = dict(list(citations.items())[:1])
    if coverage == "incomplete":
        outcome = {"status": "incomplete", "hypothesis": None}
        citations = {}
    receipt.update(
        status=outcome["status"], hypothesis=outcome["hypothesis"], citations=citations
    )
    finish["args"].update(
        status=outcome["status"],
        hypothesis=outcome["hypothesis"],
        citations=list(citations),
    )
    case = {
        **CASE,
        "status": outcome["status"],
        "hypothesis": outcome["hypothesis"],
        "elapsed_ms": 10,
        "calls": recorded,
    }
    report = score(
        {"schema_version": 1, "mode": "test", "cases": [case]},
        {
            "schema_version": 1,
            "cases": [{**case, "public_scope": CASE}],
        },
    )
    assert report["correct_count"] == 1
    assert report["completed_count"] == int(coverage != "baseline")
    assert report["qualified_count"] == int(coverage in ("all", "duplicate"))
