"""Guard independent scoring against case omission and unsupported proposals."""

from copy import deepcopy
import hashlib
import json

from examples.incident_replay.score import score
import pytest


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
    evidence = {"evidence_id": evidence_id, "run_id": run_id, "complete": True}
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
            "args": {"run_id": run_id},
            "response": {"result": {"structuredContent": evidence}},
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
