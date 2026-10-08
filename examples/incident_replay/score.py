"""Independently score replay predictions against operator-owned private truth."""

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
from typing import Any

if __package__:
    from .runner import qualify
else:
    from runner import qualify

STATUSES = {"proposed", "abstained", "incomplete"}
HYPOTHESIS_FIELDS = ("kind", "service", "version", "zone")


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def _cases(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if document.get("schema_version") != 1 or not isinstance(
        document.get("cases"), list
    ):
        raise ValueError("Require schema_version=1 and a cases array.")
    cases: dict[str, dict[str, Any]] = {}
    for case in document["cases"]:
        if not isinstance(case, dict) or not isinstance(case.get("case_id"), str):
            raise ValueError("Every case requires a string case_id.")
        identity = case["case_id"]
        if not identity or identity in cases:
            raise ValueError("Empty or duplicate case_id.")
        cases[identity] = case
    if not cases:
        raise ValueError("An empty evaluation cannot be scored.")
    return cases


def _outcome(case: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    status = case.get("status")
    if status not in STATUSES:
        raise ValueError("Outcome requires proposed, abstained, or incomplete status.")
    hypothesis = case.get("hypothesis")
    if status != "proposed":
        if hypothesis is not None:
            raise ValueError("Non-proposed outcomes cannot carry a hypothesis.")
        return status, None
    if not isinstance(hypothesis, dict) or set(hypothesis) - set(HYPOTHESIS_FIELDS):
        raise ValueError("Proposed requires a bounded hypothesis with known fields.")
    normalized = {field: hypothesis.get(field) for field in HYPOTHESIS_FIELDS}
    if normalized["kind"] not in {
        "deployment",
        "traffic_switch",
        "experiment",
        "environment",
    } or any(
        value is not None and not isinstance(value, str)
        for value in normalized.values()
    ):
        raise ValueError("Invalid hypothesis fields.")
    return status, normalized


def _verified_finish(case: dict[str, Any], scope: dict[str, Any]) -> bool:
    """Bind a recorded successful finish to its actual run, evidence, and outcome."""
    calls = case.get("calls", [])
    if not isinstance(calls, list) or any(not isinstance(call, dict) for call in calls):
        return False
    begins = [call for call in calls if call.get("tool") == "begin_investigation"]
    finishes = [call for call in calls if call.get("tool") == "finish_investigation"]
    if len(begins) != 1 or len(finishes) != 1 or finishes[0] is not calls[-1]:
        return False

    def record(call: dict[str, Any]) -> dict[str, Any]:
        wire = call.get("response", {})
        result = wire.get("result", {})
        if wire.get("error") or result.get("isError"):
            raise ValueError("MCP call did not succeed.")
        value = result.get("structuredContent")
        if not isinstance(value, dict):
            raise ValueError("MCP structured response is missing.")
        return value

    try:
        begin, finished = record(begins[0]), record(finishes[0])
        expected_begin = {
            key: scope[key]
            for key in (
                "profile_id",
                "service",
                "baseline_start_ms",
                "start_ms",
                "end_ms",
            )
        }
        if begins[0].get("args") != expected_begin:
            return False
        run_id = begin["run_id"]
        if (
            not isinstance(run_id, str)
            or re.fullmatch(r"inc_[0-9a-f]{32}", run_id) is None
        ):
            return False
        if finished.get("run_id") != run_id or _outcome(finished) != _outcome(case):
            return False
        arguments = finishes[0]["args"]
        citations = finished["citations"]
        if (
            arguments.get("run_id") != run_id
            or not isinstance(citations, dict)
            or not isinstance(arguments.get("citations"), list)
            or len(arguments["citations"]) != len(set(arguments["citations"]))
            or set(arguments["citations"]) != set(citations)
            or _outcome(dict(arguments, status=arguments.get("status", "proposed")))
            != _outcome(finished)
            or any(
                finished.get(field) is not False
                for field in (
                    "hypothesis_validated",
                    "confirmed_cause",
                    "dataset_coverage_attested",
                )
            )
        ):
            return False
        evidence = {}
        opened = False
        for call in calls[:-1]:
            if call is begins[0]:
                opened = True
            if call.get("tool") not in ("query_incident", "get_trace"):
                continue
            if (
                call["tool"] == "get_trace"
                and call.get("args", {}).get("trace_id") != scope["trace_id"]
            ):
                return False
            value = record(call)
            identity = value["evidence_id"]
            digest = hashlib.sha256(
                json.dumps(
                    {key: item for key, item in value.items() if key != "sha256"},
                    sort_keys=True,
                    allow_nan=False,
                ).encode()
            ).hexdigest()
            if (
                not opened
                or value.get("run_id") != run_id
                or call.get("args", {}).get("run_id") != run_id
                or not isinstance(identity, str)
                or re.fullmatch(r"ev_[0-9a-f]{32}", identity) is None
                or identity in evidence
                or value.get("sha256") != digest
            ):
                return False
            evidence[identity] = value
        if not citations and finished["status"] != "incomplete":
            return False
        return all(
            identity in evidence
            and evidence[identity]["sha256"] == digest
            and (
                finished["status"] == "incomplete"
                or evidence[identity].get("complete") is True
            )
            for identity, digest in citations.items()
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return False


def score(predictions: dict[str, Any], truth: dict[str, Any]) -> dict[str, Any]:
    """Compare raw predictions without trusting a runner's correctness labels."""
    expected, observed = _cases(truth), _cases(predictions)
    if set(expected) != set(observed):
        raise ValueError(
            "Missing or unexpected cases; partial evaluations are rejected."
        )
    mode = predictions.get("mode")
    if not isinstance(mode, str) or not mode:
        raise ValueError("Predictions require an explicit mode.")
    correct = completed = qualified = qualified_matches = proposals = (
        false_proposals
    ) = 0
    statuses: Counter[str] = Counter()
    tool_counts: Counter[str] = Counter()
    timings: list[float] = []
    details: list[dict[str, Any]] = []
    usage: list[dict[str, int]] = []
    for identity, case in observed.items():
        actual_outcome, expected_outcome = _outcome(case), _outcome(expected[identity])
        scope = expected[identity].get("public_scope")
        if not isinstance(scope, dict) or scope.get("case_id") != identity:
            raise ValueError("Private truth requires a matching trusted public_scope.")
        error = bool(case.get("error"))
        match = not error and actual_outcome == expected_outcome
        correct += int(match)
        statuses[actual_outcome[0]] += 1
        proposals += int(actual_outcome[0] == "proposed")
        false_proposals += int(actual_outcome[0] == "proposed" and not match)
        verified = not error and _verified_finish(case, scope)
        completed += int(verified)
        try:
            qualification = qualify(case.get("calls", []), scope)
            qualified_outcome = _outcome(qualification)
        except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
            qualification = {
                "qualified": False,
                "status": "incomplete",
                "hypothesis": None,
                "reason": "Recorded semantic evidence is malformed.",
            }
            qualified_outcome = _outcome(qualification)
        bound = verified and qualified_outcome == actual_outcome
        qualified += int(qualification.get("qualified") is True and bound)
        qualified_match = bound and qualified_outcome == expected_outcome
        qualified_matches += int(qualified_match)
        elapsed = case.get("elapsed_ms")
        if (
            type(elapsed) not in (int, float)
            or not math.isfinite(elapsed)
            or elapsed < 0
        ):
            raise ValueError("Every case requires a finite nonnegative elapsed_ms.")
        timings.append(float(elapsed))
        calls = case.get("calls")
        if not isinstance(calls, list):
            raise ValueError("Every case requires recorded tool calls.")
        for call in calls:
            name = (
                call
                if isinstance(call, str)
                else (
                    call.get("tool", call.get("name"))
                    if isinstance(call, dict)
                    else None
                )
            )
            if not isinstance(name, str) or not name:
                raise ValueError("Every tool call requires its actual tool name.")
            tool_counts[name] += 1
        receipt = case.get("provider_receipt")
        if isinstance(receipt, dict) and receipt.get("source") == "codex.exec":
            tokens = receipt.get("usage")
            if isinstance(tokens, dict) and all(
                type(tokens.get(field)) is int and tokens[field] >= 0
                for field in ("input_tokens", "output_tokens")
            ):
                usage.append(tokens)
        details.append(
            {
                "case_id": identity,
                "correct": match,
                "expected_status": expected_outcome[0],
                "status": actual_outcome[0],
                "hypothesis": actual_outcome[1],
                "qualified_state_matches": qualified_match,
                "qualification": qualification,
                "verified_finish": verified,
                "unsupported_verification_claim": case.get("verified_finish") is True
                and not verified,
                "error": error,
            }
        )
    sorted_times = sorted(timings)
    total = len(observed)
    report = {
        "schema_version": 1,
        "mode": mode,
        "case_count": total,
        "correct_count": correct,
        "accuracy": correct / total,
        "proposal_count": proposals,
        "false_proposals": false_proposals,
        "proposal_precision": (proposals - false_proposals) / proposals
        if proposals
        else None,
        "abstentions": statuses["abstained"],
        "incomplete": statuses["incomplete"],
        "completed_count": completed,
        "qualified_count": qualified,
        "qualified_state_matches": qualified_matches,
        "latency_ms": {
            "median": statistics.median(timings),
            "observed_p95": sorted_times[math.ceil(total * 0.95) - 1],
        },
        "tool_counts": dict(sorted(tool_counts.items())),
        "tool_call_count": sum(tool_counts.values()),
        "token_usage": (
            {
                "input_tokens": sum(tokens["input_tokens"] for tokens in usage),
                "output_tokens": sum(tokens["output_tokens"] for tokens in usage),
            }
            if len(usage) == total
            else "UNKNOWN"
        ),
        "cost_usd": "UNKNOWN",
        "cases": details,
        "input_sha256": {"predictions": _digest(predictions), "truth": _digest(truth)},
        "scope": "synthetic_fixture_semantics; production_RCA_and_savings_unproven",
    }
    report["sha256"] = _digest(report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--truth", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = score(
        json.loads(args.predictions.read_text()), json.loads(args.truth.read_text())
    )
    report["file_sha256"] = {
        "predictions": hashlib.sha256(args.predictions.read_bytes()).hexdigest(),
        "truth": hashlib.sha256(args.truth.read_bytes()).hexdigest(),
    }
    report["sha256"] = _digest(
        {key: value for key, value in report.items() if key != "sha256"}
    )
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
