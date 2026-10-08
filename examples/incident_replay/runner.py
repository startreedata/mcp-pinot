"""Replay public alerts through merged MCP tools; private labels are never read."""

import argparse
import asyncio
from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import tomllib

if __package__:
    from .common import executable_path, loopback_url, replay_env
else:
    from common import executable_path, loopback_url, replay_env

TOOLS = ("begin_investigation", "query_incident", "get_trace", "finish_investigation")
NAMESPACE = "io.github.startreedata/mcp-pinot"
DISABLED = (
    "shell_tool",
    "unified_exec",
    "apps",
    "plugins",
    "browser_use",
    "browser_use_external",
    "computer_use",
    "image_generation",
    "view_image",
    "memories",
    "multi_agent",
    "multi_agent_v2",
    "goals",
    "hooks",
    "sleep_tool",
    "skill_search",
    "workspace_dependencies",
    "remote_plugin",
    "skill_mcp_dependency_install",
)


def write(path: Path, value: dict) -> None:
    path.write_text(
        json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )


def decision(status: str, reason: str, hypothesis: dict | None = None) -> dict:
    return {"status": status, "hypothesis": hypothesis, "reason": reason}


def evidence_from(calls: list[dict]) -> tuple[dict, str | None]:
    evidence = {}
    for call in calls:
        if call["tool"] not in ("query_incident", "get_trace"):
            continue
        wire = call["response"]
        record = wire.get("result", {}).get("structuredContent")
        if wire.get("error") or wire.get("result", {}).get("isError") or not record:
            return {}, "A native evidence call failed."
        if not record.get("complete"):
            return {}, "Native execution was incomplete, unknown, or truncated."
        if call["args"].get("run_id") != record["run_id"] or (
            record["kind"]
            != ("trace" if call["tool"] == "get_trace" else call["args"].get("kind"))
        ):
            return (
                {},
                "The recorded evidence does not match the executed tool arguments.",
            )
        digest = hashlib.sha256(
            json.dumps(
                {k: v for k, v in record.items() if k != "sha256"},
                sort_keys=True,
                allow_nan=False,
            ).encode()
        ).hexdigest()
        metadata = record["metadata"]
        timing = wire["result"].get("_meta", {}).get(NAMESPACE, {})
        if (
            digest != record["sha256"]
            or hashlib.sha256(record["sql"].encode()).hexdigest()
            != metadata["query_sha256"]
            or not timing.get("request_id")
            or timing["request_id"] != metadata["request_id"]
        ):
            return {}, "Evidence hash or request correlation did not verify."
        evidence[record["kind"]] = {"record": record, "args": call["args"]}
    return evidence, None


def policy(evidence: dict, case: dict, *, require_onset: bool = True) -> dict:
    if not all(
        kind in evidence
        for kind in ("baseline", "incident", "watermark", "changes", "trace")
    ):
        return decision(
            "incomplete",
            "Required cohort, watermark, change, or trace evidence is missing.",
        )
    counts = {}
    for kind in ("baseline", "incident"):
        cohorts = defaultdict(lambda: defaultdict(int))
        for row in evidence[kind]["record"]["rows"]:
            count = row["count"]
            if type(count) not in (int, float) or count < 0 or int(count) != count:
                return decision("incomplete", "Cohort counts are invalid.")
            cohorts[(row["service"], row["version"], row["zone"])][
                row["errorClass"]
            ] += int(count)
        counts[kind] = cohorts

    def rate(rows: dict, *, local: bool = False) -> tuple[int, float]:
        n = sum(rows.values())
        errors = sum(
            count
            for error, count in rows.items()
            if error != "ok" and (not local or error != "dependency_timeout")
        )
        return n, errors / n if n else 0.0

    services = {key[0] for cohorts in counts.values() for key in cohorts}
    if case["service"] not in services:
        return decision("incomplete", "The alert service has no observed cohorts.")
    watermarks = {
        row["service"]: row["watermark_ms"]
        for row in evidence["watermark"]["record"]["rows"]
    }
    if any(watermarks.get(service, -1) < case["end_ms"] - 1000 for service in services):
        return decision(
            "incomplete", "Observed services lack fresh watermark evidence."
        )
    for kind in counts:
        if any(
            sum(
                sum(errors.values())
                for key, errors in counts[kind].items()
                if key[0] == service
            )
            < 40
            for service in services
        ):
            return decision(
                "incomplete", "A service lacks baseline or incident samples."
            )
        if any(sum(errors.values()) < 40 for errors in counts[kind].values()):
            return decision(
                "incomplete", "A service/version/zone cohort has fewer than 40 spans."
            )

    target_rates = []
    for kind in counts:
        target = defaultdict(int)
        for (service, _, _), errors in counts[kind].items():
            if service == case["service"]:
                for error, count in errors.items():
                    target[error] += count
        target_rates.append(rate(target)[1])
    if target_rates[1] < 0.10 or target_rates[1] - target_rates[0] < 0.10:
        return decision(
            "abstained", "The alert service lacks a qualifying error increase."
        )

    candidates = defaultdict(set)
    for key, errors in counts["incident"].items():
        baseline = counts["baseline"].get(key, {})
        n, incident_rate = rate(errors, local=True)
        before_n, before_rate = rate(baseline, local=True)
        if (
            n >= 40
            and before_n >= 40
            and incident_rate >= 0.10
            and incident_rate - before_rate >= 0.10
        ):
            candidates[key[:2]].add(key[2])
    if len(candidates) != 1:
        return decision("abstained", "No unique local-error service/version candidate.")
    (service, version), zones = next(iter(candidates.items()))
    for zone in zones:
        controls = [
            key
            for key, errors in counts["incident"].items()
            if key[0] == service
            and key[1] != version
            and key[2] == zone
            and rate(errors)[0] >= 40
            and rate(errors)[1] <= 0.05
            and rate(counts["baseline"].get(key, {}))[0] >= 40
            and rate(counts["baseline"].get(key, {}))[1] <= 0.05
        ]
        if not controls:
            return decision(
                "abstained",
                "Version and zone are confounded; matched healthy controls are absent.",
            )
    hypothesis = {
        "kind": "deployment",
        "service": service,
        "version": version,
        "zone": next(iter(zones)) if len(zones) == 1 else None,
    }
    spans = evidence["trace"]["record"]["rows"]
    by_id = {row["spanId"]: row for row in spans}
    if not spans or len(by_id) != len(spans) or any(not key for key in by_id):
        return decision(
            "incomplete",
            "The recorded trace has missing or ambiguous span identifiers.",
        )
    if any(row["parentSpanId"] and row["parentSpanId"] not in by_id for row in spans):
        return decision("incomplete", "The recorded trace has a missing parent span.")
    visited = set()
    for span_id in by_id:
        path = set()
        while span_id and span_id not in visited:
            if span_id in path:
                return decision("incomplete", "The recorded trace has a parent cycle.")
            path.add(span_id)
            span_id = by_id[span_id]["parentSpanId"]
        visited.update(path)
    connected = False
    for row in spans:
        if row["service"] != service or row["errorClass"] in (
            "ok",
            "dependency_timeout",
        ):
            continue
        seen = set()
        while row["spanId"] not in seen:
            seen.add(row["spanId"])
            if row["service"] == case["service"] and row["errorClass"] != "ok":
                connected = True
                break
            row = by_id.get(row["parentSpanId"])
            if row is None:
                break
    if not connected:
        return decision(
            "abstained", "No recorded local-error span chain reaches the alert service."
        )
    changes = evidence["changes"]["record"]["rows"]
    matching = [
        row
        for row in changes
        if row["eventType"] in ("deploy", "deployment")
        and row["service"] == service
        and row["version"] == version
        and row["zone"] in ("", "all", *zones)
    ]
    if not matching:
        return decision("abstained", "No actual matching deployment event.")
    if require_onset:
        onset = evidence.get("onset")
        if (
            not onset
            or {key: onset["args"].get("candidate", {}).get(key) for key in hypothesis}
            != hypothesis
        ):
            return decision(
                "incomplete", "The matching full-window onset query is missing."
            )
        rows = onset["record"]["rows"]
        if len(rows) != 1 or rows[0]["count"] <= 0:
            return decision("incomplete", "The candidate onset has no observed errors.")
        if not any(row["eventTs"] <= rows[0]["min_eventTs"] for row in matching):
            return decision(
                "abstained", "The deployment follows the observed error onset."
            )
    return decision(
        "proposed",
        "Observed association passes the synthetic checks; cause remains unvalidated.",
        hypothesis,
    )


def qualify(calls: list[dict], case: dict) -> dict:
    if any(
        call["tool"] == "get_trace" and call["args"].get("trace_id") != case["trace_id"]
        for call in calls
    ):
        return {
            **decision(
                "incomplete", "The trace differs from the public alert context."
            ),
            "qualified": False,
        }
    if any(call["tool"] not in TOOLS or call.get("rejected") for call in calls):
        return {
            **decision("incomplete", "A forbidden tool was attempted."),
            "qualified": False,
        }
    evidence, error = evidence_from(calls)
    outcome = decision("incomplete", error) if error else policy(evidence, case)
    outcome["qualified"] = outcome["status"] != "incomplete"
    return outcome


def actual_finish(calls: list[dict]) -> dict | None:
    """Observe the successful server finish independently of host qualification."""
    for call in reversed(calls):
        if call["tool"] != "finish_investigation":
            continue
        result = call["response"].get("result", {})
        receipt = result.get("structuredContent")
        if (
            not call["response"].get("error")
            and not result.get("isError")
            and isinstance(receipt, dict)
            and receipt.get("status") in ("proposed", "abstained", "incomplete")
        ):
            return receipt
    return None


def finish_receipt(calls: list[dict], case: dict) -> dict | None:
    finishes = [call for call in calls if call["tool"] == "finish_investigation"]
    if len(finishes) != 1:
        return None
    call = finishes[0]
    result = call["response"].get("result", {})
    receipt = result.get("structuredContent")
    if call["response"].get("error") or result.get("isError") or not receipt:
        return None
    arguments = call["args"]
    supplied = arguments.get("hypothesis")
    hypothesis = (
        {key: supplied.get(key) for key in ("kind", "service", "version", "zone")}
        if supplied is not None
        else None
    )
    if (
        arguments.get("run_id") != receipt["run_id"]
        or arguments.get("status", "proposed") != receipt["status"]
        or hypothesis != receipt["hypothesis"]
        or set(arguments.get("citations", [])) != set(receipt["citations"])
    ):
        return None
    _evidence, error = evidence_from(calls)
    if error and receipt["status"] != "incomplete":
        return None
    begins = [c for c in calls if c["tool"] == "begin_investigation"]
    expected = {
        key: case[key]
        for key in ("profile_id", "service", "baseline_start_ms", "start_ms", "end_ms")
    }
    if len(begins) != 1 or begins[0]["args"] != expected:
        return None
    begin = begins[0]["response"].get("result", {}).get("structuredContent", {})
    if begin.get("run_id") != receipt["run_id"]:
        return None
    records = [
        c["response"].get("result", {}).get("structuredContent", {})
        for c in calls
        if c["tool"] in ("query_incident", "get_trace")
    ]
    for record in records:
        if not record:
            continue
        digest = hashlib.sha256(
            json.dumps(
                {key: value for key, value in record.items() if key != "sha256"},
                sort_keys=True,
                allow_nan=False,
            ).encode()
        ).hexdigest()
        if record.get("run_id") != receipt["run_id"] or digest != record.get("sha256"):
            return None
    retained = {
        r.get("evidence_id"): r for r in records if r.get("run_id") == receipt["run_id"]
    }
    if any(
        key not in retained or retained[key].get("sha256") != sha
        for key, sha in receipt["citations"].items()
    ):
        return None
    if any(
        receipt.get(key) is not False
        for key in (
            "hypothesis_validated",
            "confirmed_cause",
            "dataset_coverage_attested",
        )
    ):
        return None
    return receipt


async def scripted(
    args: argparse.Namespace, case: dict, directory: Path
) -> tuple[list[dict], dict]:
    from fastmcp import Client
    from fastmcp.client.transports import StdioTransport

    calls = []
    transport = StdioTransport(
        command=sys.executable,
        args=[
            "-m",
            "mcp_pinot.server",
            "--incident-profiles",
            str(args.dataset / "profiles.json"),
        ],
        env=replay_env(args.broker, args.controller),
        cwd=str(Path(__file__).resolve().parents[2]),
        log_file=Path(os.devnull),
    )
    async with Client(transport) as client:

        async def call(tool: str, arguments: dict) -> dict:
            started = time.monotonic()
            wire = await client.call_tool_mcp(tool, arguments, timeout=args.timeout)
            response = {"result": wire.model_dump(mode="json", by_alias=True)}
            calls.append(
                {
                    "tool": tool,
                    "args": arguments,
                    "response": response,
                    "elapsed_ms": (time.monotonic() - started) * 1000,
                }
            )
            write(directory / "calls.json", {"calls": calls})
            if wire.is_error or wire.structured_content is None:
                raise ValueError("MCP tool failed: " + tool)
            return wire.structured_content

        opened = await call(
            "begin_investigation",
            {
                key: case[key]
                for key in (
                    "profile_id",
                    "service",
                    "baseline_start_ms",
                    "start_ms",
                    "end_ms",
                )
            },
        )
        run_id = opened["run_id"]
        for kind in ("baseline", "incident", "watermark", "changes"):
            await call("query_incident", {"run_id": run_id, "kind": kind})
        await call("get_trace", {"run_id": run_id, "trace_id": case["trace_id"]})
        evidence, error = evidence_from(calls)
        outcome = (
            decision("incomplete", error)
            if error
            else policy(evidence, case, require_onset=False)
        )
        if outcome["status"] == "proposed":
            await call(
                "query_incident",
                {"run_id": run_id, "kind": "onset", "candidate": outcome["hypothesis"]},
            )
        outcome = qualify(calls, case)
        citations = [
            c["response"]["result"]["structuredContent"]["evidence_id"]
            for c in calls
            if c["tool"] in ("query_incident", "get_trace")
        ]
        await call(
            "finish_investigation",
            {
                "run_id": run_id,
                "citations": citations,
                "status": outcome["status"],
                "hypothesis": outcome["hypothesis"],
                "reason": outcome["reason"],
            },
        )
    return calls, {"model_usage": "UNKNOWN"}


def model_selection(*, cli_defaults: bool = False) -> dict:
    if cli_defaults:
        return {}
    config_path = (
        Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "config.toml"
    )
    config = tomllib.loads(config_path.read_text()) if config_path.exists() else {}
    if config.get("profile"):
        config = {**config, **config.get("profiles", {}).get(config["profile"], {})}
    selection = {
        key: config[key]
        for key in ("model", "model_reasoning_effort", "model_provider")
        if isinstance(config.get(key), str)
    }
    if selection.get("model_provider", "openai") != "openai":
        raise ValueError(
            "Custom CLI providers require an audited launcher; no provider fallback."
        )
    return selection


def normalized_result(result: dict, *, default_error: bool = False) -> dict:
    """Normalize transport spellings while preserving the complete tool response."""
    result = dict(result)
    for source, target in (
        ("structured_content", "structuredContent"),
        ("is_error", "isError"),
    ):
        if source in result:
            if target in result and json.dumps(
                result[target], sort_keys=True
            ) != json.dumps(result[source], sort_keys=True):
                raise ValueError("Conflicting MCP result aliases.")
            result[target] = result.pop(source)
    result.setdefault("isError", default_error)
    if result.get("structuredContent") is None:
        result.pop("structuredContent", None)
    return result


def cli_receipts(lines: list[str], calls: list[dict]) -> dict:
    """Bind completed CLI receipts to actual audited responses, including finish."""
    usage, observed_model = "UNKNOWN", None
    completed, issues, warnings = [], [], []
    for line in lines:
        try:
            event = json.loads(line)
        except ValueError:
            issues.append("Malformed CLI event.")
            continue
        if event.get("type") == "turn.completed" and event.get("usage"):
            usage = event["usage"]
        observed_model = event.get("model") or observed_model
        if event.get("type") in ("error", "turn.failed"):
            issues.append("CLI reported a failed turn.")
        if event.get("type") not in ("item.started", "item.completed"):
            continue
        item = event.get("item", {})
        warning_prefix = (
            "Under-development features enabled: skip_host_skill_discovery. "
            "Under-development features are incomplete and may behave unpredictably. "
            "To suppress this warning, set "
            "`suppress_unstable_features_warning = true` in "
        )
        if (
            item.get("type") == "error"
            and isinstance(item.get("message"), str)
            and item["message"].startswith(warning_prefix)
            and item["message"].endswith("/config.toml.")
        ):
            warnings.append(item)
            continue
        if item.get("type") in ("reasoning", "agent_message", "plan"):
            continue
        if (
            item.get("type") != "mcp_tool_call"
            or item.get("server") != "incident_reader"
            or item.get("tool") not in TOOLS
        ):
            issues.append("CLI attempted an unexpected tool.")
            continue
        if event["type"] != "item.completed":
            continue
        result = item.get("result")
        if (
            item.get("status") != "completed"
            or item.get("error")
            or not isinstance(result, dict)
        ):
            issues.append("CLI tool call failed: " + item["tool"])
        if isinstance(result, dict):
            try:
                result = normalized_result(
                    result, default_error=item.get("status") == "failed"
                )
            except ValueError:
                issues.append("CLI returned conflicting result aliases.")
            if result.get("isError") is not False:
                issues.append("CLI returned a tool error: " + item["tool"])
        completed.append((item["tool"], item.get("arguments"), result))
    captured = []
    for call in calls:
        result = call["response"].get("result")
        if isinstance(result, dict):
            result = normalized_result(result)
        if (
            call["response"].get("error")
            or not isinstance(result, dict)
            or result["isError"]
        ):
            issues.append("Production MCP returned an error: " + str(call["tool"]))
        captured.append((call["tool"], call["args"], result))
    if Counter(json.dumps(item, sort_keys=True) for item in completed) != Counter(
        json.dumps(item, sort_keys=True) for item in captured
    ):
        issues.append("CLI responses do not match the production MCP audit.")
    return {
        "model_usage": usage,
        "observed_model": observed_model or "UNKNOWN",
        "cli_issues": issues,
        "cli_warnings": warnings,
    }


def model(
    args: argparse.Namespace, case: dict, directory: Path
) -> tuple[list[dict], dict]:
    codex = executable_path(args.codex, program="codex")
    workspace = directory / "workspace"
    workspace.mkdir()
    selection = model_selection(cli_defaults=args.cli_defaults)
    argv = [
        codex,
        "exec",
        "--ignore-user-config",
        "--ignore-rules",
        "--ephemeral",
        "--json",
        "--sandbox",
        "read-only",
        "--skip-git-repo-check",
        "--color",
        "never",
        "-C",
        str(workspace),
    ]
    for feature in DISABLED:
        argv += ["--disable", feature]
    argv += ["--enable", "skip_host_skill_discovery", "--enable", "code_mode_host"]
    config = {
        **selection,
        "approval_policy": "never",
        "web_search": "disabled",
        "project_doc_max_bytes": 0,
        "suppress_unstable_features_warning": True,
        "mcp_servers.incident_reader.command": sys.executable,
        "mcp_servers.incident_reader.args": [
            str(Path(__file__).with_name("model.py")),
            "--profiles",
            str(args.dataset / "profiles.json"),
            "--broker",
            args.broker,
            "--controller",
            args.controller,
            "--audit",
            str(directory / "calls.jsonl"),
        ],
        "mcp_servers.incident_reader.enabled_tools": list(TOOLS),
        "mcp_servers.incident_reader.required": True,
        "mcp_servers.incident_reader.default_tools_approval_mode": "auto",
    }
    for key, value in config.items():
        argv += ["-c", key + "=" + json.dumps(value)]
    argv += ["-"]
    prompt = (
        "Investigate this public synthetic alert using only incident_reader MCP tools. "
        "Begin one run with the profile/service/windows below. Collect baseline, "
        "incident, watermark, changes, and get_trace using the public trace_id. "
        "Collect evidence sequentially: await each query before starting the next. "
        "The run admits at most four concurrent queries. "
        "Require >=40 spans per cohort and fresh watermarks within 1000ms of end. "
        "Alert error rate must be >=10% and increase >=10 percentage points. "
        "A deployment proposal needs a unique local-error service/version increase, "
        "healthy same-zone other-version controls (<=5%), actual span-parent chain "
        "to the alert service, and matching deployment before full-window onset. "
        "Query onset for the actual candidate. Multiple affected zones mean zone=null. "
        "Healthy, unrelated, or confounded "
        "observations support abstention. Missing/sparse/partial evidence "
        "is incomplete. "
        "Call finish_investigation with actual same-run citations, a hypothesis "
        "{kind,service,version,zone} or null, and a reason of at most 240 characters. "
        "Finish remains an unvalidated association, never confirmed cause. "
        "Do not access files or other tools. Public case: " + json.dumps(case)
    )
    with (
        (directory / "events.jsonl").open("x") as events,
        (directory / "cli-stderr.log").open("x") as stderr,
    ):
        process = subprocess.Popen(  # noqa: S603
            argv,
            stdin=subprocess.PIPE,
            stdout=events,
            stderr=stderr,
            text=True,
            start_new_session=True,
            shell=False,
        )
        timed_out = False
        try:
            process.communicate(prompt, timeout=args.timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)
    audit = directory / "calls.jsonl"
    calls = (
        [json.loads(line) for line in audit.read_text().splitlines()]
        if audit.exists()
        else []
    )
    cli = cli_receipts((directory / "events.jsonl").read_text().splitlines(), calls)
    runtime = {
        "selection_mode": "cli_defaults" if args.cli_defaults else "configured",
        **cli,
        "configured_selection": selection,
        "exit_code": process.returncode,
        "timed_out": timed_out,
    }
    if cli["model_usage"] != "UNKNOWN":
        runtime["provider_receipt"] = {
            "source": "codex.exec",
            "usage": cli["model_usage"],
        }
    return calls, runtime


async def run(args: argparse.Namespace) -> dict:
    public = json.loads((args.dataset / "public.json").read_text())
    if public.get("schema_version") != 1:
        raise ValueError("Unsupported public fixture schema.")
    args.output.mkdir(parents=True, exist_ok=False)
    predictions = {"schema_version": 1, "mode": args.mode, "cases": []}
    for index, case in enumerate(public["cases"]):
        directory = args.output / str(index)
        directory.mkdir()
        started = time.monotonic()
        calls, runtime = [], {}
        error = None
        try:
            calls, runtime = (
                await asyncio.wait_for(scripted(args, case, directory), args.timeout)
                if args.mode == "scripted"
                else model(args, case, directory)
            )
        except Exception as exc:
            error = type(exc).__name__ + ": " + str(exc)[:512]
            saved = directory / "calls.json"
            if saved.exists():
                calls = json.loads(saved.read_text())["calls"]
        raw_finish = actual_finish(calls)
        receipt = finish_receipt(calls, case)
        qualification = qualify(calls, case)
        if receipt and qualification["qualified"]:
            evidence, _ = evidence_from(calls)
            required = {entry["record"]["evidence_id"] for entry in evidence.values()}
            if not required.issubset(receipt["citations"]):
                qualification["qualified"] = False
                qualification["reason"] = (
                    "Actual finish omits supporting evidence citations."
                )
        if receipt is not None and (
            receipt["status"] != qualification["status"]
            or receipt["hypothesis"] != qualification["hypothesis"]
        ):
            qualification["qualified"] = False
            qualification["reason"] = (
                "Actual finish does not match independently qualified evidence."
            )
        elapsed_ms = (time.monotonic() - started) * 1000
        verified = (
            receipt is not None
            and not error
            and not runtime.get("timed_out", False)
            and runtime.get("exit_code", 0) == 0
            and not runtime.get("cli_issues")
            and elapsed_ms <= args.timeout * 1000
        )
        if not verified:
            qualification["qualified"] = False
        prediction = {
            "case_id": case["case_id"],
            "status": raw_finish["status"] if raw_finish else "incomplete",
            "hypothesis": raw_finish["hypothesis"] if raw_finish else None,
            "elapsed_ms": elapsed_ms,
            "calls": calls,
            "raw_finish": raw_finish,
            "verified_finish": verified,
            "qualification": qualification,
            **runtime,
        }
        if error or not verified:
            prediction["error"] = (
                error or "No verified finish within the host deadline."
            )
        predictions["cases"].append(prediction)
        write(args.output / "predictions.json", predictions)
    return predictions


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--broker", type=loopback_url, required=True)
    parser.add_argument("--controller", type=loopback_url, required=True)
    parser.add_argument("--mode", choices=("scripted", "model"), default="scripted")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--codex", default=shutil.which("codex") or "codex")
    parser.add_argument(
        "--cli-defaults",
        action="store_true",
        help="Let the CLI select its model and effort instead of copying user settings",
    )
    args = parser.parse_args()
    args.dataset, args.output = args.dataset.resolve(), args.output.resolve()
    if not 0 < args.timeout <= 300:
        parser.error("timeout must be positive and at most 300 seconds")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
