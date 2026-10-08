"""Live MCP protocol fault checks, excluded from model-accuracy evaluation.

The MCP HTTP/auth stack runs in process. Every submitted SQL request is forwarded
to a real, explicitly selected loopback Pinot broker through a receipt proxy.
Only the partial-response check modifies a response, and its receipt records both
the original native payload and the injected server-counter mismatch.
"""

import argparse
import asyncio
from contextlib import contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.metadata import version
import json
import os
from pathlib import Path
import re
import secrets
from threading import Thread
import time
from types import ModuleType
from typing import Any

if __package__:
    from .common import loopback_url, replay_env
else:
    from common import loopback_url, replay_env

from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier
import httpx
import httpx2

from mcp_pinot.config import PinotConfig
from mcp_pinot.incidents import IncidentProfile, IncidentService
from mcp_pinot.pinot_client import PinotClient


def _load_local_server(broker: str, controller: str) -> ModuleType:
    """Initialize the server under local config without changing the caller env."""
    env = replay_env(broker, controller)
    original = dict(os.environ)
    try:
        os.environ.clear()
        os.environ.update(env)
        import mcp_pinot.server as server

        return server
    finally:
        os.environ.clear()
        os.environ.update(original)


class _BrokerProxy:
    """Forward real SQL and retain native receipts; inject only tagged partials."""

    def __init__(self, broker: str) -> None:
        self.broker = broker
        self.partial = False
        self.receipts: list[dict[str, Any]] = []

    @contextmanager
    def running(self):
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                request = json.loads(body)
                options = str(request.get("queryOptions", ""))
                query_id = re.search(r"(?:^|;)clientQueryId=([^;]+)", options)
                receipt: dict[str, Any] = {
                    "sequence": len(owner.receipts),
                    "request": request,
                    "client_query_id": query_id.group(1) if query_id else None,
                    "sql_sha256": hashlib.sha256(
                        str(request.get("sql", "")).encode()
                    ).hexdigest(),
                    "injected_fault": None,
                }
                try:
                    response = httpx.post(
                        owner.broker + "/query/sql",
                        content=body,
                        headers={"Content-Type": "application/json"},
                        timeout=30,
                        follow_redirects=False,
                    )
                    native = response.json()
                    receipt["native_http_status"] = response.status_code
                    receipt["native_response"] = native
                    receipt["native_query_id"] = native.get("requestId")
                    delivered = json.loads(json.dumps(native))
                    if owner.partial:
                        queried = native.get("numServersQueried")
                        responded = native.get("numServersResponded")
                        if (
                            response.status_code != 200
                            or native.get("exceptions")
                            or type(queried) is not int
                            or type(responded) is not int
                            or queried != responded
                            or queried <= 0
                        ):
                            raise ValueError(
                                "Cannot inject partial into failed native SQL."
                            )
                        delivered["numServersQueried"] = queried + 1
                        receipt["injected_fault"] = {
                            "field": "numServersQueried",
                            "original": queried,
                            "delivered": queried + 1,
                        }
                    receipt["delivered_response"] = delivered
                    status, payload = (
                        response.status_code,
                        json.dumps(delivered).encode(),
                    )
                except Exception as error:
                    receipt["proxy_error_type"] = type(error).__name__
                    status, payload = 502, b'{"error":"upstream or injection failed"}'
                owner.receipts.append(receipt)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        http_server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(
            target=http_server.serve_forever, kwargs={"poll_interval": 0.05}
        )
        thread.start()
        try:
            yield http_server.server_address[1]
        finally:
            http_server.shutdown()
            http_server.server_close()
            thread.join(timeout=2)


async def run_probes(dataset: Path, broker: str, controller: str) -> dict[str, Any]:
    """Use the first public fixture scope; keep faults separate from predictions."""
    broker, controller = loopback_url(broker), loopback_url(controller)
    public = json.loads((dataset / "public.json").read_text())
    scope = public["cases"][0]
    configured = json.loads((dataset / "profiles.json").read_text())
    source = IncidentProfile.model_validate(configured[scope["profile_id"]])
    principals = ["subject:replay-alice", "subject:replay-bob"]
    base = source.model_copy(update={"authorized_principals": principals})
    profiles = {
        "probe": base,
        "probe-expiry": base.model_copy(update={"deadline_seconds": 1}),
        "probe-native-failure": base.model_copy(
            update={"table": "replay_missing_" + secrets.token_hex(8)}
        ),
    }
    tokens = {name: secrets.token_urlsafe(32) for name in ("alice", "bob")}
    verifier = StaticTokenVerifier(
        tokens={
            token: {
                "client_id": "replay-shared-client",
                "sub": "replay-" + name,
                "scopes": ["pinot:read"],
            }
            for name, token in tokens.items()
        },
        required_scopes=["pinot:read"],
    )
    report: dict[str, Any] = {
        "schema_version": 1,
        "mode": "live_protocol_fault_probes",
        "excluded_from_model_accuracy": True,
        "mcp_transport": "streamable_http_over_asgi",
        "authentication": "StaticTokenVerifier, two bearer subjects, shared client",
        "broker": broker,
        "pinotdb_version": version("pinotdb"),
        "fixture_case_id": scope["case_id"],
        "checks": [],
        "calls": [],
        "all_passed": False,
    }
    proxy = _BrokerProxy(broker)
    report["broker_receipts"] = proxy.receipts
    server = _load_local_server(broker, controller)
    saved = server._auth, server.mcp.auth, server.pinot_client, server._incident_service
    active_probe = "setup"

    async def call(client: Client, principal: str, tool: str, args: dict[str, Any]):
        # Exercise the production limiter without making burst capacity a fixture.
        await asyncio.sleep(1 / server._RATE_LIMIT_RPS + 0.01)
        started = time.monotonic()
        receipt = {
            "probe": active_probe,
            "principal": principal,
            "tool": tool,
            "args": args,
        }
        report["calls"].append(receipt)
        try:
            result = await client.call_tool_mcp(tool, args)
            wire = result.model_dump(mode="json", by_alias=True)
            receipt["wire_result"] = wire
            return wire
        except Exception as error:
            receipt["protocol_error_type"] = type(error).__name__
            raise
        finally:
            receipt["elapsed_ms"] = round((time.monotonic() - started) * 1000)

    def check(name: str, passed: bool, **details: Any) -> None:
        report["checks"].append({"name": name, "passed": passed, **details})

    def data(wire: dict[str, Any]) -> dict[str, Any]:
        return wire.get("structuredContent") or {}

    def rejected(wire: dict[str, Any], message: str) -> bool:
        return wire.get("isError") is True and any(
            message in item.get("text", "") for item in wire.get("content", [])
        )

    def begin_args(profile_id: str) -> dict[str, Any]:
        return {"profile_id": profile_id} | {
            key: scope[key]
            for key in ("service", "baseline_start_ms", "start_ms", "end_ms")
        }

    try:
        with proxy.running() as port:
            server._auth = server.mcp.auth = verifier
            server.pinot_client = PinotClient(
                PinotConfig(
                    controller_url=controller,
                    broker_host="127.0.0.1",
                    broker_port=port,
                    broker_scheme="http",
                    username=None,
                    password=None,
                    token=None,
                    database="",
                    use_msqe=False,
                    request_timeout=30,
                    connection_timeout=5,
                    query_timeout=30,
                )
            )
            server._incident_service = IncidentService(
                profiles, server._execute_incident_query
            )
            app = server.mcp.http_app(
                path="/mcp", stateless_http=True, json_response=True
            )

            def factory(**kwargs):
                return httpx2.AsyncClient(
                    transport=httpx2.ASGITransport(app=app), **kwargs
                )

            def client(name: str) -> Client:
                return Client(
                    StreamableHttpTransport(
                        "http://127.0.0.1/mcp",
                        auth=tokens[name],
                        httpx_client_factory=factory,
                    )
                )

            async with (
                app.lifespan(app),
                client("alice") as alice,
                client("bob") as bob,
            ):
                active_probe = "healthy_and_foreign_owner"
                opened = await call(
                    alice, "alice", "begin_investigation", begin_args("probe")
                )
                run_id = data(opened)["run_id"]
                own_bob = await call(
                    bob, "bob", "begin_investigation", begin_args("probe")
                )
                before = len(proxy.receipts)
                forbidden = [
                    await call(bob, "bob", tool, args)
                    for tool, args in (
                        ("query_incident", {"run_id": run_id, "kind": "incident"}),
                        (
                            "get_trace",
                            {"run_id": run_id, "trace_id": scope["trace_id"]},
                        ),
                        (
                            "finish_investigation",
                            {
                                "run_id": run_id,
                                "citations": [],
                                "status": "incomplete",
                                "reason": "foreign",
                            },
                        ),
                    )
                ]
                check(
                    "foreign_owner",
                    all(rejected(w, "unauthorized") for w in forbidden)
                    and len(proxy.receipts) == before
                    and not own_bob.get("isError"),
                    native_submissions=len(proxy.receipts) - before,
                )
                evidence = [
                    data(
                        await call(
                            alice,
                            "alice",
                            "query_incident",
                            {"run_id": run_id, "kind": kind},
                        )
                    )
                    for kind in ("baseline", "incident")
                ]
                trace = data(
                    await call(
                        alice,
                        "alice",
                        "get_trace",
                        {"run_id": run_id, "trace_id": scope["trace_id"]},
                    )
                )
                evidence.append(trace)
                spans = {row.get(base.span_column) for row in trace.get("rows", [])}
                linked = any(
                    row.get(base.parent_span_column) in spans
                    and row.get(base.parent_span_column)
                    for row in trace.get("rows", [])
                )
                finished = data(
                    await call(
                        alice,
                        "alice",
                        "finish_investigation",
                        {
                            "run_id": run_id,
                            "citations": [e["evidence_id"] for e in evidence],
                            "status": "abstained",
                        },
                    )
                )
                check(
                    "healthy_four_tools",
                    all(e.get("complete") is True for e in evidence)
                    and bool(linked)
                    and finished.get("status") == "abstained"
                    and finished.get("citations")
                    == {e["evidence_id"]: e["sha256"] for e in evidence},
                )

                active_probe = "expiry"
                expired_run = data(
                    await call(
                        alice,
                        "alice",
                        "begin_investigation",
                        begin_args("probe-expiry"),
                    )
                )["run_id"]
                before = len(proxy.receipts)
                await asyncio.sleep(1.05)
                expired_query = await call(
                    alice,
                    "alice",
                    "query_incident",
                    {"run_id": expired_run, "kind": "incident"},
                )
                expired_finish = await call(
                    alice,
                    "alice",
                    "finish_investigation",
                    {
                        "run_id": expired_run,
                        "citations": [],
                        "status": "incomplete",
                        "reason": "expired",
                    },
                )
                check(
                    "expiry",
                    rejected(expired_query, "expired")
                    and rejected(expired_finish, "expired")
                    and len(proxy.receipts) == before,
                    native_submissions=len(proxy.receipts) - before,
                )

                active_probe = "injected_partial"
                partial_run = data(
                    await call(
                        alice, "alice", "begin_investigation", begin_args("probe")
                    )
                )["run_id"]
                proxy.partial = True
                try:
                    partial = data(
                        await call(
                            alice,
                            "alice",
                            "query_incident",
                            {"run_id": partial_run, "kind": "incident"},
                        )
                    )
                finally:
                    proxy.partial = False
                refused = await call(
                    alice,
                    "alice",
                    "finish_investigation",
                    {
                        "run_id": partial_run,
                        "citations": [partial["evidence_id"]],
                        "status": "abstained",
                    },
                )
                incomplete = data(
                    await call(
                        alice,
                        "alice",
                        "finish_investigation",
                        {
                            "run_id": partial_run,
                            "citations": [partial["evidence_id"]],
                            "status": "incomplete",
                            "reason": "Injected native server-counter mismatch.",
                        },
                    )
                )
                check(
                    "partial_rejected_for_complete_finish",
                    partial.get("complete") is False
                    and partial.get("metadata", {}).get("completeness") == "partial"
                    and rejected(refused, "Incomplete evidence")
                    and incomplete.get("failed_evidence_ids")
                    == [partial["evidence_id"]],
                    injected=True,
                )

                active_probe = "native_failure"
                missing_run = data(
                    await call(
                        alice,
                        "alice",
                        "begin_investigation",
                        begin_args("probe-native-failure"),
                    )
                )["run_id"]
                failed = data(
                    await call(
                        alice,
                        "alice",
                        "query_incident",
                        {"run_id": missing_run, "kind": "incident"},
                    )
                )
                native_receipt = proxy.receipts[-1]
                native_error = (
                    native_receipt.get("native_response", {}).get("exceptions")
                    or native_receipt.get("native_http_status", 0) >= 400
                )
                failed_finish = data(
                    await call(
                        alice,
                        "alice",
                        "finish_investigation",
                        {
                            "run_id": missing_run,
                            "citations": [failed["evidence_id"]],
                            "status": "incomplete",
                            "reason": "Broker rejected an intentionally missing table.",
                        },
                    )
                )
                check(
                    "native_failure_retained",
                    failed.get("complete") is False
                    and bool(native_error)
                    and native_receipt.get("injected_fault") is None
                    and failed_finish.get("failed_evidence_ids")
                    == [failed["evidence_id"]],
                    injected=False,
                )

                native_calls = [
                    call
                    for call in report["calls"]
                    if call["tool"] in ("query_incident", "get_trace")
                    and data(call.get("wire_result", {})).get("evidence_id")
                ]
                submitted_ids = {
                    receipt["client_query_id"] for receipt in proxy.receipts
                }
                correlated = all(
                    call["wire_result"]["_meta"]["io.github.startreedata/mcp-pinot"][
                        "request_id"
                    ]
                    in submitted_ids
                    for call in native_calls
                )
                check(
                    "native_request_correlation",
                    bool(native_calls)
                    and correlated
                    and len(native_calls) == len(proxy.receipts),
                    submitted_native_queries=len(proxy.receipts),
                )
        report["all_passed"] = len(report["checks"]) == 6 and all(
            check["passed"] for check in report["checks"]
        )
    except Exception as error:
        report["failure"] = {"probe": active_probe, "error_type": type(error).__name__}
    finally:
        server._auth, server.mcp.auth, server.pinot_client, server._incident_service = (
            saved
        )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--broker", required=True)
    parser.add_argument("--controller", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(run_probes(args.dataset, args.broker, args.controller))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "all_passed": report["all_passed"],
                "checks": report["checks"],
                "output": str(args.output),
            }
        )
    )
    return 0 if report["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
