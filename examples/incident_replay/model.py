"""Transparent, four-tool stdio audit proxy for the production Pinot MCP server."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
from threading import Lock, Thread
import time

if __package__:
    from .common import replay_env
else:
    from common import replay_env

TOOLS = {"begin_investigation", "query_incident", "get_trace", "finish_investigation"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", type=Path, required=True)
    parser.add_argument("--broker", required=True)
    parser.add_argument("--controller", required=True)
    parser.add_argument("--audit", type=Path, required=True)
    parser.add_argument("--host-started-ns", type=int)
    args = parser.parse_args()
    host_started_ns = (
        args.host_started_ns
        if args.host_started_ns is not None
        else time.monotonic_ns()
    )
    env = replay_env(args.broker, args.controller)
    audit = args.audit.open("x", encoding="utf-8")
    child = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "mcp_pinot.server",
            "--incident-profiles",
            str(args.profiles),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=env,
        cwd=Path(__file__).resolve().parents[2],
    )
    if child.stdin is None or child.stdout is None:
        raise RuntimeError("Production server pipes are unavailable.")
    pending: dict[str | int, dict] = {}
    lock, output_lock = Lock(), Lock()

    def send(value: dict) -> None:
        with output_lock:
            sys.stdout.write(json.dumps(value, allow_nan=False) + "\n")
            sys.stdout.flush()

    def forward(line: str) -> None:
        with output_lock:
            sys.stdout.write(line)
            sys.stdout.flush()

    with audit:

        def receive() -> None:
            for line in child.stdout:
                completed_ns = time.monotonic_ns()
                response = json.loads(line)
                with lock:
                    request = pending.pop(response.get("id"), None)
                    if request and request["method"] == "tools/call":
                        audit.write(
                            json.dumps(
                                {
                                    "tool": request["tool"],
                                    "args": request["args"],
                                    "response": response,
                                    "elapsed_ms": (completed_ns - request["started_ns"])
                                    / 1e6,
                                    "started_ms": (
                                        request["started_ns"] - host_started_ns
                                    )
                                    / 1e6,
                                    "completed_ms": (completed_ns - host_started_ns)
                                    / 1e6,
                                },
                                allow_nan=False,
                            )
                            + "\n"
                        )
                        audit.flush()
                if request and request["method"] == "tools/list":
                    response["result"]["tools"] = [
                        tool
                        for tool in response["result"]["tools"]
                        if tool["name"] in TOOLS
                    ]
                    send(response)
                else:
                    forward(line)

        reader = Thread(target=receive, daemon=True)
        reader.start()
        try:
            for line in sys.stdin:
                request = json.loads(line)
                method, params = request.get("method"), request.get("params", {})
                tool = params.get("name")
                allowed = method in {
                    "initialize",
                    "ping",
                    "tools/list",
                    "tools/call",
                    "notifications/initialized",
                    "notifications/cancelled",
                }
                if not allowed or (method == "tools/call" and tool not in TOOLS):
                    response = {
                        "jsonrpc": "2.0",
                        "id": request.get("id"),
                        "error": {
                            "code": -32602,
                            "message": "Only the four incident tools are allowed.",
                        },
                    }
                    with lock:
                        audit.write(
                            json.dumps(
                                {
                                    "tool": tool or "protocol:" + str(method),
                                    "args": params.get("arguments"),
                                    "response": response,
                                    "rejected": True,
                                }
                            )
                            + "\n"
                        )
                        audit.flush()
                    send(response)
                    continue
                if "id" in request:
                    with lock:
                        pending[request["id"]] = {
                            "method": method,
                            "tool": tool,
                            "args": params.get("arguments", {}),
                            "started_ns": time.monotonic_ns(),
                        }
                child.stdin.write(line)
                child.stdin.flush()
        finally:
            child.stdin.close()
            reader.join(timeout=5)
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)


if __name__ == "__main__":
    main()
