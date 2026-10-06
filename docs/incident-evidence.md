# Incident evidence tools (opt-in foundation)

Start a local server with a server-owned profile file:

```bash
uv run mcp-pinot --incident-profiles examples/incident-profiles.example.json
```

The example authorizes `local` for stdio or an unauthenticated loopback server.
For an authenticated deployment, replace it with exact `subject:<verified sub>`
entries for OAuth users or `client:<verified client_id>` entries for service
clients. Subjects take precedence over client IDs, so users of a shared OAuth
client do not share ownership. Unknown profiles and principals are rejected.
Profile configuration is read once at startup, validates unknown fields and
identifiers, and is never supplied by a tool caller. Existing Pinot table filters
and service-credential authorization still apply. These profiles scope incident
tools; they do not change the authorization of general-purpose `read_query`.

The table needs the configured epoch-millisecond time, tenant, service, event,
error, version, zone, event-ID, and trace columns. Defaults are `eventTs`, `tenant`,
`service`, `eventType`, `errorClass`, `version`, `zone`, `eventId`, and `traceId`.
Baseline/incident queries group `span` events; `watermark` queries read explicit
`watermark` events. Changes include deployment, traffic switch, experiment, and
configuration events. An absent watermark or empty result does not attest to
telemetry coverage. Configure actual `span_column`/`parent_span_column` for trace
lookup; for the StarTree OTel decoder, map `trace_column` to `Traceid`,
`span_column` to `spanid`, and `parent_span_column` to `parent_tid`.

Use the MCP tools in this order, with closed windows ending at or before the
current time. Compute epoch milliseconds for your data; the combined baseline
and incident interval must fit `max_window_ms`. Historical closed windows are
allowed; this setting limits duration, not data age.

```python
import time

from fastmcp import Client

async def collect(client: Client):
    end = int(time.time() * 1000)
    opened = await client.call_tool("begin_investigation", {
        "profile_id": "local-demo", "service": "checkout",
        "baseline_start_ms": end - 20 * 60_000,
        "start_ms": end - 10 * 60_000, "end_ms": end,
    })
    run_id = opened.structured_content["run_id"]
    evidence = await client.call_tool("query_incident", {
        "run_id": run_id, "kind": "incident",
    })
    record = evidence.structured_content
    if not record["complete"]:
        return await client.call_tool("finish_investigation", {
            "run_id": run_id, "citations": [record["evidence_id"]],
            "status": "incomplete", "reason": "Native execution was incomplete.",
        })
    # This is an explicitly unverified hypothesis, even with complete execution.
    return await client.call_tool("finish_investigation", {
        "run_id": run_id, "citations": [record["evidence_id"]],
        "status": "proposed", "hypothesis": {
            "kind": "deployment", "service": "checkout", "version": "candidate-v2",
        },
    })
```

Additional queries use `kind=baseline|watermark|changes|onset`; onset requires
candidate `service` and `version` and accepts an optional `zone`. Call `get_trace`
with `run_id` and a scoped `trace_id` to retrieve recorded span/parent-span rows.
Every query uses fixed tenant/time predicates and produces an immutable evidence
ID and SHA-256. `finish_investigation` accepts only authentic citations from the
same run and owner. Complete citations are required for `proposed`/`abstained`;
`incomplete` requires a reason. Pending queries prevent finish, and finish closes
once. Failed attempts consume query budget and remain visible as failed evidence.

Runs have a monotonic total deadline plus inflight, query, per-response and total
retained row/byte limits. SQL requests use the remaining deadline, and late
responses cannot qualify. HTTP timeouts are inactivity limits, not cancellation;
inflight permits remain held until native callbacks return. A runaway broker can
therefore still occupy a process worker after expiry. Overflow evidence is
marked incomplete and its rows are discarded. Global MCP response limits also
apply to the serialized tool envelope; choose profile byte limits below that
limit to leave room for MCP text and structured copies.

State is bounded, in memory, and local to one process. Restart invalidates run
IDs; multiple replicas require sticky routing and still lose runs on restart.
A shared durable state store, native cancellation, production coverage checks,
and semantic root-cause qualification remain rollout work. Outputs always retain
`hypothesis_validated=false`, `confirmed_cause=false`, and
`dataset_coverage_attested=false`. Successful evidence assembly does not establish
cause, telemetry completeness, or competitive latency/cost savings.
