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
Table components may include hyphens or start with digits, such as
`checkout-events` or `observability.2026_events`; generated SQL quotes each component.

For Docker, build the current checkout, mount the policy read-only, and pass its
container path after the image. The entrypoint preserves paths with spaces:

```bash
docker build -t mcp-pinot-incidents .
docker run --rm -i \
  -e MCP_TRANSPORT=stdio \
  -e PINOT_CONTROLLER_URL=http://host.docker.internal:9000 \
  -e PINOT_BROKER_URL=http://host.docker.internal:8000 \
  --mount "type=bind,source=$(pwd)/examples/incident-profiles.example.json,target=/app/config/incident profiles.json,readonly" \
  mcp-pinot-incidents \
  --incident-profiles "/app/config/incident profiles.json"
```

For Helm, set `mcp.incidentProfilesFile` and mount a profile through the chart's
existing additional volumes. For example, save this as `incident-values.yaml`:

```yaml
mcp:
  incidentProfilesFile: /app/config/incidents/profiles.json
volumes:
  additional:
    - name: incident-profiles
      configMap:
        name: incident-profiles
volumeMounts:
  additional:
    - name: incident-profiles
      mountPath: /app/config/incidents
      readOnly: true
```

Create `profiles.json` from the [example profile](../examples/incident-profiles.example.json),
using the deployed table/tenant and authenticated principals. For the chart's
static-token provider, authorize `client:mcp-static-client`; for OAuth, use the
verified subject described above. Then deploy with your existing authenticated
HTTP values and the additional profile values. Configure `image.repository` and
`image.tag` to use a registry image containing these tools:

```bash
kubectl create configmap incident-profiles --from-file=profiles.json
helm upgrade --install mcp-pinot ./helm/mcp-pinot \
  -f deployment-values.yaml -f incident-values.yaml
```

The ConfigMap and release must use the same namespace. Restart the server after
changing the policy: profiles are loaded once at startup. An empty
`mcp.incidentProfilesFile` preserves the default deployment without incident tools.

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

async def collect(client: Client, trace_id: str | None = None):
    end = int(time.time() * 1000)
    opened = await client.call_tool("begin_investigation", {
        "profile_id": "local-demo", "service": "checkout",
        "baseline_start_ms": end - 20 * 60_000,
        "start_ms": end - 10 * 60_000, "end_ms": end,
    })
    run_id = opened.structured_content["run_id"]
    requests = [("query_incident", {"kind": kind})
                for kind in ("baseline", "incident", "watermark", "changes")]
    if trace_id is not None:  # An exact trace ID already observed in scoped telemetry.
        requests.append(("get_trace", {"trace_id": trace_id}))
    records, timings = [], []
    for tool, arguments in requests:
        response = await client.call_tool(tool, {"run_id": run_id, **arguments})
        record = response.structured_content
        records.append(record)
        timings.append(response.meta["io.github.startreedata/mcp-pinot"])
        if not record["complete"]:
            break
    incomplete = any(not record["complete"] for record in records)
    finished = await client.call_tool("finish_investigation", {
        "run_id": run_id,
        "citations": [record["evidence_id"] for record in records],
        "status": "incomplete",
        "reason": ("Evidence execution was incomplete, unknown, or truncated."
                   if incomplete else "Coverage and association sufficiency have not been evaluated."),
    })
    return {"finish": finished.structured_content,
            "evidence": records, "timings": timings}
```

Call `collect` inside an existing FastMCP `Client` session. This example collects
execution evidence and finishes `incomplete` pending a coverage and association
assessment. `complete=true` qualifies
only that bounded SQL execution; an empty watermark result or a recent watermark
alone does not prove continuous coverage of every service in the incident window.
The baseline and incident aggregations cover services in the tenant/window;
the service passed to `begin_investigation` identifies the investigation target.

An additional `kind="onset"` query requires a `candidate` with hypothesis `kind`,
`service`, and `version`, and accepts an optional `zone`. Choose these from actual
evidence. `get_trace` uses `run_id` and an exact scoped `trace_id` to retrieve
recorded span/parent-span rows; it does not discover trace IDs or infer causality.
Every query uses fixed tenant/time predicates and produces an immutable evidence
ID and SHA-256. `finish_investigation` accepts only authentic citations from the
same run and owner. Complete citations are required for `proposed`/`abstained`;
`proposed` also requires an explicitly unverified `hypothesis`, while `incomplete`
requires a reason. Pending queries prevent finish, and finish closes once. Failed
attempts consume query budget and remain visible as failed evidence. An expired
run cannot be finished; preserve received evidence rather than retrying it blindly.

Choose the terminal status after evaluating the received observations:

| Status | Caller decision |
| --- | --- |
| `proposed` | Adequate observations support a specific candidate association; the hypothesis remains unverified. |
| `abstained` | Adequate observations show a healthy target, unrelated changes, or confounding that prevents identifying a supported candidate. |
| `incomplete` | Missing, sparse, stale, partial, or failed evidence prevents evaluating the association. |

Absent matched controls can reveal confounding in otherwise adequate data. That
supports abstention. Insufficient observations of an expected cohort, missing
coverage checkpoints, or stale telemetry support `incomplete`, even when the SQL
queries all executed completely. The server validates ownership and citation
integrity and enforces execution-completeness requirements; evidence sufficiency,
the chosen semantic status, coverage, and cause remain caller judgments.

All four tools expose the response timing envelope through `response.meta`
(wire `_meta`). For `query_incident` and `get_trace`, its `request_id` is submitted
as the native `clientQueryId` and matches `metadata.request_id` when native
execution metadata is retained. Query failures can leave native IDs unknown;
use the envelope ID for optional history lookup. The query uses
`applicationName=mcp-pinot-incident` for correlation where history/logs are configured.
Inspect MCP `queue_wait_ms` separately from `metadata.native_stats.timeUsedMs`
when present. Admitted `execution_ms` includes SDK HTTP work and validation;
`status="success"` does not imply `record["complete"]` or dataset coverage.
Forward timings and evidence to the agent if its host hides protocol metadata;
query-history/log access is optional. See the [response timing guide](../README.md#query-execution-evidence).

Runs have a monotonic total deadline plus inflight, query, per-response and total
retained row/byte limits. Startup rejects byte budgets that cannot hold the
configured profile's mandatory failure evidence, including full SQL and the
maximum bounded candidate/trace values. Each admitted query reserves space for
its failure record before Pinot submission; when the remaining budget cannot
hold that record, admission fails without executing or charging another query.
Closed or expired runs release capacity after their pending queries and finish
verification have returned.
The service also caps retained evidence and in-flight failure reservations across
all runs at 64 MiB per process. When this shared budget is exhausted, queries are
rejected before Pinot submission or their returned rows are discarded to retain
a bounded incomplete record. Finishing verifies evidence outside the service
lock; queries and duplicate finishes on that same run are rejected during
verification, while other runs can continue.
SQL requests use the remaining deadline, and late
responses cannot qualify. HTTP timeouts are inactivity limits, not cancellation;
inflight permits remain held until native callbacks return. A runaway broker can
therefore still occupy a process worker after expiry. Overflow evidence is
marked incomplete and its rows are discarded. Global MCP response limits also
apply to the serialized tool payload before the bounded timing metadata is added;
choose profile byte limits below that limit to leave room for MCP text and
structured copies. The example uses a 64 KiB per-evidence and 256 KiB total
retention budget; adapt these and the row limits to your deployment.

State is bounded, in memory, and local to one process. Restart invalidates run
IDs; multiple replicas require sticky routing and still lose runs on restart.
A shared durable state store, native cancellation, production coverage checks,
and semantic root-cause qualification remain rollout work. Outputs always retain
`hypothesis_validated=false`, `confirmed_cause=false`, and
`dataset_coverage_attested=false`. Successful evidence assembly does not establish
cause, telemetry completeness, or competitive latency/cost savings.

For a reproducible synthetic host replay, full-row native parity checks, and
separate protocol/fault probes, see [the incident replay example](../examples/incident_replay/README.md).
