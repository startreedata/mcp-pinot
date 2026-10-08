# Incident tool replay, 2026-10-08

The merged incident APIs can assemble bounded, cited evidence from native Pinot.
The scripted investigator passed 12/12 constructed cases. This establishes the
API path and the fixture's conservative decisions; it does not establish Wix
production RCA, feature equivalence with Wild Moose, or a 50% total-cost advantage.
The real host-model results and retained failed trials are reported separately.

## What changed

This example replaces the historical PoC adapter with the production
`mcp_pinot.server`, published pinotdb 9.2.1, and the four tools introduced in
[#157](https://github.com/startreedata/mcp-pinot/pull/157). It adds a native loader,
public-alert runner, private-label scorer, authenticated protocol/fault probes,
and a scripted CI replay. There are no Pinot engine or MCP production changes.
[The tutorial](README.md) contains runnable commands.

## Evaluation contract

Each trial uses three seeds of four templates: a deployment association, a healthy
alert with an unrelated deployment/failure, version/zone confounding, and missing
watermarks. The 12 cases are variations of four constructed scenarios, not 12
independent production incidents. Each seed has 2,572 events; the loader compares
all 13 fields of all 7,716 native rows with the source, including empty strings.
The selected alert trace has actual parent/child references. Public cases expose
an already observed trace ID; this does not test trace discovery.

Only the scorer reads private labels. It binds recorded begin/finish scope,
same-run citations, evidence and SQL hashes, request correlation, raw outcomes,
and the conservative host policy. That policy requires a deteriorating alert,
adequate samples, matched healthy version controls, a real acyclic parent chain,
and deployment before measured error onset. The semantic policy is shared with
the scripted host; the private outcome comparison is separate. These checks
qualify associations and abstention, not causal proof.

`correct_count` includes delivered status/hypothesis and counts delivery errors as
incorrect. `completed_count` requires an actual verified MCP finish.
`qualified_count` requires agreement with the evidence policy; a correct
missing-watermark `incomplete` finish is completed but is not a qualified RCA.
Raw MCP finishes are retained even when the host delivery or qualification fails.

## Retained trials

| Trial | Scripted correct / completed | Real model correct / completed | Interpretation |
| --- | --- | --- | --- |
| v6, seeds 53–55 | 12/12, 12/12 | 0/12, 0/12 | Configured CLI model rejected before any tool call; no model-quality measurement |
| v7, seeds 56–58 | 12/12, 12/12 | 0/12, 0/12 | Startup-warning classification and concurrent admission failures; retained as a failed host trial |

In v6 the signed-in CLI rejected the copied model selection with HTTP 400. We did
not silently switch models inside that trial. v7 explicitly used isolated CLI
defaults. Its actual inference exposed a harness bug: a feature warning appeared
as an event item and was classified as an unexpected tool. It also dispatched
five evidence requests concurrently against a four-request admission limit.
Genuine rejected calls remain failures even when retried.

The v7 audit contains 12 successful production finish responses: three proposals,
five abstentions, and four incomplete outcomes. Eleven match private labels before
host checks; the remaining confounded case is incomplete. Three host deadlines
were exceeded and ten admission calls failed. The original runner also replaced
some unverified raw statuses with `incomplete` in its prediction summary. The
recorded MCP responses preserve them; the final runner retains raw outcomes
directly while keeping verification and qualification strict. The exploratory
11/12 raw match is not the verified 0/12 result.

The final trial suppresses the startup warning, asks for sequential queries, and
matches completed CLI response content and structured results against the actual
production MCP audit. The earlier failures are not rescored as successes.

The backend is the cached Apache Pinot 1.5.1 distribution, JAR SHA-256
`64c2d0fda4efd1f1b6d89a4b828d5f794782f297d25c3ee350072d913ba2b37a`,
with JDK 25, Codex CLI 0.153.4, and pinotdb 9.2.1 / MCP 2.1.1 / FastMCP 4.0.2. Production and harness
source hashes stayed unchanged throughout v6 and v7. Git HEAD was the merged
#157 base with the replay changes uncommitted; per-file hashes identify those
changes. This is an OSS-native compatibility run, not a successful StarTree
distribution certification.

Earlier startup/parity failures are preserved locally: two cached StarTree JARs
did not reach startup within 300 seconds, the first OSS ingestion converted empty
strings to the default STRING null value, and a multi-directory bootstrap caused
an OSS quickstart failure. The fixture now explicitly preserves empty strings;
the orchestrator bootstraps one table and loads the other seeds through the native
controller. No canonical comparison was relaxed. Native shutdown is bounded;
Pinot exceeded the 15-second graceful allowance, so only the owned process was
killed after the run.

In v7, the scripted median case latency was 1.185 seconds; its successful native
SQL median was 3 ms, MCP execution median 16 ms, and client-observed evidence-call
median 21.75 ms. The failed model trial had a 55.30-second median case latency.
These overlapping timings are not additive. They indicate host overhead in this
small replay, not loaded-cluster throughput or an engine comparison. Scripted
runs precede model runs on each seed, so cache warmth is not controlled. The
reported observed p95 is the nearest-rank statistic of 12 cases, not a production
tail-latency estimate.

Ten of v7's 12 cases have actual CLI usage receipts: their partial totals are
1,103,712 input tokens (847,232 cached) and 8,715 output tokens. The full-trial
token total and dollar cost remain `UNKNOWN`; missing receipts are not zero. The
CLI default model identity is not exposed in these events, so no model-specific
quality claim is made.

## Protocol and evidence checks

Both completed trials passed six separate probes: verified-subject ownership,
all four healthy tools, real monotonic expiry, partial-response rejection, a real
missing-table broker failure, and native request-ID correlation. Ownership and
expiry rejected without an additional SQL submission. The partial-response probe
deliberately changed server counters and retained both payloads; it is excluded
from model accuracy. Authentication uses real MCP HTTP serialization and local
verified bearer subjects, not an external OAuth issuer or browser login.

The production APIs retain evidence, limits, integrity, and unvalidated-hypothesis
flags. They do not certify collector coverage or apply the replay's entire semantic
policy. A fresh constructed watermark is a checkpoint, not proof of continuous
telemetry coverage. The malicious strings in source rows are not returned by these
four tools, so this is not a general prompt-injection evaluation.

## What remains before a competitive claim

1. Validate the real host delivery and abstention behavior with the frozen final
   trial, then release the merged API version through the normal release process.
2. Replay independently labeled production incidents, including traffic switches,
   experiments, environment changes, sparse/late telemetry, and competing causes.
   Measure useful hypotheses, unsupported proposals, abstention, and time to evidence.
3. Compare StarTree and ClickHouse using the same events, queries, hardware,
   retention, HA, concurrency, and correctness rules. Include ingestion, storage,
   replicas, model/tool costs, and operations in unit cost. No matched ClickHouse
   run or complete dollar-cost receipt exists in this API replay.
4. Make engine changes only after that workload identifies a measured Pinot
   bottleneck. The failures observed here concern the host experiment and native
   bootstrap path; they do not establish an engine performance regression.

The 50% target remains an acceptance criterion for that matched comparison.
