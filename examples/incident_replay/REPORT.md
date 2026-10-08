# Incident tool replay, 2026-10-08

The merged incident APIs can assemble bounded, cited evidence from native Pinot.
The scripted investigator passed 12/12 constructed cases; the real host model
matched 10/12, with all 12 finishes verified within 60 seconds and no false
proposals. This establishes the API path and these constructed decisions; it does not establish Wix
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
| v8, seeds 59–61 | 12/12, 12/12 | 10/12, 12/12 | Frozen committed harness, sequential calls, full CLI response binding; two terminal-status misses |

### Primary frozen trial: v8

| Measure | Scripted | Real host model |
| --- | ---: | ---: |
| Exact expected outcome | 12/12 | 10/12 |
| Verified MCP finish | 12/12 | 12/12 |
| Host-qualified proposal or abstention | 9/12 | 7/12 |
| Proposed / abstained / incomplete | 3 / 6 / 3 | 3 / 4 / 5 |
| False proposals against synthetic truth | 0 of 3 | 0 of 3 |
| Median whole-case latency | 1.201 s | 45.375 s |
| Observed p95 of 12 cases | 3.008 s | 52.717 s |
| MCP calls / native SQL evidence submissions | 87 / 63 | 96 / 72 |
| Native `timeUsedMs` median | 3 ms | 7 ms |
| MCP evidence execution median | 14 ms | 32 ms |
| Client-observed evidence-call median | 19.10 ms | 34.65 ms |
| Tool errors / host timeouts | 0 / 0 | 0 / 0 |
| Actual CLI usage receipts | N/A | 12/12 |

The two misses are `case_4cc772c3d8cf8c9322dc081605d38191` and
`case_2e15b7f9056ba4644095e57079e840ac`: the model returned `incomplete` where
adequate evidence of confounding required `abstained`. It withheld causal
proposals, but the decision taxonomy was wrong. Future work should distinguish
complete but unidentifiable associations from missing coverage, then evaluate
that contract on fresh cases. These cases are not relabeled.

Model usage receipts total 1,981,819 input tokens (1,691,904 cached) and 10,474
output tokens. Cached input is part of input, not an additional amount. Dollar
cost remains `UNKNOWN`: the CLI does not expose a priced billing receipt or
observed model identity in these events. The timing gap points to overhead in
the host/model workflow; this does not identify a Pinot engine bottleneck
or establish the cost of an API-only model host.

V8 ran from clean commit `a70c99276385e4fa0cec6ce19568bbdbbd45fe1a` and its
production/harness source hashes stayed unchanged through shutdown. A subsequent
Windows assertion fix changes only tests. Any later launcher hardening is separate
from this measured snapshot; its checks must not be presented as v8 measurements.
All three full-row parity checks and the six protocol probes passed.

Post-trial launcher hardening selects Java/Codex from independent system/PATH
discovery, rejects unlisted driver paths, and explicitly disables shell execution. Real version probes
selected the same Temurin 25 and Codex CLI 0.153.4 paths as before. This validates
the launcher change, not a new model-quality trial. Local validation passed
526 tests with seven existing remote-cluster tests skipped, Ruff, formatting,
mypy, and the frozen dependency lock. Live native validation is reported above.

[The evidence bundle](evidence/2026-10-08/README.md) includes original predictions,
truth, scores, native parity, probes, runtime identities, and filtered completed
CLI tool/usage receipts, with archive/member hashes. Excluded model reasoning and
local native data remain under ignored `output/`. Historical scores are preserved.

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

V8 suppresses the startup warning, asks for sequential queries, and
matches completed CLI response content and structured results against the actual
production MCP audit. The earlier failures are not rescored as successes.

The backend is the cached Apache Pinot 1.5.1 distribution, JAR SHA-256
`64c2d0fda4efd1f1b6d89a4b828d5f794782f297d25c3ee350072d913ba2b37a`,
with JDK 25, Codex CLI 0.153.4, and pinotdb 9.2.1 / MCP 2.1.1 / FastMCP 4.0.2.
Production and harness source hashes stayed unchanged throughout v6, v7, and v8.
In v6/v7, Git HEAD was the merged #157 base with replay changes uncommitted;
per-file hashes identify those changes. V8 has the committed identity above.
This is an OSS-native compatibility run, not a successful StarTree
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

All three completed trials passed six separate probes: verified-subject ownership,
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

1. Tighten the agent's abstention/incomplete decision contract and validate it on
   new cases, then release the merged API version through the normal release process.
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
