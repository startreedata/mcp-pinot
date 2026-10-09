# Planned incident collection: paired native replay

This follow-up tests the next measured gap after MCP #161: most investigation
latency was outside native Pinot and MCP execution. The change is an opt-in host
collection strategy and timing instrumentation in the replay harness. Production
Pinot code, pinotdb decoding, the four MCP APIs, and evidence policy are unchanged.

## Decision and measured results

Planned collection reduced median host latency from **39.587s to 25.206s
(36.3% lower)** across all 18 pairs. It was faster in every pair; the median
within-pair saving was 14.054s. The mean raw elapsed time fell 38.7%.

| Measure | Default | Planned |
|---|---:|---:|
| Raw outcome matches | 18/18 | 18/18 |
| Correct, verified delivery within the host deadline | 17/18 | 18/18 |
| Independently qualified state matches | 17/18 | 18/18 |
| Qualified adequate-evidence outcomes | 9 | 9 |
| False proposals | 0 | 0 |
| Median host elapsed | 39.587s | 25.206s |
| Observed maximum host elapsed | 60.016s | 37.670s |
| Actual MCP calls / native SQL submissions | 130 / 94 | 129 / 93 |
| Completed usage receipts | 17/18 | 18/18 |

Default case `case_5c8a64759ca808ce6fe48e37c2d6e3fe` delivered its
`incomplete` finish at launch+59.756s, but exceeded the 60-second host deadline
before CLI completion. It remains a failed delivery, with no completed usage
receipt. Its natural completion time is unobserved; the reported elapsed includes
host cancellation. No attempt was rerun to replace this failure. All synthetic
raw status/hypothesis outcomes match in both arms, so the difference is delivery
within the deadline rather than a demonstrated gain in semantic diagnosis.

The **same 17 pairs with actual usage receipts in both arms** show:

| Usage on the available 17-pair subset | Default | Planned | Observed reduction |
|---|---:|---:|---:|
| Input tokens | 2,622,828 | 1,267,150 | 51.7% |
| Cached input (subset of input) | 2,150,144 | 918,144 | — |
| Uncached input | 472,684 | 349,006 | 26.2% |
| Output tokens | 13,612 | 10,169 | 25.3% |

This subset excludes the timeout only because its completed usage receipt is
missing. Quality and latency still include all 18 pairs. Full default-arm and
full-trial paired token totals/ratios are **UNKNOWN**; the complete planned arm
used 1,337,528 input and 10,745 output tokens. Cached input is included in input,
and reasoning output is included in output; subsets must not be added to totals.

Initial collection (begin through trace response) had a median wall time of
15.558s versus 0.103s. Both arms followed the requested initial order sequentially
in all 18 cases. Planned collection took longer to reach the first request
(median 12.874s versus 9.991s), but eliminated most between-call gaps. The union
of actual tool intervals occupied only 0.30% of aggregate default CLI time and
0.47% of planned CLI time. Time outside those intervals still dominates and is
not directly attributable to generation alone. Default made one unnecessary
onset query on an incomplete case; planned made only the three proposal onset
queries. Native/MCP/proxy timings overlap host time and must not be added to it.

Dollar cost and the original goal of taking over Wix at 50% lower total cost
remain **UNKNOWN**. This is a synthetic orchestration experiment using local
Apache Pinot, rather than a StarTree Cloud/ClickHouse production comparison.

## What changed

- `model.py` records each actual call's start/end against CLI launch and forwards
  original MCP response lines. Filtering of the four allowed tools remains intact.
- `runner.py --planned-collection` enables the installed CLI's `code_mode` feature
  and asks for one sequential initial collection: begin, baseline, incident,
  watermark, changes, and trace. Full responses and citations remain available;
  later candidate onset and finish retain the original evidence gates.
- `--compare-collection` runs both strategies on identical public alerts, alternating
  first-arm order within a seed. `live.py` reverses the start on alternate seeds.
  Both prediction documents retain `mode="model"`; every actual response must still
  match the trusted MCP audit before a finish can be verified.
- Timing separates launch-to-first-request, actual tool occupancy (interval union),
  between-call gaps, and last-response-to-exit. The remaining host time combines
  generation, provider/network, startup, orchestration and shutdown.

No query batching endpoint or native query optimization was added. A production
collector would need separate native child request IDs, evidence bindings and
aggregate budget handling; this experiment first tests the existing APIs.

## Method and provenance

The paired run began from clean commit
`4ee80d333fd735306d3f1fac06094fd716700e6a`, based on merged #161
(`51489909d5c7a6f91a5388fc9d51ab6573e60cc3`). Production and harness source
hashes remained identical through completion. Runtime receipts identify local
Apache Pinot 1.5.1, JDK 25, Python 3.13.9, pinotdb 9.2.1, MCP 2.1.1 and FastMCP
4.0.2; installed CLI was 0.153.4. The Pinot JAR SHA-256 is
`64c2d0fda4efd1f1b6d89a4b828d5f794782f297d25c3ee350072d913ba2b37a`.

All **10,038 rows across 13 fields** passed full native/source parity, with no
mismatch. All six protocol/fault probes passed: ownership rejection, healthy
four-tool path, expiry, injected partial response rejection, real missing-table
broker failure retention, and native request correlation. These auth probes use
local test bearer identities rather than an external OAuth issuer. The owned
native process exceeded its graceful shutdown wait and was killed; its stopped
receipt and exit code `-9` are retained. Evaluation completed validly with no
source drift; that does not turn the one model timeout into a passing case.

[Sealed evidence](evidence/2026-10-08-collection-efficiency/README.md) preserves
107 regular archive members (480,739 bytes). Archive SHA-256:
`4585c0202c839e8051e0c8a592846f493c3cc746f69cdbb871ef6e466a55b945`.
Original JSON and MCP audit bytes, filtered actual CLI responses/usage/errors,
lineage hashes and both independent scores are retained. Sealing independently
rechecked actual CLI receipts and raw-versus-filtered validation summaries.

Three fresh extended fixtures, seeds 300–302, contain six opaque alerts each:
deployment association, unrelated change, version/zone confounding, absent
watermark, sparse spans, and stale watermark. The same public cases, telemetry,
profiles and private scoring labels are used by both arms. The prediction runner
never reads private gold; the trusted launcher aggregates labels separately for
scoring/export, outside the empty agent workspace. There are
18 pairs, with nine default-first and nine planned-first; fixed extended-template
positions reverse their first arm on alternate seeds.

Both arms use CLI defaults through the same installed CLI and signed-in account.
Actual model identity is **UNKNOWN** without a provider identity receipt; equal
request settings do not prove equal provider execution. Both use the unchanged
60-second host deadline, production run budgets and qualification policy. All
attempts, including timeouts and failed deliveries, are retained. This comparison
changes both the collection instruction and the CLI feature flag, so it cannot
isolate the contribution of either one.

The prior one-case compatibility probe matched all seven completed MCP responses
against the audit. Initial collection was sequential within 159ms, but the CLI
exported no outer Code Mode wrapper: **single-block execution is unproven**.
A temporary probe exporter failed after execution; retained receipts were recovered
without a model rerun, while its elapsed time and exit code remain unknown. That
probe is separate from the 18-pair quality/timing evaluation.

## Reproduce and verify

```bash
uv sync --frozen --all-extras
uv run --frozen python examples/incident_replay/live.py \
  --jar /absolute/path/pinot-distribution-1.5.1-shaded.jar \
  --java /absolute/path/jdk-25/bin/java \
  --output examples/incident_replay/output/collection-efficiency-new \
  --seeds 3 --start-seed 300 --mode model --timeout 60 \
  --cli-defaults --extended-cases --compare-collection
```

Use a fresh output directory. The launcher refuses occupied ports, verifies all
native rows and fields, owns/stops only its local process, and invalidates results
if production or harness source changes during the run. Per-arm calls, actual CLI
receipts, predictions and scores are preserved. The evidence README contains
archive/member hashes and offline rescoring instructions.

Local validation: 529 tests passed, seven external-cluster tests skipped; Ruff and
production mypy passed; independent source review found no actionable issues.
Explicit harness mypy still has the same pre-existing typing errors as the clean
base. No receipt validator exceptions, benchmark tests or unrelated repairs were
added.

## Limits and next work

These fixtures test constructed associations and safe abstention/incomplete
semantics, not production root-cause accuracy. Three seeds are repeated synthetic
templates, not 18 independent incident types. The short run does not establish
tail latency or population effects; host/provider/cache/order variation remains.
Trace IDs are given by alert context rather than discovered by the tools, and
full telemetry coverage and prompt-injection resistance remain untested.

Keep planned collection opt-in while validating a named provider/model with
actual identity and billing receipts. Repeat on representative independently
labeled production incidents, including trace discovery, telemetry coverage,
sparse/late data and transport failures, before changing production defaults.
There is no measured reason here to optimize the Pinot engine or add a fifth
MCP API; the actionable local gain comes from host collection orchestration.

A Wix/ClickHouse efficiency claim still needs the same real workload, data,
hardware, retention, freshness, concurrency and correctness criteria, plus ingest,
storage, HA and model billing measurements. Historical evidence from #160/#161
remains unchanged; it is not a paired control for this run.
