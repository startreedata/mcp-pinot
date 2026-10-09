# Incident terminal decisions, 2026-10-08

This follow-up makes the agent's `proposed`, `abstained`, and `incomplete`
decisions explicit in the actual MCP tool description and status parameter.
Both the scripted investigator and real host model matched 18/18 constructed
outcomes, with all finishes verified and no false proposals or timeouts. The
original [#160 report](REPORT.md) and its scores remain unchanged. This validates
the clarified contract on synthetic cases; production accuracy and savings remain
unproven.

## What changed

| Decision | Caller judgment |
| --- | --- |
| `proposed` | Adequate evidence supports a candidate association, with cause still unvalidated. |
| `abstained` | Adequate observations support withholding a proposal because they are healthy, unrelated, or confounded. |
| `incomplete` | Missing, sparse, stale, partial, or failed evidence prevents evaluating the association. |

Complete SQL execution alone does not establish adequate observations. Conversely,
absent matched version controls in otherwise adequately populated cohorts can
demonstrate confounding and support abstention. The server still validates
ownership, citations, and native execution; it does not validate these semantic
choices, evidence sufficiency, telemetry coverage, or causality.

The [collector tutorial](../../docs/incident-evidence.md) now finishes `incomplete`
until coverage and association sufficiency are assessed. The replay prompt follows
the same caller contract. No new payload fields, enum values, defaults, or runtime
checks were added. No Pinot engine changes were made.

## Fresh evaluation

The extended fixture uses six templates across seeds 200–202: deployment
association, healthy/unrelated observations, version/zone confounding, absent
watermarks, sparse spans, and stale watermarks. Each template has three seed
variations, not three independent production incidents. New sparse cohorts contain
eight spans per version/zone/window; new stale checkpoints lag window end by five
seconds. Both require `incomplete`. The original four default templates remain
byte-identical when the extension is disabled.

Each seed contains 3,346 rows. Before either investigator runs, all 13 fields of
all native rows are compared with source data, including empty strings. The model
receives public alerts and actual MCP results; private expected outcomes remain
outside its workspace and are read only by the scorer. The scorer and conservative
evidence policy are unchanged. The semantic policy is shared with the scripted
host; raw receipt verification and private outcome comparison are separate.
Original failures are neither relabeled nor rescored as successes.

The run uses clean source commit `3e26431b7e46781450d2c28dea134af485679097`, Apache
Pinot 1.5.1, JDK 25, pinotdb 9.2.1, MCP 2.1.1, FastMCP 4.0.2, and Codex CLI
0.153.4. The JAR SHA-256 is
`64c2d0fda4efd1f1b6d89a4b828d5f794782f297d25c3ee350072d913ba2b37a`.
The real model uses isolated CLI defaults and a 60-second host deadline. The CLI
does not expose the observed inference-model identity or a priced billing receipt.

To repeat this fixture on an owned local process, use the standalone setup in the
[tutorial](README.md) and a fresh output directory:

```bash
uv run --frozen python examples/incident_replay/live.py \
  --jar /absolute/path/pinot-distribution-1.5.1-shaded.jar \
  --java /absolute/path/jdk-25/bin/java \
  --output examples/incident_replay/output/fresh-terminal-contract \
  --seeds 3 --start-seed 200 --mode both --timeout 60 \
  --cli-defaults --extended-cases
```

## Results

| Measure | Scripted | Real host model |
| --- | ---: | ---: |
| Exact expected outcome | 18/18 | 18/18 |
| Verified MCP finish | 18/18 | 18/18 |
| Recomputed policy state matches | 18/18 | 18/18 |
| Qualified proposal or abstention | 9/18 | 9/18 |
| Proposed / abstained / incomplete | 3 / 6 / 9 | 3 / 6 / 9 |
| False proposals against synthetic truth | 0 of 3 | 0 of 3 |
| Median whole-case latency | 1.187 s | 46.373 s |
| Observed p95 of 18 cases | 5.419 s | 58.285 s |
| MCP calls / native SQL evidence submissions | 129 / 93 | 132 / 96 |
| Native `timeUsedMs` median | 3 ms | 6.5 ms |
| MCP evidence execution median | 17 ms | 24.5 ms |
| Client-observed evidence-call median | 21.22 ms | 29.64 ms |
| Tool errors / host timeouts | 0 / 0 | 0 / 0 |

Native, MCP, and client timings overlap whole-case time and are not additive.

All six scenario families matched three of three expected outcomes in both
modes. The nine correctly incomplete cases are finished investigations, but are
not policy-qualified proposals or abstentions. All 10,038 rows passed full-field
parity; all six separate protocol/fault probes passed. These use local verified
bearer subjects rather than an external OAuth issuer; the intentional native
failure and injected partial response are excluded from investigator accuracy.
The production/harness source hashes stayed unchanged through shutdown and the
runtime marks evaluation complete and valid.
The owned Pinot process exceeded its 15-second graceful allowance, so the harness
stopped only that process with a bounded kill (`native_exit_code: -9`). This does
not certify graceful shutdown behavior.

Actual CLI usage receipts cover all 18 model cases: 2,789,803 input tokens
(2,253,952 cached) and 14,762 output tokens (189 reasoning). Cached and reasoning
counts are subsets, not additional tokens. Dollar cost and observed model
identity remain `UNKNOWN`.
The model workflow's 46-second median leaves substantial host-latency work; these
measurements do not establish a loaded Pinot bottleneck.

Exactly three unnecessary onset queries followed evidence that already justified
`incomplete`: seed-200 stale checkpoints and seed-201 missing/stale checkpoints.
Correct terminal decisions do not establish perfect early stopping or minimal
tool use. Prompt ordering remains guidance; the server does not enforce the
host's full semantic policy.

[The separate evidence bundle](evidence/2026-10-08-terminal-contract/README.md)
preserves original predictions, private gold, scores, full-row parity, probes,
source identities, and filtered actual CLI tool/usage receipts. It provides
archive/member hashes and offline scoring commands. Model reasoning and agent
messages are excluded; synthetic SQL response rows remain in the receipts.
The original #160 archive's SHA-256 is unchanged.

## Validation and limits

Local checks passed: 529 tests, seven existing remote-cluster skips, Ruff,
formatting, mypy, and the frozen dependency lock. CI's extended native replay
passed six of six scripted cases, verified every finish and expected qualification
state, and passed all six separate protocol/fault probes. Its three correctly
incomplete cases are completed investigations, not policy-qualified proposals
or abstentions.

The new run tests a clarified contract on a larger constructed fixture. It is not
a matched controlled comparison with #160's 12-case model trial, and latency or
accuracy differences must not be attributed solely to the wording change.
Scripted investigators precede model investigators on each seed, so cache warmth
is not controlled. Small-sample observed percentiles are not production SLOs.

This remains an Apache-native compatibility replay. A successful StarTree
distribution run, independently labeled production incidents, and a matched
ClickHouse comparison are still needed. Neither Wix takeover nor a 50% total-cost
advantage is established. Dollar cost remains `UNKNOWN`; token receipts exclude
ingestion, storage, retention, replication, and operations costs.

After review and release, the next validation should replay production incidents
with traffic switches, experiments, late telemetry, and competing causes, then
compare useful hypotheses, unsupported proposals, time to evidence, and full
cost on matched StarTree/ClickHouse deployments.
