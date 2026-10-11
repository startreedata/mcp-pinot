# Owned host metadata: native incident replay

The opt-in `--host-telemetry` path records the model selected by the local host
and observed token snapshots without changing model selection, the four MCP
tools, investigation deadlines, or independent scoring. Keep it opt-in: provider
execution identity and billing remain unknown.

The original combined validation **failed** its injected timeout cleanup check.
All 36 normal predictions and both scores had finished before that fault; they
are independently revalidated below without rerunning or replacing any case.
The failed probe, its late request, and the corrected separate probe are retained.
The original runtime remains `evaluation_complete=false`, `evaluation_valid=false`.

## Completed normal comparison

Seeds 450–452 contain six extended synthetic templates each. Both arms use
identical public alerts, profiles, native rows, CLI defaults and a 60-second host
deadline. First-arm order is balanced 9/9 and reversed across seeds. Both arms
retain local sessions with host telemetry enabled; this is a fresh comparison,
not a telemetry-on/off experiment or a control for the older #163 trial.

| Measure, all 18 cases per arm | Default | Planned |
|---|---:|---:|
| Correct verified delivery / independently matched state | 18/18 | 18/18 |
| Adequate-evidence outcomes | 9 | 9 |
| Proposals / abstentions / incomplete | 3 / 6 / 9 | 3 / 6 / 9 |
| False proposals / normal timeouts | 0 / 0 | 0 / 0 |
| Median investigation elapsed | 34.704s | 21.963s |
| Median full elapsed, including diagnostics and cleanup | 36.505s | 23.927s |
| Observed maximum investigation elapsed | 42.176s | 31.180s |
| Actual MCP calls / native SQL submissions | 133 / 97 | 129 / 93 |
| Completed CLI usage receipts | 18/18 | 18/18 |
| Host-selected `gpt-6-astra` | 18/18 | 18/18 |
| Host metadata errors | 0 | 0 |
| Owned sessions archived with identical bytes | 18/18 | 18/18 |

Planned collection was faster in all 18 pairs. Median investigation elapsed fell
**36.7%**; the median within-pair saving was 11.647s. Median full elapsed fell
**34.5%**. Raw synthetic outcomes match in both arms, so this shows an orchestration
latency improvement rather than better semantic diagnosis. Default made seven
onset queries and planned made the three proposal onset queries. An outer Code
Mode execution-block receipt is still unavailable; requested settings and actual
call order do not prove one-block execution.

| Genuine completed usage, same full 18 pairs | Default | Planned | Change |
|---|---:|---:|---:|
| Input tokens | 2,836,055 | 1,338,420 | 52.8% lower |
| Cached input, included in input | 2,493,312 | 962,304 | — |
| Uncached input | 342,743 | 376,116 | **9.7% higher** |
| Output tokens | 14,721 | 11,069 | 24.8% lower |

Every value above comes from actual completed `codex.exec` usage, with complete
coverage in both normal arms. Cached input is a subset of input and reasoning
output a subset of output; subsets are not added to totals. Local snapshots do
not substitute for missing completed receipts. Token reduction is **not a dollar
cost estimate**, particularly when uncached input increased. Actual billing,
StarTree/ClickHouse total cost and a 50% cheaper Wix takeover remain unknown.

## New evidence and its overhead

Actual CLI `thread.started` binds a fresh session UUID to its exact empty
workspace, one turn and the UTC execution window. The parser reads only matching
UUID filenames under bounded local/UTC dates or the archive. It exports only
allowlisted model/effort/provider fields, numeric counters and record hashes;
raw reasoning and messages stay local. Current normal runs all record host
selection `gpt-6-astra`; this is distinct from provider-attested execution identity.
All 36 host records also name provider `openai`; effort is `UNKNOWN`. These
are host configuration observations, not provider attestations.

Every local token snapshot has `complete: false`, including snapshots observed
after a completed CLI turn. All 36 latest snapshots match their actual completed
CLI usage. Median observed snapshot counts were nine default and four planned;
these are local observations, not billing receipts. Genuine completed usage stays
in the existing `provider_receipt` and independent scorer path.

| Added post-investigation phase, median | Default | Planned |
|---|---:|---:|
| Metadata capture | 11.475ms | 11.273ms |
| Archive command plus byte-preservation verification | 1.723s | 1.659s |
| Entire diagnostic phase | 1.734s | 1.671s |

`elapsed_ms` retains the original investigation/deadline timing; `full_elapsed_ms`
is measured directly from the original monotonic start through final cleanup.
The table contains independent medians, which should not be added. The post-phase
measurement does not isolate session-writing overhead during model execution.
The existing ephemeral path remains the default. The option removes only
`--ephemeral`; [the CLI documentation](https://learn.chatgpt.com/docs/non-interactive-mode)
explains its effect on session persistence. Format/ownership errors remain
optional diagnostics and preserve the original investigation verification.

## Failed injection and separate repair

The first injected probe held an actual baseline request for 60 seconds against
a separate 30-second host deadline. Genuine partial counters recorded 12,091
input tokens (11,008 cached) and 60 output tokens. It timed out at 30.024s with
no raw finish, no verified finish, no completed CLI usage and no provider receipt;
its local snapshot remains incomplete and the owned raw session was archived
without changing bytes.

**The cleanup assertion failed.** The temporary injector slept in its stdin
thread and did not observe CLI EOF. The CLI and its MCP child had different
process groups; inheriting the injector group did not imply inheriting the CLI
group. At the failure-time check two child processes were still sleeping. The
injector later forwarded baseline, producing a failed audit entry at launch
+70.85s, after the prediction/report contained only `begin_investigation`.
Both the failure-time snapshot and final closed audit/marker are preserved. The
recorded group was subsequently empty. This fixture failure does not establish
that the maintained `model.py` has the same behavior: it forwards immediately,
reads EOF in its main thread and handles responses independently.

The separate corrected probe passed all **20 checks** on unchanged source, with
new hashed temporary drivers. An independent stdin reader observes EOF while
an interruptible wait holds the baseline. The held request was cancelled and
never admitted; the final audit matches the original prediction and stayed
unchanged after native cleanup. Both child PIDs were absent, their MCP group was
empty, CLI was stopped, and the original model proxy cleaned up without escalation.
Its 30.019s timeout retained 12,083 input tokens (11,008 cached) and 87 output
tokens as a single `complete: false` snapshot. Completed usage and billing remain
unknown, with no provider receipt or valid finish. The separate runtime is valid;
this does not change the failed v1 runtime.

Neither injected probe contributes to normal quality, latency or token totals.
No normal case was rerun. A separate derivation receipt validates the completed
normal arms while retaining the original failed combined runtime verbatim.

## Reproduce and provenance

The measured source is clean commit
`85ac85eefc65e3ef4538319224a9e15e65c0a82f`, based on merged #163
`8203ea6ed2220d365be62f479d4c02a1222e868f`. Maintained production and harness
source hashes stayed unchanged. Runtime receipts record local Apache Pinot 1.5.1,
JDK 25, Python 3.13.9, pinotdb 9.2.1, MCP 2.1.1 and FastMCP 4.0.2. A separate
post-run installation observation records CLI 0.153.4 and its executable hash;
this is not a per-request CLI version attestation.
All 10,038 rows across 13 fields passed full native/source parity and all six
original protocol probes passed. Only owned native processes were stopped; the
native shutdown exceeded its grace and exited `-9`, with receipts retained.

```bash
uv sync --frozen --all-extras
uv run --frozen python examples/incident_replay/live.py \
  --jar /absolute/path/pinot-distribution-1.5.1-shaded.jar \
  --java /absolute/path/jdk-25/bin/java \
  --output examples/incident_replay/output/host-receipts-new \
  --seeds 3 --start-seed 450 --mode model --timeout 60 \
  --cli-defaults --extended-cases --compare-collection --host-telemetry
```

Use a new output directory. The additional fault injectors are temporary manual
measurement tools preserved with their hashes in the evidence archive; they are
not added to the maintained harness. The normal command above omits them.

[Sealed evidence](evidence/2026-10-08-host-receipts/README.md) contains 200 regular
archive members (791,193 bytes), including a separate normal-arm derivation
receipt, both original scores, failed and corrected probes, and the actual
filtered CLI/MCP/host metadata. Archive SHA-256:
`bb304d2a140102aaf6e2aa9bcce08dac0a7d81ba8cc517d4be36cd278e005c94`.
The evidence README provides manifest/member hashes and byte-identical offline
rescoring commands. Full raw rollout hashes bind retained local originals;
private rollout bytes are not published and these hashes are not provider
attestations.

The prior one-case compatibility probe retained its initial UTC-only lookup
failure and recovery from the exact UUID on the local storage date, without a
model rerun. It is separate from the 18-pair comparison. Historical #160/#161/#163
evidence archives are unchanged.

Local source validation: 555 tests passed, seven external-cluster tests skipped;
Ruff, production mypy and helper mypy passed. Independent source and evidence
reviews found no unresolved issues after fixes. Existing unrelated harness mypy
errors are unchanged. No production MCP API, pinotdb or Pinot engine change was
needed for this experiment.

## Limits and next work

These are repeated synthetic templates across three seeds, not 18 independent
production incident types. Constructed association/abstention semantics do not
measure production root-cause accuracy, telemetry coverage, trace discovery,
prompt-injection resistance or tail latency. Host-selected model observations
improve provenance but do not attest provider execution or establish pricing.
A production comparison still needs representative independently labeled incidents
and actual ingest, storage, HA, freshness, concurrency and billing evidence on
the same workload/data/hardware/retention. Keep the feature opt-in while collecting
that evidence; a production collector should avoid adding archive work to its
request critical path.
