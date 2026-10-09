# Replay the merged incident tools

This example exercises `begin_investigation`, `query_incident`, `get_trace`, and
`finish_investigation` through the production MCP server and published pinotdb.
It compares a scripted investigation with a real Codex host using the same public
alerts. The scorer reads private synthetic labels after prediction and independently
checks the recorded finish, evidence hashes, public scope, and host qualification.
See [the original scored report](REPORT.md) and the
[terminal-decision follow-up](TERMINAL-STATUS-REPORT.md), and
[paired collection comparison](COLLECTION-EFFICIENCY-REPORT.md) for measured results and
remaining gaps.

The fixture has deployment, unrelated-change, confounded, and missing-watermark
cases. It generates unique event/span IDs, real parent references, and at least
40 spans per populated cohort and period. Public IDs are opaque; scenario names
and expected answers occur only in `truth.json`. The public trace ID represents
one already observed in alert context; the tools do not discover traces.

Add `--extended-cases` to `fixture.py` or `live.py` for two further cases:
eight spans per populated payments cohort, and present collector checkpoints
five seconds behind the incident end. Both require `incomplete`. The original
four cases and default fixture output remain unchanged.

Use `abstained` when adequate observations establish a healthy target, unrelated
changes, or confounding. Use `incomplete` when coverage or samples remain
insufficient, including stale or missing checkpoints. Complete query execution
is one evidence check; the caller also evaluates whether the observations suffice.

## Run against an owned local Pinot

From the repository root, create a fresh output directory and start an isolated
batch quickstart. The loader accepts loopback URLs and refuses existing fixture
tables or schemas. Do not point these administrative commands at a shared or
production deployment.

```bash
uv sync --frozen --all-extras
uv run --frozen python examples/incident_replay/fixture.py \
  --output examples/incident_replay/output/demo --seeds 1 --start-seed 100

docker run -d --name mcp-incident-replay \
  -p 9000:9000 -p 8000:8000 \
  apachepinot/pinot:1.5.1@sha256:dbb65d3732eea6edf2619b0b734f13466bf1c6fd73ab9abdfa98e08a807499b2 \
  QuickStart -type batch
bash .github/scripts/setup_python_test_env.sh

uv run --frozen python examples/incident_replay/load.py \
  --dataset examples/incident_replay/output/demo/seed_100 \
  --controller http://localhost:9000 --broker http://localhost:8000 \
  --output examples/incident_replay/output/demo/parity.json
uv run --frozen python examples/incident_replay/runner.py \
  --dataset examples/incident_replay/output/demo/seed_100 \
  --controller http://localhost:9000 --broker http://localhost:8000 \
  --mode scripted --output examples/incident_replay/output/demo/scripted
uv run --frozen python examples/incident_replay/score.py \
  --predictions examples/incident_replay/output/demo/scripted/predictions.json \
  --truth examples/incident_replay/output/demo/seed_100/truth.json \
  --output examples/incident_replay/output/demo/scripted-score.json
```

`load.py` checks every native row and field against the source, including empty
strings, before accepting ingestion. It does not retry a lost upload. Use
`--verify-only` to reconcile a previously loaded fixture without uploading again.
The CI quickstart runs this sequence and preserves its receipts as an artifact.

For a real host model, use the same `runner.py` command with `--mode model` and a
different fresh output directory, then score that directory's predictions.
Model mode requires a signed-in Codex CLI on POSIX. It keeps the configured model
and reasoning effort. Add `--cli-defaults` to let the isolated CLI select its model
and effort if the copied settings are unavailable to the signed-in account.
The selection mode is recorded; settings are never changed automatically after
a failed trial. Account/model compatibility follows the
[official CLI configuration guidance](https://learn.chatgpt.com/docs/config-file/config-reference).
The default 60-second host deadline includes launch, tools,
and terminal validation. The proxy exposes only the four tools and records their
actual responses. A proposed MCP finish remains an unvalidated association.
Timeouts, unsupported proposals, and failed deliveries remain in the score.

Run the separate protocol/fault checks against the loaded fixture:

```bash
uv run --frozen python examples/incident_replay/probes.py \
  --dataset examples/incident_replay/output/demo/seed_100 \
  --controller http://localhost:9000 --broker http://localhost:8000 \
  --output examples/incident_replay/output/demo/probes.json
docker stop mcp-incident-replay
docker rm mcp-incident-replay
```

These probes use actual MCP HTTP serialization and verified test bearer subjects,
with real Pinot SQL behind a local receipt proxy. Foreign ownership and expiry
must reject before another SQL submission. Only the partial-response probe alters
a native response; it preserves both original and altered payloads and is excluded
from model accuracy. A missing-table probe uses an actual native broker failure.
The auth test does not cover an external OAuth issuer or browser login.

## Optional standalone JAR run

`live.py` creates a fresh multi-seed fixture, owns its local BATCH process, runs
both modes, scores them, probes failures, and stops only its own process.

The launchers discover Java/Codex on PATH and JDK 25 through macOS `java_home`.
Executable arguments select those discovered drivers; arbitrary driver paths are
rejected. Arguments remain literal with `shell=False`. System discovery does not
establish binary provenance.

Supply an existing distribution JAR and an explicit JDK 25+ executable:

```bash
uv run --frozen python examples/incident_replay/live.py \
  --jar /absolute/path/pinot-distribution-shaded.jar \
  --java /absolute/path/jdk-25/bin/java \
  --output examples/incident_replay/output/jar-demo \
  --seeds 3 --start-seed 50 --mode both --timeout 60
```

It refuses occupied quickstart ports, bounds startup to 300 seconds, records the
JAR, Python, dependency, and production/harness source identities, and invalidates
scoring if source code changes during the run. Startup failures retain logs and
are separate from model outcomes.

## Compare collection strategies

Run both collection strategies on every public case with a fresh output directory:

```bash
uv run --frozen python examples/incident_replay/live.py \
  --jar /absolute/path/pinot-distribution-shaded.jar \
  --java /absolute/path/jdk-25/bin/java \
  --output examples/incident_replay/output/collection-comparison \
  --seeds 3 --start-seed 300 --mode model --timeout 60 \
  --cli-defaults --extended-cases --compare-collection
```

The default strategy uses the existing prompt; the planned strategy enables the
CLI Code Mode feature and requests the five required observations sequentially
in one block, through the same four MCP tools. Individual responses and citations
remain intact.
The host still checks every actual MCP response against the proxy audit.

The first case runs default then planned; the next runs planned then default,
alternating within each seed. The standalone run reverses the starting order
on alternate seeds; direct `runner.py` comparisons can use `--planned-first`.
Both arms use identical fixtures, model-selection settings, deadlines, budgets
and scoring rules. `comparison.json` records the case IDs and execution order.
Each arm retains its calls, CLI events and predictions. The standalone launcher
writes aggregate `model_default-predictions.json`, `model_planned-predictions.json`
and the corresponding `model_default-score.json` and `model_planned-score.json`
in the output directory. Direct `runner.py` comparisons write predictions under
`default/` and `planned/`; use `score.py` to score each arm separately.
`--planned-collection` runs just the planned arm instead;
the two flags are mutually exclusive and require model mode.

Per-case timing records CLI launch-to-exit, launch-to-first-tool, last-response-to-exit
and the union of tool execution intervals. These host measurements do not separate
provider/network time from generation. Code Mode receipt compatibility must be
verified with the installed CLI before relying on the planned arm's score.
The requested strategy and CLI feature flag alone do not prove a single-block
execution; assess the actual call order and gaps from the retained receipts.

## Capture host metadata

Add `--host-telemetry` to a model or paired standalone run to record the host's
selected model and observed token snapshots from that run's local CLI session.
For example, add it to the comparison command above and use a fresh output
directory. This option keeps the existing model selection and MCP receipt checks.

The default host remains ephemeral. With this option, the CLI retains its raw
session locally; the harness captures only allowlisted metadata bound to the
actual CLI UUID, workspace, turn and execution window, then archives that generated
session. Raw reasoning and messages stay local and are excluded from published
evidence. `host-telemetry.json` contains source and selected-record hashes.

`host_selected_model` describes local host selection; the provider-attested model
and billing remain `UNKNOWN`. Token snapshots always carry `complete: false` and
remain separate from completed `codex.exec` usage receipts, including on timeout.
The scorer continues to require a completed usage receipt for every case before
reporting full token totals. Unsupported storage formats or inconsistent binding
produce `host_telemetry_error` while preserving the original investigation result.

Capture and archival happen after the investigation's existing deadline and
verification checks. `host_telemetry_capture_ms`, `host_session_archive_ms` and
`telemetry_elapsed_ms` report this added work; `full_elapsed_ms` includes it.
`elapsed_ms` retains the original investigation timing. Successful archival also
checks that the raw session moved into the archive without changing its bytes.

## Read the score

`correct_count` compares the raw delivered status and hypothesis with synthetic
truth, with delivery errors counted as incorrect. `completed_count` counts actual
MCP finishes verified against the trusted public scope and cited evidence.
`qualified_count` recomputes the host's conservative evidence policy and requires
agreement with the actual finish. The policy requires matched controls, alert
deterioration, an acyclic observed span path, and deployment before measured onset.
It checks constructed associations rather than establishing causality.

The score also reports false proposals, abstentions, incomplete outcomes, total
host latency, and tool counts. Token totals require actual CLI usage receipts for
every case; incomplete receipts produce `UNKNOWN`. Dollar cost stays `UNKNOWN`.
Native/MCP timings are retained per call and overlap host time; do not add them.
This replay does not measure ingest/retention/HA costs, production root-cause
accuracy, full telemetry coverage, or a 50% cost advantage.

Generated data and raw model logs stay under ignored `output/`. The stored malicious
log strings are not returned by these four tools, so this fixture does not test
general prompt-injection resistance. Disabled tools, an empty model workspace,
and auditing constrain the experiment; they do not prove OS filesystem isolation.
