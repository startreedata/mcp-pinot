# Incident API replay evidence — 2026-10-08

[results.json](results.json) summarizes the native Pinot replay, actual MCP
outcomes, independent qualification, fault probes, and retained failures.
[receipts.tar.gz](receipts.tar.gz) contains the supporting files for
`native-replay-v6`, `native-replay-v7`, and `native-replay-v8`.

The archive preserves runtime/source identities, predictions, private synthetic
gold, original scores, source-to-Pinot parity, fault/auth probes, and actual tool
receipts. The v8 `cli-receipts/seed_<seed>/model/<case>/receipts.jsonl` files contain
completed MCP calls with their actual responses and provider usage.
Model reasoning and agent-message events are excluded from these exported CLI
files.

| Evidence | What it establishes |
| --- | --- |
| `runtime.json` | Engine/SDK identity, source hashes, source stability, process lifecycle, and execution validity. CLI version is in `results.json`. |
| `*-predictions.json` | Recorded tool responses, native IDs, hashes, timings, and errors; v8 preserves actual raw finish separately from verification and qualification. |
| `truth.json` | Evaluator-only expected outcomes and trusted public scope. |
| `*-score.json` | Exact synthetic outcome matches, verified completion, independently qualified outcomes, observed latency, and available token usage. |
| `seed_*-parity.json` | Full-field source/native equality, separate from model correctness. |
| `probes.json` | Native failure, expiry, partial-response rejection, request correlation, and authenticated ownership checks. |
| `MANIFEST.json` | SHA-256 and byte count for each archived member. |
| `CLI-LINEAGE.json` | Original private CLI event-file hashes and filter counts for the exported receipts. |

**Keep private gold and the archive outside the model workspace.** The model
receives public alerts and actual tool results; scoring reads gold only after
predictions are persisted. These repeated synthetic mechanisms do not establish
production RCA accuracy, complete telemetry coverage, Wix replacement, or cost
savings. MCP finish flags remain explicitly unvalidated; dollar cost stays
`UNKNOWN` without an actual billing receipt.

## Verify the archive and member bytes

Run from the repository root. Verification and extraction use a fresh temporary
directory and leave the original evidence unchanged.

```bash
replay_evidence_dir=examples/incident_replay/evidence/2026-10-08
replay_receipts_dir=$(mktemp -d)
python3 - "$replay_evidence_dir" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

evidence = Path(sys.argv[1])
summary = json.loads((evidence / "results.json").read_text())
archive_sha = hashlib.sha256((evidence / "receipts.tar.gz").read_bytes()).hexdigest()
assert archive_sha == summary["archive"]["sha256"], "Archive SHA-256 mismatch"
print("Archive SHA-256 verified")
PY
tar -xzf "$replay_evidence_dir/receipts.tar.gz" -C "$replay_receipts_dir"
python3 - "$replay_evidence_dir" "$replay_receipts_dir" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

evidence, extracted = map(Path, sys.argv[1:])
summary = json.loads((evidence / "results.json").read_text())
manifest_bytes = (extracted / "MANIFEST.json").read_bytes()
assert hashlib.sha256(manifest_bytes).hexdigest() == summary["archive"]["manifest_sha256"]
manifest = json.loads(manifest_bytes)
for name, expected in manifest.items():
    member = Path(name)
    assert not member.is_absolute() and ".." not in member.parts
    data = (extracted / member).read_bytes()
    assert len(data) == expected["bytes"], f"Byte-count mismatch: {name}"
    assert hashlib.sha256(data).hexdigest() == expected["sha256"], f"SHA mismatch: {name}"
print(f"Verified {len(manifest)} archived members")
PY
```

## Re-score the final v8 replay

Using this checkout's `score.py`, re-score the extracted aggregate predictions
against the archived private gold. This is offline verification: it starts no
cluster, calls no model, and does not modify the sealed predictions or scores.

```bash
for replay_mode in scripted model; do
  uv run --frozen python examples/incident_replay/score.py \
    --predictions "$replay_receipts_dir/native-replay-v8/$replay_mode-predictions.json" \
    --truth "$replay_receipts_dir/native-replay-v8/truth.json" \
    --output "$replay_receipts_dir/v8-$replay_mode-rescored.json" \
    > "$replay_receipts_dir/v8-$replay_mode-rescored.stdout.json"
done
```

Compare the reported counts and input hashes with the original v8 `*-score.json`
and [results.json](results.json). Use the recorded source hashes when identifying
the scorer version. Raw proposals, accepted MCP receipts, and qualified outcomes
are different measures; a failed or late investigation does not become a verified
success because its proposed label happens to match gold.

The original v6/v7 scores remain historical evidence of their original launcher
and parser behavior. **Do not re-score those trials with the current parser or
replace their retained failures with prospective fixes.** The final v8 trial
separately exercises the corrected receipt binding and sequential query policy.
