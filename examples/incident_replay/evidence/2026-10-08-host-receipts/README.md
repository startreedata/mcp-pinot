# Owned host receipts: normal-arm derivation and separate fault evidence

[Report](../../HOST-RECEIPTS-REPORT.md). The original `host-receipts-v1` combined run remains **failed and invalid**: its separate timeout injector failed to stop its owned delayed children, and a baseline request reached the original proxy after the deadline. The original runtime and failed check are preserved with both failure-time and final late audit/marker bytes.

All **36 original normal predictions (18 pairs)** finished and were scored before that fault. This bundle independently revalidates their IDs, immutable inputs, actual MCP/CLI receipts, scorer digests, source hashes and ordering evidence. No normal case was rerun, replaced or selected away. `NORMAL-ARMS-DERIVATION.json` explains this separate derivation; it does not promote the original combined runtime to success.

The corrected `host-timeout-cleanup-v2` is a separate one-case fault probe with **20 passing checks**, unchanged sources and stopped owned processes. Its injected timeout, partial tokens and diagnostics are excluded from all normal quality, latency and token totals. The earlier failed probe remains visible alongside it.

`host_selected_model`, provider and effort describe local host selection. Provider-attested execution identity and billing remain `UNKNOWN`. Local token snapshots always retain `complete:false`; only actual `turn.completed` events supply completed CLI usage. The archive contains original normal JSON/audit bytes, filtered CLI receipts, allowlisted host metadata and provenance, and temporary driver sources. It excludes raw Codex rollouts, reasoning, messages, credentials, stderr, native data and workspaces; synthetic SQL rows and offline scoring truth remain.

| Artifact | SHA-256 / count |
| --- | --- |
| `receipts.tar.gz` | `bb304d2a140102aaf6e2aa9bcce08dac0a7d81ba8cc517d4be36cd278e005c94` |
| Archive bytes / regular members | `791193` / `200` |
| Archived `MANIFEST.json` | `e948a28d9f08ce8b1d75e139b227fc2e1dac518377c4572f67c23cde1755b177` |
| Archived `NORMAL-ARMS-DERIVATION.json` | `6776d137e6cdb8e5502d71de41899ad49f849c707b211b2eaba25b67beb4aa86` |
| `results.json` | `dcecd53d31a6ee2c1a8f31b42fd1a9d2dc5d28b0feef3bfc15340bcd1a6bdc0d` |

From the repository root, verify the archive hash, extract into a new directory, then verify every manifested member. The manifest excludes its own entry; its separate hash above binds it.

```bash
shasum -a 256 examples/incident_replay/evidence/2026-10-08-host-receipts/receipts.tar.gz
host_receipts_review_dir=$(mktemp -d)
tar -xzf examples/incident_replay/evidence/2026-10-08-host-receipts/receipts.tar.gz -C "$host_receipts_review_dir"
python3 - "$host_receipts_review_dir" <<'PY'
import hashlib, json, sys
from pathlib import Path
root = Path(sys.argv[1])
manifest_bytes = (root / "MANIFEST.json").read_bytes()
assert hashlib.sha256(manifest_bytes).hexdigest() == "e948a28d9f08ce8b1d75e139b227fc2e1dac518377c4572f67c23cde1755b177"
manifest = json.loads(manifest_bytes)
for name, expected in manifest.items():
    path = root / name
    path.resolve().relative_to(root.resolve())
    assert path.is_file() and not path.is_symlink(), name
    data = path.read_bytes()
    assert len(data) == expected["bytes"], name
    assert hashlib.sha256(data).hexdigest() == expected["sha256"], name
assert len(manifest) + 1 == 200
print("Manifest verified")
PY
```

Recompute both original aggregate scores with the repository's independent scorer. Compare the resulting files with the archived score files; inputs, outcomes and original combined failure remain unchanged.

```bash
uv run --frozen python examples/incident_replay/score.py --predictions "$host_receipts_review_dir/host-receipts-v1/model_default-predictions.json" --truth "$host_receipts_review_dir/host-receipts-v1/truth.json" --output "$host_receipts_review_dir/default-rescore.json"
uv run --frozen python examples/incident_replay/score.py --predictions "$host_receipts_review_dir/host-receipts-v1/model_planned-predictions.json" --truth "$host_receipts_review_dir/host-receipts-v1/truth.json" --output "$host_receipts_review_dir/planned-rescore.json"
cmp "$host_receipts_review_dir/default-rescore.json" "$host_receipts_review_dir/host-receipts-v1/model_default-score.json"
cmp "$host_receipts_review_dir/planned-rescore.json" "$host_receipts_review_dir/host-receipts-v1/model_planned-score.json"
```

These synthetic results support the stated local comparison and telemetry behavior. They do not establish production RCA accuracy, Wix takeover, or full-cost superiority over ClickHouse.
