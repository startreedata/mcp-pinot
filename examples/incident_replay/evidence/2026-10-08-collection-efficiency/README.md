# Paired incident collection evidence

[results.json](results.json) records all 18 default/planned pairs from
`collection-efficiency-v1`, six synthetic templates across seeds 300–302.
[receipts.tar.gz](receipts.tar.gz) retains original runtime, public scopes,
private gold, comparisons, native parity, fault probes, predictions, scores and
MCP audits. Failures and timeouts are retained, rather than selected away.

Each CLI file retains actual completed MCP responses, usage, errors and validation
warnings. `CLI-LINEAGE.json` records the source-event hash and exported hash/count.
The sealing step independently rechecks the CLI receipts against the actual audit
and reproduces both scores. Reasoning, agent messages, credentials, stderr, native
data and workspaces are excluded; synthetic SQL rows remain in the receipts.

The separate `compatibility-probe` records seven matching MCP receipts. Its temporary
exporter failed after model execution; that error remains preserved, while exit
code and elapsed time remain `UNKNOWN`. Receipt recovery did not rerun the model.
The CLI supplies no outer Code Mode execution receipt, so a requested single block
is not proven. This probe is separate from the 18-pair quality/timing evaluation.

**Keep private gold and the archive outside the agent workspace.** Mechanical
finish verification, independent semantic qualification and synthetic outcome
matches are separate measures. Actual model identity, billing and full TCO remain
unknown; these results do not establish production RCA, Wix takeover or superiority
over ClickHouse.

## Verify and extract

Run from the repository root. Extract only into a fresh temporary directory.

```bash
replay_evidence_dir=examples/incident_replay/evidence/2026-10-08-collection-efficiency
replay_receipts_dir=$(mktemp -d)
python3 - "$replay_evidence_dir" "$replay_receipts_dir" <<'PY'
import hashlib
import json
from pathlib import Path, PurePosixPath
import sys
import tarfile

evidence, extracted = map(Path, sys.argv[1:])
summary = json.loads((evidence / "results.json").read_text())
archive_path = evidence / "receipts.tar.gz"
assert hashlib.sha256(archive_path.read_bytes()).hexdigest() == summary["archive"]["sha256"]
with tarfile.open(archive_path, "r:gz") as archive:
    members = archive.getmembers()
    names = [member.name for member in members]
    assert len(names) == len(set(names)), "Duplicate archive member"
    assert len(names) == summary["archive"]["members"]
    for member in members:
        path = PurePosixPath(member.name)
        assert member.isfile() and not path.is_absolute() and ".." not in path.parts
        assert "\\" not in member.name and ":" not in member.name and str(path) == member.name
    manifest_bytes = archive.extractfile("MANIFEST.json").read()
    assert hashlib.sha256(manifest_bytes).hexdigest() == summary["archive"]["manifest_sha256"]
    manifest = json.loads(manifest_bytes)
    assert set(names) == set(manifest) | {"MANIFEST.json"}
    for name in names:
        data = archive.extractfile(name).read()
        if name != "MANIFEST.json":
            expected = manifest[name]
            assert len(data) == expected["bytes"], name
            assert hashlib.sha256(data).hexdigest() == expected["sha256"], name
        target = extracted / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
print(f"Verified and extracted {len(names)} members")
PY
```

## Independently re-score

Use the exact source revision/hash set in the archived `runtime.json`. These
commands start no native cluster or model and write new files in the extracted
folder. Both arms keep `mode="model"`, retaining strict verification guards.

```bash
for replay_arm in default planned; do
  uv run --frozen python examples/incident_replay/score.py \
    --predictions "$replay_receipts_dir/collection-efficiency-v1/model_$replay_arm-predictions.json" \
    --truth "$replay_receipts_dir/collection-efficiency-v1/truth.json" \
    --output "$replay_receipts_dir/$replay_arm-rescored.json" \
    > "$replay_receipts_dir/$replay_arm-rescored.stdout.json"
done
```

Compare these outputs with the archived `model_default-score.json` and
`model_planned-score.json`; do not overwrite original evidence. The original
#160 and #161 bundles remain unchanged and are not paired controls for this run.
