# Incident terminal-status replay evidence

[results.json](results.json) summarizes `terminal-contract-v1`:
six constructed scenarios across three seeds, or 18 cases per mode.
[receipts.tar.gz](receipts.tar.gz) preserves the original runtime, predictions,
private gold, scores, parity receipts, fault probes, and fixture hashes.
The added scenarios have sparse spans or present but stale watermarks; neither
changes the four original scenarios or their expected outcomes.

Filtered CLI receipts retain only completed MCP calls with actual responses and
`turn.completed` usage. `CLI-LINEAGE.json` binds each filtered file to its original
event-file hash and records its exported hash and retained-record count.
Reasoning, agent messages, credentials, native-data files, and model workspaces
are excluded. Synthetic SQL response rows, errors, and raw finish outcomes remain
in the original receipts.

**Keep private gold and this archive outside the model workspace.** Raw outcome
matches, verified MCP finishes, and independently qualified outcomes are separate
measures. A mechanically accepted finish does not validate evidence sufficiency,
coverage, or cause. These repeated synthetic mechanisms do not establish
production RCA accuracy, Wix replacement, or cost savings; billing cost remains
`UNKNOWN`.

## Verify and extract

Run from the repository root. This verifies archive and member SHA-256 values,
rejects unsafe paths or links, and extracts only into a fresh temporary directory.

```bash
replay_evidence_dir=examples/incident_replay/evidence/2026-10-08-terminal-contract
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

Use a checkout matching the revision and source hashes in the archived
`runtime.json`. The scorer rechecks actual wire receipts and semantic evidence;
it does not trust the runner's completion or qualification flags. These commands
start no cluster or model and write new files only in the temporary directory.
Do not replace archived gold or original scores with these outputs.

```bash
for replay_mode in scripted model; do
  uv run --frozen python examples/incident_replay/score.py \
    --predictions "$replay_receipts_dir/terminal-contract-v1/$replay_mode-predictions.json" \
    --truth "$replay_receipts_dir/terminal-contract-v1/truth.json" \
    --output "$replay_receipts_dir/terminal-$replay_mode-rescored.json" \
    > "$replay_receipts_dir/terminal-$replay_mode-rescored.stdout.json"
done
```

Compare metrics and input hashes with the archived `*-score.json` files and
[results.json](results.json). Earlier trial bundles remain historical evidence
of their original source, launcher, and parser behavior.
