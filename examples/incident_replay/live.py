"""Run the replay on an owned local BATCH cluster using an explicit Pinot JAR."""

import argparse
from datetime import UTC, datetime
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
import zipfile

if __package__:
    from .common import executable_path
else:
    from common import executable_path


def source_hashes(root: Path) -> dict[str, str]:
    """Identify both the harness and the production code it actually imports."""
    paths = (
        list((root / "mcp_pinot").rglob("*.py"))
        + list((root / "examples" / "incident_replay").glob("*.py"))
        + [root / "uv.lock"]
    )
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
        if path.is_file()
    }


def git_identity(root: Path) -> dict[str, str | bool]:
    try:
        head = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],  # noqa: S607
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        ).strip()
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=normal"],  # noqa: S607
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        return {"head": head, "dirty": bool(dirty)}
    except (OSError, subprocess.SubprocessError):
        return {"head": "UNKNOWN", "dirty": "UNKNOWN"}


def run_script(name: str, *arguments: str) -> None:
    subprocess.run(  # noqa: S603
        [sys.executable, str(Path(__file__).with_name(name)), *arguments], check=True
    )


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument(
        "--java", required=True, help="Explicit JDK 25+ java executable"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seeds", type=int, default=3)
    parser.add_argument("--start-seed", type=int, default=0)
    parser.add_argument("--mode", choices=["scripted", "model", "both"], default="both")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--cli-defaults", action="store_true")
    parser.add_argument("--extended-cases", action="store_true")
    parser.add_argument("--planned-collection", action="store_true")
    parser.add_argument("--compare-collection", action="store_true")
    args = parser.parse_args()
    if args.planned_collection and args.compare_collection:
        parser.error("Choose planned collection or the paired comparison, not both.")
    if args.mode == "scripted" and (args.planned_collection or args.compare_collection):
        parser.error("Collection strategies require model mode.")
    java = executable_path(args.java, program="java")
    java_version = subprocess.check_output(  # noqa: S603
        [java, "-version"], stderr=subprocess.STDOUT, text=True, shell=False
    )
    match = re.search(r'version "(\d+)', java_version)
    if match is None or int(match[1]) < 25:
        parser.error("Use an explicit JDK 25+ executable.")
    if not args.jar.is_file() or not 1 <= args.seeds <= 10 or args.timeout <= 0:
        parser.error("Require an existing JAR, 1..10 seeds, and a positive timeout.")
    # These ports belong to the BATCH quickstart. Never stop an existing process.
    for port in (
        2123,
        6000,
        7050,
        7051,
        7052,
        7100,
        7101,
        7102,
        7500,
        7501,
        7502,
        8000,
        8010,
        8080,
        9000,
    ):
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", port))
            except OSError as error:
                parser.error(f"Port {port} is occupied: {error}")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[2]
    source = source_hashes(root)
    dataset = output / "dataset"
    run_script(
        "fixture.py",
        "--output",
        str(dataset),
        "--seeds",
        str(args.seeds),
        "--start-seed",
        str(args.start_seed),
        *(["--extended-cases"] if args.extended_cases else []),
    )
    with args.jar.open("rb") as stream:
        jar_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    with zipfile.ZipFile(args.jar) as archive:
        manifest = archive.read("META-INF/MANIFEST.MF").decode().replace("\r\n", "\n")
        manifest = manifest.replace("\n ", "")
    runtime = {
        "scope": "local synthetic replay; no production accuracy or cost claim",
        "fixture_suite": "extended" if args.extended_cases else "baseline",
        "collection_strategy": (
            "counterbalanced_paired"
            if args.compare_collection
            else "planned_sequential"
            if args.planned_collection
            else "default_sequential"
        ),
        "started_at": datetime.now(UTC).isoformat(),
        "jar": str(args.jar.resolve()),
        "jar_sha256": jar_hash,
        "jar_manifest": manifest,
        "java_version": java_version,
        "source_sha256": source,
        "python": {"version": sys.version, "executable": sys.executable},
        "git": git_identity(root),
        "evaluation_complete": False,
        "evaluation_valid": False,
        "dependencies": {name: version(name) for name in ("pinotdb", "mcp", "fastmcp")},
        "broker": "http://127.0.0.1:8000",
        "controller": "http://127.0.0.1:9000",
    }
    write_json(output / "runtime.json", runtime)
    native_data = output / "native-data"
    native_data.mkdir()
    command = [java, "-Xmx2g", "--enable-native-access=ALL-UNNAMED"]
    for package in (
        "java.nio",
        "sun.nio.ch",
        "java.lang",
        "java.util",
        "java.lang.reflect",
        "jdk.internal.misc",
    ):
        command.append(f"--add-opens=java.base/{package}=ALL-UNNAMED")
    command += [
        "-Dio.netty.tryReflectionSetAccessible=true",
        "-Dio.grpc.netty.shaded.io.netty.tryReflectionSetAccessible=true",
        f"-Djava.io.tmpdir={native_data}",
        "-cp",
        str(args.jar.resolve()),
        "org.apache.pinot.tools.admin.PinotAdministrator",
        "QuickStart",
        "-type",
        "BATCH",
        "-tmpDir",
        str(native_data),
        "-bootstrapTableDir",
        str(sorted((dataset / "fixtures").iterdir())[0]),
    ]
    log_path = output / "quickstart.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(  # noqa: S603
            command, stdout=log, stderr=subprocess.STDOUT, shell=False
        )
        runtime["owned_pid"] = process.pid
        write_json(output / "runtime.json", runtime)
        try:
            deadline = time.monotonic() + 300
            while "Quick start setup complete" not in log_path.read_text(
                errors="replace"
            ):
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError(f"Native startup failed; inspect {log_path}")
                time.sleep(1)
            modes = ["scripted", "model"] if args.mode == "both" else [args.mode]
            result_modes = [
                result_mode
                for mode in modes
                for result_mode in (
                    ["model_default", "model_planned"]
                    if mode == "model" and args.compare_collection
                    else [mode]
                )
            ]
            combined_truth: list[dict] = []
            combined: dict[str, list[dict]] = {mode: [] for mode in result_modes}
            comparisons: list[dict] = []
            for index, seed in enumerate(sorted(dataset.glob("seed_*"))):
                run_script(
                    "load.py",
                    "--dataset",
                    str(seed),
                    *(["--verify-only"] if index == 0 else []),
                    "--broker",
                    runtime["broker"],
                    "--controller",
                    runtime["controller"],
                    "--output",
                    str(output / f"{seed.name}-parity.json"),
                )
                combined_truth += json.loads((seed / "truth.json").read_text())["cases"]
                for mode in modes:
                    target = output / seed.name / mode
                    target.parent.mkdir(exist_ok=True)
                    print(f"Running {seed.name} {mode}", flush=True)
                    run_script(
                        "runner.py",
                        "--dataset",
                        str(seed),
                        "--output",
                        str(target),
                        "--broker",
                        runtime["broker"],
                        "--controller",
                        runtime["controller"],
                        "--mode",
                        mode,
                        "--timeout",
                        str(args.timeout),
                        *(["--cli-defaults"] if args.cli_defaults else []),
                        *(
                            ["--compare-collection"]
                            if mode == "model" and args.compare_collection
                            else ["--planned-collection"]
                            if mode == "model" and args.planned_collection
                            else []
                        ),
                        *(
                            ["--planned-first"]
                            if mode == "model" and args.compare_collection and index % 2
                            else []
                        ),
                    )
                    if mode == "model" and args.compare_collection:
                        comparisons.append(
                            json.loads((target / "comparison.json").read_text())
                        )
                        for arm in ("default", "planned"):
                            combined[f"model_{arm}"] += json.loads(
                                (target / arm / "predictions.json").read_text()
                            )["cases"]
                    else:
                        combined[mode] += json.loads(
                            (target / "predictions.json").read_text()
                        )["cases"]
            write_json(
                output / "truth.json", {"schema_version": 1, "cases": combined_truth}
            )
            if source_hashes(root) != source:
                raise RuntimeError("Sources changed during replay; refusing to score.")
            if comparisons:
                write_json(output / "comparison.json", {"seeds": comparisons})
            for mode, cases in combined.items():
                path = output / f"{mode}-predictions.json"
                write_json(
                    path,
                    {
                        "schema_version": 1,
                        "mode": "model" if mode.startswith("model_") else mode,
                        "cases": cases,
                    },
                )
                run_script(
                    "score.py",
                    "--predictions",
                    str(path),
                    "--truth",
                    str(output / "truth.json"),
                    "--output",
                    str(output / f"{mode}-score.json"),
                )
            run_script(
                "probes.py",
                "--dataset",
                str(sorted(dataset.glob("seed_*"))[0]),
                "--broker",
                runtime["broker"],
                "--controller",
                runtime["controller"],
                "--output",
                str(output / "probes.json"),
            )
            runtime["evaluation_complete"] = True
        except Exception as error:
            runtime["failure"] = {
                "error_type": type(error).__name__,
                "detail": str(error)[:512],
            }
            raise
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
            runtime["native_exit_code"] = process.returncode
            runtime["stopped_at"] = datetime.now(UTC).isoformat()
            try:
                ended_source = source_hashes(root)
            except OSError as error:
                ended_source = {}
                runtime["source_receipt_error"] = type(error).__name__
            runtime["source_sha256_end"] = ended_source
            drift = sorted(
                key
                for key in source.keys() | ended_source.keys()
                if source.get(key) != ended_source.get(key)
            )
            runtime["source_drift"] = drift
            runtime["source_unchanged"] = not drift
            runtime["evaluation_valid"] = runtime["evaluation_complete"] and not drift
            if drift:
                runtime.setdefault(
                    "failure",
                    {
                        "error_type": "SourceDrift",
                        "detail": "Sources changed during replay; results are invalid.",
                    },
                )
            write_json(output / "runtime.json", runtime)
            if drift:
                raise RuntimeError(
                    "Sources changed during replay; results are invalid."
                )


if __name__ == "__main__":
    main()
