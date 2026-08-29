"""Prepare canonical MCP Registry package metadata for a release tag."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from packaging.version import InvalidVersion, Version


def _canonical_oci_identifier(identifier: str, version: str) -> str:
    """Return an OCI reference whose release is encoded only in its tag."""
    reference = identifier.split("@", 1)[0]
    namespace, separator, image = reference.rpartition("/")
    if not separator or not namespace or not image:
        raise ValueError(f"Invalid OCI identifier: {identifier!r}")
    image = image.rsplit(":", 1)[0]
    if not image:
        raise ValueError(f"Invalid OCI identifier: {identifier!r}")
    return f"{namespace}/{image}:{version}"


def prepare_metadata(path: Path, release_tag: str) -> tuple[str, str]:
    """Update server and package versions using registry-specific conventions."""
    server_version = release_tag[1:] if release_tag.startswith("v") else release_tag
    try:
        package_version = str(Version(server_version))
    except InvalidVersion as exc:
        raise ValueError(
            f"Release tag {release_tag!r} is not a valid package version: {exc}"
        ) from exc

    metadata = json.loads(path.read_text(encoding="utf-8"))
    metadata["version"] = server_version
    for package in metadata.get("packages", []):
        if package.get("registryType") == "oci":
            package["identifier"] = _canonical_oci_identifier(
                package.get("identifier", ""), server_version
            )
            package.pop("registryBaseUrl", None)
            package.pop("version", None)
            package.pop("fileSha256", None)
        else:
            package["version"] = package_version

    path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return server_version, package_version


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("release_tag")
    parser.add_argument("--path", type=Path, default=Path("server.json"))
    args = parser.parse_args()

    server_version, package_version = prepare_metadata(args.path, args.release_tag)
    github_output = os.getenv("GITHUB_OUTPUT")
    if github_output:
        with Path(github_output).open("a", encoding="utf-8") as output:
            print(f"server_version={server_version}", file=output)
            print(f"package_version={package_version}", file=output)
    print(f"Set MCP Registry version to {server_version}")
    print(f"Set package version to {package_version}")


if __name__ == "__main__":
    main()
