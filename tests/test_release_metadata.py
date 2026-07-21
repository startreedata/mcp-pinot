"""Release metadata and published MCP contract consistency checks.

These tests deliberately inspect source files without importing ``mcp_pinot.server``.
Importing the server loads deployment configuration and is unnecessary for checking
release metadata or static tool registration.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
import re
import tomllib

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_TOOL_NAMES = {
    "test_connection",
    "reload_table_filters",
    "read_query",
    "list_tables",
    "get_table_size",
    "list_segments",
    "get_segment_index_metadata",
    "list_segment_metadata",
    "create_schema",
    "update_schema",
    "get_schema",
    "create_table_config",
    "update_table_config",
    "get_table_config",
}

REMOVED_TOOL_NAMES = {
    "table_details",
    "segment_list",
    "index_column_details",
    "segment_metadata_details",
}


def _json(path: str) -> dict:
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _project_version() -> str:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return data["project"]["version"]


def _chart_versions() -> tuple[str, str]:
    text = (ROOT / "helm/mcp-pinot/Chart.yaml").read_text(encoding="utf-8")

    def value(key: str) -> str:
        match = re.search(rf'(?m)^{key}:\s*["\']?([^"\'\s]+)', text)
        assert match, f"Chart.yaml is missing {key}"
        return match.group(1)

    return value("version"), value("appVersion")


def _registered_tool_names() -> set[str]:
    tree = ast.parse(
        (ROOT / "mcp_pinot/server.py").read_text(encoding="utf-8"),
        filename="mcp_pinot/server.py",
    )
    names: set[str] = set()

    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            target = decorator.func
            if not (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "mcp"
                and target.attr == "tool"
            ):
                continue

            explicit_name = next(
                (
                    keyword.value.value
                    for keyword in decorator.keywords
                    if keyword.arg == "name"
                    and isinstance(keyword.value, ast.Constant)
                    and isinstance(keyword.value.value, str)
                ),
                None,
            )
            names.add(explicit_name or node.name)

    return names


def _tool_auth_contract() -> dict[str, str]:
    tree = ast.parse(
        (ROOT / "mcp_pinot/server.py").read_text(encoding="utf-8"),
        filename="mcp_pinot/server.py",
    )
    contract: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            target = decorator.func
            if not (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "mcp"
                and target.attr == "tool"
            ):
                continue
            auth = next(
                (
                    keyword.value
                    for keyword in decorator.keywords
                    if keyword.arg == "auth"
                ),
                None,
            )
            assert isinstance(auth, ast.Name), f"{node.name} must declare auth"
            contract[node.name] = auth.id
    return contract


def test_release_versions_are_aligned() -> None:
    version = _project_version()
    manifest = _json("manifest.json")
    registry = _json("server.json")
    chart_version, chart_app_version = _chart_versions()
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    locked_project = next(
        package for package in lock["package"] if package["name"] == "mcp-pinot-server"
    )

    assert re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?", version)
    assert manifest["manifest_version"] == "0.4"
    assert manifest["version"] == version
    assert registry["version"] == version
    assert {package["version"] for package in registry["packages"]} == {version}
    assert chart_version == version
    assert chart_app_version == version
    assert locked_project["version"] == version


def test_manifest_tools_match_static_server_contract() -> None:
    manifest = _json("manifest.json")
    tools = manifest["tools"]
    manifest_names = {tool["name"] for tool in tools}

    assert len(manifest_names) == len(tools), "manifest tool names must be unique"
    assert manifest_names == EXPECTED_TOOL_NAMES
    assert _registered_tool_names() == EXPECTED_TOOL_NAMES
    assert manifest_names.isdisjoint(REMOVED_TOOL_NAMES)

    for tool in tools:
        assert re.fullmatch(r"[a-z][a-z0-9_]*", tool["name"])
        assert tool["description"].strip().endswith(".")


def test_every_tool_declares_least_privilege_authorization() -> None:
    contract = _tool_auth_contract()
    assert contract["reload_table_filters"] == "_ADMIN_AUTH"
    assert contract["create_schema"] == "_WRITE_AUTH"
    assert contract["update_schema"] == "_WRITE_AUTH"
    assert contract["create_table_config"] == "_WRITE_AUTH"
    assert contract["update_table_config"] == "_WRITE_AUTH"
    for name in EXPECTED_TOOL_NAMES - {
        "reload_table_filters",
        "create_schema",
        "update_schema",
        "create_table_config",
        "update_table_config",
    }:
        assert contract[name] == "_READ_AUTH"


def test_container_entrypoint_does_not_shell_expand_dotenv() -> None:
    script = (ROOT / "run.sh").read_text(encoding="utf-8")
    assert "load_dotenv" in script
    assert not re.search(r"(?m)^\s*(?:source|\.)\s+", script)
    assert "export $(" not in script
    assert "xargs" not in script


def test_registry_and_release_metadata_are_publishable_and_pinned() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    workflow = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    manifest = _json("manifest.json")
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert (
        'LABEL io.modelcontextprotocol.server.name="io.github.startreedata/mcp-pinot"'
        in dockerfile
    )
    assert "${{ steps.release_version.outputs.version }}" in workflow
    assert "ghcr.io/${{ github.repository }}:${{ github.ref_name }}" not in workflow
    assert "/mcpb/main/" not in manifest["$schema"]
    assert re.search(r"/mcpb/[0-9a-f]{40}/", manifest["$schema"])
    pinotdb = next(
        dependency
        for dependency in project["project"]["dependencies"]
        if dependency.startswith("pinotdb")
    )
    assert ">=9.1.2" in pinotdb and "<10" in pinotdb
    assert '"4.0.0"' not in (ROOT / "mcp_pinot/__init__.py").read_text(encoding="utf-8")
