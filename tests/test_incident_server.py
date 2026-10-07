"""Exercise opt-in configuration, trusted ownership, and MCP evidence round trips."""

import hashlib
import json
from types import SimpleNamespace
from unittest.mock import patch

from fastmcp import Client
from fastmcp.exceptions import ToolError
import httpx
import pytest
import sqlglot

from mcp_pinot.config import PinotConfig
from mcp_pinot.incidents import IncidentProfile, IncidentService
from mcp_pinot.models import QueryExecutionMetadata, QueryExecutionResult
from mcp_pinot.pinot_client import PinotClient
import mcp_pinot.server as server


@pytest.fixture
def service():
    def execute(sql, *, max_rows, timeout_seconds):
        return QueryExecutionResult(
            columns=sqlglot.parse_one(sql, read="trino").named_selects,
            rows=[],
            metadata=QueryExecutionMetadata(
                completeness="complete",
                partial_result=False,
                execution_limit_reached=False,
                servers_queried=1,
                servers_responded=1,
                query_sha256=hashlib.sha256(sql.encode()).hexdigest(),
            ),
        )

    return IncidentService(
        {
            "demo": IncidentProfile(
                table="events",
                tenant_value="tenant-a",
                authorized_principals=["local"],
                span_column="spanId",
                parent_span_column="parentSpanId",
            )
        },
        execute,
        wall_clock=lambda: 1000,
    )


@pytest.mark.asyncio
async def test_disabled_incident_tools_fail_without_native_work():
    with (
        patch.object(server, "_incident_service", None),
        patch.object(server, "pinot_client") as native,
    ):
        async with Client(server.mcp) as client:
            with pytest.raises(ToolError, match="disabled"):
                await client.call_tool(
                    "begin_investigation",
                    {
                        "profile_id": "demo",
                        "service": "api",
                        "baseline_start_ms": 100,
                        "start_ms": 200,
                        "end_ms": 300,
                    },
                )
        native.execute_query_with_metadata.assert_not_called()


@pytest.mark.asyncio
async def test_incident_round_trip_has_unvalidated_hypothesis_and_actual_citations(
    service,
):
    with patch.object(server, "_incident_service", service):
        async with Client(server.mcp) as client:
            opened = await client.call_tool(
                "begin_investigation",
                {
                    "profile_id": "demo",
                    "service": "api",
                    "baseline_start_ms": 100,
                    "start_ms": 200,
                    "end_ms": 300,
                },
            )
            run_id = opened.structured_content["run_id"]
            collected = await client.call_tool(
                "query_incident", {"run_id": run_id, "kind": "incident"}
            )
            evidence = collected.structured_content
            assert evidence["complete"] is True
            assert "tenant-a" in evidence["sql"]
            trace = await client.call_tool(
                "get_trace", {"run_id": run_id, "trace_id": "trace-1"}
            )
            assert '"spanId"' in trace.structured_content["sql"]
            assert '"parentSpanId"' in trace.structured_content["sql"]
            finished = await client.call_tool(
                "finish_investigation",
                {
                    "run_id": run_id,
                    "citations": [evidence["evidence_id"]],
                    "hypothesis": {"kind": "deployment", "version": "v2"},
                },
            )
            result = finished.structured_content
            assert result["citations"] == {evidence["evidence_id"]: evidence["sha256"]}
            assert result["hypothesis_validated"] is False
            assert result["confirmed_cause"] is False
            assert result["dataset_coverage_attested"] is False
            with pytest.raises(ToolError, match="closed"):
                await client.call_tool(
                    "finish_investigation",
                    {
                        "run_id": run_id,
                        "citations": [],
                        "status": "incomplete",
                        "reason": "done",
                    },
                )


@pytest.mark.parametrize(
    "token,expected",
    [
        (
            SimpleNamespace(client_id="shared-client", claims={"sub": "alice"}),
            "subject:alice",
        ),
        (SimpleNamespace(client_id="job-client", claims={}), "client:job-client"),
        (None, "local"),
    ],
)
def test_principal_comes_from_verified_identity(token, expected):
    with (
        patch.object(server, "get_access_token", return_value=token),
        patch.object(server, "_auth", None),
    ):
        assert server._incident_principal() == expected


def test_missing_identity_is_rejected_when_auth_is_enabled():
    with (
        patch.object(server, "get_access_token", return_value=None),
        patch.object(server, "_auth", object()),
    ):
        with pytest.raises(ToolError, match="authenticated principal"):
            server._incident_principal()
    with patch.object(
        server,
        "get_access_token",
        return_value=SimpleNamespace(client_id=None, claims={}),
    ):
        with pytest.raises(ToolError, match="authenticated principal"):
            server._incident_principal()


@pytest.mark.asyncio
async def test_same_oauth_client_cannot_access_another_users_run(service):
    profile = IncidentProfile(
        table="events",
        tenant_value="a",
        authorized_principals=["subject:alice", "subject:bob"],
    )
    scoped = IncidentService(
        {"demo": profile},
        lambda *args, **kwargs: pytest.fail("no query expected"),
        wall_clock=lambda: 1000,
    )
    with (
        patch.object(server, "_incident_service", scoped),
        patch.object(
            server,
            "get_access_token",
            return_value=SimpleNamespace(client_id="shared", claims={"sub": "alice"}),
        ),
    ):
        async with Client(server.mcp) as client:
            run = await client.call_tool(
                "begin_investigation",
                {
                    "profile_id": "demo",
                    "service": "api",
                    "baseline_start_ms": 100,
                    "start_ms": 200,
                    "end_ms": 300,
                },
            )
            with patch.object(
                server,
                "get_access_token",
                return_value=SimpleNamespace(client_id="shared", claims={"sub": "bob"}),
            ):
                with pytest.raises(ToolError, match="unauthorized"):
                    await client.call_tool(
                        "query_incident",
                        {
                            "run_id": run.structured_content["run_id"],
                            "kind": "incident",
                        },
                    )


def test_profile_loader_accepts_valid_file_and_masks_invalid_file_contents(tmp_path):
    path = tmp_path / "profiles.json"
    path.write_text(
        json.dumps(
            {
                "demo": {
                    "table": "events",
                    "tenant_value": "a",
                    "authorized_principals": ["local"],
                }
            }
        )
    )
    assert isinstance(server._load_incident_profiles(str(path)), IncidentService)
    path.write_text(
        json.dumps(
            {
                "demo": {
                    "table": "SECRET_TABLE",
                    "tenant_value": "PRIVATE",
                    "authorized_principals": ["*"],
                }
            }
        )
    )
    with pytest.raises(SystemExit) as error:
        server._load_incident_profiles(str(path))
    assert "SECRET_TABLE" not in str(error.value)
    assert "PRIVATE" not in str(error.value)
    path.write_text("[]")
    with pytest.raises(SystemExit):
        server._load_incident_profiles(str(path))
    path.write_text("x" * 1048577)
    with pytest.raises(SystemExit):
        server._load_incident_profiles(str(path))
    with pytest.raises(SystemExit):
        server._load_incident_profiles(str(tmp_path / "missing.json"))


def test_cli_loads_profile_before_start_and_unknown_arguments_fail(tmp_path):
    path = tmp_path / "profiles.json"
    path.write_text(
        json.dumps(
            {
                "demo": {
                    "table": "events",
                    "tenant_value": "a",
                    "authorized_principals": ["local"],
                }
            }
        )
    )
    with (
        patch.object(server, "_incident_service", None),
        patch.object(server.server_config, "transport", "stdio"),
        patch.object(server.mcp, "run") as run,
    ):
        server.main(["--incident-profiles", str(path)])
        assert isinstance(server._incident_service, IncidentService)
        run.assert_called_once_with(transport="stdio")
        server.main([])
        assert server._incident_service is None
        with pytest.raises(SystemExit):
            server.main(["--unknown-option"])


def test_incident_callback_passes_remaining_budget_and_native_correlation():
    with (
        patch.object(server, "pinot_client") as native,
        patch.object(server, "query_request_id", return_value="abc123"),
    ):
        expected = QueryExecutionResult()
        native.execute_query_with_metadata.return_value = expected
        assert (
            server._execute_incident_query("SELECT 1", max_rows=2, timeout_seconds=3.5)
            is expected
        )
        native.execute_query_with_metadata.assert_called_once_with(
            "SELECT 1",
            max_rows=2,
            timeout_seconds=3.5,
            request_id="abc123",
            application_name="mcp-pinot-incident",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_kind,completeness,complete",
    [
        ("complete", "complete", True),
        ("partial", "partial", False),
        ("unknown", "unknown", False),
        ("timeout", "unknown", False),
    ],
)
async def test_native_incident_evidence_matches_tool_request_metadata(
    monkeypatch, response_kind, completeness, complete
):
    requests = []

    def handle(request):
        requests.append(request)
        if response_kind == "timeout":
            raise httpx.ReadTimeout("private transport failure", request=request)
        body = json.loads(request.content)
        columns = sqlglot.parse_one(body["sql"], read="trino").named_selects
        payload = {
            "resultTable": {
                "dataSchema": {
                    "columnNames": columns,
                    "columnDataTypes": ["STRING"] * len(columns),
                },
                "rows": [],
            },
            "exceptions": [],
            "numServersQueried": 2 if response_kind == "partial" else 1,
            "numServersResponded": 1,
            "numGroupsLimitReached": False,
            "requestId": "broker-17",
        }
        if response_kind == "unknown":
            payload.pop("numGroupsLimitReached")
        return httpx.Response(200, json=payload)

    original_client = httpx.Client

    class NativeTestClient(original_client):
        def __init__(self, **kwargs):
            super().__init__(transport=httpx.MockTransport(handle), **kwargs)

    monkeypatch.setattr("mcp_pinot.pinot_client.httpx.Client", NativeTestClient)
    native = PinotClient(
        PinotConfig(
            controller_url="http://controller",
            broker_host="broker",
            broker_port=8099,
            broker_scheme="http",
            username=None,
            password=None,
            token=None,
            database="",
            use_msqe=False,
        )
    )
    configured = IncidentService(
        {
            "demo": IncidentProfile(
                table="events",
                tenant_value="tenant-a",
                authorized_principals=["local"],
            )
        },
        server._execute_incident_query,
        wall_clock=lambda: 1000,
    )
    monkeypatch.setattr(server, "pinot_client", native)
    monkeypatch.setattr(server, "_incident_service", configured)
    async with Client(server.mcp) as client:
        opened = await client.call_tool(
            "begin_investigation",
            {
                "profile_id": "demo",
                "service": "api",
                "baseline_start_ms": 100,
                "start_ms": 200,
                "end_ms": 300,
            },
        )
        result = await client.call_tool(
            "query_incident",
            {"run_id": opened.structured_content["run_id"], "kind": "incident"},
        )

    assert len(requests) == 1
    body = json.loads(requests[0].content)
    options = dict(part.split("=", 1) for part in body["queryOptions"].split(";"))
    envelope = result.meta["io.github.startreedata/mcp-pinot"]
    assert options["clientQueryId"] == envelope["request_id"]
    assert options["applicationName"] == "mcp-pinot-incident"
    evidence = result.structured_content
    # The tool successfully retains evidence even when native execution failed.
    assert result.is_error is False
    assert envelope["status"] == "success"
    assert evidence["complete"] is complete
    assert evidence["metadata"]["completeness"] == completeness
    assert bool(evidence["error"]) is not complete
    assert "private transport failure" not in json.dumps(evidence)
    if response_kind != "timeout":
        assert evidence["metadata"]["request_id"] == envelope["request_id"]
        assert evidence["metadata"]["native_query_id"] == "broker-17"
        assert (
            evidence["metadata"]["query_sha256"]
            == hashlib.sha256(body["sql"].encode()).hexdigest()
        )
