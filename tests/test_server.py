import json
from types import SimpleNamespace
from unittest.mock import call, patch

from fastmcp import Client
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.server.middleware.rate_limiting import RateLimitError
import pytest

from mcp_pinot.server import _is_loopback_host, _ToolRateLimitMiddleware, main, mcp

SCHEMA_INPUT = {"schemaName": "test", "dimensionFieldSpecs": []}
TABLE_CONFIG_INPUT = {
    "tableName": "test",
    "tableType": "OFFLINE",
    "segmentsConfig": {},
    "tableIndexConfig": {},
}


@pytest.fixture
def mock_pinot_client():
    """Fixture to mock the PinotClient with realistic return types."""
    with patch("mcp_pinot.server.pinot_client") as mock_client:
        mock_client.test_connection.return_value = {
            "connection_test": True,
            "query_test": True,
            "tables_test": True,
            "error": None,
            "tables_count": 1,
            "sample_tables": ["test_table"],
        }
        # execute_query returns a list of row dicts (matches the real client).
        mock_client.execute_query.return_value = [{"col1": "test", "col2": "data"}]
        mock_client.reload_table_filters.side_effect = lambda dry_run=True, **_kwargs: {
            "status": "preview" if dry_run else "success",
            "message": (
                "Table filters validated; no change applied"
                if dry_run
                else "Table filters reloaded successfully"
            ),
            "applied": not dry_run,
            "previous_filter_count": 0,
            "new_filter_count": 2,
            "previous_filters": None,
            "new_filters": ["prod_*", "analytics"],
        }
        # get_tables returns a list of names (matches the real client).
        mock_client.get_tables.return_value = ["test_table"]
        mock_client.get_table_detail.return_value = {
            "tableName": "test_table",
            "reportedSizeInBytes": 1024,
        }
        mock_client.get_segments.return_value = {"OFFLINE": ["segment1"]}
        mock_client.get_index_column_detail.return_value = {"indexes": ["index1"]}
        mock_client.get_segment_metadata_detail.return_value = {
            "segment1": {"segmentName": "segment1", "totalDocs": 10}
        }
        mock_client.get_tableconfig_schema_detail.return_value = {"config": "test"}
        mock_client.create_schema.return_value = {"status": "created"}
        mock_client.update_schema.return_value = {"status": "updated"}
        mock_client.get_schema.return_value = {"schema": "test"}
        mock_client.create_table_config.return_value = {"status": "created"}
        mock_client.update_table_config.return_value = {"status": "updated"}
        mock_client.validate_table_config.return_value = {"unrecognizedProperties": {}}
        table_config_response = {
            "OFFLINE": {
                "tableName": "test",
                "tableType": "OFFLINE",
                "ingestionConfig": {"password": "must-not-leak"},
            }
        }
        mock_client.get_table_config.side_effect = lambda tableName, tableType=None: (
            table_config_response[tableType] if tableType else table_config_response
        )
        yield mock_client


class TestFastMCPServer:
    """Test the FastMCP-based server implementation"""

    def test_mcp_instance_creation(self):
        """Test that the FastMCP instance is created correctly"""
        assert mcp is not None
        assert mcp.name == "Pinot MCP Server"

    @pytest.mark.asyncio
    async def test_tools_registration(self):
        """Test that all tools are properly registered"""
        tools = await mcp.list_tools()
        tool_names = [t.name for t in tools]

        # Exact names are maintained once in test_release_metadata, which also
        # checks the manifest and static decorators. This runtime test only needs
        # to prove that registration produced the expected count without duplicates.
        assert len(tool_names) == 14
        assert len(set(tool_names)) == len(tool_names)

    @pytest.mark.asyncio
    async def test_every_tool_has_output_schema_and_annotations(self):
        """Definition-quality contract: every tool documents output + annotations."""
        async with Client(mcp) as client:
            tools = await client.list_tools()

        assert tools, "no tools registered"
        for tool in tools:
            assert tool.inputSchema, f"{tool.name} missing inputSchema"
            assert tool.outputSchema, f"{tool.name} missing outputSchema"
            assert tool.annotations is not None, f"{tool.name} missing annotations"
            assert tool.annotations.title, f"{tool.name} missing annotation title"
            assert tool.annotations.readOnlyHint is not None, (
                f"{tool.name} missing readOnlyHint"
            )

    @pytest.mark.asyncio
    async def test_read_query_param_constraints_in_schema(self):
        """read_query advertises the pagination bounds in its input schema."""
        async with Client(mcp) as client:
            tools = {t.name: t for t in await client.list_tools()}
        props = tools["read_query"].inputSchema["properties"]
        assert props["limit"]["maximum"] == 500
        assert props["limit"]["minimum"] == 1
        assert props["offset"]["minimum"] == 0

    @pytest.mark.asyncio
    async def test_inspection_tools_document_output_fields(self):
        """Pass-through inspection tools publish documented output schemas."""
        async with Client(mcp) as client:
            tools = {t.name: t for t in await client.list_tools()}

        def schema_text(name: str) -> str:
            return json.dumps(tools[name].outputSchema)

        assert "schemaName" in schema_text("get_schema")
        assert "dimensionFieldSpecs" in schema_text("get_schema")
        assert "reportedSizeInBytes" in schema_text("get_table_size")
        assert "tableType" in schema_text("get_table_config")
        assert "OFFLINE" in schema_text("list_segments")
        assert "segments" in schema_text("list_segment_metadata")
        assert "has_more" in schema_text("list_segment_metadata")

    @pytest.mark.asyncio
    async def test_every_tool_documents_failure_recovery(self):
        """Every tool tells an agent how to recover instead of retrying blindly."""
        async with Client(mcp) as client:
            tools = await client.list_tools()

        for tool in tools:
            assert "Failure recovery:" in (tool.description or ""), (
                f"{tool.name} missing recovery guidance"
            )

    @pytest.mark.asyncio
    async def test_identifier_constraints_are_advertised(self):
        """Pinot identifiers expose accepted characters in their input schemas."""
        async with Client(mcp) as client:
            tools = {t.name: t for t in await client.list_tools()}

        table_prop = tools["get_table_size"].inputSchema["properties"]["table_name"]
        segment_prop = tools["get_segment_index_metadata"].inputSchema["properties"][
            "segment_name"
        ]
        assert table_prop["pattern"] == ("^(?:[A-Za-z0-9_-]+\\.)?[A-Za-z0-9_-]+$")
        assert table_prop["minLength"] == 1
        assert segment_prop["minLength"] == 1
        assert "opaque" in segment_prop["description"]

    @pytest.mark.asyncio
    async def test_write_tools_require_structured_payloads(self):
        """Write tools expose constrained structured inputs, not raw JSON strings."""
        async with Client(mcp) as client:
            tools = {t.name: t for t in await client.list_tools()}
        schema_prop = tools["create_schema"].inputSchema["properties"]["schema"]
        config_prop = tools["create_table_config"].inputSchema["properties"][
            "table_config"
        ]
        assert schema_prop["type"] == "object"
        assert "schemaName" in schema_prop["required"]
        assert config_prop["type"] == "object"
        assert {"tableName", "tableType", "segmentsConfig", "tableIndexConfig"} <= set(
            config_prop["required"]
        )

    @pytest.mark.asyncio
    async def test_create_schema_accepts_structured_object(self, mock_pinot_client):
        """A structured object payload is serialized to JSON for the client."""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "create_schema",
                {
                    "schema": {
                        "schemaName": "obj_schema",
                        "dimensionFieldSpecs": [],
                    }
                },
            )
        assert result.structured_content["status"] == "preview"
        assert result.structured_content["confirmation_token"]
        mock_pinot_client.create_schema.assert_not_called()

    def test_segment_list_normalizes_list_form(self):
        """SegmentList merges Pinot's list-of-maps form into one object."""
        from mcp_pinot.models import SegmentList

        merged = SegmentList.model_validate([{"OFFLINE": ["s1"]}, {"REALTIME": ["s2"]}])
        assert merged.OFFLINE == ["s1"]
        assert merged.REALTIME == ["s2"]

    @pytest.mark.asyncio
    async def test_get_table_config_table_type_is_enum(self):
        """tableType is constrained to the valid Pinot table types."""
        async with Client(mcp) as client:
            tools = {t.name: t for t in await client.list_tools()}
        schema = tools["get_table_config"].inputSchema
        # Literal[...] | None renders as an enum (often via anyOf); assert the
        # allowed values appear somewhere in the property schema.
        assert "OFFLINE" in str(schema) and "REALTIME" in str(schema)

    @pytest.mark.asyncio
    async def test_prompts_registration(self):
        """Test that prompts are properly registered"""
        prompts = await mcp.list_prompts()
        prompt_names = [p.name for p in prompts]
        assert "pinot_query" in prompt_names, (
            "Prompt pinot_query not found in registered prompts"
        )

    @pytest.mark.asyncio
    async def test_explore_table_prompt_renders(self):
        """The explore_table prompt renders with the provided table name."""
        async with Client(mcp) as client:
            result = await client.get_prompt("explore_table", {"table_name": "orders"})
        text = " ".join(
            m.content.text for m in result.messages if hasattr(m.content, "text")
        )
        assert "orders" in text

    @pytest.mark.asyncio
    async def test_resources_registered(self, mock_pinot_client):
        """Catalog resources are registered (static + templated)."""
        async with Client(mcp) as client:
            resources = await client.list_resources()
            templates = await client.list_resource_templates()
        static_uris = {str(r.uri) for r in resources}
        template_uris = {t.uriTemplate for t in templates}
        assert "pinot://tables" in static_uris
        assert any("pinot://schema/" in u for u in template_uris)
        assert any("pinot://table-config/" in u for u in template_uris)

    @pytest.mark.asyncio
    async def test_read_tables_and_schema_resources(self, mock_pinot_client):
        """The static and templated resources read through to the client."""
        async with Client(mcp) as client:
            tables = await client.read_resource("pinot://tables")
            schema = await client.read_resource("pinot://schema/test_table")
            config = await client.read_resource("pinot://table-config/test_table")
        tables_text = " ".join(c.text for c in tables if hasattr(c, "text"))
        schema_text = " ".join(c.text for c in schema if hasattr(c, "text"))
        config_text = " ".join(c.text for c in config if hasattr(c, "text"))
        assert "test_table" in tables_text
        assert "schema" in schema_text
        assert "tableName" in config_text
        assert "must-not-leak" not in config_text

    @pytest.mark.asyncio
    async def test_tool_test_connection(self, mock_pinot_client):
        """test_connection returns typed diagnostics."""
        async with Client(mcp) as client:
            result = await client.call_tool("test_connection", {})

        assert result.is_error is False
        assert result.structured_content["connection_test"] is True
        assert result.structured_content["tables_count"] == 1

    @pytest.mark.asyncio
    async def test_tool_test_connection_does_not_leak_internals(
        self, mock_pinot_client
    ):
        """The internal config block (broker host/port, controller URL) is not surfaced.

        The real PinotClient.test_connection() never raises — it returns a dict that
        also contains a 'config' block with connection internals. The tool must not
        pass those through to callers (ConnectionDiagnostics drops undeclared fields).
        """
        mock_pinot_client.test_connection.return_value = {
            "connection_test": False,
            "query_test": False,
            "tables_test": False,
            "error": "connection failed",
            "config": {
                "broker_host": "broker-internal.svc",
                "broker_port": 8099,
                "controller_url": "https://controller-internal.svc:9000",
            },
        }
        async with Client(mcp) as client:
            result = await client.call_tool("test_connection", {})

        sc = result.structured_content
        assert result.is_error is False
        assert sc["connection_test"] is False
        # The structured internal config block must NOT pass through.
        assert "config" not in sc
        assert "broker_host" not in sc
        assert "controller_url" not in sc

    @pytest.mark.asyncio
    async def test_tool_read_query(self, mock_pinot_client):
        """read_query returns a typed, paginated QueryResult."""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "read_query", {"query": "SELECT * FROM test_table"}
            )

        sc = result.structured_content
        assert result.is_error is False
        assert sc["row_count"] == 1
        assert sc["total_rows"] == 1
        assert sc["has_more"] is False
        assert sc["columns"] == ["col1", "col2"]
        assert sc["rows"][0]["col1"] == "test"
        mock_pinot_client.execute_query.assert_called_once_with(
            query="SELECT * FROM test_table", max_rows=101
        )

    @pytest.mark.asyncio
    async def test_tool_read_query_paginates(self, mock_pinot_client):
        """read_query honors limit/offset and reports has_more."""
        mock_pinot_client.execute_query.return_value = [
            {"n": 1},
            {"n": 2},
            {"n": 3},
        ]
        async with Client(mcp) as client:
            result = await client.call_tool(
                "read_query",
                {"query": "SELECT n FROM t", "limit": 2, "offset": 0},
            )

        sc = result.structured_content
        assert sc["row_count"] == 2
        assert sc["total_rows"] == 3
        assert sc["has_more"] is True
        mock_pinot_client.execute_query.assert_called_once_with(
            query="SELECT n FROM t", max_rows=3
        )

    @pytest.mark.asyncio
    async def test_tool_read_query_rejects_out_of_range_limit(self, mock_pinot_client):
        """Schema constraints reject an invalid limit before the tool runs."""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "read_query",
                {"query": "SELECT 1", "limit": 0},
                raise_on_error=False,
            )
        assert result.is_error is True
        mock_pinot_client.execute_query.assert_not_called()

    @pytest.mark.asyncio
    async def test_tool_read_query_invalid_passes_message_through(
        self, mock_pinot_client
    ):
        """Validation ValueErrors surface verbatim so the model can self-correct."""
        mock_pinot_client.execute_query.side_effect = ValueError(
            "Only read-only SELECT queries are allowed for read-query"
        )

        async with Client(mcp) as client:
            with pytest.raises(
                ToolError, match="Only read-only SELECT queries are allowed"
            ):
                await client.call_tool(
                    "read_query", {"query": "INSERT INTO test_table VALUES (1)"}
                )

    @pytest.mark.asyncio
    async def test_tool_read_query_error_is_masked(self, mock_pinot_client):
        """Non-validation errors are masked behind an actionable message."""
        mock_pinot_client.execute_query.side_effect = Exception("secret-host:7000")

        async with Client(mcp) as client:
            with pytest.raises(ToolError) as exc_info:
                await client.call_tool(
                    "read_query", {"query": "SELECT * FROM test_table"}
                )

        message = str(exc_info.value)
        assert "read_query failed" in message
        assert "secret-host" not in message

    @pytest.mark.asyncio
    async def test_tool_list_tables(self, mock_pinot_client):
        """list_tables returns a typed, paginated TableList."""
        async with Client(mcp) as client:
            result = await client.call_tool("list_tables", {})

        sc = result.structured_content
        assert sc["tables"] == ["test_table"]
        assert sc["table_count"] == 1
        assert sc["total_tables"] == 1
        assert sc["has_more"] is False

    @pytest.mark.asyncio
    async def test_tool_reload_table_filters_previews_by_default(
        self, mock_pinot_client
    ):
        """reload_table_filters previews and returns a confirmation token by default."""
        async with Client(mcp) as client:
            result = await client.call_tool("reload_table_filters", {})

        assert result.structured_content["status"] == "preview"
        assert result.structured_content["applied"] is False
        assert result.structured_content["new_filter_count"] == 2
        mock_pinot_client.reload_table_filters.assert_called_once_with(dry_run=True)

    @pytest.mark.asyncio
    async def test_tool_reload_table_filters_applies_after_explicit_confirmation(
        self, mock_pinot_client
    ):
        async with Client(mcp) as client:
            preview = await client.call_tool("reload_table_filters", {})
            result = await client.call_tool(
                "reload_table_filters",
                {
                    "dry_run": False,
                    "confirmation_token": preview.structured_content[
                        "confirmation_token"
                    ],
                },
            )

        assert result.structured_content["status"] == "success"
        assert result.structured_content["applied"] is True
        assert mock_pinot_client.reload_table_filters.call_args_list == [
            call(dry_run=True),
            call(dry_run=True),
            call(
                dry_run=False,
                expected_filters=["prod_*", "analytics"],
                require_expected=True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_tool_reload_table_filters_rejects_changed_candidate(
        self, mock_pinot_client
    ):
        base = {
            "status": "preview",
            "message": "preview",
            "applied": False,
            "previous_filter_count": 0,
            "new_filter_count": 1,
            "previous_filters": None,
        }
        mock_pinot_client.reload_table_filters.side_effect = [
            {**base, "new_filters": ["first_*"]},
            {**base, "new_filters": ["changed_*"]},
        ]
        async with Client(mcp) as client:
            preview = await client.call_tool("reload_table_filters", {})
            result = await client.call_tool(
                "reload_table_filters",
                {
                    "dry_run": False,
                    "confirmation_token": preview.structured_content[
                        "confirmation_token"
                    ],
                },
                raise_on_error=False,
            )

        assert result.is_error is True
        assert mock_pinot_client.reload_table_filters.call_count == 2

    @pytest.mark.asyncio
    async def test_tool_get_table_size(self, mock_pinot_client):
        async with Client(mcp) as client:
            result = await client.call_tool(
                "get_table_size", {"table_name": "test_table"}
            )
        assert result.structured_content["tableName"] == "test_table"

    @pytest.mark.asyncio
    async def test_tool_list_segments(self, mock_pinot_client):
        async with Client(mcp) as client:
            result = await client.call_tool(
                "list_segments", {"table_name": "test_table"}
            )
        assert "OFFLINE" in result.structured_content

    @pytest.mark.asyncio
    async def test_list_segments_paginates(self, mock_pinot_client):
        """list_segments caps output and reports pagination metadata."""
        mock_pinot_client.get_segments.return_value = {
            "OFFLINE": [f"seg{i}" for i in range(5)],
            "REALTIME": [f"rt{i}" for i in range(5)],
        }
        async with Client(mcp) as client:
            result = await client.call_tool(
                "list_segments", {"table_name": "t", "limit": 3}
            )
        sc = result.structured_content
        assert sc["total_segments"] == 10
        assert sc["returned_segments"] == 3
        assert sc["has_more"] is True
        assert sc["OFFLINE"] == ["seg0", "seg1", "seg2"]
        assert sc["REALTIME"] == []

    @pytest.mark.asyncio
    async def test_tool_get_segment_index_metadata(self, mock_pinot_client):
        async with Client(mcp) as client:
            result = await client.call_tool(
                "get_segment_index_metadata",
                {"table_name": "test_table", "segment_name": "segment1"},
            )
        assert "indexes" in result.structured_content

    @pytest.mark.asyncio
    async def test_tool_list_segment_metadata(self, mock_pinot_client):
        async with Client(mcp) as client:
            result = await client.call_tool(
                "list_segment_metadata", {"table_name": "test_table"}
            )
        sc = result.structured_content
        assert sc["segments"]["segment1"]["totalDocs"] == 10
        assert sc["returned_segments"] == 1
        assert sc["has_more"] is False

    @pytest.mark.asyncio
    async def test_list_segment_metadata_paginates_deterministically(
        self, mock_pinot_client
    ):
        mock_pinot_client.get_segment_metadata_detail.return_value = {
            f"segment{i}": {"totalDocs": i} for i in range(5)
        }
        async with Client(mcp) as client:
            result = await client.call_tool(
                "list_segment_metadata",
                {"table_name": "test_table", "limit": 2, "offset": 1},
            )
        sc = result.structured_content
        assert list(sc["segments"]) == ["segment1", "segment2"]
        assert sc["returned_segments"] == 2
        assert sc["total_segments"] == 5
        assert sc["offset"] == 1
        assert sc["has_more"] is True

    @pytest.mark.asyncio
    async def test_tool_create_schema(self, mock_pinot_client):
        async with Client(mcp) as client:
            preview = await client.call_tool("create_schema", {"schema": SCHEMA_INPUT})
            result = await client.call_tool(
                "create_schema",
                {
                    "schema": SCHEMA_INPUT,
                    "dry_run": False,
                    "confirmation_token": preview.structured_content[
                        "confirmation_token"
                    ],
                },
            )
        assert result.structured_content["status"] == "success"
        assert result.structured_content["applied"] is True
        mock_pinot_client.create_schema.assert_called_once()

    @pytest.mark.asyncio
    async def test_tool_create_schema_handles_string_success_body(
        self, mock_pinot_client
    ):
        """Pinot can return a bare JSON string on success; the tool must not crash."""
        mock_pinot_client.create_schema.return_value = "myschema successfully added"
        async with Client(mcp) as client:
            preview = await client.call_tool("create_schema", {"schema": SCHEMA_INPUT})
            result = await client.call_tool(
                "create_schema",
                {
                    "schema": SCHEMA_INPUT,
                    "dry_run": False,
                    "confirmation_token": preview.structured_content[
                        "confirmation_token"
                    ],
                },
            )
        sc = result.structured_content
        assert result.is_error is False
        assert sc["status"] == "success"
        assert "myschema successfully added" in sc["response_summary"]

    @pytest.mark.asyncio
    async def test_tool_create_schema_dry_run_does_not_apply(self, mock_pinot_client):
        """dry_run previews without mutating and validates the payload."""
        schema = {"schemaName": "previewed", "dimensionFieldSpecs": []}
        async with Client(mcp) as client:
            result = await client.call_tool(
                "create_schema", {"schema": schema, "dry_run": True}
            )
        assert result.structured_content["status"] == "preview"
        assert result.structured_content["resource_name"] == "previewed"
        assert result.structured_content["confirmation_token"]
        mock_pinot_client.create_schema.assert_not_called()

    @pytest.mark.asyncio
    async def test_tool_create_schema_dry_run_rejects_bad_json(self, mock_pinot_client):
        """Structured validation rejects malformed schema inputs before mutation."""
        async with Client(mcp) as client:
            result = await client.call_tool(
                "create_schema",
                {"schema": "{not json", "dry_run": True},
                raise_on_error=False,
            )
        assert result.is_error is True
        mock_pinot_client.create_schema.assert_not_called()

    @pytest.mark.asyncio
    async def test_tool_create_schema_requires_matching_single_use_confirmation(
        self, mock_pinot_client
    ):
        """No schema write occurs without a matching, unused preview token."""
        async with Client(mcp) as client:
            missing = await client.call_tool(
                "create_schema",
                {"schema": SCHEMA_INPUT, "dry_run": False},
                raise_on_error=False,
            )
            assert missing.is_error is True

            preview = await client.call_tool("create_schema", {"schema": SCHEMA_INPUT})
            token = preview.structured_content["confirmation_token"]
            changed = await client.call_tool(
                "create_schema",
                {
                    "schema": {
                        "schemaName": "different",
                        "dimensionFieldSpecs": [],
                    },
                    "dry_run": False,
                    "confirmation_token": token,
                },
                raise_on_error=False,
            )
            assert changed.is_error is True

            applied = await client.call_tool(
                "create_schema",
                {
                    "schema": SCHEMA_INPUT,
                    "dry_run": False,
                    "confirmation_token": token,
                },
            )
            replay = await client.call_tool(
                "create_schema",
                {
                    "schema": SCHEMA_INPUT,
                    "dry_run": False,
                    "confirmation_token": token,
                },
                raise_on_error=False,
            )
            malleated_replay = await client.call_tool(
                "create_schema",
                {
                    "schema": SCHEMA_INPUT,
                    "dry_run": False,
                    "confirmation_token": token + "!",
                },
                raise_on_error=False,
            )

        assert applied.structured_content["applied"] is True
        assert replay.is_error is True
        assert malleated_replay.is_error is True
        mock_pinot_client.create_schema.assert_called_once()

    @pytest.mark.asyncio
    async def test_confirmation_rejects_tampered_signature_and_expired_token(
        self, mock_pinot_client
    ):
        import mcp_pinot.server as server

        async with Client(mcp) as client:
            with patch("mcp_pinot.server.time.time", return_value=1_000):
                preview = await client.call_tool(
                    "create_schema", {"schema": SCHEMA_INPUT}
                )
            token = preview.structured_content["confirmation_token"]
            encoded, signature = token.split(".")
            replacement = "A" if signature[-1] != "A" else "B"
            tampered = f"{encoded}.{signature[:-1]}{replacement}"
            bad_signature = await client.call_tool(
                "create_schema",
                {
                    "schema": SCHEMA_INPUT,
                    "dry_run": False,
                    "confirmation_token": tampered,
                },
                raise_on_error=False,
            )
            with patch(
                "mcp_pinot.server.time.time",
                return_value=1_000 + server._CONFIRMATION_TTL_SECONDS,
            ):
                expired = await client.call_tool(
                    "create_schema",
                    {
                        "schema": SCHEMA_INPUT,
                        "dry_run": False,
                        "confirmation_token": token,
                    },
                    raise_on_error=False,
                )

        assert bad_signature.is_error is True
        assert expired.is_error is True
        mock_pinot_client.create_schema.assert_not_called()

    @pytest.mark.asyncio
    async def test_confirmation_token_cannot_cross_operations(self, mock_pinot_client):
        async with Client(mcp) as client:
            preview = await client.call_tool("create_schema", {"schema": SCHEMA_INPUT})
            result = await client.call_tool(
                "update_schema",
                {
                    "schema_name": "test",
                    "schema": SCHEMA_INPUT,
                    "dry_run": False,
                    "confirmation_token": preview.structured_content[
                        "confirmation_token"
                    ],
                },
                raise_on_error=False,
            )

        assert result.is_error is True
        mock_pinot_client.update_schema.assert_not_called()

    @pytest.mark.asyncio
    async def test_rate_limiter_uses_stable_http_peer_and_bounds_cache(self):
        middleware = _ToolRateLimitMiddleware(
            0.001, 1, max_clients=2, idle_ttl_seconds=600
        )

        def context(session_id: str, peer: str) -> MiddlewareContext:
            request = SimpleNamespace(client=SimpleNamespace(host=peer))
            request_context = SimpleNamespace(request=request)
            fastmcp_context = SimpleNamespace(
                request_context=request_context, session_id=session_id
            )
            return MiddlewareContext(
                message=SimpleNamespace(name="test_connection"),
                fastmcp_context=fastmcp_context,
                method="tools/call",
            )

        async def call_next(_context):
            return "ok"

        assert (
            await middleware.on_call_tool(context("session-a", "127.0.0.1"), call_next)
            == "ok"
        )
        with pytest.raises(RateLimitError):
            await middleware.on_call_tool(context("session-b", "127.0.0.1"), call_next)

        middleware._limiter_for("peer:one")
        middleware._limiter_for("peer:two")
        middleware._limiter_for("peer:three")
        assert list(middleware._limiters) == ["peer:two", "peer:three"]

    @pytest.mark.asyncio
    async def test_tool_update_schema(self, mock_pinot_client):
        async with Client(mcp) as client:
            preview = await client.call_tool(
                "update_schema",
                {"schema_name": "test", "schema": SCHEMA_INPUT},
            )
            result = await client.call_tool(
                "update_schema",
                {
                    "schema_name": "test",
                    "schema": SCHEMA_INPUT,
                    "dry_run": False,
                    "confirmation_token": preview.structured_content[
                        "confirmation_token"
                    ],
                },
            )
        assert result.structured_content["status"] == "success"
        assert result.structured_content["applied"] is True

    @pytest.mark.asyncio
    async def test_tool_get_schema(self, mock_pinot_client):
        async with Client(mcp) as client:
            result = await client.call_tool("get_schema", {"schema_name": "test"})
        assert "schema" in result.structured_content

    @pytest.mark.asyncio
    async def test_tool_create_table_config(self, mock_pinot_client):
        async with Client(mcp) as client:
            preview = await client.call_tool(
                "create_table_config", {"table_config": TABLE_CONFIG_INPUT}
            )
            result = await client.call_tool(
                "create_table_config",
                {
                    "table_config": TABLE_CONFIG_INPUT,
                    "dry_run": False,
                    "confirmation_token": preview.structured_content[
                        "confirmation_token"
                    ],
                },
            )
        assert preview.structured_content["status"] == "preview"
        assert result.structured_content["status"] == "success"
        mock_pinot_client.validate_table_config.assert_called()

    @pytest.mark.asyncio
    async def test_tool_update_table_config(self, mock_pinot_client):
        async with Client(mcp) as client:
            preview = await client.call_tool(
                "update_table_config",
                {"table_name": "test", "table_config": TABLE_CONFIG_INPUT},
            )
            result = await client.call_tool(
                "update_table_config",
                {
                    "table_name": "test",
                    "table_config": TABLE_CONFIG_INPUT,
                    "dry_run": False,
                    "confirmation_token": preview.structured_content[
                        "confirmation_token"
                    ],
                },
            )
        assert result.structured_content["status"] == "success"
        assert result.structured_content["applied"] is True

    @pytest.mark.asyncio
    async def test_tool_get_table_config(self, mock_pinot_client):
        async with Client(mcp) as client:
            result = await client.call_tool("get_table_config", {"table_name": "test"})
        assert result.structured_content["offline"]["tableName"] == "test"
        assert (
            result.structured_content["offline"]["ingestionConfig"]["password"]
            == "[REDACTED]"
        )

    @pytest.mark.asyncio
    async def test_prompt_pinot_query(self):
        async with Client(mcp) as client:
            result = await client.get_prompt("pinot_query", {})
        assert len(result.messages) > 0
        assert hasattr(result.messages[0].content, "text")
        assert len(result.messages[0].content.text) > 0


class TestMainFunction:
    """Test the main function with different configurations"""

    @pytest.mark.parametrize(
        "host",
        ["127.0.0.1", "127.1.2.3", "::1", "localhost"],
    )
    def test_is_loopback_host(self, host):
        """Test loopback host detection."""
        assert _is_loopback_host(host) is True

    @pytest.mark.parametrize(
        "host",
        ["0.0.0.0", "::", "192.168.1.10", "example.com"],
    )
    def test_is_loopback_host_rejects_network_hosts(self, host):
        """Test network-reachable hosts are not treated as loopback."""
        assert _is_loopback_host(host) is False

    def test_main_function_http_transport(self, mock_pinot_client):
        """main() serves the guarded ASGI app for HTTP."""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "http"
            mock_server_config.host = "127.0.0.1"
            mock_server_config.port = 8000
            mock_server_config.path = "/mcp"
            mock_server_config.ssl_keyfile = None
            mock_server_config.ssl_certfile = None
            mock_server_config.oauth_enabled = False
            mock_server_config.allowed_hosts = ("127.0.0.1:8000",)
            mock_server_config.allowed_origins = ()

            guarded_app = object()
            with (
                patch("mcp_pinot.server._create_http_app", return_value=guarded_app),
                patch("mcp_pinot.server.uvicorn.run") as mock_uvicorn_run,
            ):
                main()

                mock_uvicorn_run.assert_called_once()
                args, kwargs = mock_uvicorn_run.call_args
                assert args[0] is guarded_app
                assert kwargs["host"] == "127.0.0.1"
                assert kwargs["port"] == 8000
                assert "ssl_keyfile" not in kwargs

    def test_main_function_http_transport_with_ssl(self, mock_pinot_client):
        """Test the main function with HTTP transport and SSL"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "http"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.ssl_keyfile = "/path/to/key.pem"
            mock_server_config.ssl_certfile = "/path/to/cert.pem"
            mock_server_config.path = "/mcp"
            mock_server_config.oauth_enabled = True
            mock_server_config.allowed_hosts = ("mcp.example",)
            mock_server_config.allowed_origins = ()

            guarded_app = object()
            with (
                patch("mcp_pinot.server._auth", object()),
                patch("mcp_pinot.server._create_http_app", return_value=guarded_app),
                patch("mcp_pinot.server.uvicorn.run") as mock_uvicorn_run,
            ):
                main()

                mock_uvicorn_run.assert_called_once()
                call_args = mock_uvicorn_run.call_args
                assert call_args.args[0] is guarded_app
                assert call_args[1]["ssl_keyfile"] == "/path/to/key.pem"
                assert call_args[1]["ssl_certfile"] == "/path/to/cert.pem"

    def test_main_function_streamable_http_transport(self, mock_pinot_client):
        """Test the main function with streamable-http transport"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "streamable-http"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.ssl_keyfile = None
            mock_server_config.ssl_certfile = None
            mock_server_config.oauth_enabled = True
            mock_server_config.path = "/mcp"
            mock_server_config.allowed_hosts = ("mcp.example",)
            mock_server_config.allowed_origins = ()

            guarded_app = object()
            with (
                patch("mcp_pinot.server._auth", object()),
                patch("mcp_pinot.server._create_http_app", return_value=guarded_app),
                patch("mcp_pinot.server.uvicorn.run") as mock_uvicorn_run,
            ):
                main()

                mock_uvicorn_run.assert_called_once()
                assert mock_uvicorn_run.call_args.args[0] is guarded_app

    def test_main_function_stdio_transport(self, mock_pinot_client):
        """Test the main function with STDIO transport"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "stdio"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.path = "/mcp"
            mock_server_config.ssl_keyfile = None
            mock_server_config.ssl_certfile = None
            mock_server_config.oauth_enabled = False

            with patch("mcp_pinot.server.mcp.run") as mock_mcp_run:
                main()

                mock_mcp_run.assert_called_once()
                call_args = mock_mcp_run.call_args
                assert call_args[1]["transport"] == "stdio"

    def test_main_function_refuses_network_http_without_oauth(self, mock_pinot_client):
        """Test HTTP transport fails closed on non-loopback hosts without OAuth."""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "http"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.path = "/mcp"
            mock_server_config.ssl_keyfile = None
            mock_server_config.ssl_certfile = None
            mock_server_config.oauth_enabled = False

            with patch("mcp_pinot.server.mcp.run") as mock_mcp_run:
                with pytest.raises(SystemExit, match="Refusing to start"):
                    main()

                mock_mcp_run.assert_not_called()

    def test_main_function_refuses_network_https_without_oauth(self, mock_pinot_client):
        """Test TLS alone is not accepted as HTTP authentication."""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "http"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.path = "/mcp"
            mock_server_config.ssl_keyfile = "/path/to/key.pem"
            mock_server_config.ssl_certfile = "/path/to/cert.pem"
            mock_server_config.oauth_enabled = False

            with patch("mcp_pinot.server.uvicorn.run") as mock_uvicorn_run:
                with pytest.raises(SystemExit, match="without authentication"):
                    main()

                mock_uvicorn_run.assert_not_called()

    def test_main_function_refuses_tls_settings_for_stdio(self, mock_pinot_client):
        """STDIO never opens a listener, even when TLS paths were provided."""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "stdio"
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8000
            mock_server_config.path = "/mcp"
            mock_server_config.ssl_keyfile = "/path/to/key.pem"
            mock_server_config.ssl_certfile = "/path/to/cert.pem"
            mock_server_config.oauth_enabled = False

            with (
                patch("mcp_pinot.server.mcp.run") as mock_mcp_run,
                patch("mcp_pinot.server.uvicorn.run") as mock_uvicorn_run,
            ):
                with pytest.raises(SystemExit, match="only valid with an HTTP"):
                    main()

                mock_mcp_run.assert_not_called()
                mock_uvicorn_run.assert_not_called()
