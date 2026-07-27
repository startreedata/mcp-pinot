from unittest.mock import MagicMock, patch

from fastmcp import Client
from mcp.shared.exceptions import McpError
import pytest

from mcp_pinot.config import PinotConfig
from mcp_pinot.pinot_client import (
    PinotClient,
    encode_pinot_path_component,
    validate_pinot_path_component,
)
from mcp_pinot.server import mcp


@pytest.fixture
def pinot_client() -> PinotClient:
    return PinotClient(
        PinotConfig(
            controller_url="https://controller.example",
            broker_host="broker.example",
            broker_port=443,
            broker_scheme="https",
            username=None,
            password=None,
            token=None,
            database="",
            use_msqe=False,
        )
    )


@pytest.mark.parametrize(
    "value",
    [
        "",
        ".",
        "..",
        "../tables",
        "..\\tables",
        "table/name",
        "table\\name",
        "table?type=OFFLINE",
        "table#fragment",
        "table\x00name",
        "table\nname",
        "%2e%2e",
        "%252e%252e",
        "%25252525252E%25252525252E",
        "table%2fname",
        "table%5cname",
        "table%3fname",
        "table%23name",
        "table%00name",
    ],
)
def test_path_component_validator_rejects_adversarial_values(value):
    with pytest.raises(ValueError):
        validate_pinot_path_component(value, "test component")


def test_path_component_encoder_percent_encodes_safe_non_ascii_and_spaces():
    assert (
        encode_pinot_path_component("café events", "table name") == "caf%C3%A9%20events"
    )


def test_table_and_segment_components_are_encoded_in_controller_url(pinot_client):
    response = MagicMock()
    response.json.return_value = {"indexes": []}
    with patch.object(pinot_client, "http_request", return_value=response) as request:
        pinot_client.get_index_column_detail("café events", "segment 1+blue")

    url = request.call_args.args[0]
    assert url == (
        "https://controller.example/segments/"
        "caf%C3%A9%20events_REALTIME/segment%201%2Bblue/metadata?columns=*"
    )


@pytest.mark.parametrize(
    "operation",
    [
        lambda client, value: client.get_table_detail(value),
        lambda client, value: client.get_segment_metadata_detail(value),
        lambda client, value: client.get_segments(value),
        lambda client, value: client.get_tableconfig_schema_detail(value),
        lambda client, value: client.get_schema(value),
        lambda client, value: client.get_table_config(value),
        lambda client, value: client.update_schema(value, "{}"),
        lambda client, value: client.update_table_config(value, "{}"),
        lambda client, value: client.get_index_column_detail("events", value),
    ],
)
def test_controller_operations_reject_traversal_before_http(operation, pinot_client):
    with (
        patch.object(pinot_client, "http_request") as http_request,
        patch("mcp_pinot.pinot_client.requests") as requests,
        pytest.raises(ValueError),
    ):
        operation(pinot_client, "..%2Ftables")

    http_request.assert_not_called()
    requests.get.assert_not_called()
    requests.put.assert_not_called()


@pytest.mark.parametrize(
    ("payload", "method"),
    [
        ('{"schemaName":"../tables"}', "create_schema"),
        ('{"tableName":"table%2Fname"}', "create_table_config"),
    ],
)
def test_create_payload_names_are_validated_before_http(pinot_client, payload, method):
    with patch("mcp_pinot.pinot_client.requests") as requests:
        with pytest.raises(ValueError):
            getattr(pinot_client, method)(payload)

    requests.post.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uri",
    [
        "pinot://schema/..%2Ftables",
        "pinot://schema/%2e%2e",
        "pinot://table-config/evil%5Cname",
        "pinot://schema/evil%3Fquery",
    ],
)
async def test_resource_templates_reject_encoded_path_attacks(uri):
    """Protocol-level resource reads never forward decoded hostile components."""
    with patch("mcp_pinot.server.pinot_client") as client:
        with pytest.raises(McpError):
            async with Client(mcp) as mcp_client:
                await mcp_client.read_resource(uri)

    client.get_schema.assert_not_called()
    client.get_table_config.assert_not_called()
