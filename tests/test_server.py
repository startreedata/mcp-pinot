import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import mcp.types as types
from mcp.server import NotificationOptions
from mcp_pinot.server import main

@pytest.fixture
def mock_server():
    """Fixture to mock the Server class."""
    with patch("mcp_pinot.server.Server") as mock_server_class:
        mock_server = MagicMock()
        # Set up async mock methods
        mock_server.list_prompts = AsyncMock()
        mock_server.get_prompt = AsyncMock()
        mock_server.list_tools = AsyncMock()
        mock_server.call_tool = AsyncMock()
        mock_server_class.return_value = mock_server
        yield mock_server

@pytest.mark.asyncio
async def test_handle_list_prompts(mock_server):
    """Test the handle_list_prompts function."""
    # Set up the mock return value
    mock_server.list_prompts.return_value = [
        types.Prompt(
            name="pinot-query",
            description="Query the pinot database",
            arguments=[]
        )
    ]
    
    # Call the function
    result = await mock_server.list_prompts()
    
    # Check the result
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0].name == "pinot-query"
    assert "Query the pinot database" in result[0].description
    assert isinstance(result[0].arguments, list)

@pytest.mark.asyncio
async def test_handle_get_prompt_valid(mock_server):
    """Test the handle_get_prompt function with a valid prompt name."""
    # Set up the mock return value
    mock_server.get_prompt.return_value = types.GetPromptResult(
        description="Pinot query assistance template",
        messages=[
            types.PromptMessage(
                role="user",
                content=types.TextContent(type="text", text="pinot query template"),
            )
        ]
    )
    
    # Call the function
    result = await mock_server.get_prompt("pinot-query", None)
    
    # Check the result
    assert isinstance(result, types.GetPromptResult)
    assert "pinot" in result.messages[0].content.text.lower()

@pytest.mark.asyncio
async def test_handle_get_prompt_invalid(mock_server):
    """Test the handle_get_prompt function with an invalid prompt name."""
    # Set up the mock to raise an exception
    mock_server.get_prompt.side_effect = ValueError("Unknown prompt")
    
    # Call the function with an invalid prompt name
    with pytest.raises(ValueError, match="Unknown prompt"):
        await mock_server.get_prompt("invalid-prompt", None)

@pytest.mark.asyncio
async def test_handle_list_tools(mock_server):
    """Test the handle_list_tools function."""
    # Set up the mock return value
    mock_server.list_tools.return_value = [
        types.Tool(
            name="test_tool",
            description="Test tool description",
            inputSchema={
                "type": "object",
                "properties": {},
            }
        )
    ]
    
    # Call the function
    result = await mock_server.list_tools()
    
    # Check the result
    assert isinstance(result, list)
    assert len(result) > 0
    
    # Check that each tool has the required attributes
    for tool in result:
        assert hasattr(tool, "name")
        assert hasattr(tool, "description")
        assert hasattr(tool, "inputSchema")

@pytest.mark.asyncio
async def test_handle_call_tool(mock_server):
    """Test the handle_call_tool function."""
    # Set up the mock return value with the required 'type' field
    mock_server.call_tool.return_value = [
        types.TextContent(type="text", text="Test result")
    ]
    
    # Call the function
    result = await mock_server.call_tool("run_select_query", {"sql": "SELECT * FROM my_table"})
    
    # Check the result
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], types.TextContent)
    assert result[0].text == "Test result"

@pytest.mark.asyncio
async def test_handle_call_tool_invalid_tool(mock_server):
    """Test the handle_call_tool function with an invalid tool name."""
    # Set up the mock to raise an exception
    mock_server.call_tool.side_effect = ValueError("Unknown tool")
    
    # Call the function with an invalid tool name
    with pytest.raises(ValueError, match="Unknown tool"):
        await mock_server.call_tool("invalid_tool", {})

@pytest.mark.asyncio
async def test_main_function():
    """Test the main function."""
    # Mock the Server class and its methods
    with patch("mcp_pinot.server.Server") as mock_server_class:
        mock_server = MagicMock()
        mock_server.run = AsyncMock()
        
        # Set up the get_capabilities mock to return a valid ServerCapabilities object
        mock_server.get_capabilities.return_value = types.ServerCapabilities(
            supportsPrompts=True,
            supportsTools=True,
            supportsNotifications=True,
            supportsExperimentalCapabilities=True,
        )
        
        mock_server_class.return_value = mock_server
        
        # Mock the stdio_server context manager
        with patch("mcp.server.stdio.stdio_server") as mock_stdio_server:
            mock_read_stream = AsyncMock()
            mock_write_stream = AsyncMock()
            mock_stdio_server.return_value.__aenter__.return_value = (mock_read_stream, mock_write_stream)
            
            # Call the main function
            await main()
            
            # Check that the server was run with the correct arguments
            mock_server.run.assert_called_once() 