import pytest
from unittest.mock import patch, MagicMock
import mcp.types as types
from mcp_pinot.server import main, pinot_instance

@pytest.fixture
def mock_server():
    """Fixture to mock the Server class."""
    with patch("mcp_pinot.server.Server") as mock_server_class:
        mock_server = MagicMock()
        mock_server_class.return_value = mock_server
        yield mock_server

@pytest.mark.asyncio
async def test_handle_list_prompts(mock_server):
    """Test the handle_list_prompts function."""
    # Get the handle_list_prompts function from the server
    handle_list_prompts = mock_server.list_prompts.return_value
    
    # Call the function
    result = await handle_list_prompts()
    
    # Check the result
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0].name == "pinot-query"
    assert "Query the pinot database" in result[0].description
    assert isinstance(result[0].arguments, list)

@pytest.mark.asyncio
async def test_handle_get_prompt_valid(mock_server):
    """Test the handle_get_prompt function with a valid prompt name."""
    # Get the handle_get_prompt function from the server
    handle_get_prompt = mock_server.get_prompt.return_value
    
    # Call the function
    result = await handle_get_prompt("pinot-query", None)
    
    # Check the result
    assert isinstance(result, types.GetPromptResult)
    assert "pinot" in result.prompt.lower()

@pytest.mark.asyncio
async def test_handle_get_prompt_invalid(mock_server):
    """Test the handle_get_prompt function with an invalid prompt name."""
    # Get the handle_get_prompt function from the server
    handle_get_prompt = mock_server.get_prompt.return_value
    
    # Call the function with an invalid prompt name
    with pytest.raises(ValueError, match="Unknown prompt"):
        await handle_get_prompt("invalid-prompt", None)

@pytest.mark.asyncio
async def test_handle_list_tools(mock_server):
    """Test the handle_list_tools function."""
    # Get the handle_list_tools function from the server
    handle_list_tools = mock_server.list_tools.return_value
    
    # Call the function
    result = await handle_list_tools()
    
    # Check the result
    assert isinstance(result, list)
    assert len(result) > 0
    
    # Check that each tool has the required attributes
    for tool in result:
        assert hasattr(tool, "name")
        assert hasattr(tool, "description")
        assert hasattr(tool, "parameters")

@pytest.mark.asyncio
async def test_handle_call_tool(mock_server):
    """Test the handle_call_tool function."""
    # Get the handle_call_tool function from the server
    handle_call_tool = mock_server.call_tool.return_value
    
    # Mock the pinot_instance.handle_tool method
    with patch.object(pinot_instance, "handle_tool") as mock_handle_tool:
        mock_handle_tool.return_value = [types.TextContent(text="Test result")]
        
        # Call the function
        result = await handle_call_tool("run_select_query", {"sql": "SELECT * FROM my_table"})
        
        # Check the result
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], types.TextContent)
        assert result[0].text == "Test result"

@pytest.mark.asyncio
async def test_handle_call_tool_invalid_tool(mock_server):
    """Test the handle_call_tool function with an invalid tool name."""
    # Get the handle_call_tool function from the server
    handle_call_tool = mock_server.call_tool.return_value
    
    # Call the function with an invalid tool name
    with pytest.raises(ValueError, match="Unknown tool"):
        await handle_call_tool("invalid_tool", {})

@pytest.mark.asyncio
async def test_main_function(mock_server):
    """Test the main function."""
    # Mock the asyncio.run function
    with patch("asyncio.run") as mock_run:
        # Call the main function
        main()
        
        # Check that asyncio.run was called
        mock_run.assert_called_once() 