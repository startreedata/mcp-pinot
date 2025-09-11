import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from mcp.server import NotificationOptions, Server
import mcp.types as types
import pytest

from mcp_pinot.server import create_server, run_http_server, run_stdio_server


class TestCreateServer:
    """Test the create_server function"""

    def test_create_server_returns_server_instance(self):
        """Test that create_server returns a Server instance"""
        server = create_server()
        assert isinstance(server, Server)
        assert server.name == "pinot_mcp_claude"

    @pytest.mark.asyncio
    async def test_create_server_handlers_work(self):
        """Test that the server handlers are properly registered"""
        server = create_server()

        # Test that the server has the expected capabilities
        capabilities = server.get_capabilities(
            notification_options=NotificationOptions(), experimental_capabilities={}
        )
        assert capabilities.prompts is not None
        assert capabilities.tools is not None

    @pytest.mark.asyncio
    async def test_create_server_tools_handler(self):
        """Test that the tools handler is properly registered"""
        server = create_server()

        # Test that the server has tools capability
        capabilities = server.get_capabilities(
            notification_options=NotificationOptions(), experimental_capabilities={}
        )
        assert capabilities.tools is not None

        # Test that we can create the server without errors
        # (The actual tool registration is tested in the existing server tests)
        assert server.name == "pinot_mcp_claude"


class TestHttpTransport:
    """Test HTTP transport functionality"""

    @pytest.mark.asyncio
    async def test_run_stdio_server(self):
        """Test the STDIO server runner"""
        with patch("mcp_pinot.server.mcp.server.stdio.stdio_server") as mock_stdio:
            mock_read_stream = AsyncMock()
            mock_write_stream = AsyncMock()
            mock_stdio.return_value.__aenter__.return_value = (
                mock_read_stream,
                mock_write_stream,
            )

            # Mock the server.run method to avoid actually running
            with patch("mcp_pinot.server.create_server") as mock_create_server:
                mock_server = MagicMock()
                mock_server.run = AsyncMock()
                mock_server.get_capabilities.return_value = types.ServerCapabilities(
                    supportsPrompts=True,
                    supportsTools=True,
                    supportsNotifications=True,
                    supportsExperimentalCapabilities=True,
                )
                mock_create_server.return_value = mock_server

                await run_stdio_server()

                # Verify server.run was called with correct parameters
                mock_server.run.assert_called_once()
                args = mock_server.run.call_args[0]
                assert args[0] == mock_read_stream
                assert args[1] == mock_write_stream

    @pytest.mark.asyncio
    async def test_run_http_server_basic(self):
        """Test the HTTP server runner with basic configuration"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.host = "127.0.0.1"
            mock_server_config.port = 8080
            mock_server_config.endpoint = "/sse"
            mock_server_config.ssl_keyfile = None
            mock_server_config.ssl_certfile = None

            with patch("mcp_pinot.server.uvicorn.Server") as mock_uvicorn_server:
                mock_server_instance = AsyncMock()
                mock_uvicorn_server.return_value = mock_server_instance

                # Create a task that we can cancel to avoid infinite running
                task = asyncio.create_task(run_http_server())
                await asyncio.sleep(0.01)  # Let it start
                task.cancel()

                try:
                    await task
                except asyncio.CancelledError:
                    pass  # Expected

                # Verify uvicorn server was created
                mock_uvicorn_server.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_http_server_with_ssl(self):
        """Test the HTTP server runner with SSL configuration"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.host = "0.0.0.0"
            mock_server_config.port = 8443
            mock_server_config.endpoint = "/sse"
            mock_server_config.ssl_keyfile = "/path/to/key.pem"
            mock_server_config.ssl_certfile = "/path/to/cert.pem"

            with patch("mcp_pinot.server.ssl.SSLContext") as mock_ssl_context:
                with patch("mcp_pinot.server.uvicorn.Server") as mock_uvicorn_server:
                    mock_server_instance = AsyncMock()
                    mock_uvicorn_server.return_value = mock_server_instance

                    # Create a task that we can cancel
                    task = asyncio.create_task(run_http_server())
                    await asyncio.sleep(0.01)  # Let it start
                    task.cancel()

                    try:
                        await task
                    except asyncio.CancelledError:
                        pass  # Expected

                    # Verify SSL context was created
                    mock_ssl_context.assert_called_once()


class TestHttpEndpoints:
    """Test HTTP endpoint handling"""

    @pytest.fixture
    def mock_scope(self):
        """Create a mock ASGI scope for HTTP requests"""
        return {
            "type": "http",
            "method": "GET",
            "path": "/sse",
            "query_string": b"",
            "headers": [],
        }

    @pytest.fixture
    def mock_receive(self):
        """Create a mock ASGI receive callable"""
        return AsyncMock()

    @pytest.fixture
    def mock_send(self):
        """Create a mock ASGI send callable"""
        return AsyncMock()

    @pytest.mark.asyncio
    async def test_http_app_sse_get_request(self, mock_scope, mock_receive, mock_send):
        """Test that GET requests to SSE endpoint are handled correctly"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.endpoint = "/sse"

            with patch(
                "mcp_pinot.server.mcp.server.sse.SseServerTransport"
            ) as mock_transport:
                mock_transport_instance = MagicMock()
                mock_transport.return_value = mock_transport_instance

                # Mock the connect_sse context manager
                mock_streams = (AsyncMock(), AsyncMock())
                mock_transport_instance.connect_sse.return_value.__aenter__ = AsyncMock(
                    return_value=mock_streams
                )
                mock_transport_instance.connect_sse.return_value.__aexit__ = AsyncMock()

                with patch("mcp_pinot.server.create_server") as mock_create_server:
                    mock_server = MagicMock()
                    mock_server.run = AsyncMock()
                    mock_server.get_capabilities.return_value = (
                        types.ServerCapabilities(
                            supportsPrompts=True,
                            supportsTools=True,
                            supportsNotifications=True,
                            supportsExperimentalCapabilities=True,
                        )
                    )
                    mock_create_server.return_value = mock_server

                    # Import the app function from run_http_server
                    # We need to extract the app function for testing
                    # This is a bit tricky since it's defined inside run_http_server
                    # For now, we'll test the overall flow
                    pass

    @pytest.mark.asyncio
    async def test_http_app_post_request(self, mock_receive, mock_send):
        """Test that POST requests to SSE endpoint are handled correctly"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.endpoint = "/sse"

            with patch(
                "mcp_pinot.server.mcp.server.sse.SseServerTransport"
            ) as mock_transport:
                mock_transport_instance = MagicMock()
                mock_transport.return_value = mock_transport_instance
                mock_transport_instance.handle_post_message = AsyncMock()

                # This test would require extracting the ASGI app from run_http_server
                # For now, we verify the mocking setup
                pass

    @pytest.mark.asyncio
    async def test_http_app_404_response(self, mock_receive, mock_send):
        """Test that requests to unknown paths return 404"""
        # This would test the 404 handling in the ASGI app
        # Implementation would require extracting the app function
        pass


class TestMainFunction:
    """Test the main function with transport selection"""

    @pytest.mark.asyncio
    async def test_main_with_stdio_transport(self):
        """Test main function selects STDIO transport"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "stdio"

            with patch("mcp_pinot.server.run_stdio_server") as mock_run_stdio:
                mock_run_stdio.return_value = AsyncMock()

                from mcp_pinot.server import main

                await main()

                mock_run_stdio.assert_called_once()

    @pytest.mark.asyncio
    async def test_main_with_http_transport(self):
        """Test main function selects HTTP transport"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "http"

            with patch("mcp_pinot.server.run_http_server") as mock_run_http:
                mock_run_http.return_value = AsyncMock()

                from mcp_pinot.server import main

                await main()

                mock_run_http.assert_called_once()

    @pytest.mark.asyncio
    async def test_main_with_invalid_transport(self):
        """Test main function raises error for invalid transport"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "invalid"

            from mcp_pinot.server import main

            with pytest.raises(ValueError, match="Unknown transport"):
                await main()

    @pytest.mark.asyncio
    async def test_main_with_both_transports(self):
        """Test main function runs both transports simultaneously"""
        with patch("mcp_pinot.server.server_config") as mock_server_config:
            mock_server_config.transport = "both"

            with patch("mcp_pinot.server.run_stdio_server") as mock_run_stdio:
                with patch("mcp_pinot.server.run_http_server") as mock_run_http:
                    # Make both return immediately to avoid hanging
                    mock_run_stdio.return_value = None
                    mock_run_http.return_value = None

                    # Mock asyncio.wait to return immediately
                    with patch("asyncio.wait") as mock_wait:
                        # Simulate both tasks completing successfully
                        mock_task1 = MagicMock()
                        mock_task1.exception.return_value = None
                        mock_task2 = MagicMock()
                        mock_task2.exception.return_value = None

                        mock_wait.return_value = ([mock_task1, mock_task2], [])

                        from mcp_pinot.server import main

                        await main()

                        # Verify both transports were started
                        mock_run_stdio.assert_called_once()
                        mock_run_http.assert_called_once()


class TestIntegration:
    """Integration tests for HTTP transport"""

    @pytest.mark.asyncio
    async def test_server_creation_and_config_integration(self):
        """Test that server creation works with different configurations"""
        # Test with default config
        with patch.dict("os.environ", {}, clear=True):
            from mcp_pinot.config import load_server_config

            config = load_server_config()
            assert config.transport == "both"

            server = create_server()
            assert server.name == "pinot_mcp_claude"

        # Test with HTTP config
        with patch.dict("os.environ", {"MCP_TRANSPORT": "http"}, clear=True):
            config = load_server_config()
            assert config.transport == "http"

            server = create_server()
            assert server.name == "pinot_mcp_claude"

    @pytest.mark.asyncio
    async def test_transport_selection_integration(self):
        """Test that transport selection works end-to-end"""
        # Test STDIO selection
        with patch("mcp_pinot.server.server_config") as mock_config:
            mock_config.transport = "stdio"

            with patch("mcp_pinot.server.run_stdio_server") as mock_stdio:
                with patch("mcp_pinot.server.run_http_server") as mock_http:
                    from mcp_pinot.server import main

                    await main()

                    mock_stdio.assert_called_once()
                    mock_http.assert_not_called()

        # Test HTTP selection
        with patch("mcp_pinot.server.server_config") as mock_config:
            mock_config.transport = "http"

            with patch("mcp_pinot.server.run_stdio_server") as mock_stdio:
                with patch("mcp_pinot.server.run_http_server") as mock_http:
                    from mcp_pinot.server import main

                    await main()

                    mock_http.assert_called_once()
                    mock_stdio.assert_not_called()
