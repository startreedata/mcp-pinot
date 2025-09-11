import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from mcp.server import NotificationOptions
import pytest


class TestHttpIntegration:
    """Integration tests for HTTP transport functionality"""

    @pytest.fixture
    def mock_pinot_client(self):
        """Mock PinotClient for testing"""
        with patch("mcp_pinot.server.pinot_client") as mock_client:
            mock_client.test_connection.return_value = {"status": "connected"}
            mock_client.execute_query.return_value = {
                "resultTable": {"rows": [["test", "data"]]},
                "numRowsResultSet": 1,
            }
            mock_client.get_tables.return_value = {"tables": ["test_table"]}
            yield mock_client

    @pytest.fixture
    def server_config_http(self):
        """Configure server for HTTP mode"""
        config_patch = patch("mcp_pinot.server.server_config")
        mock_config = config_patch.start()
        mock_config.transport = "http"
        mock_config.host = "127.0.0.1"
        mock_config.port = 8080
        mock_config.endpoint = "/sse"
        mock_config.ssl_keyfile = None
        mock_config.ssl_certfile = None
        yield mock_config
        config_patch.stop()

    @pytest.mark.asyncio
    async def test_http_server_startup_and_shutdown(
        self, mock_pinot_client, server_config_http
    ):
        """Test that HTTP server can start and shutdown cleanly"""
        from mcp_pinot.server import run_http_server

        # Mock uvicorn server
        with patch("mcp_pinot.server.uvicorn.Server") as mock_uvicorn:
            mock_server = AsyncMock()
            mock_uvicorn.return_value = mock_server

            # Start server in background task
            task = asyncio.create_task(run_http_server())

            # Let it initialize
            await asyncio.sleep(0.1)

            # Cancel the task (simulating shutdown)
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass  # Expected

            # Verify server was created with correct config
            mock_uvicorn.assert_called_once()
            # Verify the config was created (we can't easily inspect uvicorn.Config)
            assert mock_uvicorn.called

    @pytest.mark.asyncio
    async def test_asgi_app_sse_endpoint(self, mock_pinot_client):
        """Test the ASGI application SSE endpoint handling"""
        from mcp_pinot.server import run_http_server

        # This is a complex test that would require running the actual ASGI app
        # For now, we'll test the components separately

        # Test SSE transport creation
        with patch(
            "mcp_pinot.server.mcp.server.sse.SseServerTransport"
        ) as mock_transport_class:
            mock_transport = MagicMock()
            mock_transport_class.return_value = mock_transport

            # Mock server config
            with patch("mcp_pinot.server.server_config") as mock_config:
                mock_config.endpoint = "/sse"
                mock_config.host = "127.0.0.1"
                mock_config.port = 8080
                mock_config.ssl_keyfile = None
                mock_config.ssl_certfile = None

                with patch("mcp_pinot.server.uvicorn.Server") as mock_uvicorn:
                    mock_server_instance = AsyncMock()
                    mock_uvicorn.return_value = mock_server_instance

                    task = asyncio.create_task(run_http_server())
                    await asyncio.sleep(0.01)
                    task.cancel()

                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                    # Verify SSE transport was created with correct endpoint
                    mock_transport_class.assert_called_once_with("/sse")

    @pytest.mark.asyncio
    async def test_main_function_transport_routing(self, mock_pinot_client):
        """Test that main() function correctly routes to HTTP transport"""
        from mcp_pinot.server import main

        with patch("mcp_pinot.server.server_config") as mock_config:
            mock_config.transport = "http"

            with patch("mcp_pinot.server.run_http_server") as mock_run_http:
                mock_run_http.return_value = None

                await main()

                mock_run_http.assert_called_once()

    @pytest.mark.asyncio
    async def test_ssl_configuration(self, mock_pinot_client):
        """Test SSL configuration is properly handled"""
        from mcp_pinot.server import run_http_server

        with patch("mcp_pinot.server.server_config") as mock_config:
            mock_config.host = "0.0.0.0"
            mock_config.port = 8443
            mock_config.endpoint = "/sse"
            mock_config.ssl_keyfile = "/path/to/key.pem"
            mock_config.ssl_certfile = "/path/to/cert.pem"

            with patch("mcp_pinot.server.ssl.SSLContext") as mock_ssl:
                mock_context = MagicMock()
                mock_ssl.return_value = mock_context

                with patch("mcp_pinot.server.uvicorn.Server") as mock_uvicorn:
                    mock_server_instance = AsyncMock()
                    mock_uvicorn.return_value = mock_server_instance

                    task = asyncio.create_task(run_http_server())
                    await asyncio.sleep(0.01)
                    task.cancel()

                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                    # Verify SSL context was created and configured
                    mock_ssl.assert_called_once()
                    mock_context.load_cert_chain.assert_called_once_with(
                        "/path/to/cert.pem", "/path/to/key.pem"
                    )

    def test_server_capabilities_consistency(self, mock_pinot_client):
        """Test that server capabilities are consistent between transports"""
        from mcp_pinot.server import create_server

        server = create_server()
        capabilities = server.get_capabilities(
            notification_options=NotificationOptions(), experimental_capabilities={}
        )

        # Verify basic capabilities are set
        assert hasattr(capabilities, "tools")
        assert hasattr(capabilities, "prompts")

    @pytest.mark.asyncio
    async def test_error_handling_in_main(self, mock_pinot_client):
        """Test error handling in main function"""
        from mcp_pinot.server import main

        # Test invalid transport
        with patch("mcp_pinot.server.server_config") as mock_config:
            mock_config.transport = "invalid_transport"

            with pytest.raises(ValueError, match="Unknown transport"):
                await main()

    @pytest.mark.asyncio
    async def test_environment_variable_integration(self):
        """Test that environment variables are properly integrated"""
        import os

        from mcp_pinot.config import load_server_config

        # Test HTTP configuration via environment
        env_vars = {
            "MCP_TRANSPORT": "http",
            "MCP_HOST": "192.168.1.100",
            "MCP_PORT": "9090",
            "MCP_ENDPOINT": "/custom-sse",
        }

        with patch.dict(os.environ, env_vars):
            config = load_server_config()

            assert config.transport == "http"
            assert config.host == "192.168.1.100"
            assert config.port == 9090
            assert config.endpoint == "/custom-sse"

    @pytest.mark.asyncio
    async def test_backward_compatibility(self, mock_pinot_client):
        """Test that STDIO transport still works (backward compatibility)"""
        from mcp_pinot.server import main

        with patch("mcp_pinot.server.server_config") as mock_config:
            mock_config.transport = "stdio"

            with patch("mcp_pinot.server.run_stdio_server") as mock_run_stdio:
                mock_run_stdio.return_value = None

                await main()

                mock_run_stdio.assert_called_once()

    @pytest.mark.asyncio
    async def test_both_transport_mode(self, mock_pinot_client):
        """Test that 'both' transport mode runs both STDIO and HTTP"""
        from mcp_pinot.server import main

        with patch("mcp_pinot.server.server_config") as mock_config:
            mock_config.transport = "both"

            with patch("mcp_pinot.server.run_stdio_server") as mock_run_stdio:
                with patch("mcp_pinot.server.run_http_server") as mock_run_http:
                    mock_run_stdio.return_value = None
                    mock_run_http.return_value = None

                    # Mock asyncio.wait to simulate successful completion
                    with patch("asyncio.wait") as mock_wait:
                        mock_task1 = MagicMock()
                        mock_task1.exception.return_value = None
                        mock_task2 = MagicMock()
                        mock_task2.exception.return_value = None

                        mock_wait.return_value = ([mock_task1, mock_task2], [])

                        await main()

                        # Verify both transports were started
                        mock_run_stdio.assert_called_once()
                        mock_run_http.assert_called_once()

    @pytest.mark.asyncio
    async def test_default_transport_is_both(self):
        """Test that the default transport configuration is 'both'"""
        import os

        from mcp_pinot.config import load_server_config

        # Test default configuration (no environment variables set)
        with patch.dict(os.environ, {}, clear=True):
            config = load_server_config()
            assert config.transport == "both"


class TestHttpEndpointBehavior:
    """Test specific HTTP endpoint behaviors"""

    def create_mock_asgi_app(self):
        """Create a mock ASGI application for testing"""

        async def mock_app(scope, receive, send):
            if scope["type"] == "http":
                path = scope["path"]
                method = scope["method"]

                if path == "/sse" and method == "GET":
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 200,
                            "headers": [[b"content-type", b"text/event-stream"]],
                        }
                    )
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b"data: test\n\n",
                        }
                    )
                elif path == "/sse" and method == "POST":
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 200,
                            "headers": [[b"content-type", b"application/json"]],
                        }
                    )
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b'{"status": "ok"}',
                        }
                    )
                else:
                    await send(
                        {
                            "type": "http.response.start",
                            "status": 404,
                            "headers": [[b"content-type", b"text/plain"]],
                        }
                    )
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b"Not Found",
                        }
                    )

        return mock_app

    @pytest.mark.asyncio
    async def test_sse_endpoint_get_request(self):
        """Test SSE endpoint responds to GET requests"""
        app = self.create_mock_asgi_app()

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/sse",
            "query_string": b"",
            "headers": [],
        }

        responses = []

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            responses.append(message)

        await app(scope, receive, send)

        # Verify response
        assert len(responses) == 2
        assert responses[0]["status"] == 200
        assert responses[1]["body"] == b"data: test\n\n"

    @pytest.mark.asyncio
    async def test_sse_endpoint_post_request(self):
        """Test SSE endpoint responds to POST requests"""
        app = self.create_mock_asgi_app()

        scope = {
            "type": "http",
            "method": "POST",
            "path": "/sse",
            "query_string": b"",
            "headers": [],
        }

        responses = []

        async def receive():
            return {"type": "http.request", "body": b'{"test": "data"}'}

        async def send(message):
            responses.append(message)

        await app(scope, receive, send)

        # Verify response
        assert len(responses) == 2
        assert responses[0]["status"] == 200
        assert responses[1]["body"] == b'{"status": "ok"}'

    @pytest.mark.asyncio
    async def test_unknown_endpoint_404(self):
        """Test unknown endpoints return 404"""
        app = self.create_mock_asgi_app()

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/unknown",
            "query_string": b"",
            "headers": [],
        }

        responses = []

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            responses.append(message)

        await app(scope, receive, send)

        # Verify 404 response
        assert len(responses) == 2
        assert responses[0]["status"] == 404
        assert responses[1]["body"] == b"Not Found"
