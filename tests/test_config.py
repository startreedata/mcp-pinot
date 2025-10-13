import os
from unittest.mock import patch

from mcp_pinot.config import (
    ServerConfig,
    _parse_broker_url,
    load_pinot_config,
    load_server_config,
)


class TestParseBrokerUrl:
    """Test the _parse_broker_url function"""

    def test_parse_full_url(self):
        """Test parsing a full URL with all components"""
        host, port, scheme = _parse_broker_url("https://broker.example.com:8443")
        assert host == "broker.example.com"
        assert port == 8443
        assert scheme == "https"

    def test_parse_url_without_port(self):
        """Test parsing URL without explicit port uses defaults"""
        host, port, scheme = _parse_broker_url("https://broker.example.com")
        assert host == "broker.example.com"
        assert port == 443  # Default for https
        assert scheme == "https"

        host, port, scheme = _parse_broker_url("http://broker.example.com")
        assert host == "broker.example.com"
        assert port == 80  # Default for http
        assert scheme == "http"

    def test_parse_localhost_url(self):
        """Test parsing localhost URLs"""
        host, port, scheme = _parse_broker_url("http://localhost:8099")
        assert host == "localhost"
        assert port == 8099
        assert scheme == "http"

    def test_parse_invalid_url(self):
        """Test parsing invalid URL falls back to defaults"""
        host, port, scheme = _parse_broker_url("invalid-url")
        assert host == "localhost"
        assert port == 80
        assert scheme == "http"

    def test_parse_url_with_path(self):
        """Test parsing URL with path ignores the path"""
        host, port, scheme = _parse_broker_url(
            "https://broker.example.com:8443/some/path"
        )
        assert host == "broker.example.com"
        assert port == 8443
        assert scheme == "https"


class TestLoadPinotConfig:
    """Test the load_pinot_config function"""

    def test_individual_configs_only(self):
        """Test loading config with only individual broker configs"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_BROKER_HOST": "broker.example.com",
            "PINOT_BROKER_PORT": "8099",
            "PINOT_BROKER_SCHEME": "http",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.broker_host == "broker.example.com"
                assert config.broker_port == 8099
                assert config.broker_scheme == "http"

    def test_broker_url_only(self):
        """Test loading config with only PINOT_BROKER_URL"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_BROKER_URL": "https://broker.example.com:8443",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.broker_host == "broker.example.com"
                assert config.broker_port == 8443
                assert config.broker_scheme == "https"

    def test_broker_url_with_individual_overrides(self):
        """Test that individual configs override URL values"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_BROKER_URL": "https://broker.example.com:8443",
            "PINOT_BROKER_HOST": "override.example.com",
            "PINOT_BROKER_PORT": "9000",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.broker_host == "override.example.com"
                assert config.broker_port == 9000
                assert config.broker_scheme == "https"  # From URL, not overridden

    def test_broker_url_with_scheme_override(self):
        """Test that PINOT_BROKER_SCHEME overrides URL scheme"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_BROKER_URL": "https://broker.example.com:8443",
            "PINOT_BROKER_SCHEME": "http",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.broker_host == "broker.example.com"
                assert config.broker_port == 8443
                assert config.broker_scheme == "http"  # Overridden

    def test_no_broker_config(self):
        """Test default values when no broker config is provided"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.controller_url == "http://controller:9000"
                assert config.broker_host == "localhost"
                assert config.broker_port == 8000
                assert config.broker_scheme == "http"

    def test_quickstart_defaults(self):
        """Test that quickstart defaults are used when no config is provided"""
        env_vars = {}  # No config at all

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.controller_url == "http://localhost:9000"
                assert config.broker_host == "localhost"
                assert config.broker_port == 8000
                assert config.broker_scheme == "http"

    def test_broker_url_default_ports(self):
        """Test that URL parsing uses correct default ports"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_BROKER_URL": "http://broker.example.com",  # No port specified
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.broker_host == "broker.example.com"
                assert config.broker_port == 80  # Default for http
                assert config.broker_scheme == "http"

        # Test HTTPS default
        env_vars["PINOT_BROKER_URL"] = "https://broker.example.com"
        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.broker_port == 443  # Default for https

    def test_all_config_fields_present(self):
        """Test that all expected config fields are present"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_BROKER_URL": "https://broker.example.com:8443",
            "PINOT_USERNAME": "testuser",
            "PINOT_PASSWORD": "testpass",
            "PINOT_TOKEN": "testtoken",
            "PINOT_DATABASE": "testdb",
            "PINOT_USE_MSQE": "true",
            "PINOT_REQUEST_TIMEOUT": "30",
            "PINOT_CONNECTION_TIMEOUT": "20",
            "PINOT_QUERY_TIMEOUT": "40",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.controller_url == "http://controller:9000"
                assert config.broker_host == "broker.example.com"
                assert config.broker_port == 8443
                assert config.broker_scheme == "https"
                assert config.username == "testuser"
                assert config.password == "testpass"  # noqa: S105
                assert config.token == "testtoken"  # noqa: S105
                assert config.database == "testdb"
                assert config.use_msqe is True
                assert config.request_timeout == 30
                assert config.connection_timeout == 20
                assert config.query_timeout == 40


class TestServerConfig:
    """Test the ServerConfig class and load_server_config function"""

    def test_server_config_defaults(self):
        """Test ServerConfig default values"""
        config = ServerConfig()
        assert config.transport == "http"
        assert config.host == "0.0.0.0"
        assert config.port == 8080
        assert config.ssl_keyfile is None
        assert config.ssl_certfile is None

    def test_server_config_custom_values(self):
        """Test ServerConfig with custom values"""
        config = ServerConfig(
            transport="http",
            host="127.0.0.1",
            port=9090,
            ssl_keyfile="/path/to/key.pem",
            ssl_certfile="/path/to/cert.pem",
        )
        assert config.transport == "http"
        assert config.host == "127.0.0.1"
        assert config.port == 9090
        assert config.ssl_keyfile == "/path/to/key.pem"
        assert config.ssl_certfile == "/path/to/cert.pem"


class TestLoadServerConfig:
    """Test the load_server_config function"""

    def test_load_server_config_defaults(self):
        """Test loading server config with default values"""
        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, {}, clear=True):
                config = load_server_config()
                assert config.transport == "http"
                assert config.host == "0.0.0.0"
                assert config.port == 8080
                assert config.ssl_keyfile is None
                assert config.ssl_certfile is None

    def test_load_server_config_from_env(self):
        """Test loading server config from environment variables"""
        env_vars = {
            "MCP_TRANSPORT": "http",
            "MCP_HOST": "192.168.1.100",
            "MCP_PORT": "9999",
            "MCP_SSL_KEYFILE": "/etc/ssl/private/server.key",
            "MCP_SSL_CERTFILE": "/etc/ssl/certs/server.crt",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_server_config()
                assert config.transport == "http"
                assert config.host == "192.168.1.100"
                assert config.port == 9999
                assert config.ssl_keyfile == "/etc/ssl/private/server.key"
                assert config.ssl_certfile == "/etc/ssl/certs/server.crt"

    def test_load_server_config_transport_case_insensitive(self):
        """Test that transport value is converted to lowercase"""
        env_vars = {"MCP_TRANSPORT": "HTTP"}

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_server_config()
                assert config.transport == "http"

    def test_load_server_config_partial_env(self):
        """Test loading server config with only some env vars set"""
        env_vars = {"MCP_TRANSPORT": "http", "MCP_PORT": "3000"}

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_server_config()
                assert config.transport == "http"
                assert config.host == "0.0.0.0"  # default
                assert config.port == 3000
                assert config.ssl_keyfile is None  # default
                assert config.ssl_certfile is None  # default

    def test_load_server_config_invalid_port(self):
        """Test that invalid port values raise ValueError"""
        env_vars = {"MCP_PORT": "not_a_number"}

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                try:
                    load_server_config()
                    assert False, "Should have raised ValueError for invalid port"
                except ValueError:
                    pass  # Expected behavior


    def test_load_server_config_all_transport_types(self):
        """Test all valid transport types"""
        for transport in ["stdio", "http", "streamable-http"]:
            env_vars = {"MCP_TRANSPORT": transport}

            with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_server_config()
                    assert config.transport == transport


