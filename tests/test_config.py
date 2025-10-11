import os
from unittest.mock import patch

from mcp_pinot.config import (
    ServerConfig,
    OAuthConfig,
    _parse_broker_url,
    load_pinot_config,
    load_server_config,
    load_oauth_config,
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
        assert config.port == 8000
        assert config.ssl_keyfile is None
        assert config.ssl_certfile is None
        assert config.oauth_enabled is False

    def test_server_config_custom_values(self):
        """Test ServerConfig with custom values"""
        config = ServerConfig(
            transport="http",
            host="127.0.0.1",
            port=9090,
            ssl_keyfile="/path/to/key.pem",
            ssl_certfile="/path/to/cert.pem",
            oauth_enabled=True,
        )
        assert config.transport == "http"
        assert config.host == "127.0.0.1"
        assert config.port == 9090
        assert config.ssl_keyfile == "/path/to/key.pem"
        assert config.ssl_certfile == "/path/to/cert.pem"
        assert config.oauth_enabled is True


class TestLoadServerConfig:
    """Test the load_server_config function"""

    def test_load_server_config_defaults(self):
        """Test loading server config with default values"""
        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, {}, clear=True):
                config = load_server_config()
                assert config.transport == "http"
                assert config.host == "0.0.0.0"
                assert config.port == 8000
                assert config.ssl_keyfile is None
                assert config.ssl_certfile is None
                assert config.oauth_enabled is False

    def test_load_server_config_from_env(self):
        """Test loading server config from environment variables"""
        env_vars = {
            "MCP_TRANSPORT": "http",
            "MCP_HOST": "192.168.1.100",
            "MCP_PORT": "9999",
            "MCP_SSL_KEYFILE": "/etc/ssl/private/server.key",
            "MCP_SSL_CERTFILE": "/etc/ssl/certs/server.crt",
            "OAUTH_ENABLED": "true",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_server_config()
                assert config.transport == "http"
                assert config.host == "192.168.1.100"
                assert config.port == 9999
                assert config.ssl_keyfile == "/etc/ssl/private/server.key"
                assert config.ssl_certfile == "/etc/ssl/certs/server.crt"
                assert config.oauth_enabled is True

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
                assert config.oauth_enabled is False  # default

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

    def test_load_server_config_oauth_enabled(self):
        """Test loading server config with OAuth enabled"""
        env_vars = {"OAUTH_ENABLED": "true"}

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_server_config()
                assert config.oauth_enabled is True

    def test_load_server_config_all_transport_types(self):
        """Test all valid transport types"""
        for transport in ["stdio", "http", "streamable-http"]:
            env_vars = {"MCP_TRANSPORT": transport}

            with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_server_config()
                    assert config.transport == transport


class TestOAuthConfig:
    """Test the OAuthConfig class and load_oauth_config function"""

    def test_oauth_config_defaults(self):
        """Test OAuthConfig default values"""
        config = OAuthConfig(
            client_id="test_client",
            client_secret="test_secret",
            base_url="http://localhost:8000",
            upstream_authorization_endpoint="http://auth.example.com/authorize",
            upstream_token_endpoint="http://auth.example.com/token",
            jwks_uri="http://auth.example.com/.well-known/jwks.json",
            issuer="http://auth.example.com",
        )
        assert config.client_id == "test_client"
        assert config.client_secret == "test_secret"
        assert config.base_url == "http://localhost:8000"
        assert config.upstream_authorization_endpoint == "http://auth.example.com/authorize"
        assert config.upstream_token_endpoint == "http://auth.example.com/token"
        assert config.jwks_uri == "http://auth.example.com/.well-known/jwks.json"
        assert config.issuer == "http://auth.example.com"
        assert config.audience is None
        assert config.extra_authorize_params is None

    def test_oauth_config_with_audience(self):
        """Test OAuthConfig with audience"""
        config = OAuthConfig(
            client_id="test_client",
            client_secret="test_secret",
            base_url="http://localhost:8000",
            upstream_authorization_endpoint="http://auth.example.com/authorize",
            upstream_token_endpoint="http://auth.example.com/token",
            jwks_uri="http://auth.example.com/.well-known/jwks.json",
            issuer="http://auth.example.com",
            audience="test_audience",
        )
        assert config.audience == "test_audience"

    def test_oauth_config_with_extra_params(self):
        """Test OAuthConfig with extra authorization parameters"""
        extra_params = {"scope": "read write", "response_type": "code"}
        config = OAuthConfig(
            client_id="test_client",
            client_secret="test_secret",
            base_url="http://localhost:8000",
            upstream_authorization_endpoint="http://auth.example.com/authorize",
            upstream_token_endpoint="http://auth.example.com/token",
            jwks_uri="http://auth.example.com/.well-known/jwks.json",
            issuer="http://auth.example.com",
            extra_authorize_params=extra_params,
        )
        assert config.extra_authorize_params == extra_params


class TestLoadOAuthConfig:
    """Test the load_oauth_config function"""

    def test_load_oauth_config_defaults(self):
        """Test loading OAuth config with default values"""
        env_vars = {
            "OAUTH_CLIENT_ID": "test_client",
            "OAUTH_CLIENT_SECRET": "test_secret",
            "OAUTH_BASE_URL": "http://localhost:8000",
            "OAUTH_AUTHORIZATION_ENDPOINT": "http://auth.example.com/authorize",
            "OAUTH_TOKEN_ENDPOINT": "http://auth.example.com/token",
            "OAUTH_JWKS_URI": "http://auth.example.com/.well-known/jwks.json",
            "OAUTH_ISSUER": "http://auth.example.com",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_oauth_config()
                assert config.client_id == "test_client"
                assert config.client_secret == "test_secret"
                assert config.base_url == "http://localhost:8000"
                assert config.upstream_authorization_endpoint == "http://auth.example.com/authorize"
                assert config.upstream_token_endpoint == "http://auth.example.com/token"
                assert config.jwks_uri == "http://auth.example.com/.well-known/jwks.json"
                assert config.issuer == "http://auth.example.com"
                assert config.audience is None
                assert config.extra_authorize_params is None

    def test_load_oauth_config_with_audience(self):
        """Test loading OAuth config with audience"""
        env_vars = {
            "OAUTH_CLIENT_ID": "test_client",
            "OAUTH_CLIENT_SECRET": "test_secret",
            "OAUTH_BASE_URL": "http://localhost:8000",
            "OAUTH_AUTHORIZATION_ENDPOINT": "http://auth.example.com/authorize",
            "OAUTH_TOKEN_ENDPOINT": "http://auth.example.com/token",
            "OAUTH_JWKS_URI": "http://auth.example.com/.well-known/jwks.json",
            "OAUTH_ISSUER": "http://auth.example.com",
            "OAUTH_AUDIENCE": "test_audience",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_oauth_config()
                assert config.audience == "test_audience"

    def test_load_oauth_config_with_extra_params(self):
        """Test loading OAuth config with extra authorization parameters"""
        env_vars = {
            "OAUTH_CLIENT_ID": "test_client",
            "OAUTH_CLIENT_SECRET": "test_secret",
            "OAUTH_BASE_URL": "http://localhost:8000",
            "OAUTH_AUTHORIZATION_ENDPOINT": "http://auth.example.com/authorize",
            "OAUTH_TOKEN_ENDPOINT": "http://auth.example.com/token",
            "OAUTH_JWKS_URI": "http://auth.example.com/.well-known/jwks.json",
            "OAUTH_ISSUER": "http://auth.example.com",
            "OAUTH_EXTRA_AUTH_PARAMS": '{"scope": "read write", "response_type": "code"}',
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_oauth_config()
                assert config.extra_authorize_params == {"scope": "read write", "response_type": "code"}

    def test_load_oauth_config_invalid_extra_params(self):
        """Test loading OAuth config with invalid extra authorization parameters"""
        env_vars = {
            "OAUTH_CLIENT_ID": "test_client",
            "OAUTH_CLIENT_SECRET": "test_secret",
            "OAUTH_BASE_URL": "http://localhost:8000",
            "OAUTH_AUTHORIZATION_ENDPOINT": "http://auth.example.com/authorize",
            "OAUTH_TOKEN_ENDPOINT": "http://auth.example.com/token",
            "OAUTH_JWKS_URI": "http://auth.example.com/.well-known/jwks.json",
            "OAUTH_ISSUER": "http://auth.example.com",
            "OAUTH_EXTRA_AUTH_PARAMS": "invalid_json",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_oauth_config()
                assert config.extra_authorize_params is None

    def test_load_oauth_config_extra_params_not_dict(self):
        """Test loading OAuth config with extra params that are not a dict"""
        env_vars = {
            "OAUTH_CLIENT_ID": "test_client",
            "OAUTH_CLIENT_SECRET": "test_secret",
            "OAUTH_BASE_URL": "http://localhost:8000",
            "OAUTH_AUTHORIZATION_ENDPOINT": "http://auth.example.com/authorize",
            "OAUTH_TOKEN_ENDPOINT": "http://auth.example.com/token",
            "OAUTH_JWKS_URI": "http://auth.example.com/.well-known/jwks.json",
            "OAUTH_ISSUER": "http://auth.example.com",
            "OAUTH_EXTRA_AUTH_PARAMS": '"not_a_dict"',
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_oauth_config()
                assert config.extra_authorize_params is None
