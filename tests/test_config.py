import os
import tempfile
from unittest.mock import patch

import pytest

from mcp_pinot.config import (
    OAuthConfig,
    ServerConfig,
    _load_table_filters,
    _parse_broker_url,
    _parse_table_filter_config,
    _read_token_from_file,
    load_oauth_config,
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
                assert config.port == 8080
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
        assert (
            config.upstream_authorization_endpoint
            == "http://auth.example.com/authorize"
        )
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
                assert (
                    config.upstream_authorization_endpoint
                    == "http://auth.example.com/authorize"
                )
                assert config.upstream_token_endpoint == "http://auth.example.com/token"
                assert (
                    config.jwks_uri == "http://auth.example.com/.well-known/jwks.json"
                )
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
            "OAUTH_EXTRA_AUTH_PARAMS": (
                '{"scope": "read write", "response_type": "code"}'
            ),
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_oauth_config()
                assert config.extra_authorize_params == {
                    "scope": "read write",
                    "response_type": "code",
                }

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


class TestReadTokenFromFile:
    """Test the _read_token_from_file function"""

    def test_read_token_from_valid_file(self):
        """Test reading token from a valid file"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test_token_123")
            temp_file = f.name

        try:
            token = _read_token_from_file(temp_file)
            assert token == "Bearer test_token_123"
        finally:
            os.unlink(temp_file)

    def test_read_token_from_file_with_whitespace(self):
        """Test reading token from file with leading/trailing whitespace"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("  \n  test_token_123  \n  ")
            temp_file = f.name

        try:
            token = _read_token_from_file(temp_file)
            assert token == "Bearer test_token_123"
        finally:
            os.unlink(temp_file)

    def test_read_token_from_nonexistent_file(self):
        """Test reading token from non-existent file"""
        token = _read_token_from_file("/nonexistent/file/path")
        assert token is None

    def test_read_token_from_directory(self):
        """Test reading token from directory (should fail)"""
        with tempfile.TemporaryDirectory() as temp_dir:
            token = _read_token_from_file(temp_dir)
            assert token is None

    def test_read_token_from_empty_file(self):
        """Test reading token from empty file"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("")
            temp_file = f.name

        try:
            token = _read_token_from_file(temp_file)
            assert token is None
        finally:
            os.unlink(temp_file)

    def test_read_token_from_file_with_only_whitespace(self):
        """Test reading token from file with only whitespace"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("   \n\t  \n  ")
            temp_file = f.name

        try:
            token = _read_token_from_file(temp_file)
            assert token is None
        finally:
            os.unlink(temp_file)

    def test_read_token_from_file_permission_denied(self):
        """Test reading token from file with no read permission"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test_token")
            temp_file = f.name

        try:
            # Remove read permission
            os.chmod(temp_file, 0o000)
            token = _read_token_from_file(temp_file)
            assert token is None
        finally:
            # Restore permissions for cleanup
            os.chmod(temp_file, 0o644)
            os.unlink(temp_file)


class TestLoadPinotConfigTokenFilename:
    """Test token filename functionality in load_pinot_config"""

    def test_token_filename_only(self):
        """Test loading config with only token filename"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("test_token_from_file")
            temp_file = f.name

        try:
            env_vars = {
                "PINOT_CONTROLLER_URL": "http://controller:9000",
                "PINOT_TOKEN_FILENAME": temp_file,
            }

            with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_pinot_config()
                    assert config.token == "Bearer test_token_from_file"
        finally:
            os.unlink(temp_file)

    def test_token_filename_with_direct_token(self):
        """Test that direct token takes precedence over token filename"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("token_from_file")
            temp_file = f.name

        try:
            env_vars = {
                "PINOT_CONTROLLER_URL": "http://controller:9000",
                "PINOT_TOKEN": "direct_token",
                "PINOT_TOKEN_FILENAME": temp_file,
            }

            with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_pinot_config()
                    assert config.token == "direct_token"
        finally:
            os.unlink(temp_file)

    def test_token_filename_nonexistent_file(self):
        """Test loading config with non-existent token file"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_TOKEN_FILENAME": "/nonexistent/file/path",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.token is None

    def test_token_filename_empty_file(self):
        """Test loading config with empty token file"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("")
            temp_file = f.name

        try:
            env_vars = {
                "PINOT_CONTROLLER_URL": "http://controller:9000",
                "PINOT_TOKEN_FILENAME": temp_file,
            }

            with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_pinot_config()
                    assert config.token is None
        finally:
            os.unlink(temp_file)

    def test_token_filename_with_username_password(self):
        """Test that token filename works alongside username/password"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("token_from_file")
            temp_file = f.name

        try:
            env_vars = {
                "PINOT_CONTROLLER_URL": "http://controller:9000",
                "PINOT_USERNAME": "testuser",
                "PINOT_PASSWORD": "testpass",
                "PINOT_TOKEN_FILENAME": temp_file,
            }

            with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_pinot_config()
                    assert config.token == "Bearer token_from_file"
                    assert config.username == "testuser"
                    assert config.password == "testpass"
        finally:
            os.unlink(temp_file)

    def test_no_token_config(self):
        """Test loading config with no token configuration"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.token is None

    def test_token_filename_with_bearer_prefix(self):
        """Test that Bearer prefix is not added if already present"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("Bearer existing_token")
            temp_file = f.name

        try:
            env_vars = {
                "PINOT_CONTROLLER_URL": "http://controller:9000",
                "PINOT_TOKEN_FILENAME": temp_file,
            }

            with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_pinot_config()
                    assert config.token == "Bearer existing_token"
        finally:
            os.unlink(temp_file)

    def test_token_filename_field_present(self):
        """Test that token_filename environment variable is processed correctly"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_TOKEN_FILENAME": "/some/file/path",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                # Token should be None since the file doesn't exist
                assert config.token is None


class TestParseTableFilterConfig:
    """Test the _parse_table_filter_config function"""

    def test_valid_yaml_with_tables(self):
        """Test parsing valid YAML with table list"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".yaml"
        ) as f:
            f.write("included_tables:\n  - table1\n  - table2\n  - table3")
            temp_file = f.name

        try:
            config = _parse_table_filter_config(temp_file)
            assert config is not None
            assert "included_tables" in config
            assert config["included_tables"] == ["table1", "table2", "table3"]
        finally:
            os.unlink(temp_file)


class TestLoadTableFilters:
    """Test the _load_table_filters function"""

    def test_none_path_returns_none(self):
        """Test that None path returns None"""
        result = _load_table_filters(None)
        assert result is None

    def test_empty_table_list_returns_none(self):
        """Test that empty table list returns None"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".yaml"
        ) as f:
            f.write("included_tables: []")
            temp_file = f.name

        try:
            result = _load_table_filters(temp_file)
            assert result is None
        finally:
            os.unlink(temp_file)

    def test_valid_table_list_returns_list(self):
        """Test that valid table list is returned"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".yaml"
        ) as f:
            f.write("included_tables:\n  - table1\n  - table2\n  - table3")
            temp_file = f.name

        try:
            result = _load_table_filters(temp_file)
            assert result == ["table1", "table2", "table3"]
        finally:
            os.unlink(temp_file)

    def test_nonexistent_file_raises_exception(self):
        """Test that nonexistent filter file raises FileNotFoundError"""
        nonexistent_path = "/path/to/nonexistent/filter.yaml"
        with pytest.raises(
            FileNotFoundError, match="Table filter file not found.*filter.yaml"
        ):
            _load_table_filters(nonexistent_path)


class TestLoadPinotConfigWithTableFilters:
    """Test table filter integration with load_pinot_config"""

    def test_no_filter_file_configured(self):
        """Test that config loads without filter file"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
        }

        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                config = load_pinot_config()
                assert config.included_tables is None

    def test_filter_file_with_tables(self):
        """Test that table filters are loaded from file"""
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".yaml"
        ) as f:
            f.write("included_tables:\n  - table1\n  - table2")
            temp_file = f.name

        try:
            env_vars = {
                "PINOT_CONTROLLER_URL": "http://controller:9000",
                "PINOT_TABLE_FILTER_FILE": temp_file,
            }

            with patch("mcp_pinot.config.load_dotenv"):
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_pinot_config()
                    assert config.included_tables == ["table1", "table2"]
        finally:
            os.unlink(temp_file)

    def test_nonexistent_filter_file_raises_exception(self):
        """Test that nonexistent filter file raises FileNotFoundError"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_TABLE_FILTER_FILE": "/path/to/nonexistent/filter.yaml",
        }

        with patch("mcp_pinot.config.load_dotenv"):
            with patch.dict(os.environ, env_vars, clear=True):
                with pytest.raises(
                    FileNotFoundError,
                    match="Table filter file not found.*nonexistent/filter.yaml",
                ):
                    load_pinot_config()
