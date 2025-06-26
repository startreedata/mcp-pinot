import os
from unittest.mock import patch

from mcp_pinot.config import _parse_broker_url, load_pinot_config


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
        with patch("mcp_pinot.config.logging.warning") as mock_warning:
            host, port, scheme = _parse_broker_url("invalid-url")
            assert host == "localhost"
            assert port == 80
            assert scheme == "http"
            mock_warning.assert_called_once()

    def test_parse_url_with_path(self):
        """Test parsing URL with path ignores the path"""
        host, port, scheme = _parse_broker_url("https://broker.example.com:8443/some/path")
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
            with patch("mcp_pinot.config.logging.warning") as mock_warning:
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_pinot_config()
                    assert config.broker_host == "override.example.com"
                    assert config.broker_port == 9000
                    assert config.broker_scheme == "https"  # From URL, not overridden

                    # Should warn about overrides
                    assert mock_warning.call_count == 2
                    warning_calls = [
                        call.args[0] for call in mock_warning.call_args_list
                    ]
                    assert any("PINOT_BROKER_HOST" in call for call in warning_calls)
                    assert any("PINOT_BROKER_PORT" in call for call in warning_calls)

    def test_broker_url_with_scheme_override(self):
        """Test that PINOT_BROKER_SCHEME overrides URL scheme"""
        env_vars = {
            "PINOT_CONTROLLER_URL": "http://controller:9000",
            "PINOT_BROKER_URL": "https://broker.example.com:8443",
            "PINOT_BROKER_SCHEME": "http",
        }

        with patch("mcp_pinot.config.load_dotenv"):  # Disable .env loading
            with patch("mcp_pinot.config.logging.warning") as mock_warning:
                with patch.dict(os.environ, env_vars, clear=True):
                    config = load_pinot_config()
                    assert config.broker_host == "broker.example.com"
                    assert config.broker_port == 8443
                    assert config.broker_scheme == "http"  # Overridden

                    # Should warn about scheme override
                    mock_warning.assert_called_once()
                    assert "PINOT_BROKER_SCHEME" in mock_warning.call_args[0][0]

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
                assert config.password == "testpass"
                assert config.token == "testtoken"
                assert config.database == "testdb"
                assert config.use_msqe is True
                assert config.request_timeout == 30
                assert config.connection_timeout == 20
                assert config.query_timeout == 40
