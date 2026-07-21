from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("mcp-pinot-server")
except PackageNotFoundError:  # pragma: no cover - source tree without installation
    # Source-only checkouts have no distribution metadata. Avoid duplicating the
    # release version here; installed wheels and MCP bundles always use metadata.
    __version__ = "0+unknown"


def main():
    """Main entry point for the package."""
    from .server import main as run_server

    run_server()


# Optionally expose other important items at package level
__all__ = ["__version__", "main"]
