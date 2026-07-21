from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("mcp-pinot-server")
except PackageNotFoundError:  # pragma: no cover - source tree without installation
    __version__ = "4.0.0"


def main():
    """Main entry point for the package."""
    from .server import main as run_server

    run_server()


# Optionally expose other important items at package level
__all__ = ["__version__", "main"]
