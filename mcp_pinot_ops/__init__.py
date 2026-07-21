"""Legacy pre-FastMCP prototype; not part of the published package.

The supported implementation is :mod:`mcp_pinot`. See ``mcp_pinot_ops/README.md``
before reading or modifying this historical code.
"""

import asyncio

from . import server


def main():
    """Main entry point for the package."""
    asyncio.run(server.main())


# Optionally expose other important items at package level
__all__ = ["main", "server"]
