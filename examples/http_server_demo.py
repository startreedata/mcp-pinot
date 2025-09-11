#!/usr/bin/env python3
"""
HTTP Server Demo for MCP Pinot Server

This script demonstrates how to run the MCP Pinot Server with HTTP transport.
It shows both HTTP and HTTPS configurations.
"""

import asyncio
import os
from pathlib import Path
import sys

# Add the parent directory to the Python path so we can import mcp_pinot
sys.path.insert(0, str(Path(__file__).parent.parent))

from mcp_pinot.server import main


async def demo_http_server():
    """Demo running the server with HTTP transport"""
    print("🚀 Starting MCP Pinot Server HTTP Demo")
    print("=" * 50)

    # Configure for HTTP transport
    os.environ["MCP_TRANSPORT"] = "http"
    os.environ["MCP_HOST"] = "127.0.0.1"
    os.environ["MCP_PORT"] = "8080"
    os.environ["MCP_ENDPOINT"] = "/sse"

    # Configure Pinot connection (using defaults for quickstart)
    os.environ["PINOT_CONTROLLER_URL"] = "http://localhost:9000"
    os.environ["PINOT_BROKER_URL"] = "http://localhost:8000"

    print("Configuration:")
    print(f"  Transport: {os.environ['MCP_TRANSPORT']}")
    print(f"  Host: {os.environ['MCP_HOST']}")
    print(f"  Port: {os.environ['MCP_PORT']}")
    print(f"  SSE Endpoint: {os.environ['MCP_ENDPOINT']}")
    print(f"  Pinot Controller: {os.environ['PINOT_CONTROLLER_URL']}")
    print(f"  Pinot Broker: {os.environ['PINOT_BROKER_URL']}")
    print()

    print("📡 Server will be available at:")
    print(
        f"  HTTP: http://{os.environ['MCP_HOST']}:{os.environ['MCP_PORT']}{os.environ['MCP_ENDPOINT']}"
    )
    print()

    print("🔍 To test the server:")
    print("  # Health check (should return 404 - expected)")
    print(f"  curl -i http://{os.environ['MCP_HOST']}:{os.environ['MCP_PORT']}/health")
    print()
    print("  # SSE endpoint (for MCP clients)")
    print(
        f"  curl -i http://{os.environ['MCP_HOST']}:{os.environ['MCP_PORT']}{os.environ['MCP_ENDPOINT']}"
    )
    print()

    print("🛑 Press Ctrl+C to stop the server")
    print("=" * 50)

    try:
        await main()
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"\n❌ Server error: {e}")


async def demo_https_server():
    """Demo running the server with HTTPS transport"""
    print("🔒 Starting MCP Pinot Server HTTPS Demo")
    print("=" * 50)

    # Configure for HTTPS transport
    os.environ["MCP_TRANSPORT"] = "http"
    os.environ["MCP_HOST"] = "0.0.0.0"
    os.environ["MCP_PORT"] = "8443"
    os.environ["MCP_ENDPOINT"] = "/sse"

    # SSL configuration (you would need real certificates)
    # os.environ["MCP_SSL_KEYFILE"] = "/path/to/server.key"
    # os.environ["MCP_SSL_CERTFILE"] = "/path/to/server.crt"

    # Configure Pinot connection
    os.environ["PINOT_CONTROLLER_URL"] = "http://localhost:9000"
    os.environ["PINOT_BROKER_URL"] = "http://localhost:8000"

    print("Configuration:")
    print(f"  Transport: {os.environ['MCP_TRANSPORT']}")
    print(f"  Host: {os.environ['MCP_HOST']}")
    print(f"  Port: {os.environ['MCP_PORT']}")
    print(f"  SSE Endpoint: {os.environ['MCP_ENDPOINT']}")

    if "MCP_SSL_KEYFILE" in os.environ:
        print(f"  SSL Key: {os.environ['MCP_SSL_KEYFILE']}")
        print(f"  SSL Cert: {os.environ['MCP_SSL_CERTFILE']}")
        protocol = "https"
    else:
        print("  SSL: Not configured (running HTTP)")
        protocol = "http"

    print()
    print("📡 Server will be available at:")
    print(
        f"  {protocol.upper()}: {protocol}://{os.environ['MCP_HOST']}:{os.environ['MCP_PORT']}{os.environ['MCP_ENDPOINT']}"
    )
    print()

    print("🛑 Press Ctrl+C to stop the server")
    print("=" * 50)

    try:
        await main()
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"\n❌ Server error: {e}")


async def demo_stdio_server():
    """Demo running the server with STDIO transport (original)"""
    print("📺 Starting MCP Pinot Server STDIO Demo")
    print("=" * 50)

    # Configure for STDIO transport (default)
    os.environ["MCP_TRANSPORT"] = "stdio"

    # Configure Pinot connection
    os.environ["PINOT_CONTROLLER_URL"] = "http://localhost:9000"
    os.environ["PINOT_BROKER_URL"] = "http://localhost:8000"

    print("Configuration:")
    print(f"  Transport: {os.environ['MCP_TRANSPORT']}")
    print(f"  Pinot Controller: {os.environ['PINOT_CONTROLLER_URL']}")
    print(f"  Pinot Broker: {os.environ['PINOT_BROKER_URL']}")
    print()

    print("📡 Server running with STDIO transport")
    print("  This mode is designed for use with Claude Desktop or other MCP clients")
    print("  that communicate via standard input/output.")
    print()

    print("🛑 Press Ctrl+C to stop the server")
    print("=" * 50)

    try:
        await main()
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"\n❌ Server error: {e}")


async def demo_both_transports():
    """Demo running the server with both STDIO and HTTP transports simultaneously"""
    print("🚀📺 Starting MCP Pinot Server DUAL TRANSPORT Demo")
    print("=" * 60)

    # Configure for both transports (default)
    os.environ["MCP_TRANSPORT"] = "both"
    os.environ["MCP_HOST"] = "127.0.0.1"
    os.environ["MCP_PORT"] = "8080"
    os.environ["MCP_ENDPOINT"] = "/sse"

    # Configure Pinot connection
    os.environ["PINOT_CONTROLLER_URL"] = "http://localhost:9000"
    os.environ["PINOT_BROKER_URL"] = "http://localhost:8000"

    print("Configuration:")
    print(f"  Transport: {os.environ['MCP_TRANSPORT']} (STDIO + HTTP)")
    print(f"  HTTP Host: {os.environ['MCP_HOST']}")
    print(f"  HTTP Port: {os.environ['MCP_PORT']}")
    print(f"  SSE Endpoint: {os.environ['MCP_ENDPOINT']}")
    print(f"  Pinot Controller: {os.environ['PINOT_CONTROLLER_URL']}")
    print(f"  Pinot Broker: {os.environ['PINOT_BROKER_URL']}")
    print()

    print("📡 Server will be available via:")
    print("  📺 STDIO: For Claude Desktop and other MCP clients")
    print(
        f"  🌐 HTTP: http://{os.environ['MCP_HOST']}:{os.environ['MCP_PORT']}{os.environ['MCP_ENDPOINT']}"
    )
    print()

    print("🔍 To test the HTTP endpoint:")
    print(
        f"  curl -i http://{os.environ['MCP_HOST']}:{os.environ['MCP_PORT']}{os.environ['MCP_ENDPOINT']}"
    )
    print()

    print("✨ Benefits of dual transport:")
    print("  • STDIO: Perfect for Claude Desktop integration")
    print("  • HTTP: Enables web clients, multiple connections, Kubernetes deployment")
    print("  • Both run simultaneously - no need to choose!")
    print()

    print("🛑 Press Ctrl+C to stop both transports")
    print("=" * 60)

    try:
        await main()
    except KeyboardInterrupt:
        print("\n👋 Both transports stopped by user")
    except Exception as e:
        print(f"\n❌ Server error: {e}")


def main_demo():
    """Main demo function with transport selection"""
    if len(sys.argv) < 2:
        print("MCP Pinot Server Transport Demo")
        print("=" * 40)
        print()
        print("Usage:")
        print("  python examples/http_server_demo.py <transport>")
        print()
        print("Available transports:")
        print("  both    - Both STDIO and HTTP simultaneously (default)")
        print("  http    - HTTP server only (port 8080)")
        print("  https   - HTTPS server only (port 8443, requires certificates)")
        print("  stdio   - STDIO transport only")
        print()
        print("Examples:")
        print("  python examples/http_server_demo.py both")
        print("  python examples/http_server_demo.py http")
        print("  python examples/http_server_demo.py https")
        print("  python examples/http_server_demo.py stdio")
        print()
        return

    transport = sys.argv[1].lower()

    if transport == "both":
        asyncio.run(demo_both_transports())
    elif transport == "http":
        asyncio.run(demo_http_server())
    elif transport == "https":
        asyncio.run(demo_https_server())
    elif transport == "stdio":
        asyncio.run(demo_stdio_server())
    else:
        print(f"❌ Unknown transport: {transport}")
        print("Available transports: both, http, https, stdio")
        sys.exit(1)


if __name__ == "__main__":
    main_demo()
