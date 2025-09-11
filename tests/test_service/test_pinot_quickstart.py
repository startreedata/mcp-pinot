import asyncio
import json
import time
import urllib.parse
import urllib.request

import requests

from mcp_pinot.config import load_pinot_config
from mcp_pinot.pinot_client import PinotClient


def wait_for_pinot_ready():
    """Wait for Pinot Controller to be ready"""
    max_retries = 30
    retry_interval = 2

    for i in range(max_retries):
        try:
            response = requests.get("http://localhost:9000/health", timeout=10)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(retry_interval)
    return False


def wait_for_mcp_server_ready():
    """Wait for MCP HTTP server to be ready"""
    max_retries = 15
    retry_interval = 1

    for i in range(max_retries):
        try:
            with urllib.request.urlopen(
                "http://127.0.0.1:8080/api/tools/list"
            ) as response:
                if response.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(retry_interval)
    return False


def test_mcp_http_endpoints():
    """Test MCP server HTTP endpoints (only if server is running)"""
    print("🌐 Testing MCP HTTP endpoints...")

    base_url = "http://127.0.0.1:8080"

    # Check if MCP server is running first
    if not wait_for_mcp_server_ready():
        print("⚠️  MCP HTTP server not running - skipping HTTP endpoint tests")
        print("   (This is expected if server is not running in HTTP mode)")
        return

    # Test 1: List available tools
    print("  📋 Testing tools list endpoint...")
    try:
        with urllib.request.urlopen(f"{base_url}/api/tools/list") as response:
            data = json.loads(response.read().decode())
            tools = data.get("tools", [])
            assert len(tools) >= 4, f"Expected at least 4 tools, got {len(tools)}"
            tool_names = [tool["name"] for tool in tools]
            assert "list-tables" in tool_names, "list-tables tool not found"
            assert "read-query" in tool_names, "read-query tool not found"
            print(f"    ✅ Found {len(tools)} tools")
    except Exception as e:
        print(f"    ❌ Tools list test failed: {e}")
        raise

    # Test 2: List tables via HTTP API
    print("  📊 Testing list-tables via HTTP...")
    try:
        request_data = {"name": "list-tables", "arguments": {}}
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            f"{base_url}/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            tables = result.get("result", [])
            assert len(tables) >= 5, f"Expected at least 5 tables, got {len(tables)}"
            assert "airlineStats" in tables, "airlineStats table not found"
            print(f"    ✅ Found {len(tables)} tables via HTTP")
    except Exception as e:
        print(f"    ❌ List tables test failed: {e}")
        raise

    # Test 3: Execute query via HTTP API
    print("  🔍 Testing query execution via HTTP...")
    try:
        request_data = {
            "name": "read-query",
            "arguments": {"query": "SELECT COUNT(*) as total FROM airlineStats"},
        }
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            f"{base_url}/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            query_result = result.get("result", [])
            assert len(query_result) > 0, "Query returned no results"
            assert "total" in query_result[0], "Query result missing 'total' field"
            count = query_result[0]["total"]
            assert count > 0, f"Expected positive count, got {count}"
            print(f"    ✅ Query returned {count} records")
    except Exception as e:
        print(f"    ❌ Query execution test failed: {e}")
        raise

    # Test 4: Test connection via HTTP API
    print("  🔗 Testing connection via HTTP...")
    try:
        request_data = {"name": "test-connection", "arguments": {}}
        data = json.dumps(request_data).encode("utf-8")
        req = urllib.request.Request(
            f"{base_url}/api/tools/call",
            data=data,
            headers={"Content-Type": "application/json"},
        )

        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            conn_result = result.get("result", {})
            assert conn_result.get("connection_test") is True, "Connection test failed"
            assert conn_result.get("query_test") is True, "Query test failed"
            assert conn_result.get("tables_test") is True, "Tables test failed"
            print("    ✅ Connection test passed")
    except Exception as e:
        print(f"    ❌ Connection test failed: {e}")
        raise

    print("🎉 All HTTP endpoint tests passed!")


async def main():
    print("🧪 Testing MCP Pinot Server with Quickstart")
    print("=" * 50)

    # Test 1: Pinot readiness
    print("1️⃣ Checking Pinot readiness...")
    if not wait_for_pinot_ready():
        print("❌ Pinot is not ready after maximum retries")
        return

    print("✅ Pinot Controller is ready")

    # Test 2: Direct Pinot client testing
    print("\n2️⃣ Testing direct Pinot client...")
    try:
        # Initialize Pinot client
        config = load_pinot_config()
        pinot = PinotClient(config)

        # Test connection
        connection_result = pinot.test_connection()
        print(
            "✅ Direct connection test:",
            connection_result.get("connection_test", False),
        )

        # Execute a query
        result = pinot.execute_query("SELECT * FROM airlineStats LIMIT 5")
        print(f"✅ Direct query returned {len(result) if result else 0} records")

        # Verify the result contains our sample data
        if not result or "Carrier" not in str(result):
            print("⚠️  Expected sample column not found in query results")

    except Exception as e:
        print(f"❌ Direct client error: {e}")
        raise

    # Test 3: MCP HTTP server testing
    print("\n3️⃣ Testing MCP HTTP server...")
    if wait_for_mcp_server_ready():
        print("✅ MCP HTTP server is ready")
        try:
            test_mcp_http_endpoints()
        except Exception as e:
            print(f"❌ HTTP endpoint tests failed: {e}")
            raise
    else:
        print("⚠️  MCP HTTP server not ready - skipping HTTP tests")
        print("   (This is expected if server is not running in HTTP mode)")

    print("\n🎉 All tests completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
