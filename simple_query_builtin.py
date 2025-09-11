#!/usr/bin/env python3
"""
Simple query script using only built-in Python modules
No external dependencies required!
"""

import json
import urllib.parse
import urllib.request


def query_mcp_server():
    """Query the MCP server using built-in urllib"""

    print("🔍 Querying MCP Pinot Server (Built-in Python)")
    print("=" * 50)

    base_url = "http://127.0.0.1:8080"

    # Test 1: List available tools
    print("1️⃣  Listing available tools...")
    try:
        with urllib.request.urlopen(f"{base_url}/api/tools/list") as response:
            data = json.loads(response.read().decode())
            print("✅ Available tools:")
            for tool in data.get("tools", []):
                print(f"   • {tool['name']}: {tool['description']}")
    except Exception as e:
        print(f"❌ Error: {e}")

    print()

    # Test 2: List all tables
    print("2️⃣  Listing all Pinot tables...")
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
            print(f"✅ Found {len(tables)} tables:")
            for i, table in enumerate(tables, 1):
                print(f"   {i:2d}. {table}")

    except Exception as e:
        print(f"❌ Error: {e}")

    print()

    # Test 3: Test connection
    print("3️⃣  Testing Pinot connection...")
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
            print(f"✅ Connection test: {conn_result.get('connection_test', False)}")
            print(f"✅ Query test: {conn_result.get('query_test', False)}")
            print(f"✅ Tables count: {conn_result.get('tables_count', 0)}")

    except Exception as e:
        print(f"❌ Error: {e}")

    print()

    # Test 4: Count records in airlineStats
    print("4️⃣  Counting records in airlineStats...")
    try:
        request_data = {
            "name": "read-query",
            "arguments": {
                "query": "SELECT COUNT(*) as total_records FROM airlineStats"
            },
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
            if query_result and len(query_result) > 0:
                count = query_result[0].get("total_records", 0)
                print(f"✅ airlineStats has {count:,} records")
            else:
                print("⚠️  No results returned")

    except Exception as e:
        print(f"❌ Error: {e}")

    print()

    # Test 5: Sample data from githubEvents
    print("5️⃣  Getting sample data from githubEvents...")
    try:
        request_data = {
            "name": "read-query",
            "arguments": {
                "query": "SELECT id, type, created_at FROM githubEvents LIMIT 3"
            },
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
            if query_result:
                print("✅ Sample GitHub events:")
                for i, event in enumerate(query_result[:3], 1):
                    event_id = event.get("id")
                    event_type = event.get("type")
                    event_date = event.get("created_at")
                    print(
                        f"   {i}. ID: {event_id}, Type: {event_type}, "
                        f"Date: {event_date}"
                    )
            else:
                print("⚠️  No results returned")

    except Exception as e:
        print(f"❌ Error: {e}")

    print()
    print("🎉 Query testing completed!")
    print("=" * 50)
    print()
    print("💡 Usage Summary:")
    print("• Server is running at: http://127.0.0.1:8080")
    print("• REST API endpoints:")
    print("  - GET  /api/tools/list")
    print("  - POST /api/tools/call")
    print("• All queries work with built-in Python!")


if __name__ == "__main__":
    query_mcp_server()
