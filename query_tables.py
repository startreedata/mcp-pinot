#!/usr/bin/env python3
"""
Simple script to query MCP Pinot Server for tables via HTTP
This shows the exact steps a user needs to follow
"""

import json
import re

import requests


def query_mcp_server_for_tables():
    """Query the MCP server for all existing tables"""

    print("🔍 How to Query MCP Server for Tables via HTTP")
    print("=" * 50)

    # Step 1: Get session ID from SSE endpoint
    print("Step 1: Getting session ID...")

    try:
        response = requests.get("http://127.0.0.1:8080/sse", stream=True, timeout=5)

        if response.status_code == 200:
            # Read the first few lines to get session ID
            session_id = None
            for line in response.iter_lines(decode_unicode=True):
                if line and line.startswith("data: "):
                    data = line[6:]  # Remove "data: " prefix
                    if "session_id=" in data:
                        match = re.search(r"session_id=([a-f0-9]+)", data)
                        if match:
                            session_id = match.group(1)
                            print(f"✅ Session ID: {session_id}")
                            break
                # Only read first few lines
                if session_id:
                    break

            # Close the streaming connection
            response.close()

            if not session_id:
                print("❌ Could not extract session ID")
                return

        else:
            print(f"❌ Failed to connect to SSE endpoint: {response.status_code}")
            return

    except Exception as e:
        print(f"❌ Error connecting to SSE endpoint: {e}")
        return

    # Step 2: Send MCP request to list tables
    print("\nStep 2: Querying for tables...")

    # MCP JSON-RPC request to list tables
    mcp_request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "list-tables", "arguments": {}},
    }

    try:
        # Send POST request with session ID
        response = requests.post(
            f"http://127.0.0.1:8080/sse?session_id={session_id}",
            json=mcp_request,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

        print(f"Response Status: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        print(f"Response Body: {response.text}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Tables query successful!")
            print(json.dumps(result, indent=2))
        else:
            print(f"❌ Request failed with status {response.status_code}")

    except Exception as e:
        print(f"❌ Error sending MCP request: {e}")

    print("\n" + "=" * 50)
    print("📋 Summary for Users:")
    print("1. GET http://127.0.0.1:8080/sse to get session ID")
    print("2. POST http://127.0.0.1:8080/sse?session_id=<ID> with MCP JSON-RPC")
    print("3. Use 'list-tables' tool to get all tables")


if __name__ == "__main__":
    query_mcp_server_for_tables()
