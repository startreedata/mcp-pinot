#!/bin/bash

echo "🔍 Testing MCP Pinot Server HTTP API"
echo "=================================="

# Step 1: Get session ID
echo "Step 1: Getting session ID..."
SESSION_RESPONSE=$(timeout 3 curl -s -N http://127.0.0.1:8080/sse 2>/dev/null | head -5)
SESSION_ID=$(echo "$SESSION_RESPONSE" | grep -o 'session_id=[a-f0-9]*' | head -1 | cut -d'=' -f2)

if [ -z "$SESSION_ID" ]; then
    echo "❌ Could not get session ID. Is the server running?"
    echo "Expected server at: http://127.0.0.1:8080/sse"
    exit 1
fi

echo "✅ Session ID: $SESSION_ID"

# Step 2: List all tables
echo -e "\nStep 2: Listing all Pinot tables..."
TABLES_RESPONSE=$(curl -s -X POST "http://127.0.0.1:8080/sse?session_id=$SESSION_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/call",
    "params": {
      "name": "list-tables",
      "arguments": {}
    }
  }')

echo "Tables Response: $TABLES_RESPONSE"

# Step 3: Test connection
echo -e "\nStep 3: Testing Pinot connection..."
CONNECTION_RESPONSE=$(curl -s -X POST "http://127.0.0.1:8080/sse?session_id=$SESSION_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/call",
    "params": {
      "name": "test-connection",
      "arguments": {}
    }
  }')

echo "Connection Response: $CONNECTION_RESPONSE"

# Step 4: Sample query
echo -e "\nStep 4: Running sample query..."
QUERY_RESPONSE=$(curl -s -X POST "http://127.0.0.1:8080/sse?session_id=$SESSION_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "read-query",
      "arguments": {
        "query": "SELECT COUNT(*) as record_count FROM airlineStats LIMIT 1"
      }
    }
  }')

echo "Query Response: $QUERY_RESPONSE"

echo -e "\n🎉 HTTP API testing completed!"
echo "=================================="
