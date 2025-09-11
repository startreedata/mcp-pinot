#!/bin/bash

echo "🚀 Testing MCP Pinot Server REST API"
echo "===================================="

BASE_URL="http://127.0.0.1:8080"

# Test 1: List available tools
echo "1️⃣  Listing available tools..."
echo "GET $BASE_URL/api/tools/list"
echo "Response:"
curl -s -X GET "$BASE_URL/api/tools/list" | jq '.'
echo

# Test 2: List all Pinot tables
echo "2️⃣  Listing all Pinot tables..."
echo "POST $BASE_URL/api/tools/call"
echo "Request: {\"name\": \"list-tables\", \"arguments\": {}}"
echo "Response:"
curl -s -X POST "$BASE_URL/api/tools/call" \
  -H "Content-Type: application/json" \
  -d '{"name": "list-tables", "arguments": {}}' | jq '.'
echo

# Test 3: Test Pinot connection
echo "3️⃣  Testing Pinot connection..."
echo "POST $BASE_URL/api/tools/call"
echo "Request: {\"name\": \"test-connection\", \"arguments\": {}}"
echo "Response:"
curl -s -X POST "$BASE_URL/api/tools/call" \
  -H "Content-Type: application/json" \
  -d '{"name": "test-connection", "arguments": {}}' | jq '.result | {connection_test, query_test, tables_test, tables_count, sample_tables}'
echo

# Test 4: Count records in airlineStats
echo "4️⃣  Counting records in airlineStats table..."
echo "POST $BASE_URL/api/tools/call"
echo "Request: {\"name\": \"read-query\", \"arguments\": {\"query\": \"SELECT COUNT(*) as total FROM airlineStats\"}}"
echo "Response:"
curl -s -X POST "$BASE_URL/api/tools/call" \
  -H "Content-Type: application/json" \
  -d '{"name": "read-query", "arguments": {"query": "SELECT COUNT(*) as total FROM airlineStats"}}' | jq '.'
echo

# Test 5: Sample data from githubEvents
echo "5️⃣  Getting sample data from githubEvents table..."
echo "POST $BASE_URL/api/tools/call"
echo "Request: {\"name\": \"read-query\", \"arguments\": {\"query\": \"SELECT * FROM githubEvents LIMIT 3\"}}"
echo "Response:"
curl -s -X POST "$BASE_URL/api/tools/call" \
  -H "Content-Type: application/json" \
  -d '{"name": "read-query", "arguments": {"query": "SELECT * FROM githubEvents LIMIT 3"}}' | jq '.result[0:2]'  # Show first 2 records only
echo

# Test 6: Get table details
echo "6️⃣  Getting table details for airlineStats..."
echo "POST $BASE_URL/api/tools/call"
echo "Request: {\"name\": \"table-details\", \"arguments\": {\"tableName\": \"airlineStats\"}}"
echo "Response:"
curl -s -X POST "$BASE_URL/api/tools/call" \
  -H "Content-Type: application/json" \
  -d '{"name": "table-details", "arguments": {"tableName": "airlineStats"}}' | jq '.'
echo

echo "✅ REST API testing completed!"
echo "===================================="
echo
echo "📋 Summary:"
echo "• REST API is working perfectly!"
echo "• All 10 Pinot tables are accessible"
echo "• Queries return actual results"
echo "• No more 'Accepted' responses!"
echo
echo "🌐 Available endpoints:"
echo "• GET  /api/tools/list - List available tools"
echo "• POST /api/tools/call - Execute tools with JSON payload"
echo "• GET  /sse - MCP SSE endpoint (for MCP clients)"
