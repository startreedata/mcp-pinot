#!/bin/bash

echo "🔍 Simple MCP Pinot Server Query Script"
echo "======================================"

BASE_URL="http://127.0.0.1:8080"

# Check if server is running
echo "🔌 Checking if server is running..."
if curl -s "$BASE_URL/api/tools/list" > /dev/null 2>&1; then
    echo "✅ Server is running!"
else
    echo "❌ Server is not running at $BASE_URL"
    echo "   Please start it with: uv run python examples/http_server_demo.py both"
    exit 1
fi

echo

# List all tables
echo "📋 Listing all Pinot tables..."
TABLES_JSON=$(curl -s -X POST "$BASE_URL/api/tools/call" \
  -H "Content-Type: application/json" \
  -d '{"name": "list-tables", "arguments": {}}')

# Extract table names (simple parsing)
TABLES=$(echo "$TABLES_JSON" | grep -o '"[^"]*"' | grep -v '"result"' | grep -v '"name"' | grep -v '"arguments"' | sed 's/"//g')

echo "✅ Found tables:"
i=1
for table in $TABLES; do
    if [[ $table != "result" && $table != "name" && $table != "arguments" && $table != "list-tables" ]]; then
        echo "   $i. $table"
        ((i++))
    fi
done

echo

# Count records in first few tables
echo "🔢 Counting records in sample tables..."

for table in airlineStats githubEvents meetupRsvp; do
    echo -n "   $table: "
    RESULT=$(curl -s -X POST "$BASE_URL/api/tools/call" \
      -H "Content-Type: application/json" \
      -d "{\"name\": \"read-query\", \"arguments\": {\"query\": \"SELECT COUNT(*) as count FROM $table\"}}")
    
    # Simple extraction of count (works for most cases)
    COUNT=$(echo "$RESULT" | grep -o '"count":[0-9]*' | grep -o '[0-9]*')
    if [[ -n "$COUNT" ]]; then
        echo "$COUNT records"
    else
        echo "Error querying"
    fi
done

echo

# Sample data from githubEvents
echo "📊 Sample data from githubEvents table..."
SAMPLE_RESULT=$(curl -s -X POST "$BASE_URL/api/tools/call" \
  -H "Content-Type: application/json" \
  -d '{"name": "read-query", "arguments": {"query": "SELECT id, type FROM githubEvents LIMIT 3"}}')

echo "✅ Sample GitHub events:"
echo "$SAMPLE_RESULT" | grep -o '"id":"[^"]*"' | head -3 | sed 's/"id":"//g' | sed 's/"//g' | nl -w2 -s'. ID: '

echo

# Test connection
echo "🔗 Testing Pinot connection..."
CONNECTION_RESULT=$(curl -s -X POST "$BASE_URL/api/tools/call" \
  -H "Content-Type: application/json" \
  -d '{"name": "test-connection", "arguments": {}}')

if echo "$CONNECTION_RESULT" | grep -q '"connection_test":true'; then
    echo "✅ Connection test: PASSED"
else
    echo "❌ Connection test: FAILED"
fi

echo

echo "🎉 Query testing completed!"
echo "======================================"
echo
echo "💡 Quick Reference:"
echo "• List tables:    curl -X POST $BASE_URL/api/tools/call -H 'Content-Type: application/json' -d '{\"name\": \"list-tables\", \"arguments\": {}}'"
echo "• Run query:      curl -X POST $BASE_URL/api/tools/call -H 'Content-Type: application/json' -d '{\"name\": \"read-query\", \"arguments\": {\"query\": \"SELECT * FROM tablename LIMIT 5\"}}'"
echo "• Test connection: curl -X POST $BASE_URL/api/tools/call -H 'Content-Type: application/json' -d '{\"name\": \"test-connection\", \"arguments\": {}}'"
