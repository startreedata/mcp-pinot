# 🚀 MCP Pinot Server - User Guide

Your MCP Pinot Server is running successfully with **dual transport** support!

## 🌐 Server Status

- **✅ STDIO Transport**: Running (for Claude Desktop)
- **✅ HTTP Transport**: Running at `http://127.0.0.1:8080`
- **✅ REST API**: Available at `/api/tools/*` endpoints
- **✅ Pinot Connection**: Connected to local quickstart
- **✅ Tables Found**: 10 tables with sample data

## 📊 Your Pinot Tables

1. **airlineStats** - 1,824+ records
2. **dailySales** - Sales data
3. **fineFoodReviews** - Food review data  
4. **githubEvents** - GitHub events data
5. **meetupRsvp** - Meetup RSVP data
6. **meetupRsvpComplexType** - Complex meetup data
7. **meetupRsvpJson** - JSON meetup data
8. **upsertJsonMeetupRsvp** - Upsert JSON data
9. **upsertMeetupRsvp** - Upsert meetup data
10. **upsertPartialMeetupRsvp** - Partial upsert data

## 🔧 How to Query Tables

### Method 1: Python Script (No Dependencies)
```bash
python3 simple_query_builtin.py
```

### Method 2: Direct curl Commands

#### List All Tables
```bash
curl -X POST http://127.0.0.1:8080/api/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name": "list-tables", "arguments": {}}'
```

#### Count Records in a Table
```bash
curl -X POST http://127.0.0.1:8080/api/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name": "read-query", "arguments": {"query": "SELECT COUNT(*) as total FROM airlineStats"}}'
```

#### Get Sample Data
```bash
curl -X POST http://127.0.0.1:8080/api/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name": "read-query", "arguments": {"query": "SELECT * FROM githubEvents LIMIT 5"}}'
```

#### Test Connection
```bash
curl -X POST http://127.0.0.1:8080/api/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name": "test-connection", "arguments": {}}'
```

#### Get Table Details
```bash
curl -X POST http://127.0.0.1:8080/api/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name": "table-details", "arguments": {"tableName": "airlineStats"}}'
```

### Method 3: Comprehensive Test Script
```bash
./test_rest_api.sh
```

## 🛠 Available Tools

| Tool | Description | Example |
|------|-------------|---------|
| `list-tables` | List all tables | `{"name": "list-tables", "arguments": {}}` |
| `read-query` | Execute SQL SELECT | `{"name": "read-query", "arguments": {"query": "SELECT * FROM table LIMIT 5"}}` |
| `test-connection` | Test Pinot connection | `{"name": "test-connection", "arguments": {}}` |
| `table-details` | Get table size info | `{"name": "table-details", "arguments": {"tableName": "airlineStats"}}` |

## 📈 Sample Queries You Can Try

### Basic Queries
```sql
-- Count all records
SELECT COUNT(*) FROM airlineStats

-- Get recent GitHub events
SELECT id, type, created_at FROM githubEvents ORDER BY created_at DESC LIMIT 10

-- Analyze meetup data
SELECT COUNT(*) as total_rsvps FROM meetupRsvp

-- Sample airline data
SELECT * FROM airlineStats LIMIT 5
```

### Advanced Queries
```sql
-- Group by event type
SELECT type, COUNT(*) as count FROM githubEvents GROUP BY type ORDER BY count DESC LIMIT 5

-- Date-based analysis (if date columns exist)
SELECT DATE_TRUNC('day', created_at) as day, COUNT(*) 
FROM githubEvents 
GROUP BY DATE_TRUNC('day', created_at) 
ORDER BY day DESC LIMIT 7
```

## 🌐 API Endpoints

- **GET** `/api/tools/list` - List available tools
- **POST** `/api/tools/call` - Execute tool calls
- **GET** `/sse` - MCP SSE endpoint (for MCP clients)

## 🎯 Quick Examples

### Count Records in All Tables
```bash
for table in airlineStats githubEvents meetupRsvp dailySales; do
  echo -n "$table: "
  curl -s -X POST http://127.0.0.1:8080/api/tools/call \
    -H "Content-Type: application/json" \
    -d "{\"name\": \"read-query\", \"arguments\": {\"query\": \"SELECT COUNT(*) as count FROM $table\"}}" \
    | grep -o '"count":[0-9]*' | cut -d':' -f2
done
```

### Get Schema Information
```bash
# List all available tools
curl -s http://127.0.0.1:8080/api/tools/list | jq '.tools[].name'

# Test if server is responsive
curl -s -X POST http://127.0.0.1:8080/api/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name": "test-connection", "arguments": {}}' | jq '.result.connection_test'
```

## 🚀 Integration Options

### For Claude Desktop
- Use the STDIO transport (already running)
- Configure in Claude Desktop settings

### For Web Applications
- Use the REST API endpoints
- Base URL: `http://127.0.0.1:8080`
- JSON request/response format

### For Kubernetes
- Use the provided k8s manifests in `k8s/` directory
- Supports HTTPS with Ingress

## 🔍 Troubleshooting

### Server Not Responding
```bash
# Check if server is running
curl -s http://127.0.0.1:8080/api/tools/list

# Restart server if needed
uv run python examples/http_server_demo.py both
```

### Query Errors
- Only SELECT queries are allowed for security
- Table names are case-sensitive
- Use proper SQL syntax for Pinot

### Connection Issues
- Ensure Pinot quickstart is running (ports 8000, 9000)
- Check if tables are loaded: `curl -s http://localhost:9000/tables`

## 🎉 Success!

Your MCP Pinot Server is working perfectly with:
- ✅ 10 tables loaded and queryable
- ✅ REST API returning actual results
- ✅ Dual transport (STDIO + HTTP) 
- ✅ Production-ready for Kubernetes

You can now query your Pinot data using simple HTTP requests! 🚀