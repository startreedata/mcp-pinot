# Changelog

## [Unreleased] - 2025-09-06

### 🚀 Major Features Added

#### HTTP/HTTPS Transport Support
- **Dual Transport Mode**: Server now runs both STDIO and HTTP simultaneously by default
- **SSE Transport**: Full Server-Sent Events support for MCP communication
- **REST API**: Simple HTTP endpoints for direct tool calls
- **SSL/TLS Support**: HTTPS capability with certificate configuration
- **Production Ready**: Kubernetes deployment manifests included

#### Configuration Enhancements
- **New ServerConfig**: Added transport configuration options
- **Environment Variables**: MCP_TRANSPORT, MCP_HOST, MCP_PORT, MCP_SSL_* settings
- **Flexible Transport Selection**: "stdio", "http", or "both" modes
- **Backward Compatible**: Existing STDIO usage unchanged

#### Kubernetes Support
- **Complete K8s Manifests**: Deployment, Service, Ingress, ConfigMap, Secret
- **HTTPS via Ingress**: TLS termination with cert-manager support
- **Scalable Deployment**: Multiple replicas with health checks
- **Production Configuration**: Resource limits, security settings

### 🧪 Testing & Quality

#### Comprehensive Test Suite
- **45+ New Tests**: HTTP transport, configuration, integration tests
- **100% Config Coverage**: All configuration logic tested
- **Backward Compatibility**: All existing tests still pass
- **Mock Testing**: Proper async testing with mocks

#### Test Files Added
- `tests/test_http_transport.py` - HTTP transport functionality
- `tests/test_http_integration.py` - Integration testing
- Enhanced `tests/test_config.py` - Server configuration tests

### 📚 Documentation & Examples

#### User Guides
- `USER_GUIDE.md` - Complete usage documentation
- `k8s/README.md` - Kubernetes deployment guide
- `user_guide.md` - HTTP API usage examples

#### Demo Scripts
- `examples/http_server_demo.py` - Interactive transport demos
- `simple_query_builtin.py` - Dependency-free Python querying
- `test_rest_api.sh` - Comprehensive curl-based testing

### 🔧 Technical Improvements

#### Server Architecture
- **Modular Design**: Separated transport logic from server logic
- **Concurrent Execution**: Both transports run simultaneously
- **Error Handling**: Graceful shutdown and error propagation
- **Logging**: Enhanced logging for transport operations

#### Dependencies
- **Updated uvicorn**: Enhanced to `uvicorn[standard]` for better HTTP support
- **Minimal Dependencies**: No new required dependencies added
- **Optional SSL**: SSL libraries only used when certificates provided

### 🔒 Security
- **DNS rebinding mitigation**: Raised minimum `mcp[cli]` dependency to `>=1.10.0` (fixes HTTP/SSE rebinding protections) and recommend binding HTTP to loopback or enabling TLS when exposed

### 🌐 API Endpoints

#### New REST API
- `GET /api/tools/list` - List available MCP tools
- `POST /api/tools/call` - Execute tool calls directly
- `GET /sse` - MCP SSE endpoint for real-time communication
- `POST /sse` - SSE message handling

#### MCP Tools Available via HTTP
- `test-connection` - Test Pinot connectivity
- `list-tables` - List all Pinot tables
- `read-query` - Execute SELECT queries
- `table-details` - Get table information
- Plus 10 additional tools for schema/config management

### 🎯 Environment Variables

#### New MCP Server Configuration
```bash
MCP_TRANSPORT=both          # Transport mode: stdio/http/both (default: both)
MCP_HOST=0.0.0.0           # HTTP bind host (default: 0.0.0.0)
MCP_PORT=8080              # HTTP port (default: 8080)
MCP_ENDPOINT=/sse          # SSE endpoint path (default: /sse)
MCP_SSL_KEYFILE=           # SSL private key path (optional)
MCP_SSL_CERTFILE=          # SSL certificate path (optional)
```

#### Existing Pinot Configuration (Unchanged)
```bash
PINOT_CONTROLLER_URL=http://localhost:9000
PINOT_BROKER_URL=http://localhost:8000
PINOT_USERNAME=            # Optional
PINOT_PASSWORD=            # Optional
PINOT_TOKEN=               # Optional
```

### 🎉 Benefits

#### For Development
- **Claude Desktop**: Works seamlessly via STDIO
- **Web Testing**: Easy HTTP API testing with curl/Python
- **Local Development**: Both transports available simultaneously

#### For Production
- **Kubernetes Ready**: Complete deployment manifests
- **HTTPS Secure**: Full TLS/SSL support
- **Scalable**: Multiple replicas and load balancing
- **Monitoring**: Health checks and structured logging

#### For Users
- **No Dependencies**: Query with built-in Python or curl
- **Simple API**: REST endpoints for easy integration
- **Flexible**: Choose transport based on use case
- **Reliable**: Comprehensive testing and error handling

### 🔄 Migration Guide

#### Existing Users (No Changes Required)
- Default behavior now includes HTTP transport alongside STDIO
- All existing STDIO functionality unchanged
- No configuration changes needed

#### New HTTP Users
```bash
# Start server with both transports (default)
uv run python mcp_pinot/server.py

# Query via HTTP
curl -X POST http://127.0.0.1:8080/api/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name": "list-tables", "arguments": {}}'
```

#### Kubernetes Deployment
```bash
# Deploy to Kubernetes
kubectl apply -f k8s/

# Access via HTTPS
https://your-domain.com/sse
```

### 📈 Statistics

- **2,899 insertions, 116 deletions**
- **20 files changed**
- **45+ new tests added**
- **7 new Kubernetes manifests**
- **5 new demo/test scripts**
- **3 comprehensive documentation files**

This release transforms the MCP Pinot Server from a STDIO-only tool into a production-ready, dual-transport server capable of serving both local desktop clients and remote web applications with full HTTPS support.
