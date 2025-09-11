# Kubernetes Deployment for MCP Pinot Server

This directory contains Kubernetes manifests for deploying the MCP Pinot Server with HTTPS support.

## Files Overview

- `deployment.yaml` - Main deployment configuration
- `service.yaml` - Service to expose the deployment
- `ingress.yaml` - Ingress with HTTPS/TLS termination
- `configmap.yaml` - Configuration for Pinot connection settings
- `secret.yaml` - Secret for Pinot authentication credentials

## Deployment Options

### Option 1: HTTPS via Ingress (Recommended)

This is the recommended approach for Kubernetes deployments:

1. **TLS Termination at Ingress**: The Ingress controller handles HTTPS termination
2. **Internal HTTP**: The MCP server runs HTTP internally within the cluster
3. **Automatic Certificate Management**: Using cert-manager for Let's Encrypt certificates

```bash
# Deploy all resources
kubectl apply -f k8s/

# Check deployment status
kubectl get pods -l app=mcp-pinot-server
kubectl get ingress mcp-pinot-ingress
```

### Option 2: Direct HTTPS (Alternative)

For direct HTTPS support, uncomment the SSL-related sections in `deployment.yaml`:

1. Uncomment the SSL environment variables
2. Uncomment the volume mounts and volumes
3. Create a TLS secret with your certificates:

```bash
kubectl create secret tls mcp-pinot-tls \
  --cert=path/to/tls.crt \
  --key=path/to/tls.key
```

## Configuration

### Environment Variables

The server supports the following configuration options:

#### MCP Server Settings
- `MCP_TRANSPORT`: Transport type ("stdio", "http", or "both") - Default: "both"
- `MCP_HOST`: Host to bind to - Default: "0.0.0.0"
- `MCP_PORT`: Port to listen on - Default: 8080
- `MCP_ENDPOINT`: SSE endpoint path - Default: "/sse"
- `MCP_SSL_KEYFILE`: Path to SSL private key (optional)
- `MCP_SSL_CERTFILE`: Path to SSL certificate (optional)

#### Pinot Connection Settings
- `PINOT_CONTROLLER_URL`: Pinot controller URL
- `PINOT_BROKER_URL`: Pinot broker URL
- `PINOT_USERNAME`: Username for authentication (optional)
- `PINOT_PASSWORD`: Password for authentication (optional)
- `PINOT_TOKEN`: Token for authentication (optional)
- `PINOT_USE_MSQE`: Enable Multi-Stage Query Engine (optional)
- `PINOT_REQUEST_TIMEOUT`: Request timeout in seconds (optional)

### Updating Configuration

1. Edit `configmap.yaml` for non-sensitive configuration
2. Edit `secret.yaml` for credentials (remember to base64 encode values)
3. Apply changes: `kubectl apply -f k8s/configmap.yaml k8s/secret.yaml`
4. Restart deployment: `kubectl rollout restart deployment/mcp-pinot-server`

## Usage Examples

### Local Development Examples

#### Default Mode (Both Transports)
```bash
# Uses default configuration - runs both STDIO and HTTP
export PINOT_CONTROLLER_URL=http://localhost:9000
export PINOT_BROKER_URL=http://localhost:8000

# Run the server (both STDIO and HTTP on port 8080)
uv run python mcp_pinot/server.py
```

#### HTTP Only Mode
```bash
# Set environment variables for HTTP only
export MCP_TRANSPORT=http
export MCP_HOST=localhost
export MCP_PORT=8080
export PINOT_CONTROLLER_URL=http://localhost:9000
export PINOT_BROKER_URL=http://localhost:8000

# Run the server
uv run python mcp_pinot/server.py
```

#### STDIO Only Mode
```bash
# Set environment variables for STDIO only
export MCP_TRANSPORT=stdio
export PINOT_CONTROLLER_URL=http://localhost:9000
export PINOT_BROKER_URL=http://localhost:8000

# Run the server
uv run python mcp_pinot/server.py
```

### Testing the HTTP Endpoint

```bash
# Test SSE endpoint (should return 404 for GET without proper SSE client)
curl -i https://mcp-pinot.yourdomain.com/sse

# Health check (if implemented)
curl -i https://mcp-pinot.yourdomain.com/health
```

## Security Considerations

1. **HTTPS Only**: Always use HTTPS in production
2. **Network Policies**: Consider implementing Kubernetes Network Policies
3. **RBAC**: Use proper RBAC for service accounts
4. **Secrets Management**: Use external secret management systems for production
5. **Resource Limits**: Set appropriate CPU/memory limits

## Monitoring and Logging

The deployment includes:
- Liveness and readiness probes
- Resource limits and requests
- Structured logging via the application

Consider adding:
- Prometheus metrics endpoint
- Log aggregation (ELK stack, Fluentd, etc.)
- Distributed tracing
- Service mesh (Istio, Linkerd)

## Troubleshooting

### Common Issues

1. **Pod not starting**: Check logs with `kubectl logs -l app=mcp-pinot-server`
2. **Connection refused**: Verify service and ingress configuration
3. **TLS certificate issues**: Check cert-manager logs and certificate status
4. **Pinot connection issues**: Verify Pinot URLs and credentials in configmap/secret

### Debug Commands

```bash
# Check pod status
kubectl get pods -l app=mcp-pinot-server

# View pod logs
kubectl logs -l app=mcp-pinot-server --tail=100

# Describe deployment
kubectl describe deployment mcp-pinot-server

# Check ingress status
kubectl describe ingress mcp-pinot-ingress

# Port forward for local testing
kubectl port-forward svc/mcp-pinot-service 8080:80
```
