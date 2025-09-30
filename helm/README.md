# MCP Pinot Helm Chart

This Helm chart deploys the MCP Pinot server to Kubernetes.

## Features

- Deploys MCP Pinot server with configurable environment variables
- Supports both HTTP and HTTPS transport modes
- Includes health checks (liveness and readiness probes)
- **Traefik IngressRoute support** for external access
- Configurable resource limits and security contexts
- Optional Pinot authentication support

## Quick Start

```bash
# Install with default values
helm install mcp-pinot ./helm/mcp-pinot --namespace mcp-pinot --create-namespace

# Install with custom image
helm install mcp-pinot ./helm/mcp-pinot \
  --namespace mcp-pinot \
  --create-namespace \
  --set image.tag=v1.0.0
```

## Traefik Integration

The chart includes Traefik IngressRoute support. Enable it by setting:

```yaml
traefik:
  enabled: true
  match: "Host(`mcp-pinot.yourdomain.com`)"
  tls: {}
```

## Configuration

See `values.yaml` for all available configuration options including:
- Pinot cluster connection settings
- MCP server transport configuration
- Resource limits and security contexts
- Health check settings
