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
# Install local-only defaults. This does not create a Service.
helm install mcp-pinot ./helm/mcp-pinot --namespace mcp-pinot --create-namespace

# Install with custom image
helm install mcp-pinot ./helm/mcp-pinot \
  --namespace mcp-pinot \
  --create-namespace \
  --set image.tag=v1.0.0
```

To expose the server through a Kubernetes Service, enable OAuth and bind the
container to the pod interface:

```bash
helm install mcp-pinot ./helm/mcp-pinot \
  --namespace mcp-pinot \
  --create-namespace \
  --set service.enabled=true \
  --set mcp.host=0.0.0.0 \
  --set mcp.oauth.enabled=true
```

Set the OAuth endpoint, issuer, audience, client ID, and client secret values
for your identity provider before exposing the Service.

## Traefik Integration

The chart includes Traefik IngressRoute support. Enable it by setting:

```yaml
traefik:
  enabled: true
  match: "Host(`mcp-pinot.yourdomain.com`)"
  tls: {}
```

Only expose the MCP HTTP endpoint outside a trusted network when OAuth is enabled
and the route is protected by TLS or an authenticated ingress/reverse proxy. The
chart defaults to local-only mode with no Service; set `service.enabled=true` and
`mcp.host=0.0.0.0` only together with `mcp.oauth.enabled=true`, otherwise the
chart refuses to render or the server refuses to start.

## Configuration

See `values.yaml` for all available configuration options including:
- Pinot cluster connection settings
- MCP server transport configuration
- Resource limits and security contexts
- Health check settings
