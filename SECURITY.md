# Security

## Reporting a vulnerability

Do not open a public GitHub issue for a suspected vulnerability.

Report security issues through GitHub private vulnerability reporting or a
private security advisory for this repository. Include:

- Affected version, commit, or container image tag
- Deployment mode: stdio, HTTP, HTTPS, Docker, Helm, or Kubernetes
- Relevant configuration with secrets removed
- Steps to reproduce and expected impact
- Any known workaround or mitigation

The maintainers will triage the report, confirm the affected scope, and
coordinate a fix and disclosure timeline before public details are shared.

## Supported security scope

Security fixes target the current `main` branch and the latest published
package or container release. If a vulnerability affects an older release,
upgrade guidance will be included with the advisory when practical.

## Security categories

### Network exposure and authentication

The MCP HTTP transport is intended to be local by default. It binds to
`127.0.0.1` unless configured otherwise. Binding HTTP or HTTPS to a non-loopback
address requires an active OAuth or static-token provider. Use TLS directly or
an authenticated reverse proxy as well; transport encryption does not replace
the server's inbound authentication requirement.

For Helm deployments, exposing the server through a Kubernetes Service or
Traefik requires an explicit non-loopback bind host and OAuth-enabled
configuration.

### Tool invocation and query safety

The `read_query` tool accepts one read-only `SELECT` or `WITH ... SELECT`
statement. It rejects stacked statements and write, DDL, or administrative SQL
keywords before sending SQL to Pinot.

This validation is a defense-in-depth guardrail. It is not a replacement for
Pinot authentication, Pinot authorization, network controls, or least-privilege
cluster credentials.

### Pinot data access

Use Pinot's native authentication and table-level access controls for production
authorization. MCP table filtering is a usability feature for limiting visible
tables and reducing cognitive load; it is not a security boundary.

### Secrets and deployment configuration

Never commit real Pinot credentials, OAuth client secrets, TLS private keys, or
cluster-specific tokens. Use environment variables, Kubernetes Secrets, or your
organization's secret manager.

Review `.env.example` and the Helm chart values before exposing the MCP HTTP
endpoint outside a local development environment.

## Security checklist before exposing HTTP

- Set `MCP_HOST=0.0.0.0` only when remote clients need network access.
- Set `AUTH_PROVIDER=oauth` (or `static` with `MCP_STATIC_TOKEN`) before binding
  to a non-loopback host.
- Use TLS directly or terminate TLS at an authenticated reverse proxy.
- Configure Pinot-side authentication and authorization.
- Use least-privilege credentials for the MCP server's Pinot connection.
- Confirm Helm `service.enabled`, `traefik.enabled`, and health checks match the
  intended exposure model.
