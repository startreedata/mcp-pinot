# MCP SDK compatibility and migration

Server 5.0.0 uses stable MCP SDK 2.3.0 and FastMCP 4.1.0. FastMCP is
exactly pinned; MCP stays within major version 2. The committed `uv.lock` freezes
both versions for CI, containers, and MCP bundles. Dependabot proposes updates;
the existing OS/Python test matrix, lint, native Pinot quickstart, and Docker
runtime smoke must pass before an update lands.

MCP SDK 2 is already stable and supported by FastMCP 4, so it runs in the main
validation lane. The earlier proposal for a separate SDK 2 prerelease lane is
superseded. Server `5.0.0` promotes the v5 line to a stable release.

## Migrate an integration

MCP clients using the wire protocol continue to negotiate their protocol version.
Python integrations that import SDK models must use snake-case attributes such as
`input_schema` and `output_schema`, and the renamed `MCPError` exception. The
server tests run with `FASTMCP_MCP_CAMELCASE_COMPAT=false` to catch reliance on old
Python aliases. Oversized responses now raise a bounded `ToolError` instead of
returning text outside the advertised output schema.

In a checkout of the v5 line, install the frozen environment and check its contract:

```sh
uv sync --frozen --all-extras
FASTMCP_MCP_CAMELCASE_COMPAT=false uv run --frozen pytest tests/
helm lint helm/mcp-pinot
bash helm/smoke-test.sh
```

Before rolling out v5, exercise your actual client's STDIO or
Streamable HTTP workflow and, for hosted deployments, its identity provider login
and OAuth/static authorization. Preserve the previous image/chart version and
deployment configuration. CI validates authentication behavior with controlled
fixtures; it does not validate your identity provider or Kubernetes storage class.

## Roll back

Restore the previously deployed image and Helm chart, or the previous Python
package version and its matching lockfile. Restore any Python integration code
that used SDK 1 model attributes together with that environment. Keep the same
endpoint, credentials, and OAuth state volume, then verify client initialization,
`test_connection`, and a bounded `read_query` before returning traffic.

Release metadata tests keep the wheel, Helm, MCPB, and registry versions aligned.
The release workflow validates the MCPB manifest and publishes stable versions to
the MCP Registry; server prereleases remain outside the registry's default lane.
