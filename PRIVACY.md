# Privacy Policy

> **Draft.** Required for listing in Anthropic's Claude Connectors Directory (a
> missing/incomplete privacy policy is an automatic rejection). Review with your
> legal/security team and host the final version at a stable HTTPS URL before
> submitting.

_Last updated: 2026-06-26_

The Apache Pinot MCP Server (`mcp-pinot-server`, "the Server") is open-source
software that an operator self-hosts to let an MCP client (e.g. Claude) query and
inspect an Apache Pinot cluster the operator controls.

## What the Server accesses

- **Pinot cluster data and metadata** — table data, schemas, segments, and table
  configurations from the Pinot cluster the operator configures it to connect to,
  in order to answer the MCP client's tool/resource calls.
- **Connection configuration** — endpoints and credentials (e.g. `PINOT_TOKEN`)
  supplied by the operator through environment variables.

## What the Server does *not* do

- It does **not** collect, store, or transmit your data to the project maintainers
  or any third party. The Server runs entirely within the operator's environment.
- It does **not** persist query results or cluster data beyond the lifetime of a
  request; it holds no database of its own.
- It does **not** send analytics or telemetry.

## Data handling

- Requests flow only between the MCP client, the Server, and the operator's Pinot
  cluster. Network egress is limited to the configured Pinot endpoints (and, when
  an auth provider is enabled, the configured identity/authorization service).
- Credentials are read from environment variables and used only to authenticate to
  Pinot; they are not logged. See [SECURITY.md](SECURITY.md) for the security model
  and production-exposure checklist.
- Read-only query enforcement and optional table filtering limit what the Server
  can access; write tools require explicit invocation and support `dry_run`.

## Data retention

The Server retains no data after a request completes. Any retention of Pinot data
is governed by the operator's own Pinot deployment and policies.

## Contact

Report privacy or security concerns via the process in
[SECURITY.md](SECURITY.md). For general questions, open a
[GitHub issue](https://github.com/startreedata/mcp-pinot/issues).
