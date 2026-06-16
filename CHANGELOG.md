# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Pluggable authentication provider system: the active provider is selected with
  `AUTH_PROVIDER` and resolved through a registry with Python entry-point
  discovery (group `mcp_pinot.auth_providers`). External or proprietary providers
  can be added without modifying the server.
- `OAUTH_SCOPES` configuration (default `openid profile email`) controlling the
  scopes advertised in OAuth discovery metadata and required on access tokens.
- Structured, typed tool outputs: every tool now returns a documented output
  schema (`structuredContent`) instead of an opaque JSON string.
- MCP tool annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`) on
  all tools, and per-parameter descriptions and validation constraints.
- Pagination (`limit`/`offset` with a `has_more` flag) for `read_query` and
  `list_tables`.
- `dry_run` previews for the schema and table-config write tools.
- Server `instructions` to guide MCP clients.

### Changed
- OAuth discovery now advertises a non-empty `scopes_supported`, so the
  `mcp-remote` bridge (Claude Desktop) completes the OAuth flow instead of
  refusing it. (See fastmcp#1716.)
- Tool failures now raise structured `ToolError`s with actionable messages;
  internal error details are masked (`mask_error_details=True`) to avoid leaking
  connection internals.
- OAuth construction moved behind a single `build_auth()` seam; the non-loopback
  HTTP safety check now applies to any active auth provider, not only OAuth.

### Security
- HTTP transport binds to `127.0.0.1` by default; the server refuses to start on
  a non-loopback host unless an auth provider is enabled.
- `read_query` enforces single-statement, read-only SQL (SELECT / WITH ... SELECT)
  via `sqlglot`, rejecting stacked statements and write/DDL/admin keywords.
