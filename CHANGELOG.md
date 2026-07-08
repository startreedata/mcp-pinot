# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Built-in `static` auth provider for service-to-service callers: set
  `AUTH_PROVIDER=static` and `MCP_STATIC_TOKEN=<shared secret>`; a trusted backend
  presents it as `Authorization: Bearer <token>`. Satisfies the non-loopback-bind
  auth requirement without a full OIDC flow. Missing/blank `MCP_STATIC_TOKEN`
  fails startup rather than booting unauthenticated.
- Configurable streamable-HTTP Host/Origin (DNS-rebinding) protection via
  `MCP_HOST_ORIGIN_PROTECTION`, `MCP_ALLOWED_HOSTS`, and `MCP_ALLOWED_ORIGINS`.
  FastMCP 3.4 enables this protection by default with a localhost-only allow-list,
  so a server reached through an ingress/Service under its real hostname returned
  `421 Misdirected Request`; these settings let operators allow-list the hostname
  or disable protection for a bearer-authenticated, TLS-fronted deployment.

## [3.2.0] - 2026-06-16

### Breaking Changes
- Tool results are now **structured** (typed `outputSchema` + `structuredContent`).
  The JSON text shape also changed — e.g. `read_query` returns
  `{columns, rows, row_count, total_rows, has_more}` instead of a bare array, and
  `list_tables` returns `{tables, ...}`. A JSON text block is still emitted for
  backward compatibility, but its shape differs.
- `read_query` and `list_tables` now **paginate** and default to `limit=100`
  (previously all rows/tables were returned). Use `limit`/`offset` and `has_more`.
- Tool failures now raise `ToolError` (surfaced as `isError`) instead of returning
  an `"Error: ..."` string in the success channel.

### Added
- Pluggable authentication provider system: the active provider is selected with
  `AUTH_PROVIDER` and resolved through a registry with Python entry-point
  discovery (group `mcp_pinot.auth_providers`). External or proprietary providers
  can be added without modifying the server.
- `OAUTH_SCOPES` (default `openid profile email`) controlling the scopes
  **advertised** in OAuth discovery metadata (`scopes_supported`), and a separate
  `OAUTH_REQUIRED_SCOPES` (default: none) to **enforce** scopes on access tokens.
- Structured, typed tool outputs: every tool now returns a documented output
  schema (`structuredContent`) instead of an opaque JSON string.
- MCP tool annotations (`readOnlyHint`, `destructiveHint`, `idempotentHint`) on
  all tools, and per-parameter descriptions and validation constraints.
- Pagination (`limit`/`offset` with a `has_more` flag) for `read_query` and
  `list_tables`.
- `dry_run` previews for the schema and table-config write tools.
- Server `instructions` to guide MCP clients.
- Documented output schemas for the inspection tools (`table_details`,
  `segment_list`, `index_column_details`, `segment_metadata_details`,
  `tableconfig_schema_details`, `get_schema`, `get_table_config`) via typed
  Pydantic models; declared fields are documented while extra fields are
  preserved (`extra="allow"`), so response shapes are unchanged.
- The schema/table-config write tools accept their JSON payload as a structured
  object **or** a JSON string (back-compatible).
- MCP **resources** (`pinot://tables`, `pinot://schema/{name}`,
  `pinot://table-config/{name}`) and an `explore_table` prompt.
- Repo supportability: `SUPPORT.md`, GitHub issue templates, and README status
  badges.

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
