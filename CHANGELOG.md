# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [4.0.0] - 2026-07-22

### Breaking Changes
- Renamed four noun-first tools for unambiguous, verb-first agent discovery:
  `table_details` to `get_table_size`, `segment_list` to `list_segments`,
  `index_column_details` to `get_segment_index_metadata`, and
  `segment_metadata_details` to `list_segment_metadata`.
- Aligned package, MCP Registry, MCPB, and Helm metadata on version `4.0.0` for
  the breaking tool contract.

### Added
- Built-in `static` auth provider for service-to-service callers: set
  `AUTH_PROVIDER=static` and `MCP_STATIC_TOKEN=<shared secret>`; a trusted backend
  presents it as `Authorization: Bearer <token>`. Satisfies the non-loopback-bind
  auth requirement without a full OIDC flow. Missing/blank `MCP_STATIC_TOKEN`
  fails startup rather than booting unauthenticated.
- Bounded, deterministic pagination for `list_segment_metadata`, with a typed
  `{segments, returned_segments, total_segments, offset, has_more}` result.
- Explicit failure classification and recovery guidance in every MCP tool
  description, plus advertised Pinot identifier constraints.
- A safe preview mode for `reload_table_filters`; it now defaults to
  `dry_run=true` and requires an explicit `dry_run=false` to apply validated YAML.
- STDIO is now the safe default transport; HTTP must be selected explicitly.

### Removed
- The ambiguous `tableconfig_schema_details` MCP tool. Use the single-purpose
  `get_schema` and `get_table_config` tools instead.

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
- Pagination (`limit`/`offset` + `has_more`) for `segment_list`, and richer
  descriptions on the inspection tools clarifying when to use each.
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
