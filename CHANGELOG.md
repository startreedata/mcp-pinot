# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `OAUTH_GRANTED_SCOPES` (default `pinot:read pinot:write pinot:admin`): Pinot
  authorization scopes granted to every principal the OAuth provider
  authenticates, unioned onto the scopes the token already carries. General-purpose
  OIDC providers issue a fixed scope catalog and cannot mint `pinot:*`, so before
  this an authenticated user's token carried no Pinot scope and every tool call was
  denied. Set to `pinot:read` for a read-only deployment.
- Chart value `mcp.oauth.grantedScopes` renders `OAUTH_GRANTED_SCOPES`, so a
  read-only OIDC deployment is expressible from Helm (`[pinot:read]`).
- Chart values `mcp.allowedHosts` / `mcp.allowedOrigins` render `MCP_ALLOWED_HOSTS`
  / `MCP_ALLOWED_ORIGINS`, and the chart now fails render when `mcp.host` is a
  wildcard bind with no Host allowlist. Without them a chart-managed deployment
  bound to 0.0.0.0 exited at startup ("Refusing to start HTTP transport without an
  exact Host allowlist") with no chart-level way to fix it; both variables were
  also undocumented.
- Helm chart auto-generates the `static` shared token when `mcp.auth.staticToken`
  is left empty (with `mcp.auth.provider=static`): a random token is minted on
  first install and persisted in the `-secrets` Secret, reused on every upgrade via
  `lookup`. Makes the static provider zero-touch per environment — operators never
  pick, paste, or distribute a token.

### Fixed
- `test_connection` no longer reports a healthy deployment as broken. It probed
  through the pinotdb DB-API connection, which no tool uses: against a broker whose
  response pinotdb rejects (`check_sufficient_responded`) it returned
  `connection_test`/`query_test`/`tables_test` all false while `read_query`,
  `list_tables` and the inspection tools all worked. Each check now runs
  independently through the same path as the tool it stands in for, the DB-API probe
  is reported separately as informational `dbapi_test`, and the error names which
  checks actually failed.
- Health probes follow `mcp.ssl.enabled` instead of always using `http://`, so a
  deployment that terminates TLS in the server can keep probes enabled — previously
  the probe could never succeed there and had to be turned off, leaving Kubernetes
  with no readiness signal.
- Made confirmation tokens canonical and replay-safe by keying consumption on the
  signed nonce; malformed base64url, expiry, signature tampering, cross-operation
  reuse, and string-malleated replays are rejected.
- Added the same exact-candidate confirmation-token flow to
  `reload_table_filters`, including protection against file changes between preview
  and apply.
- Bound unauthenticated loopback HTTP rate limits to the direct peer instead of a
  fresh stateless session ID, and bounded/expired the limiter cache.
- Restored application logging initialization, added the MCP Registry OCI ownership
  label, and corrected v-stripped Docker release references.
- Rejected excessive nested percent encoding, classified Pinot SQL errors as safe
  invalid input, made direct filter reloads preview-first, and replaced quadratic
  duplicate-field detection.

### Changed
- `OAUTH_AUDIENCE` is now optional and defaults to the canonical MCP resource URI
  (`OAUTH_BASE_URL` + `MCP_PATH`). A value that differs from that URI is honoured
  with a warning instead of refusing to start, because providers that set `aud` to
  the client ID cannot issue the resource URI.
- Helm intentionally supports one replica while confirmation and rate-limit state
  are process-local; rolling updates no longer overlap two server processes and the
  optional PDB defaults to `maxUnavailable: 1`.
- Static-token principals can be restricted with `MCP_STATIC_SCOPES`.
- PinotDB is bounded to the supported 9.x line and MCPB schema metadata is pinned to
  an immutable commit.

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
  `dry_run=true` and requires `dry_run=false` plus the preview's confirmation token
  to apply unchanged, validated YAML.
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
