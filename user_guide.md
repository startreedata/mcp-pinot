# MCP Pinot Server User Guide

This guide shows how to connect an MCP client to Apache Pinot, discover the
available tools, run read-only analytics, and safely preview configuration
changes. It does not assume a particular set of Pinot tables or sample data.

## Prerequisites

- Python 3.12 or later
- `uv`
- An Apache Pinot controller and broker reachable from this machine

Configure the endpoints in `.env` or in the MCP client's environment:

```dotenv
PINOT_CONTROLLER_URL=http://localhost:9000
PINOT_BROKER_URL=http://localhost:8000
```

Use `PINOT_TOKEN` or `PINOT_USERNAME`/`PINOT_PASSWORD` when the Pinot cluster
requires authentication.

## Choose a transport

The server exposes standard MCP transports. It does not provide custom
`/api/tools/list` or `/api/tools/call` REST endpoints.

### STDIO

STDIO is the simplest option for a desktop MCP host. The host starts and manages
the server process:

```json
{
  "mcpServers": {
    "pinot": {
      "command": "/absolute/path/to/uv",
      "args": [
        "--directory",
        "/absolute/path/to/mcp-pinot",
        "run",
        "mcp-pinot"
      ],
      "env": {
        "MCP_TRANSPORT": "stdio",
        "PINOT_CONTROLLER_URL": "http://localhost:9000",
        "PINOT_BROKER_URL": "http://localhost:8000"
      }
    }
  }
}
```

### Streamable HTTP

For a local MCP client, start the server on the loopback interface:

```bash
MCP_TRANSPORT=http MCP_HOST=127.0.0.1 MCP_PORT=8080 uv run mcp-pinot
```

The MCP endpoint is `http://127.0.0.1:8080/mcp`. Use an MCP client library to
perform initialization, capability negotiation, and tool calls. The bundled
example uses FastMCP:

```bash
uv run python examples/example_client.py
```

The server refuses an unauthenticated non-loopback HTTP bind. Configure
`AUTH_PROVIDER=static` with `MCP_STATIC_TOKEN`, or configure the OAuth provider,
before exposing it to a network. See [SECURITY.md](SECURITY.md).

## Available tools

Tool names are case-sensitive and use underscores.

| Tool | Purpose |
|---|---|
| `test_connection` | Diagnose broker, controller, and query connectivity. |
| `list_tables` | List tables visible through the configured table filter. |
| `get_schema` | Get one Pinot schema and its column definitions. |
| `get_table_config` | Get indexing, retention, tenant, and ingestion configuration. |
| `get_table_size` | Get reported and estimated storage size for a table. |
| `list_segments` | List exact segment names for a table. |
| `list_segment_metadata` | Page through row, size, and time metadata for segments. |
| `get_segment_index_metadata` | Get per-column index metadata for one exact segment. |
| `read_query` | Run one read-only `SELECT` or `WITH ... SELECT`. |
| `create_schema` | Preview or create a Pinot schema. |
| `update_schema` | Preview or update an existing schema. |
| `create_table_config` | Preview or create a table configuration. |
| `update_table_config` | Preview or update a table configuration. |
| `reload_table_filters` | Preview or apply the configured table-filter YAML. |

MCP clients discover the authoritative input and output JSON Schemas through
`tools/list`. Prefer those schemas over copying argument shapes from old examples.

## Explore and query data

A reliable exploration flow is:

1. Call `test_connection` if connectivity is uncertain.
2. Call `list_tables` and use an exact returned table name.
3. Call `get_schema` and `get_table_config` before composing SQL.
4. Use `get_table_size`, `list_segments`, or the segment metadata tools only when
   that level of operational detail is relevant.
5. Call `read_query` with one read-only statement and a small page size.

Example analytics statements:

```sql
SELECT COUNT(*) AS row_count FROM airlineStats
```

```sql
SELECT * FROM airlineStats ORDER BY FlightDate DESC LIMIT 10
```

`read_query` rejects stacked statements and DML, DDL, and administrative
keywords. It is a safety guardrail, not a replacement for Pinot authentication,
authorization, resource limits, or native table-level ACLs.

## Preview writes before applying

Schema and table-config tools can change live Pinot metadata. Use this sequence:

1. Call the intended `create_*` or `update_*` tool with `dry_run=true`.
2. Show the exact target, options, and preview to the user.
3. Obtain confirmation for that exact change.
4. Call the same tool with `dry_run=false` and the preview's one-time
   `confirmation_token`.
5. Verify the result with `get_schema` or `get_table_config`.

A successful preview confirms that the MCP server could parse the proposed
payload; it does not guarantee that Pinot will accept the apply call. Do not tell
the user a change was applied until the apply result succeeds.

`reload_table_filters` previews by default. Pass `dry_run=false` only after
reviewing the candidate filters. These filters improve tool discovery and reduce
cognitive load; they are not an authorization boundary. Use Pinot ACLs for access
control.

## Prompts and resources

The server exposes:

- `pinot_query`, a reusable analytics workflow prompt;
- `explore_table`, a guided table-exploration prompt;
- `pinot://tables`, the visible table catalog;
- `pinot://schema/{schema_name}`;
- `pinot://table-config/{table_name}`.

Prompts and resources are optional MCP conveniences. The tools and their schemas
remain the authoritative execution interface.

## Troubleshooting

### No tables are returned

- Verify `PINOT_CONTROLLER_URL` and credentials.
- Call `test_connection`.
- Check `PINOT_TABLE_FILTER_FILE`; an empty result can be caused by its include
  patterns.
- Verify the tables directly in Pinot.

### A query fails

- Copy the exact case-sensitive name from `list_tables`.
- Inspect `get_schema` before referencing columns.
- Send one read-only statement only.
- Check Pinot permissions, broker availability, and query limits.

### HTTP clients cannot connect

- Use `/mcp`, not the removed custom `/api/tools/*` paths.
- Use an MCP SDK or compatible host rather than a bare REST client.
- Bind `127.0.0.1` for local use.
- Configure an auth provider before a non-loopback bind.

### A write preview succeeded but apply failed

The preview is intentionally non-mutating and is not an authoritative Pinot
validation response. Read the apply error, correct the payload or permissions,
preview again, and request confirmation for the revised change.
