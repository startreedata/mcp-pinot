# Pinot MCP Server

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Docker Build](#docker-build)
- [Claude Desktop Integration](#claude-desktop-integration)
- [Try a Prompt](#try-a-prompt)
- [Developer Notes](#developer-notes)

## Overview

This project is a Python-based [Model Context Protocol (MCP)](https://github.com/anthropic-ai/mcp) server for interacting with Apache Pinot. It is built using the [FastMCP framework](https://github.com/jlowin/fastmcp). It is designed to integrate with Claude Desktop to enable real-time analytics and metadata queries on a Pinot cluster.

It allows you to
- List tables, segments, and schema info from Pinot
- Execute read-only SQL queries
- View index/column-level metadata
- Designed to assist business users via Claude integration
- and much more.

<a href="https://glama.ai/mcp/servers/@startreedata/mcp-pinot">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@startreedata/mcp-pinot/badge" alt="StarTree Server for Apache Pinot MCP server" />
</a>

## Pinot MCP in Action

See Pinot MCP in action below:

### Fetching Metadata
![Pinot MCP fetching metadata](assets/pinot-mcp-in-action.png)

### Fetching Data, followed by analysis

Prompt:
Can you do a histogram plot on the GitHub events against time
![Pinot MCP fetching data and analyzing table](assets/github-events-analysis.png)

### Sample Prompts
Once Claude is running, click the hammer 🛠️ icon and try these prompts:

- Can you help me analyse my data in Pinot? Use the Pinot tool and look at the list of tables to begin with.
- Can you do a histogram plot on the GitHub events against time


## Quick Start

### Prerequisites

#### Install uv (if not already installed)
[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver, written in Rust. It's designed to be a drop-in replacement for pip with significantly better performance.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh

# Reload your bashrc/zshrc to take effect. Alternatively, restart your terminal
# source ~/.bashrc
```


### Installation
```bash
# Clone the repository
git clone https://github.com/startreedata/mcp-pinot.git
cd mcp-pinot
uv pip install -e . # Install dependencies

# For development dependencies (including testing tools), use:
# uv pip install -e .[dev] 
```

### Configure Pinot Cluster
The MCP server expects a uvicorn config style `.env` file in the root directory to configure the Pinot cluster connection. This repo includes a sample `.env.example` file that assumes a pinot quickstart setup.
```bash
mv .env.example .env
```

### Configure Table Filtering (Optional)

> ⚠️ **Security Note:** For production access control, use [Pinot's native table-level ACLs](https://docs.pinot.apache.org/operators/operating-pinot/access-control) (available since Pinot 0.8.0+). Table filtering in this MCP server is a convenience feature for organizing tables and improving UX, not a security boundary. It uses best-effort SQL parsing and should not be relied upon for security.

Table filtering allows you to control which Pinot tables are visible through the MCP server. This is useful for:
- **Reduce Cognitive Load**: Focus on relevant tables when your Pinot cluster has hundreds or thousands of tables
- **Multi-Tenancy UX**: Run multiple MCP server instances against the same Pinot cluster, each showing different table subsets for different teams or use cases
- **Environment Separation**: Deploy different MCP server instances (dev, staging, prod) that show only environment-specific tables
- **Hide System Tables**: Filter out internal, test, or deprecated tables from end-user view

When table filtering is enabled, **all table operations** are filtered to show only the configured tables.

#### What Gets Filtered

Table filtering applies across **all MCP operations**:

1. **Table Listing** - Only configured tables appear in table lists
2. **Query Execution** - SQL queries are checked to ensure all referenced tables (in FROM, JOIN, subqueries, CTEs, etc.) match the configured patterns
3. **Table Operations** - Direct table access operations filter by table name:
   - Get table details, size, and metadata
   - Get table segments and segment metadata
   - Get index/column details
   - Get/update table configurations
4. **Schema Operations** - Schema operations filter by schema name:
   - Get/create/update schemas
   - Create table configurations

#### Setup
Copy the example configuration file:
```bash
cp table_filters.yaml.example table_filters.yaml
```

Edit `table_filters.yaml` to specify which tables to include:
```yaml
included_tables:
  - production_*        # All tables starting with "production_"
  - analytics_events    # Specific table name
  - metrics_*          # All tables starting with "metrics_"
```

Configure the filter file path in your `.env`:
```bash
PINOT_TABLE_FILTER_FILE=table_filters.yaml
```

#### Pattern Matching
The filter supports glob-style patterns using standard Unix filename pattern matching:
- `exact_table_name` - Matches exactly this table
- `prefix_*` - Matches all tables starting with "prefix_"
- `*_suffix` - Matches all tables ending with "_suffix"
- `*pattern*` - Matches all tables containing "pattern"
- `sharded_table_?` - Matches tables with exactly one character after the underscore (e.g., `sharded_table_1`, `sharded_table_a`)

#### Query Filtering
When filtering is enabled, SQL queries are checked before execution:

- **Supported SQL Features**: FROM clauses, JOIN clauses (INNER, LEFT, RIGHT, OUTER, CROSS), subqueries, CTEs (WITH), UNION queries, comma-separated table lists
- **Quoted Identifiers**: Supports both double-quoted (`"table name"`) and backtick-quoted (`` `table_name` ``) table names
- **Schema Prefixes**: Handles schema-qualified table names (e.g., `database.schema.table`)
- **Comments**: Removes SQL comments before checking

**Example filtered query:**
```sql
SELECT * FROM allowed_table
JOIN other_table ON allowed_table.id = other_table.id
```
**Error:** `Query references unauthorized tables: other_table. Allowed tables: allowed_table, prod_*`

#### Configuration Features

**Fail-Fast Validation:**
- ⚠️ If `PINOT_TABLE_FILTER_FILE` is configured but the file doesn't exist, the server will **fail to start** with a `FileNotFoundError`
- This prevents accidentally showing all tables due to misconfiguration
- Empty filter files or missing `included_tables` key will show all tables (no filtering)

**Comprehensive Filtering:**
- All MCP tools that access tables apply filtering before execution
- Consistent filtering across all table access points
- Clear error messages indicate which tables don't match the configured patterns

#### Disabling Table Filtering

To disable table filtering, either:
1. Remove the `PINOT_TABLE_FILTER_FILE` environment variable, or
2. Don't configure it in your `.env` file

When not configured, all tables in the Pinot cluster are visible.

### Configure OAuth Authentication (Optional)
To enable OAuth authentication, set the following environment variables in your `.env` file:

**Required variables (when `OAUTH_ENABLED=true`):**
- `OAUTH_CLIENT_ID`: OAuth client ID
- `OAUTH_CLIENT_SECRET`: OAuth client secret
- `OAUTH_BASE_URL`: Your MCP server base URL
- `OAUTH_AUTHORIZATION_ENDPOINT`: OAuth authorization endpoint URL
- `OAUTH_TOKEN_ENDPOINT`: OAuth token endpoint URL
- `OAUTH_JWKS_URI`: JSON Web Key Set URI for token verification
- `OAUTH_ISSUER`: Token issuer identifier

**Optional variables:**
- `OAUTH_AUDIENCE`: Expected audience claim for token validation
- `OAUTH_EXTRA_AUTH_PARAMS`: Additional authorization parameters as JSON object (e.g., `{"scope": "openid profile"}`)

Example configuration:
```bash
OAUTH_ENABLED=true
OAUTH_CLIENT_ID=client-id
OAUTH_CLIENT_SECRET=client-secret
OAUTH_BASE_URL=http://localhost:8000
OAUTH_AUTHORIZATION_ENDPOINT=https://example.com/oauth/authorize
OAUTH_TOKEN_ENDPOINT=https://example.com/oauth/token
OAUTH_JWKS_URI=https://example.com/.well-known/jwks.json
OAUTH_ISSUER=https://example.com
OAUTH_AUDIENCE=client-id
OAUTH_EXTRA_AUTH_PARAMS={"scope": "openid profile"}
```

### Run the server

```bash
uv --directory . run mcp_pinot/server.py
```
You should see logs indicating that the server is running.

> Security notes:
> - The HTTP transport binds to `0.0.0.0` by default; prefer the `stdio` transport for Claude Desktop, or bind HTTP to `127.0.0.1` via `MCP_HOST=127.0.0.1`, or enable TLS (`MCP_SSL_KEYFILE`/`MCP_SSL_CERTFILE`) before exposing it.
> - Ensure you are using `mcp[cli]` version `>=1.10.0`, which includes DNS rebinding protections for the HTTP/SSE server.

### Launch Pinot Quickstart (Optional)

Start Pinot QuickStart using docker:

```bash
docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batch
```

Query MCP Server

```bash
uv --directory . run examples/example_client.py
```

This quickstart just checks all the tools and queries the airlineStats table.

## Claude Desktop Integration

### Open Claude's config file
```bash
vi ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

### Add an MCP server entry
```json
{
  "mcpServers": {
      "pinot_mcp": {
          "command": "/path/to/uv",
          "args": [
              "--directory",
              "/path/to/mcp-pinot-repo",
              "run",
              "mcp_pinot/server.py"
          ],
          "env": {
            // You can also include your .env config here
          }
      }
  }
}
```
Replace `/path/to/uv` with the absolute path to the uv command, you can run `which uv` to figure it out.

Replace `/path/to/mcp-pinot` with the absolute path to the folder where you cloned this repo.

Note: you must use stdio transport when running your server to use with Claude desktop.

You could also configure environment variables here instead of the `.env` file, in case you want to connect to multiple pinot clusters as MCP servers.

### Restart Claude Desktop

Claude will now auto-launch the MCP server on startup and recognize the new Pinot-based tools.

## Using DXT Extension

Apache Pinot MCP server now supports DXT desktop extensions file 

To use it, you first need to install dxt via 
```
npm install -g @anthropic-ai/dxt
```

then you can run the following commands:

```bash
uv pip install -r pyproject.toml --target mcp_pinot/lib
uv pip install . --target mcp_pinot/lib 
dxt pack
```

After this you'll get a .dxt file in your dir. Double click on that file to install it in claude desktop

## Developer

- All tools are defined in the `Pinot` class in `utils/pinot_client.py`

### Build
Build the project with

```bash
pip install -e ".[dev]"
```

### Test
Test the repo with:
```bash
pytest
```

### Build the Docker image
```bash
docker build -t mcp-pinot .
```

### Run the container
```bash
docker run -v $(pwd)/.env:/app/.env mcp-pinot
```

Note: Make sure to have your `.env` file configured with the appropriate Pinot cluster settings before running the container.
