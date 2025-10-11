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

Note: you must use stdio transport when running your server.

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