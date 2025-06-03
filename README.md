# Pinot MCP Server

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Docker Build](#docker-build)
- [Claude Desktop Integration](#claude-desktop-integration)
- [Try a Prompt](#try-a-prompt)
- [Developer Notes](#developer-notes)

## Overview

This project is a Python-based [Model Context Protocol (MCP)](https://github.com/anthropic-ai/mcp) server for interacting with Apache Pinot. It is designed to integrate with Claude Desktop to enable real-time analytics and metadata queries on a Pinot cluster.

It allows you to
- List tables, segments, and schema info from Pinot
- Execute read-only SQL queries
- View index/column-level metadata
- Designed to assist business users via Claude integration
- Interactive CLI configuration setup
- and much more.

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
git clone git@github.com:startreedata/mcp-pinot.git
cd mcp-pinot
uv pip install -e . # Install dependencies

# For development dependencies (including testing tools), use:
# uv pip install -e .[dev] 
```

## Configuration

The MCP Pinot Server supports multiple ways to configure your Pinot cluster connection:

### 1. Interactive CLI Setup (Recommended for first-time users)

Simply run the server, and it will automatically prompt you for missing configuration:

```bash
mcp-pinot
```

If required configurations are missing, you'll see an interactive setup like:

```
============================================================
🔧 MCP Pinot Server Configuration
============================================================
Some required configurations are missing.
Please provide the following information:

📋 Missing Required Configurations:
  • PINOT_CONTROLLER_URL: URL of the Pinot Controller (e.g., http://localhost:9000)
  • PINOT_BROKER_URL: Complete URL of the Pinot Broker (e.g., https://localhost:443 or http://pinot-broker.yourcompany.com:8099)

PINOT_CONTROLLER_URL: URL of the Pinot Controller (e.g., http://localhost:9000) [REQUIRED]: http://localhost:9000
PINOT_BROKER_URL: Complete URL of the Pinot Broker (e.g., https://localhost:443) [REQUIRED]: https://localhost:443

Configure optional settings? (y/N): y
PINOT_USERNAME: Username for Pinot authentication (optional): myuser
PINOT_PASSWORD: Password for Pinot authentication (optional): [hidden input]

✅ Configuration complete!
```

### 2. Environment Variables

Set the following environment variables:

```bash
export PINOT_CONTROLLER_URL="http://localhost:9000"
export PINOT_BROKER_URL="https://localhost:443"  # Complete broker URL including scheme, host and port
export PINOT_USERNAME="myuser"  # Optional
export PINOT_PASSWORD="mypass"  # Optional
export PINOT_TOKEN="Bearer xyz123"  # Optional, alternative to username/password
export PINOT_DATABASE="mydb"  # Optional
export PINOT_USE_MSQE="true"  # Optional, defaults to false
```

### 3. .env File

Create a `.env` file in the root directory:

```bash
# Copy the example file
cp .env.example .env

# Edit with your configuration
vi .env
```

Example `.env` file:
```
PINOT_CONTROLLER_URL=http://localhost:9000
PINOT_BROKER_URL=https://localhost:443
PINOT_USERNAME=myuser
PINOT_PASSWORD=mypass
PINOT_USE_MSQE=false
```

### Configuration Commands

```bash
# Show current configuration (passwords will be masked)
mcp-pinot --show-config

# Show help and configuration options
mcp-pinot --help

# Show version
mcp-pinot --version
```

### Required Configuration

- **PINOT_CONTROLLER_URL**: URL of the Pinot Controller - the central management service that coordinates your Pinot cluster. This is typically running on port 9000.
  - Examples: `http://localhost:9000` (local), `https://pinot-controller.yourcompany.com` (production)

- **PINOT_BROKER_URL**: Complete URL of the Pinot Broker - the query processing service that handles SQL queries. This includes the scheme (http/https), hostname, and port in a single URL.
  - Examples: `https://localhost:443` (local with HTTPS), `http://localhost:8099` (local with HTTP), `https://pinot-broker.yourcompany.com:8099` (production)
  - The URL is automatically parsed to extract the host, port, and scheme components
  - If no port is specified, defaults to 443 for https and 8099 for http

### Optional Configuration

- **PINOT_USERNAME**: Username for Pinot authentication (if your cluster requires login)
  - Leave empty if your Pinot cluster doesn't use authentication (common in development)
  - For production clusters with security enabled: `admin`, `analyst`, `service-account`, etc.

- **PINOT_PASSWORD**: Password for Pinot authentication (if your cluster requires login, will be masked in displays)
  - Only needed when PINOT_USERNAME is set
  - This will be securely prompted and masked during input

- **PINOT_TOKEN**: Bearer token or API key for token-based authentication (alternative to username/password, will be masked)
  - Format: `Bearer your-jwt-token` or `your-api-key`
  - Common in cloud-managed Pinot services or when using JWT authentication
  - Use either this OR username/password, not both

- **PINOT_DATABASE**: Database name for multi-tenant Pinot deployments
  - Most single-tenant Pinot clusters don't need this (leave empty)
  - In multi-tenant setups, this isolates your tables and queries to a specific database namespace
  - Examples: `analytics`, `prod`, `team-data`

- **PINOT_USE_MSQE**: Enable Multi-Stage Query Engine (MSQE) - `true` or `false` (default: `false`)
  - Pinot's new distributed query engine that supports complex SQL operations like JOINs, subqueries, and window functions
  - Set to `true` for advanced SQL features, `false` for standard Pinot queries
  - MSQE requires Pinot 0.11+ and may have different performance characteristics

### Run the server

```bash
mcp-pinot
```

You should see the configuration summary and then logs indicating that the server is running and listening on STDIO.

### Launch Pinot Quickstart (Optional)

Start Pinot QuickStart using docker:

```bash
docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batch
```

Query MCP Server

```bash
uv --directory . run tests/test_service/test_pinot_quickstart.py
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
      "pinot_mcp_claude": {
          "command": "/path/to/uv",
          "args": [
              "--directory",
              "/path/to/mcp-pinot-repo",
              "run",
              "mcp-pinot"
          ],
          "env": {
            // You can also include your .env config here instead of prompting
            // "PINOT_CONTROLLER_URL": "http://localhost:9000",
            // "PINOT_BROKER_URL": "https://localhost:443"
          }
      }
  }
}
```

Replace `/path/to/uv` with the absolute path to the uv command, you can run `which uv` to figure it out.

Replace `/path/to/mcp-pinot` with the absolute path to the folder where you cloned this repo.

**Note**: When running via Claude Desktop, the interactive configuration prompts won't work. Make sure to either:
1. Set up your configuration via `.env` file, or
2. Include environment variables in the Claude Desktop config, or  
3. Run `mcp-pinot` once manually to set up configuration before using with Claude

### Restart Claude Desktop

Claude will now auto-launch the MCP server on startup and recognize the new Pinot-based tools.

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