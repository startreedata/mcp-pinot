# MCP Pinot Server

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Docker Build](#docker-build)
- [Claude Desktop Integration](#claude-desktop-integration)
- [Try a Prompt](#try-a-prompt)
- [Developer Notes](#developer-notes)

---

## Overview

This project is a Python-based [Model Context Protocol (MCP)](https://github.com/anthropic-ai/mcp) server for interacting with Apache Pinot. It is designed to integrate with Claude Desktop to enable real-time analytics and metadata queries on a Pinot cluster.

---

## Features

- List tables, segments, and schema info from Pinot
- Execute read-only SQL queries
- View index/column-level metadata
- Designed to assist business users via Claude integration

---

## Quick Start

### Install `uv` (if not already installed)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Then **restart your terminal** to register the `uv` command.

---

### Clone and set up the project

```bash
git clone git@github.com:startreedata/mcp-pinot.git
cd mcp-pinot
pip install -e .
```

---

### Install dependencies

```bash
uv add httpx
uv add pinotdb
uv add "mcp[cli]"
uv add pandas
uv add requests
```

---

### Configure Pinot Cluster connection

```bash
cp .env.example .env
```
Edit `.env` file with all the corresponding Pinot configurations.

Below is a sample config file for `.env`:

```
PINOT_CONTROLLER_URL=https://pinot.xxx.yyy.startree.cloud
PINOT_BROKER_HOST=broker.pinot.xxx.yyy.startree.cloud
PINOT_BROKER_PORT=443
PINOT_BROKER_SCHEME=https
PINOT_USERNAME=pinotuser
PINOT_PASSWORD=supersecure
PINOT_TOKEN=Bearer st-token
PINOT_USE_MSQE=true
PINOT_DATABASE=default
```

If you are running Pinot quickstart locally, here is the `.env` file:

```
PINOT_CONTROLLER_URL=http://localhost:9000
PINOT_BROKER_HOST=localhost
PINOT_BROKER_PORT=8000
PINOT_BROKER_SCHEME=http
PINOT_USE_MSQE=true
```

---

### Run the server

```bash
uv --directory . run mcp_pinot/server.py
```

You should see logs indicating that the server is running and listening on STDIO.

---

### (Optional) Test locally with Pinot Quickstart

Start Pinot QuickStart using docker:

```bash
docker run --name pinot-quickstart -p 2123:2123 -p 9000:9000 -p 8000:8000 -d apachepinot/pinot:latest QuickStart -type batch
```

Query MCP Server

```bash
uv --directory . run tests/test_service/test_pinot_quickstart.py
```

This quickstart just checks all the tools and queries the airlineStats table.

---

## Docker Build

### Build the Docker image

```bash
docker build -t mcp-pinot .
```

### Run the container

```bash
docker run -v $(pwd)/.env:/app/.env mcp-pinot
```

Note: Make sure to have your `.env` file configured with the appropriate Pinot cluster settings before running the container.

---

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
              "/path/to/mcp-pinot",
              "run",
              "mcp_pinot/server.py"
          ],
          "env": {
          }
      }
  }
}
```
Replace `/path/to/uv` with the absolute path to the uv command, you can run `which uv` to figure it out.

Replace `/path/to/mcp-pinot` with the absolute path to the folder where you cloned this repo.

You could also configure environment variables here instead of the `.env` file, in case you want to connect to multiple pinot clusters as MCP servers.

---

### Restart Claude Desktop

Claude will now auto-launch the MCP server on startup and recognize the new Pinot-based tools.

---

## Try a Prompt

Once Claude is running, click the hammer 🛠️ icon and try this prompt:

> Can you help me analyse my data in Pinot? Use the Pinot tool and look at the list of tables to begin with.

---

## Developer Notes

- All tools are defined in the `Pinot` class in `utils/pinot_client.py`

Build the project with

```bash
pip install -e ".[dev]"
```

Test the repo with:

```bash
pytest
```
