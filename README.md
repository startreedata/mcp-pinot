# MCP Pinot Server

This project is a Python-based [Model Context Protocol (MCP)](https://github.com/anthropic-ai/mcp) server for interacting with Apache Pinot. It is designed to integrate with Claude Desktop to enable real-time analytics and metadata queries on a Pinot cluster.

---

## 🧩 Features
- List tables, segments, and schema info from Pinot
- Execute read-only SQL queries
- View index/column-level metadata
- Designed to assist business users via Claude integration

---

## 🚀 Quick Start

### 1. Install `uv` (if not already installed)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
Then **restart your terminal** to register the `uv` command.

---

### 2. Clone and set up the project
```bash
git clone git@github.com:startreedata/mcp-pinot.git
cd mcp-pinot
uv init mcp-pinot
```

---

### 3. Install dependencies
```bash
uv add httpx
uv add pinotdb
uv add "mcp[cli]"
uv add pandas
uv add requests
```

---

### 4. Configure Pinot Cluster connection
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
```

---

### 5. Run the server
```bash
uv run mcp_pinot/server.py
```

You should see logs indicating that the server is running and listening on STDIO.

---

## 🧠 Claude Desktop Integration

### 1. Open Claude’s config file
```bash
vi ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

### 2. Add an MCP server entry
```json
{
  "mcpServers": {
      "pinot_mcp_claude": {
          "command": "uv",
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
Replace `/path/to/mcp-pinot` with the absolute path to the folder where you cloned this repo.

You could also configure environment variables here instead of the `.env` file, in case you want to connect to multiple pinot clusters.

---

### 3. Restart Claude Desktop

Claude will now auto-launch the MCP server on startup and recognize the new Pinot-based tools.

---

## ✅ Try a Prompt
Once Claude is running, click the hammer 🛠️ icon and try this prompt:

> Can you help me analyse my data in Pinot? Use the Pinot tool and look at the list of tables to begin with.

---

## 🧪 Developer Notes
- You can add your credentials and Pinot URLs to `utils/pinot_client.py` or load them from `.env`
- All tools are defined in the `Pinot` class in `utils/pinot_client.py`
- Modular design allows easy extension for more tools, logging, or tests

---

## 🐳 Optional: Docker Support
Coming soon. Let us know if you want a Dockerfile right away.

---

## 📜 License
MIT or Apache 2.0 depending on your preference.

---

Questions? Ping the maintainer or post issues on the GitHub repo.

