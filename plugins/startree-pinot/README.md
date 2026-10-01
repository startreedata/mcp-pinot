# StarTree Pinot

Ask Claude about the data in your Apache Pinot cluster or StarTree Cloud environment. This plugin connects Claude to StarTree's open-source [Pinot MCP server](https://github.com/startreedata/mcp-pinot), so Claude can find your tables, read their schemas and configs, inspect segments, and answer questions with read-only SQL. It also includes a skill that teaches Claude how to explore an unfamiliar cluster and keep queries small.

## What you need

- A Pinot cluster or StarTree Cloud environment, and its controller and broker URLs. A local [Pinot quickstart](https://docs.pinot.apache.org/basics/getting-started/running-pinot-locally) works for trying it out.
- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed, because the plugin starts the server with `uvx`.
- A token, or a username and password, if your cluster requires authentication. On StarTree Cloud you can create an API token in the Data Portal.

When you enable the plugin, Claude asks for the controller URL, broker URL and any credentials. Tokens and passwords are stored in your system's secure credential store, not in a settings file.

## What it runs and where data goes

- On first use, `uvx` downloads the `mcp-pinot-server` package, pinned to version 4.1.0, from PyPI and runs it on your machine over stdio.
- The server talks only to the controller and broker URLs you configure, and sends your token or password only to them.
- Query results come back into your Claude conversation. The plugin stores nothing and sends nothing anywhere else. See [PRIVACY.md](https://github.com/startreedata/mcp-pinot/blob/main/PRIVACY.md).

## Tools

- **Explore:** `list_tables`, `get_schema`, `get_table_config`
- **Query:** `read_query` runs one read-only `SELECT` (or `WITH ... SELECT`) and returns results a page at a time
- **Storage:** `get_table_size`, `list_segments`, `list_segment_metadata`, `get_segment_index_metadata`
- **Diagnose:** `test_connection` checks that the broker and controller are reachable
- **Change:** `create_schema`, `update_schema`, `create_table_config`, `update_table_config`, `reload_table_filters`

## Safety

The server parses every query before it reaches Pinot and rejects anything other than a single read-only statement. Results are paginated, so a large table can't flood a conversation. Every change tool previews first and needs a one-time confirmation token from that preview before it applies anything, and the skill tells Claude to apply a change only after you confirm it. If you only want read access, use credentials that can only read.

## StarTree Cloud hosted MCP

If your StarTree Cloud environment has the MCP Server component enabled, you can also connect to it directly at `https://mcp.<your-environment>.startree.cloud/mcp` and sign in with your StarTree login instead of running the server locally. Ask your StarTree contact for the URL.

## Support

Report problems and ask questions in [GitHub issues](https://github.com/startreedata/mcp-pinot/issues). Security reports follow [SECURITY.md](https://github.com/startreedata/mcp-pinot/blob/main/SECURITY.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
