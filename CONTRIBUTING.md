# Contributing to mcp-pinot

Thanks for your interest in improving the Apache Pinot MCP server! This guide
covers the basics for a smooth contribution.

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) for environment and dependency
management.

```bash
git clone https://github.com/startreedata/mcp-pinot.git
cd mcp-pinot
uv sync                      # create the venv and install deps (incl. dev tools)
```

## Quality gates

All of these run in CI; please run them locally before opening a PR:

```bash
uv run ruff check .          # lint
uv run ruff format --check . # formatting
uv run mypy                  # static type checking
uv run pytest --cov=mcp_pinot --cov-report=term-missing   # tests + coverage (>= 85%)
```

- Code is type-annotated and checked with mypy; new code should be typed.
- Tests use the in-memory FastMCP `Client` pattern (see `tests/test_server.py`).
  Add tests for new tools, config, and error paths.
- Keep `CHANGELOG.md` up to date under the `## [Unreleased]` section, following
  [Keep a Changelog](https://keepachangelog.com/).

## Pull requests

1. Branch off `main`.
2. Make focused commits with clear messages.
3. Ensure the quality gates above pass.
4. Open a PR describing the change and its motivation; link any related issues.

## Security

Please report vulnerabilities privately as described in
[SECURITY.md](SECURITY.md) rather than opening a public issue.

## Code of Conduct

By participating, you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md).
