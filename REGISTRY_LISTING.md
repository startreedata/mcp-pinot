# Registry & Marketplace Listing

How `mcp-pinot` is (or can be) listed across MCP registries, and what's automated
vs. manual. Run the manual steps **after** a release publishes the matching version
to PyPI.

## 1. Official MCP Registry — ✅ automated on release

The `publish-mcp-registry` job in [`.github/workflows/release.yml`](.github/workflows/release.yml)
already publishes [`server.json`](server.json) on every `v*` tag:

- sets the version on all packages from the tag,
- waits for the PyPI package to be visible,
- authenticates with `mcp-publisher login github-oidc` (GitHub OIDC — the
  `io.github.startreedata/*` namespace is authorized by the repo owner, so **no
  manual login is needed**),
- runs `mcp-publisher publish`.

Ownership is verified because the `mcp-name: io.github.startreedata/mcp-pinot`
marker in [README](README.md) ships in the PyPI long-description.

**Verify after release:**
```bash
curl "https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.startreedata/mcp-pinot"
```

> `server.json` now also declares the OCI (Docker) package `ghcr.io/startreedata/mcp-pinot`.

## 2. Glama — ✅ auto-indexed

Glama crawls open-source MCP servers from GitHub; the README already shows its
badge. Optionally claim the listing at <https://glama.ai> to manage metadata.

## 3. Smithery — manual

[`smithery.yaml`](smithery.yaml) is included (runs the PyPI package via `uvx` over
stdio). Validate against the current [Smithery docs](https://smithery.ai/docs),
then connect the repo / publish at <https://smithery.ai>. Smithery may prefer the
Docker image — `ghcr.io/startreedata/mcp-pinot` is available.

## 4. PulseMCP — manual (also auto-crawls)

Use the **Submit** button at <https://www.pulsemcp.com>. It also indexes from
GitHub + the official registry, so listing in #1 helps here.

## 5. mcp.so — manual

Submit via the form at <https://mcp.so>.

## 6. Docker MCP Catalog — manual PR

Open a PR at <https://github.com/docker/mcp-registry> (you ship a Docker image).
Prefer the **Docker-built** tier for signed images + SBOM + provenance.

## 7. awesome-mcp-servers — manual PR

Add an entry via PR to <https://github.com/punkpeye/awesome-mcp-servers> (and any
other curated lists) for SEO/discovery.

## 8. Anthropic Claude Connectors Directory — manual, highest reach

In-product across Claude. Requirements:

- **Remote, internet-hosted** server (HTTPS) using **OAuth 2.0** — supported via
  `AUTH_PROVIDER` + HTTP transport.
- Every tool annotated with `title` + `readOnlyHint`/`destructiveHint` — ✅ done.
- A **Privacy Policy** at a stable HTTPS URL — see [PRIVACY.md](PRIVACY.md) (draft;
  needs legal review + hosting).
- Submit from **Claude.ai → admin settings** (needs a **Team/Enterprise** org).

## Visibility multipliers

- Add GitHub repo **topics**: `mcp`, `model-context-protocol`, `apache-pinot`, `llm`.
- Keep README badges + examples current (directories favor production-quality docs).
- Announce the release (blog/social) and link the official-registry entry.
