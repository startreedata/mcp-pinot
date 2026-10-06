# Registry & Marketplace Listing

How `mcp-pinot` is (or can be) listed across MCP registries, and what's automated
vs. manual. Run the manual steps **after** a release publishes the matching version
to PyPI.

## 1. Official MCP Registry — ✅ automated on release

The `publish-mcp-registry` job in [`.github/workflows/release.yml`](.github/workflows/release.yml)
already publishes [`server.json`](server.json) on every stable `v*` tag:

- skips pre-release tags (anything with a `-` suffix, e.g. `v5.0.0-beta.1`): the
  registry marks the highest semver as latest, so a beta would become the default
  version for everyone installing by name,
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

**Pulling a version:** run the **Set MCP Registry status** workflow
([`.github/workflows/mcp-registry-status.yml`](.github/workflows/mcp-registry-status.yml))
with the version and `deleted` (or `deprecated`, or `active` to restore). Only
`deleted` changes which version is latest. It runs as this repository through
GitHub OIDC; a personal `mcp-publisher login github` gets the
`io.github.startreedata/*` namespace only for org owners, so members get a 403.

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

## 8. Anthropic Claude directory — plugin bundle via the developer portal

In-product across Claude (claude.ai, the desktop app, Cowork and Claude Code). The
directory no longer accepts desktop extensions (`.mcpb`); a local server is listed as
a **plugin bundle**, which is [`plugins/startree-pinot/`](plugins/startree-pinot/):
the server pinned to an exact PyPI version through `uvx`, credentials asked for through
`userConfig`, and a `pinot-analytics` skill. Bump the plugin's `version` and the pinned
`mcp-pinot-server==` version together on each release.

- Check locally with `claude plugin validate ./plugins/startree-pinot`.
- Submit at [claude.ai/directory/manage](https://claude.ai/directory/manage): **Submit
  new** → **Plugin bundle**, repository `startreedata/mcp-pinot`, plugin path
  `plugins/startree-pinot`, then **Validate**. It needs a paid Claude plan, an Owner
  role on Team/Enterprise, and a GitHub account connected on claude.ai with push
  access to this repo. The listing belongs to the organization that submits it.
- The separate **MCP connector** listing needs one public HTTPS endpoint, which we
  don't have: each StarTree Cloud environment serves its own `mcp.<domain>` URL.

## Visibility multipliers

- Add GitHub repo **topics**: `mcp`, `model-context-protocol`, `apache-pinot`, `llm`.
- Keep README badges + examples current (directories favor production-quality docs).
- Announce the release (blog/social) and link the official-registry entry.
