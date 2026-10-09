# Repository maintainer integration

The reusable controller, coding worker, independent reviewer, and installer are
maintained in [startree-code-review-bot](https://github.com/startreedata/startree-code-review-bot).
This repository owns its policy and installed workflow orchestration.

See the central [setup and operations guide](https://github.com/startreedata/startree-code-review-bot/blob/main/docs/repo-maintainer.md)
for installation, identities, budgets, branch rules, hold/resume, and updates.

## This repository

- `.github/maintainer/policy.toml` defines allowed paths, validation commands,
  required check producers, and dependency manifests for mcp-pinot.
- Writer and reviewer identities can each be a GitHub App or a separate user.
  Reviewer/approver cannot be the writer, and can review PRs from other users.
- Controller and reviewer start in `observe`; merging is disabled.
- The workflow runtime is pinned to one central commit. No runtime code or
  tests are copied into this repository.
- mcp-pinot is public and the central repository is private. Installed local
  orchestration loads the pinned runtime using a read-only
  `MAINTAINER_RUNTIME_TOKEN`; it keeps writer/reviewer credentials in their
  respective jobs and gives candidate validation no credentials.

Install or update from the central checkout, replacing `CENTRAL_SHA` with a
reviewed full commit SHA:

```sh
uv run startree-repo-maintainer install \
  --target /path/to/mcp-pinot \
  --deployment local --ref CENTRAL_SHA --overwrite
```

The installer preserves existing policy bytes. Configure the independent
identities, state issue, repository secrets/variables, and current branch rules
before activation. Use the same installer for other repositories and fill each
consumer's own paths, test commands, and CI policy.
