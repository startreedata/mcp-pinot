# Repo Maintainer

Repo Maintainer reconciles an authorized GitHub work queue with repository state.
The Python controller and independent reviewer are generic;
`.github/maintainer/policy.toml` supplies this repository's paths, validation
commands, CI names, identities, and separate writer/reviewer budgets.
The checked-in mcp-pinot example starts in **observe** mode. No installation,
GitHub App, credentials, labels, ledger issue, or external notifications have been
activated by adding these files.

## What it does

The controller polls every 15 minutes and wakes on issue, PR, review, worker, and
configured CI events. A collaborator with write, maintain, or admin permission
authorizes an issue using `ai:ready`. An issue edit changes
its authorization snapshot and must be authorized again. Dependabot PRs are
tracked separately; dependency changes still require independent human approval.

The writer ledger is one authenticated writer comment on a dedicated issue.
Each task has a one-time lease, exact source SHA, attempt count, and bounded
repair count.
The controller reserves the full per-run model budget before dispatch and counts
that reservation against the UTC daily cap. A stalled lease becomes a human
handoff; it does not silently launch a duplicate worker. `ai:hold` stops work on
an item; `ai:needs-human` makes a blocked item visible.

A writer may post `/maintainer resume` on the original source issue (or the
original dependency PR) to refresh its
authorization/head snapshot and resume a human handoff. This preserves attempts,
repair rounds, and reserved spend. On the dedicated ledger issue,
`/maintainer pause-repo` pauses the queue and `/maintainer resume-repo` resumes it
only after configured CI succeeds on the current default-branch tip. Commands
are accepted only from newly created comments by an authorized collaborator.
Actions run summaries show the current reconciliation result.

Workers edit files, validate changes, and publish a PR. The controller subsequently
evaluates the exact PR head against configured CI, changed paths, independent
review, unresolved discussions, base freshness, and mergeability. The controller
does not submit approving reviews or resolve review threads. Successful merge is
followed by default-branch CI before the task is considered done.

An independent reviewer can review and approve PRs from the writer and other
contributors, including fork PRs. It does not require `ai:managed` or an issue
authorization. It rejects its own PRs and the writer's identity as reviewer.
Native reviews bind to the exact current PR head. `review.mode=review` allows
comments and requests for changes; `approve` additionally allows approval only
after current required CI, path, and unresolved-review rules pass. A clean review
can be retained while CI finishes and published as approval without another
model call. The reviewer never merges.

## Workflow boundaries

`maintainer-controller.yml` executes only code checked out from the repository's
default branch. Its observe job uses a read-only Actions token and makes no writes
or model calls. Active jobs use repository-scoped GitHub App tokens or a configured
user token. Review
events pass through a separate signal workflow with no checkout or secrets;
the controller reads live GitHub evidence after waking.

`maintainer-worker.yml` has four separate jobs, with a combined timeout budget of
45 minutes:

1. **prepare** reads the live authorization and lease with the writer token, records
   the immutable controller version, and uploads the prepared task.
2. **edit** checks out the exact source SHA and runs pinned Claude Code in an
   isolated container. Claude receives only Read, Edit, Write, Glob, and Grep tools,
   a model key, and bounded turns/spend. It receives no GitHub credential and
   cannot execute tests or shell commands. Trusted control files and the prepared
   task are mounted read-only.
3. **validate** downloads the original prepared task and patch in a fresh job,
   checks patch boundaries, then runs trusted validation commands inside a
   separate container with no secrets. Candidate build/test code never runs in
   a publisher or controller process.
4. **publish** starts from a clean checkout, verifies the current lease,
   authorization, source SHA, and successful validation job for the current
   workflow attempt, then commits and pushes the validated patch with the writer
   token. It creates or updates a non-draft managed PR without force pushing.

`maintainer-reviewer.yml` is separate from the writer. It wakes on PR events,
configured CI completion, and manual dispatch, with three jobs:

1. **prepare** checks out trusted default-branch control code, authenticates the
   independent reviewer, reserves its lease/budget, and reads bounded PR metadata
   and diffs through GitHub APIs. Its ledger is a separate reviewer-authored
   comment on the same `state_issue`.
2. **review** starts a fresh Claude session with Read, Glob, and Grep tools and
   a separate model credential. Trusted code and evidence are mounted read-only;
   only the JSON result directory is writable. It receives no GitHub credential,
   never checks out the PR head, and never executes contributor code.
3. **publish** obtains a fresh reviewer token and rechecks identity, lease, policy,
   current head, and approval gates before creating a native GitHub review.

Reviewer runs are serialized per repository. Repeated events deduplicate on PR
head and review state; the reviewer has independent daily and per-run budgets.

The CLI is pinned to Claude Code 2.1.295. Its `--bare`, explicit tool allow-list,
`--max-turns`, and `--max-budget-usd` flags bound the edit session. The CLI's
spend estimate can exceed its cap, so allow billing headroom and configure a
separate provider account limit. See the
[Claude Code CLI reference](https://code.claude.com/docs/en/cli-reference).

## Repository setup

Review and merge the implementation through the repository's normal process.
Before enabling writes, an administrator must:

1. Create a writer GitHub App installed only on intended repositories. Grant
   Contents, Issues, Pull requests, Checks, and Actions read/write permissions;
   Metadata is read-only. Do not grant organization administration or workflow
   modification permissions. The App must have no branch-rule bypass.
2. Set Actions variable `MAINTAINER_APP_ID`, secret
   `MAINTAINER_APP_PRIVATE_KEY`, and secret `MAINTAINER_ANTHROPIC_API_KEY`.
   Leave `MAINTAINER_WRITER_AUTH=app` (the default), set `app_login` to the App's
   exact bot login and `writer_type="Bot"` in policy, and `state_issue` to a
   dedicated issue number. Create `ai:ready`, `ai:managed`, `ai:hold`, and
   `ai:needs-human` labels.
3. Review allowed/protected paths and setup/validation commands. Keep all
   workflows, policy, agent instructions, credentials, auth, and release paths
   protected. Confirm the pinned Docker images run on the intended hosted runner.
4. Set `MAINTAINER_MODE=observe` and inspect a manual controller run. The example
   CI names/check names are verified for mcp-pinot; other repositories need their
   own exact check names and producing App IDs.
5. Set `MAINTAINER_MODE=maintain` to allow authorized tasks, repairs, and PR
   publication. Keep `merge.enabled=false` for this rollout.

Two separate Apps are recommended: one writer/controller App and one reviewer
App. Two separate user accounts also work. For a user writer, set
`MAINTAINER_WRITER_AUTH=user`, secret `MAINTAINER_WRITER_TOKEN`, the exact account
login in `app_login`, and `writer_type="User"`. Use a repository-scoped
fine-grained PAT with Contents, Issues, Pull requests, and Actions write;
Checks read; and Metadata read. The account needs write access and no branch-rule
bypass. The controller verifies the token's actual `/user` identity.
It uses the trusted Actions token solely to create `maintainer/policy`; for a
user writer, set `merge.policy_check_app_id=15368` (GitHub Actions). The Actions
token never enters the model or validation container.

Configure the reviewer independently, even if the writer remains in observe mode:

- For an App, set `MAINTAINER_REVIEWER_AUTH=app` (the default), Actions variable
  `MAINTAINER_REVIEWER_APP_ID`, secret `MAINTAINER_REVIEWER_APP_PRIVATE_KEY`, the
  App's exact bot login in `review.login`, and `review.identity_type="Bot"`.
  Grant Contents, Checks, and Actions read; Issues and Pull requests write; and
  Metadata read. Do not grant Contents write, workflow modification, or bypass.
- For a user, set `MAINTAINER_REVIEWER_AUTH=user`, secret
  `MAINTAINER_REVIEWER_TOKEN`, the actual account login in `review.login`, and
  `review.identity_type="User"`. Use a fine-grained PAT with Contents, Checks,
  and Actions read; Issues and Pull requests write; and Metadata read. Ensure the
  account has write access so GitHub counts its approvals.
- Set a distinct secret `MAINTAINER_REVIEWER_ANTHROPIC_API_KEY`. The reviewer uses
  a fresh model session and does not reuse the writer's conversation or result.
- Configure `review.mode` and Actions variable `MAINTAINER_REVIEWER_MODE` to
  `review` for the initial rollout; use `approve` after validating the required
  check names/App IDs, paths, and dependency policy. Both default to observe.
  Keep the reviewer login different from `app_login`. Neither workflow creates
  accounts, installs Apps, nor submits reviews before activation.

GitHub's Pull requests write permission also authorizes review API calls; it
cannot restrict a writer token to PR publication alone. Separation is enforced
by the workflows and admission policy: writer/controller jobs never submit
approving reviews, and the controller rejects the writer's approvals. Reviewer
credentials have no Contents write permission and never enter writer jobs.

The reviewer can operate across all non-draft PRs targeting the configured default
branch; it does not depend on writer mode, managed labels, or merge activation.
Manual dispatch accepts a PR number. Configure reviewer limits separately under
`[review]`; the defaults are 30 files, 200,000 input bytes, 30 turns, $5 per run,
$20 per UTC day, and a 5,400-second lease. Out-of-policy paths can receive review
feedback while approval remains blocked by policy. Dependency PRs can receive
native review and approval from either reviewer account type. With the default
`merge.require_human_for_dependencies=true`, the controller additionally requires
an allow-listed external human's current-head approval before merging. The
configured automated reviewer does not satisfy this manual gate, even when it
uses a User account. Setting the option to false permits its independent approval
to satisfy dependency merge review policy.

Autonomous merging requires a separate policy change: set
`merge.independent_approvers` to explicitly trusted reviewer logins, configure the
exact required check names/App IDs and the policy-check producer's
`merge.policy_check_app_id`, enable `merge.enabled`, and set
`MAINTAINER_MODE=autopilot`. The MVP requires an effective branch **ruleset** with
strict required status checks covering every configured CI name/App ID plus
`maintainer/policy` from the configured App, and independent PR approval. Classic
branch protection alone is insufficient for this probe: reading its policy may
require administration access, which this App does not receive. The controller
reads effective rules through `/rules/branches/<branch>` and blocks merging when
the required guarantees cannot be verified. Do not give the App bypass rights.

The controller accepts approval from an independent App or user only when its
exact login is explicitly allow-listed, the review is an actual `APPROVED` review
on the current head, and GitHub counts it as approval. A Copilot comment or
summary does not grant approval. This repository currently has an organization
rule requesting Copilot reviews on
non-draft pushes. That rule is independent of this review workflow. These
workflows do not activate Copilot. The configured `review.login` is automatically
accepted by the controller when reviewer mode is `approve`; other trusted human
or bot reviewers belong in `merge.independent_approvers`.

Optional Slack notifications require `slack.channel` and secret
`MAINTAINER_SLACK_BOT_TOKEN`. The bot needs `chat:write` and access to that
channel. Notifications report meaningful state changes in a per-task thread;
unchanged polls remain quiet.

## Local inspection and tests

The controller uses Python 3.12 and its standard library. It does not add a
dependency to the MCP server or change application packaging.

```bash
GH_TOKEN="$(gh auth token)" GITHUB_REPOSITORY=startreedata/mcp-pinot \
  python .github/maintainer/controller.py --dry-run
python -m unittest discover -s .github/maintainer/tests -v
```

A local dry run needs read access to GitHub. It cannot mutate the ledger,
dispatch workers, merge, or notify Slack. `maintainer-checks.yml` runs the same
standard-library tests on controller changes without secrets. Its container smoke
job builds the actual pinned worker image, checks the CLI versions, and repeats
the controller tests with a non-root user, a read-only control mount, an ephemeral
home/workspace, no network, and no credentials. Existing app CI,
Quickstart, Docker, Helm, Pages, and release workflows retain their triggers.

## Install into other repositories

The installer copies the same scripts, workflows, prompts, tests, documentation,
and issue form into local checkouts. It never calls GitHub, fetches, rebases,
commits, pushes, creates labels, or changes branch protection. All targets are
preflighted before any file is written. Differing runtime files require explicit
`--overwrite`; an existing repository policy is always preserved.

```bash
python .github/maintainer/install.py \
  --target /absolute/path/repo-a --target /absolute/path/repo-b --dry-run
python .github/maintainer/install.py \
  --target /absolute/path/repo-a --target /absolute/path/repo-b
```

The default branch comes from each checkout's local `origin/HEAD`. If that ref
is absent, supply `--default-branch main` with the actual branch name. Optional
repeated `--ci-workflow 'CI'` arguments configure completion wakeups for new
targets. Scheduled polling remains available when an external CI has no Actions
workflow wakeup.

New target policies use observe mode for both writer and reviewer, disable merging,
and leave repository paths, commands, dependency manifests, check names, and
identities empty. Configure
these fields before enabling maintain mode. Existing CI and release workflows
are not copied or replaced. The shared validation image contains Python, Git,
Node, Claude Code, and uv; repositories requiring Java, Go, or other toolchains
must customize the trusted image before activation.

The mcp-pinot example uses the `uv` Dependabot ecosystem so updates maintain
`pyproject.toml` and `uv.lock` together. This is supported by
[uv's Dependabot integration](https://docs.astral.sh/uv/guides/integration/dependabot/)
and [GitHub's package ecosystem reference](https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference#package-ecosystem).
The existing lock regenerated with uv 0.11.0 was checked with the repository's
uv 0.8.22; no CI tool upgrade is included.
