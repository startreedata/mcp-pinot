"""Read-only, fail-closed approval and merge gates."""

import re
from urllib.parse import quote

from config import validate_paths

THREAD_QUERY = """
query($owner: String!, $repo: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      headRefOid
      reviewThreads(first: 100, after: $cursor) {
        totalCount
        nodes { isResolved }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
"""


def merge_rule_blockers(api, policy):
    """Require server-enforced checks/reviews before admitting an automatic merge.

    The effective branch rules endpoint returns active repository and parent rules
    using Metadata read permission. Operators must keep the App off bypass lists;
    GitHub hides bypass actors from readers without write access to the ruleset.
    """
    try:
        rules = api.paginate(
            f"rules/branches/{quote(policy['default_branch'], safe='')}",
        )
        required = {
            (check["name"], check["app_id"])
            for check in policy["merge"]["required_checks"]
        }
        app_id = policy["merge"]["policy_check_app_id"]
        if type(app_id) is not int or app_id < 1 or not required:
            return [
                "Automatic merge needs configured CI and policy-check App identities"
            ]
        required.add(("maintainer/policy", app_id))
        strict_checks = set()
        guarded_review = False
        for rule in rules:
            if not isinstance(rule, dict) or not isinstance(rule.get("type"), str):
                raise ValueError("Effective branch rule is malformed")
            if rule["type"] not in {"required_status_checks", "pull_request"}:
                continue
            params = rule.get("parameters")
            if not isinstance(params, dict):
                raise ValueError("Effective branch rule parameters are missing")
            if rule["type"] == "required_status_checks":
                checks = params.get("required_status_checks")
                if not isinstance(checks, list) or any(
                    not isinstance(check, dict)
                    or not isinstance(check.get("context"), str)
                    for check in checks
                ):
                    raise ValueError("Effective required status checks are incomplete")
                if params.get("strict_required_status_checks_policy") is True:
                    for check in checks:
                        integration_id = check.get("integration_id")
                        if type(integration_id) is int and integration_id > 0:
                            strict_checks.add((check["context"], integration_id))
            else:
                count = params.get("required_approving_review_count")
                guarded_review |= (
                    type(count) is int
                    and count >= 1
                    and params.get("dismiss_stale_reviews_on_push") is True
                    and params.get("required_review_thread_resolution") is True
                )
        reasons = [
            f"Branch rules do not strictly require trusted check: {name} (App {app})"
            for name, app in sorted(required - strict_checks)
        ]
        if not guarded_review:
            reasons.append(
                "Branch rules need an approving review, stale-review dismissal, "
                "and resolved review threads"
            )
        return reasons
    except (KeyError, TypeError, ValueError, RuntimeError, AttributeError) as error:
        return [f"Effective branch protection is unknown: {error}"]


def _labels(pr):
    labels = pr.get("labels")
    if not isinstance(labels, list) or any(
        not isinstance(label, dict) or not isinstance(label.get("name"), str)
        for label in labels
    ):
        raise ValueError("Pull request labels are incomplete")
    return {label["name"] for label in labels}


def _threads(api, pr, head):
    owner, repo = api.repo.split("/", 1)
    cursor, seen, total, unresolved = None, set(), None, False
    count = 0
    for _ in range(50):
        data = api.graphql(
            THREAD_QUERY,
            {
                "owner": owner,
                "repo": repo,
                "number": pr["number"],
                "cursor": cursor,
            },
        )
        request = data["repository"]["pullRequest"]
        if request["headRefOid"] != head:
            raise ValueError(
                "Pull request head changed during review-thread inspection"
            )
        connection = request["reviewThreads"]
        current_total = connection.get("totalCount")
        if type(current_total) is not int or current_total < 0:
            raise ValueError("Review thread count is missing")
        if total is not None and current_total != total:
            raise ValueError("Review threads changed during pagination")
        total = current_total
        nodes = connection.get("nodes")
        if not isinstance(nodes, list) or len(nodes) > 100:
            raise ValueError("Review threads are incomplete")
        for thread in nodes:
            if (
                not isinstance(thread, dict)
                or type(thread.get("isResolved")) is not bool
            ):
                raise ValueError("Review thread resolution is unknown")
            unresolved |= not thread["isResolved"]
        count += len(nodes)
        page = connection["pageInfo"]
        if type(page.get("hasNextPage")) is not bool:
            raise ValueError("Review thread pagination is unknown")
        if not page["hasNextPage"]:
            if count != total:
                raise ValueError("Review threads were truncated")
            return unresolved
        cursor = page.get("endCursor")
        if not isinstance(cursor, str) or not cursor or cursor in seen or not nodes:
            raise ValueError("Review thread pagination did not advance")
        seen.add(cursor)
    raise ValueError("Review threads exceeded the complete pagination limit")


def approver_logins(policy):
    """Include the separately configured approver in the native merge gate."""
    logins = set(policy["merge"]["independent_approvers"])
    review = policy.get("review", {})
    if review.get("mode") == "approve" and review.get("login"):
        logins.add(review["login"])
    return logins


def _latest_reviews(api, pr):
    reviews = api.paginate(f"pulls/{pr['number']}/reviews")
    latest = {}
    for review in reviews:
        user = review.get("user") if isinstance(review, dict) else None
        if (
            not isinstance(user, dict)
            or not isinstance(user.get("login"), str)
            or type(review.get("id")) is not int
            or not isinstance(review.get("state"), str)
        ):
            raise ValueError("Pull request review history is incomplete")
        if review["state"] not in {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}:
            continue
        login = user["login"].casefold()
        previous = latest.get(login)
        if previous is None or review["id"] > previous["id"]:
            latest[login] = review
    return latest


def _reviews(api, policy, pr, head, require_human):
    latest = _latest_reviews(api, pr)
    reasons = []
    if any(review["state"] == "CHANGES_REQUESTED" for review in latest.values()):
        reasons.append("A reviewer still requests changes")
    author = pr.get("user", {}).get("login", "").casefold()
    writer = policy["app_login"].casefold()
    automated_reviewer = policy.get("review", {}).get("login", "").casefold()
    independent = {login.casefold() for login in approver_logins(policy)}
    approved = False
    for login, review in latest.items():
        if (
            review["state"] != "APPROVED"
            or review.get("commit_id") != head
            or review["user"].get("type") not in {"User", "Bot"}
            or login in (author, writer)
            or (require_human and login == automated_reviewer)
            or login not in independent
        ):
            continue
        if review["user"]["type"] == "Bot":
            if not require_human:
                approved = True
            continue
        permission = api.request(
            f"collaborators/{quote(review['user']['login'], safe='')}/permission"
        )
        if (
            permission.get("permission") in {"admin", "maintain", "write"}
            and permission.get("user", {}).get("type") == "User"
        ):
            approved = True
    if not approved:
        kind = "human with write access" if require_human else "configured reviewer"
        reasons.append(f"No independent {kind} approved the current head")
    return reasons


def _checks(api, policy, head):
    checks = api.paginate(f"commits/{head}/check-runs?filter=all")
    reasons = []
    required = policy["merge"]["required_checks"]
    if not required:
        return ["No trusted required checks are configured"]
    for requirement in required:
        candidates = [
            check
            for check in checks
            if check.get("name") == requirement["name"]
            and check.get("app", {}).get("id") == requirement["app_id"]
        ]
        if not candidates:
            reasons.append(f"Trusted check is missing: {requirement['name']}")
            continue
        if any(type(check.get("id")) is not int for check in candidates):
            raise ValueError("Required check has an unknown run identity")
        latest = max(candidates, key=lambda check: check["id"])
        if (
            latest.get("head_sha") != head
            or latest.get("status") != "completed"
            or latest.get("conclusion") != "success"
        ):
            reasons.append(
                f"Current-head check has not succeeded: {requirement['name']}"
            )
    return reasons


def _complete_patch(file):
    """Require every supplied hunk and GitHub's change counters to agree."""
    status = file.get("status")
    additions, deletions = file.get("additions"), file.get("deletions")
    if (
        status not in {"added", "modified", "removed", "renamed"}
        or type(additions) is not int
        or type(deletions) is not int
        or additions < 0
        or deletions < 0
        or (status == "renamed" and not isinstance(file.get("previous_filename"), str))
    ):
        return False
    patch = file.get("patch")
    if not patch and status == "renamed" and additions == deletions == 0:
        return True
    if not isinstance(patch, str) or not patch:
        return False
    expected, observed = None, [0, 0]
    added = removed = 0
    for line in patch.splitlines():
        header = re.fullmatch(
            r"@@ -[0-9]+(?:,([0-9]+))? \+[0-9]+(?:,([0-9]+))? @@.*", line
        )
        if header:
            if expected is not None and observed != expected:
                return False
            expected = [
                int(count) if count is not None else 1 for count in header.groups()
            ]
            observed = [0, 0]
        elif expected is None:
            return False
        elif line == r"\ No newline at end of file":
            continue
        elif line.startswith(" "):
            observed[0] += 1
            observed[1] += 1
        elif line.startswith("+"):
            observed[1] += 1
            added += 1
        elif line.startswith("-"):
            observed[0] += 1
            removed += 1
        else:
            return False
    return (
        expected is not None
        and observed == expected
        and (added, removed)
        == (
            additions,
            deletions,
        )
    )


def approval_blockers(api, policy, pr, reviewer_login):
    """Gate an independent native approval, including PRs written by other users."""
    reasons = []
    try:
        head, number = pr["head"]["sha"], pr["number"]
        if (
            not isinstance(head, str)
            or not re.fullmatch(r"[0-9a-f]{40}", head)
            or type(number) is not int
            or number < 1
        ):
            raise ValueError("Pull request identity is incomplete")
        author = pr["user"]["login"]
        if (
            not isinstance(author, str)
            or not author
            or not isinstance(reviewer_login, str)
        ):
            raise ValueError("Reviewer or pull request author identity is missing")
        if not reviewer_login or reviewer_login.casefold() in {
            author.casefold(),
            policy["app_login"].casefold(),
        }:
            reasons.append(
                "Reviewer must differ from the pull request author and writer"
            )
        if not policy["app_login"]:
            reasons.append("Configured writer identity is missing")
        review = policy["review"]
        if review["identity_type"] == "User":
            permission = api.request(
                f"collaborators/{quote(reviewer_login, safe='')}/permission"
            )
            if (
                permission.get("permission") not in {"admin", "maintain", "write"}
                or permission.get("user", {}).get("type") != "User"
            ):
                reasons.append("Configured reviewer User does not have write access")
        if pr.get("state") != "open" or pr.get("merged") is not False:
            reasons.append("Pull request is not open and unmerged")
        if pr.get("draft") is not False:
            reasons.append("Draft status is true or unknown")
        if policy["hold_label"] in _labels(pr):
            reasons.append("Pull request is on hold")
        if pr.get("mergeable") is not True:
            reasons.append("GitHub mergeability is false or unknown")
        base = api.request(f"git/ref/heads/{quote(policy['default_branch'], safe='')}")
        base_sha = base["object"]["sha"]
        if (
            not isinstance(base_sha, str)
            or not re.fullmatch(r"[0-9a-f]{40}", base_sha)
            or pr["base"].get("ref") != policy["default_branch"]
            or pr["base"].get("sha") != base_sha
        ):
            reasons.append("Pull request base differs from the current default branch")
        comparison = api.request(f"compare/{base_sha}...{head}")
        if comparison.get("merge_base_commit", {}).get("sha") != base_sha:
            reasons.append("Pull request does not include the current base branch")
        files = api.paginate(f"pulls/{number}/files")
        if (
            type(pr.get("changed_files")) is not int
            or len(files) != pr["changed_files"]
        ):
            reasons.append("Pull request file list is truncated or changed")
        scope = {
            **policy,
            "limits": {**policy["limits"], "max_files": review["max_files"]},
        }
        reasons.extend(validate_paths(files, scope))
        if any(
            not isinstance(file, dict) or not _complete_patch(file) for file in files
        ):
            reasons.append("Pull request diff is binary, truncated, or incomplete")
        input_bytes = sum(
            len((file.get(field) or "").encode("utf-8"))
            for file in files
            for field in ("filename", "previous_filename", "patch")
        )
        if input_bytes > review["max_input_bytes"]:
            reasons.append("Pull request diff exceeds review.max_input_bytes")
        reasons.extend(_checks(api, policy, head))
        if any(
            login != reviewer_login.casefold()
            and review["state"] == "CHANGES_REQUESTED"
            for login, review in _latest_reviews(api, pr).items()
        ):
            reasons.append("Another reviewer still requests changes")
        if _threads(api, pr, head):
            reasons.append("Unresolved review threads remain")
    except (
        KeyError,
        TypeError,
        ValueError,
        RuntimeError,
        AttributeError,
        UnicodeError,
    ) as error:
        reasons.append(f"Approval evidence is incomplete: {error}")
    return list(dict.fromkeys(reasons))


def evaluate(api, policy, pr, task, *, require_mergeable=True):
    """Return blockers without changing reviews, checks, labels, branches, or state."""
    reasons = []
    try:
        head = pr["head"]["sha"]
        number = pr["number"]
        if not re.fullmatch(r"[0-9a-f]{40}", head) or type(number) is not int:
            raise ValueError("Pull request identity is incomplete")
        if task.get("expected_sha") != head:
            reasons.append("Pull request head differs from the managed task snapshot")
        if (
            pr["head"].get("repo", {}).get("full_name") != api.repo
            or pr["head"].get("ref") != task.get("branch")
            or task.get("pr_number") != number
        ):
            reasons.append(
                "Pull request branch/repository differs from the managed task"
            )
        if pr.get("state") != "open" or pr.get("merged") is not False:
            reasons.append("Pull request is not open and unmerged")
        if pr.get("draft") is not False:
            reasons.append("Draft status is true or unknown")
        labels = _labels(pr)
        if policy["hold_label"] in labels:
            reasons.append("Pull request is on hold")
        if policy["managed_label"] not in labels:
            reasons.append("Pull request is not managed by this controller")
        if pr.get("mergeable") is not True or (
            require_mergeable and pr.get("mergeable_state") != "clean"
        ):
            reasons.append("GitHub mergeability/protection state is not clean")
        base = api.request(f"git/ref/heads/{quote(policy['default_branch'], safe='')}")
        base_sha = base["object"]["sha"]
        if (
            not re.fullmatch(r"[0-9a-f]{40}", base_sha)
            or pr["base"].get("ref") != policy["default_branch"]
            or pr["base"].get("sha") != base_sha
            or task.get("base_sha") != base_sha
        ):
            reasons.append(
                "Base branch moved or differs from the trusted task snapshot"
            )
        comparison = api.request(f"compare/{base_sha}...{head}")
        if comparison.get("merge_base_commit", {}).get("sha") != base_sha:
            reasons.append("Pull request does not include the current base branch")
        files = api.paginate(f"pulls/{number}/files")
        if (
            type(pr.get("changed_files")) is not int
            or len(files) != pr["changed_files"]
        ):
            reasons.append("Pull request file list is truncated or changed")
        reasons.extend(validate_paths(files, policy))
        reasons.extend(_checks(api, policy, head))
        dependency = (
            task.get("kind") == "dependency"
            or pr.get("user", {}).get("login") in policy["dependencies"]["authors"]
        )
        reasons.extend(
            _reviews(
                api,
                policy,
                pr,
                head,
                require_human=dependency
                and policy["merge"]["require_human_for_dependencies"],
            )
        )
        if _threads(api, pr, head):
            reasons.append("Unresolved review threads remain")
        if dependency:
            user = pr.get("user", {})
            manifest_paths = set(policy["dependencies"]["manifest_paths"])
            if (
                task.get("kind") != "dependency"
                or not policy["dependencies"]["enabled"]
                or user.get("type") != "Bot"
                or user.get("login") not in policy["dependencies"]["authors"]
                or user.get("login") != task.get("dependency_author")
            ):
                reasons.append(
                    "Dependency task does not match an authorized dependency bot"
                )
            if any(
                file.get("filename") not in manifest_paths
                or (
                    file.get("previous_filename") is not None
                    and file["previous_filename"] not in manifest_paths
                )
                for file in files
            ):
                reasons.append("Dependency diff exceeds the configured manifest paths")
    except (KeyError, TypeError, ValueError, RuntimeError, AttributeError) as error:
        reasons.append(f"Policy evidence is incomplete: {error}")
    return list(dict.fromkeys(reasons))
