"""Read-only, fail-closed merge gate for a managed pull request."""

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


def _reviews(api, policy, pr, head, require_human):
    reviews = api.paginate(f"pulls/{pr['number']}/reviews")
    latest = {}
    for review in reviews:
        user = review.get("user")
        if (
            not isinstance(user, dict)
            or not isinstance(user.get("login"), str)
            or type(review.get("id")) is not int
            or not isinstance(review.get("state"), str)
        ):
            raise ValueError("Pull request review history is incomplete")
        if review["state"] not in {"APPROVED", "CHANGES_REQUESTED", "DISMISSED"}:
            continue
        previous = latest.get(user["login"])
        if previous is None or review["id"] > previous["id"]:
            latest[user["login"]] = review
    reasons = []
    if any(review["state"] == "CHANGES_REQUESTED" for review in latest.values()):
        reasons.append("A reviewer still requests changes")
    author = pr.get("user", {}).get("login")
    independent = {login.lower() for login in policy["merge"]["independent_approvers"]}
    approved = False
    for login, review in latest.items():
        if (
            review["state"] != "APPROVED"
            or review.get("commit_id") != head
            or review["user"].get("type") not in {"User", "Bot"}
            or login == author
            or login == policy["app_login"]
            or login.lower() not in independent
        ):
            continue
        if review["user"]["type"] == "Bot":
            if not require_human:
                approved = True
            continue
        permission = api.request(f"collaborators/{quote(login, safe='')}/permission")
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
        reasons.extend(_reviews(api, policy, pr, head, require_human=dependency))
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
