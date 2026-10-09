"""Trusted, portable policy configuration and candidate path boundaries."""

from copy import deepcopy
from fnmatch import fnmatchcase
import os
from pathlib import Path, PurePosixPath
import re
import tomllib

PROTECTED_PATHS = (
    ".git",
    ".git/**",
    "**/.git/**",
    ".gitattributes",
    "**/.gitattributes",
    ".gitconfig",
    "**/.gitconfig",
    ".github/**",
    "**/.github/**",
    ".claude/**",
    "**/.claude/**",
    ".codex/**",
    "**/.codex/**",
    ".mcp.json",
    "**/.mcp.json",
    ".gitmodules",
    ".lfsconfig",
    ".npmrc",
    "**/.npmrc",
    ".yarnrc",
    ".yarnrc.*",
    "**/.yarnrc*",
    ".pypirc",
    ".pre-commit-config.yaml",
    ".husky/**",
    "**/.husky/**",
    "Dockerfile",
    "**/Dockerfile",
    "Dockerfile.*",
    "**/Dockerfile.*",
    ".gitlab-ci.yml",
    ".circleci/**",
    "Jenkinsfile",
    "AGENTS.md",
    "**/AGENTS.md",
    "CLAUDE.md",
    "**/CLAUDE.md",
    "CODEOWNERS",
    "**/CODEOWNERS",
    "auth/**",
    "**/auth/**",
    "security/**",
    "**/security/**",
    "releases/**",
    "**/releases/**",
    "release/**",
    "**/release/**",
    ".env",
    ".env.*",
    "**/.env",
    "**/.env.*",
    "*.pem",
    "**/*.pem",
    "*.key",
    "**/*.key",
)

DEFAULT_POLICY = {
    "version": 1,
    "mode": "observe",
    "default_branch": "main",
    "app_login": "",
    "writer_type": "Bot",
    "state_issue": 0,
    "worker_workflow": "maintainer-worker.yml",
    "ready_label": "ai:ready",
    "managed_label": "ai:managed",
    "hold_label": "ai:hold",
    "needs_human_label": "ai:needs-human",
    "allowed_paths": [],
    "protected_paths": [],
    "ci_workflows": [],
    "validation_commands": [],
    "setup_commands": [],
    "validation_env": {},
    "limits": {
        "max_workers": 1,
        "max_repair_rounds": 3,
        "max_attempts": 4,
        "max_files": 30,
        "max_patch_bytes": 1048576,
        "per_run_usd": 5.0,
        "daily_usd": 20.0,
        "lease_seconds": 5400,
        "max_turns": 30,
    },
    "merge": {
        "enabled": False,
        "independent_approvers": [],
        "required_checks": [],
        "policy_check_app_id": 0,
        "require_human_for_dependencies": True,
    },
    "review": {
        "mode": "observe",
        "login": "",
        "identity_type": "Bot",
        "max_files": 30,
        "max_input_bytes": 200000,
        "per_run_usd": 5.0,
        "daily_usd": 20.0,
        "max_turns": 30,
        "lease_seconds": 5400,
    },
    "dependencies": {
        "enabled": True,
        "manifest_paths": [],
        "authors": ["dependabot[bot]"],
    },
    "slack": {"channel": ""},
}


def _string(value, name, *, empty=False):
    if not isinstance(value, str) or (not empty and not value.strip()):
        raise ValueError(f"{name} must be a {'possibly empty ' if empty else ''}string")
    if len(value) > 4096 or any(ord(char) < 32 for char in value):
        raise ValueError(f"{name} contains invalid characters or is too long")


def _strings(value, name):
    if not isinstance(value, list) or len(value) > 100:
        raise ValueError(f"{name} must be a list of at most 100 strings")
    for entry in value:
        _string(entry, name)
    if len(set(value)) != len(value):
        raise ValueError(f"{name} must not contain duplicates")


def _number(value, name, low, high, *, integer=False):
    expected = int if integer else (int, float)
    if isinstance(value, bool) or not isinstance(value, expected):
        raise ValueError(f"{name} must be {'an integer' if integer else 'a number'}")
    if not low <= value <= high:
        raise ValueError(f"{name} must be between {low} and {high}")


def _identity(login, identity_type, name, *, empty=False):
    _string(login, name, empty=empty)
    if not isinstance(identity_type, str) or identity_type not in {"Bot", "User"}:
        raise ValueError(f"{name} identity type must be Bot or User")
    pattern = r"[A-Za-z0-9_-]{1,39}" + (r"\[bot\]" if identity_type == "Bot" else "")
    if login and not re.fullmatch(pattern, login):
        raise ValueError(f"{name} must be a GitHub {identity_type} login")


def _merge_defaults(raw, default, prefix=""):
    if not isinstance(raw, dict):
        raise ValueError(f"{prefix or 'policy'} must be a table")
    unknown = raw.keys() - default.keys()
    if unknown:
        raise ValueError(f"Unknown policy fields: {prefix}{', '.join(sorted(unknown))}")
    result = deepcopy(default)
    for key, value in raw.items():
        if isinstance(default[key], dict) and key != "validation_env":
            result[key] = _merge_defaults(value, default[key], f"{prefix}{key}.")
        else:
            result[key] = value
    return result


def load_policy(path):
    """Load a policy; only trusted workflow variables can override mode/identity."""
    with Path(path).open("rb") as source:
        policy = _merge_defaults(tomllib.load(source), DEFAULT_POLICY)
    for variable, field in (
        ("MAINTAINER_MODE", "mode"),
        ("MAINTAINER_APP_LOGIN", "app_login"),
    ):
        if os.environ.get(variable):
            policy[field] = os.environ[variable]
    if os.environ.get("MAINTAINER_REVIEWER_MODE"):
        policy["review"]["mode"] = os.environ["MAINTAINER_REVIEWER_MODE"]
    if type(policy["version"]) is not int or policy["version"] != 1:
        raise ValueError("Only policy version 1 is supported")
    _string(policy["mode"], "mode")
    if policy["mode"] not in {"observe", "maintain", "autopilot"}:
        raise ValueError("mode must be observe, maintain, or autopilot")
    for field in (
        "default_branch",
        "app_login",
        "worker_workflow",
        "ready_label",
        "managed_label",
        "hold_label",
        "needs_human_label",
    ):
        _string(policy[field], field, empty=field == "app_login")
    _identity(policy["app_login"], policy["writer_type"], "app_login", empty=True)
    if (
        policy["default_branch"].startswith(("/", "-"))
        or ".." in policy["default_branch"]
        or any(char in policy["default_branch"] for char in "~^:?*[\\")
    ):
        raise ValueError("default_branch must be a safe branch name")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+\.ya?ml", policy["worker_workflow"]):
        raise ValueError("worker_workflow must be a YAML workflow filename")
    _number(policy["state_issue"], "state_issue", 0, 2147483647, integer=True)
    for field in (
        "allowed_paths",
        "protected_paths",
        "ci_workflows",
        "validation_commands",
        "setup_commands",
    ):
        _strings(policy[field], field)
    for field in ("allowed_paths", "protected_paths"):
        for pattern in policy[field]:
            if not _safe_path(pattern):
                raise ValueError(f"{field} contains an unsafe path pattern")
    env = policy["validation_env"]
    if not isinstance(env, dict) or len(env) > 30:
        raise ValueError("validation_env must be a table of at most 30 strings")
    for key, value in env.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError("validation_env contains an invalid environment name")
        if any(part in key.upper() for part in ("TOKEN", "SECRET", "PASSWORD")):
            raise ValueError("validation_env cannot contain credentials")
        _string(value, f"validation_env.{key}", empty=True)
    integer_limits = {
        "max_workers": (1, 10),
        "max_repair_rounds": (0, 10),
        "max_attempts": (1, 20),
        "max_files": (1, 300),
        "max_patch_bytes": (1, 10485760),
        "lease_seconds": (3600, 86400),
        "max_turns": (1, 100),
    }
    for field, (low, high) in integer_limits.items():
        _number(policy["limits"][field], f"limits.{field}", low, high, integer=True)
    for field in ("per_run_usd", "daily_usd"):
        _number(policy["limits"][field], f"limits.{field}", 0.01, 1000)
    if policy["limits"]["daily_usd"] < policy["limits"]["per_run_usd"]:
        raise ValueError(
            "daily_usd must cover at least one full per_run_usd reservation"
        )
    review = policy["review"]
    _string(review["mode"], "review.mode")
    if review["mode"] not in {"observe", "review", "approve"}:
        raise ValueError("review.mode must be observe, review, or approve")
    _identity(review["login"], review["identity_type"], "review.login", empty=True)
    for field, (low, high) in {
        "max_files": (1, 300),
        "max_input_bytes": (1, 10485760),
        "max_turns": (1, 100),
        "lease_seconds": (3600, 86400),
    }.items():
        _number(review[field], f"review.{field}", low, high, integer=True)
    for field in ("per_run_usd", "daily_usd"):
        _number(review[field], f"review.{field}", 0.01, 1000)
    if review["daily_usd"] < review["per_run_usd"]:
        raise ValueError("review.daily_usd must cover one review reservation")
    if review["mode"] != "observe":
        if not policy["app_login"] or not review["login"]:
            raise ValueError("Active reviewer needs writer and reviewer logins")
        if review["login"].casefold() == policy["app_login"].casefold():
            raise ValueError("Writer and reviewer identities must be different")
        if not policy["state_issue"]:
            raise ValueError("Active reviewer needs a dedicated state_issue")
    merge = policy["merge"]
    deps = policy["dependencies"]
    for table, fields in (
        (merge, ("enabled", "require_human_for_dependencies")),
        (deps, ("enabled",)),
    ):
        for field in fields:
            if type(table[field]) is not bool:
                raise ValueError(f"{field} must be a boolean")
    _strings(merge["independent_approvers"], "merge.independent_approvers")
    for login in merge["independent_approvers"]:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,39}(?:\[bot\])?", login):
            raise ValueError("independent_approvers must contain GitHub logins")
    if policy["app_login"].casefold() in {
        login.casefold() for login in merge["independent_approvers"]
    }:
        raise ValueError("Writer cannot be an independent approver")
    _number(
        merge["policy_check_app_id"],
        "merge.policy_check_app_id",
        0,
        2147483647,
        integer=True,
    )
    checks = merge["required_checks"]
    if not isinstance(checks, list) or len(checks) > 50:
        raise ValueError("merge.required_checks must be a list of at most 50 checks")
    names = set()
    for check in checks:
        if not isinstance(check, dict) or check.keys() != {"name", "app_id"}:
            raise ValueError("Each required check needs exactly name and app_id")
        _string(check["name"], "merge.required_checks.name")
        if check["name"] == "maintainer/policy":
            raise ValueError("maintainer/policy is configured by policy_check_app_id")
        _number(
            check["app_id"], "merge.required_checks.app_id", 1, 2147483647, integer=True
        )
        if check["name"] in names:
            raise ValueError("Required check names must be unique")
        names.add(check["name"])
    _strings(deps["manifest_paths"], "dependencies.manifest_paths")
    _strings(deps["authors"], "dependencies.authors")
    for path in deps["manifest_paths"]:
        if not _safe_path(path):
            raise ValueError("dependencies.manifest_paths contains an unsafe path")
    _string(policy["slack"]["channel"], "slack.channel", empty=True)
    if policy["mode"] != "observe":
        if not policy["app_login"]:
            raise ValueError("Active mode requires an app_login")
        if not policy["state_issue"]:
            raise ValueError("Active mode requires a dedicated state_issue")
        if not policy["allowed_paths"] or not policy["validation_commands"]:
            raise ValueError("Active mode needs allowed_paths and validation_commands")
    if policy["mode"] == "autopilot" and not merge["enabled"]:
        raise ValueError("autopilot requires explicit merge.enabled = true")
    if merge["enabled"]:
        if not checks or not (
            merge["independent_approvers"]
            or (review["mode"] == "approve" and review["login"])
        ):
            raise ValueError("Merge needs required_checks and independent_approvers")
        if not merge["policy_check_app_id"]:
            raise ValueError("Merge requires the policy check's GitHub App ID")
    return policy


def _safe_path(path):
    return (
        isinstance(path, str)
        and bool(path)
        and not path.startswith(("/", "-"))
        and "\\" not in path
        and not any(ord(char) < 32 for char in path)
        and all(part not in {"", ".", ".."} for part in path.split("/"))
        and not PurePosixPath(path).is_absolute()
    )


def path_allowed(path, policy):
    """Return whether both configured scope and non-overridable protection allow it."""
    if not _safe_path(path):
        return False
    protected = (*PROTECTED_PATHS, *policy["protected_paths"])
    return not any(fnmatchcase(path, pattern) for pattern in protected) and any(
        fnmatchcase(path, pattern) for pattern in policy["allowed_paths"]
    )


def validate_paths(files, policy):
    """Reject incomplete paths and unsafe rename sources as well as targets."""
    reasons = []
    if not isinstance(files, list) or not files:
        return ["Changed file list is missing or empty"]
    if len(files) > policy["limits"]["max_files"]:
        reasons.append("Changed file count exceeds max_files")
    seen = set()
    for item in files:
        filename = item.get("filename") if isinstance(item, dict) else item
        previous = item.get("previous_filename") if isinstance(item, dict) else None
        if not isinstance(filename, str):
            reasons.append("Changed file has no valid filename")
            continue
        if filename in seen:
            reasons.append(f"Duplicate changed file: {filename}")
        seen.add(filename)
        for path in (filename, previous):
            if path is not None and not path_allowed(path, policy):
                reasons.append(f"Path needs human maintenance: {path}")
    return reasons
