"""Small GitHub client with bounded, complete pagination and no shell/token logging."""

import json
import os
import re
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class GitHubError(RuntimeError):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise GitHubError("GitHub API redirect refused", status=code)


class GitHub:
    def __init__(self, repo=None, token=None):
        self.repo = repo or os.environ.get("GITHUB_REPOSITORY", "")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", self.repo):
            raise ValueError("GITHUB_REPOSITORY must be owner/repo")
        self.token = (
            token
            if token is not None
            else (os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", ""))
        )
        self.api_url = os.environ.get(
            "GITHUB_API_URL", "https://api.github.com"
        ).rstrip("/")
        parsed = urlsplit(self.api_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("GITHUB_API_URL must be an HTTPS GitHub API base")
        self._headers = {}
        self._opener = build_opener(_NoRedirect())

    def request(self, path, method="GET", data=None):
        if (
            not isinstance(path, str)
            or not path
            or "://" in path
            or path.startswith("//")
        ):
            raise ValueError("GitHub API paths must be relative endpoints")
        if any(ord(char) < 32 for char in path) or "#" in path:
            raise ValueError("Invalid GitHub API endpoint")
        if path == "/graphql" and self.api_url.endswith("/api/v3"):
            url = self.api_url.removesuffix("/api/v3") + "/api/graphql"
        elif path.startswith("/"):
            url = self.api_url + path
        else:
            url = f"{self.api_url}/repos/{self.repo}/{path}"
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "repo-maintainer",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        payload = None if data is None else json.dumps(data).encode("utf-8")
        if payload is not None:
            headers["Content-Type"] = "application/json"
        # API base is HTTPS-only and paths cannot supply another origin.
        request = Request(url, data=payload, headers=headers, method=method)  # noqa: S310
        try:
            # URL origin is fixed and redirects are refused to keep tokens on GitHub.
            with self._opener.open(request, timeout=30) as response:
                self._headers = dict(response.headers.items())
                content = response.read(33554433)
        except HTTPError as error:
            raise GitHubError(
                f"GitHub API {method} failed: HTTP {error.code}", status=error.code
            ) from None
        except (URLError, TimeoutError, OSError) as error:
            raise GitHubError(
                f"GitHub API request failed: {type(error).__name__}"
            ) from None
        if len(content) > 33554432:
            raise GitHubError("GitHub API response exceeds the safe size limit")
        try:
            return json.loads(content) if content else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise GitHubError("GitHub API returned invalid JSON") from None

    def paginate(self, path):
        """Read all pages; abort instead of returning capped or malformed results."""
        parsed = urlsplit(path)
        parameters = dict(parse_qsl(parsed.query, keep_blank_values=True))
        parameters.pop("page", None)
        parameters["per_page"] = "100"
        collected = []
        expected = None
        for page in range(1, 101):
            parameters["page"] = str(page)
            endpoint = urlunsplit(("", "", parsed.path, urlencode(parameters), ""))
            self._headers = {}
            payload = self.request(endpoint)
            if isinstance(payload, list):
                entries = payload
            elif isinstance(payload, dict):
                collection_keys = [
                    key
                    for key in ("check_runs", "workflow_runs", "jobs", "workflows")
                    if key in payload
                ]
                if len(collection_keys) != 1:
                    raise GitHubError(
                        "GitHub pagination response has no unique collection"
                    )
                entries = payload[collection_keys[0]]
                total = payload.get("total_count")
                if type(total) is not int or total < 0:
                    raise GitHubError("GitHub pagination total_count is invalid")
                if expected is not None and expected != total:
                    raise GitHubError("GitHub collection changed during pagination")
                expected = total
            else:
                raise GitHubError("GitHub pagination response is malformed")
            if not isinstance(entries, list) or len(entries) > 100:
                raise GitHubError("GitHub pagination page is malformed")
            collected.extend(entries)
            link = next(
                (
                    value
                    for key, value in self._headers.items()
                    if key.lower() == "link"
                ),
                "",
            )
            has_next = 'rel="next"' in link
            if has_next and not entries:
                raise GitHubError("GitHub pagination did not advance")
            if not has_next and len(entries) < 100:
                if expected is not None and len(collected) != expected:
                    raise GitHubError("GitHub collection is truncated")
                return collected
        raise GitHubError("GitHub collection exceeds complete pagination limit")

    def graphql(self, query, variables):
        payload = self.request(
            "/graphql", method="POST", data={"query": query, "variables": variables}
        )
        if not isinstance(payload, dict) or payload.get("errors"):
            raise GitHubError("GitHub GraphQL query failed")
        if not isinstance(payload.get("data"), dict):
            raise GitHubError("GitHub GraphQL data is missing")
        return payload["data"]

    @staticmethod
    def encode(value):
        return quote(str(value), safe="")
