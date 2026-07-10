#!/usr/bin/env bash
# `helm template` smoke checks for the auth-provider wiring and exposure gate.
set -euo pipefail

CHART="$(dirname "$0")/mcp-pinot"

render() { helm template smoke "$CHART" "$@"; }

fail() { echo "FAIL: $1" >&2; exit 1; }

# Defaults: loopback, no auth provider, no AUTH_PROVIDER env, no Secret.
out=$(render)
grep -q "AUTH_PROVIDER" <<<"$out" && fail "AUTH_PROVIDER rendered by default"
grep -q "OAUTH_ENABLED" <<<"$out" || fail "OAUTH_ENABLED missing (back-compat)"
grep -q "kind: Secret" <<<"$out" && fail "Secret rendered by default"

# provider=static: AUTH_PROVIDER + MCP_STATIC_TOKEN from the chart Secret.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=static --set mcp.auth.staticToken=s3cret)
grep -q 'name: AUTH_PROVIDER' <<<"$out" || fail "AUTH_PROVIDER not rendered"
grep -q 'value: "static"' <<<"$out" || fail "AUTH_PROVIDER value wrong"
grep -q 'name: MCP_STATIC_TOKEN' <<<"$out" || fail "MCP_STATIC_TOKEN not rendered"
grep -q 'key: static-token' <<<"$out" || fail "static-token secretKeyRef missing"
grep -q "static-token: \"$(printf s3cret | base64)\"" <<<"$out" || fail "static-token not in Secret"

# provider=oauth satisfies the exposure gate without the legacy flag.
render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=oauth >/dev/null || fail "provider=oauth rejected"

# Legacy oauth.enabled=true still satisfies the gate on its own.
render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.oauth.enabled=true >/dev/null || fail "legacy oauth.enabled rejected"

# Non-loopback with no auth provider must fail to render.
if render --set service.enabled=true --set mcp.host=0.0.0.0 >/dev/null 2>&1; then
  fail "non-loopback bind allowed without an auth provider"
fi

echo "OK"
