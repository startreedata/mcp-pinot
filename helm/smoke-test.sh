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

# provider=static with no staticToken: no chart-managed MCP_STATIC_TOKEN, so a
# token supplied through env.additional is not declared twice.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=static)
grep -q 'name: MCP_STATIC_TOKEN' <<<"$out" && fail "MCP_STATIC_TOKEN rendered without a token"
grep -q "static-token:" <<<"$out" && fail "empty static-token key rendered"

# provider=oauth alone wires the full OAuth env block and Secret key.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=oauth --set mcp.oauth.clientSecret=cs3cret)
grep -q 'value: "oauth"' <<<"$out" || fail "AUTH_PROVIDER=oauth not rendered"
grep -q 'name: OAUTH_ISSUER' <<<"$out" || fail "OAUTH_* block missing for provider=oauth"
grep -q 'key: oauth-client-secret' <<<"$out" || fail "oauth-client-secret ref missing"
grep -q "oauth-client-secret: \"$(printf cs3cret | base64)\"" <<<"$out" || fail "oauth-client-secret not in Secret"

# Legacy oauth.enabled=true still wires the same block, with no AUTH_PROVIDER.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.oauth.enabled=true)
grep -q 'name: OAUTH_ISSUER' <<<"$out" || fail "OAUTH_* block missing for legacy flag"
grep -q 'name: AUTH_PROVIDER' <<<"$out" && fail "legacy flag rendered AUTH_PROVIDER"

# AUTH_PROVIDER renders normalized (trim + lowercase), matching the server's
# _resolve_auth_provider; a whitespace-only value renders no env var at all.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=" OAuth ")
grep -q 'value: "oauth"' <<<"$out" || fail "AUTH_PROVIDER not normalized"
out=$(render --set mcp.auth.provider="   ")
grep -q 'name: AUTH_PROVIDER' <<<"$out" && fail "whitespace-only provider rendered AUTH_PROVIDER"

# Non-loopback with no auth provider (or an explicit "none") must fail to render.
if render --set service.enabled=true --set mcp.host=0.0.0.0 >/dev/null 2>&1; then
  fail "non-loopback bind allowed without an auth provider"
fi
if render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=none >/dev/null 2>&1; then
  fail "non-loopback bind allowed with provider=none"
fi

echo "OK"
