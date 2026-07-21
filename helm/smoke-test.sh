#!/usr/bin/env bash
# `helm template` smoke checks for the auth-provider wiring and exposure gate.
set -euo pipefail

CHART="$(dirname "$0")/mcp-pinot"

render() { helm template smoke "$CHART" "$@"; }

fail() { echo "FAIL: $1" >&2; exit 1; }

# Avoid Bash here-strings, which allocate temporary files and can make these
# checks fail on disk-constrained CI runners.
matches() { grep -q -- "$1" < <(printf '%s\n' "$out"); }
matches_ere() { grep -Eq -- "$1" < <(printf '%s\n' "$out"); }
count_ere() { grep -Ec -- "$1" < <(printf '%s\n' "$out"); }

# Defaults: loopback, no auth provider, no AUTH_PROVIDER env, no Secret, and
# production-safe container/pod settings.
out=$(render)
matches "AUTH_PROVIDER" && fail "AUTH_PROVIDER rendered by default"
matches "OAUTH_ENABLED" || fail "OAUTH_ENABLED missing (back-compat)"
matches "kind: Secret" && fail "Secret rendered by default"
matches_ere 'image: "ghcr.io/startreedata/mcp-pinot:[^\"]+"' || fail "default appVersion image tag missing"
matches 'image: ".*:latest"' && fail "mutable latest image tag rendered"
matches 'automountServiceAccountToken: false' || fail "service account token automount is not disabled"
matches 'readOnlyRootFilesystem: true' || fail "read-only root filesystem missing"
matches 'type: RuntimeDefault' || fail "RuntimeDefault seccomp profile missing"
matches 'http://127.0.0.1:8000/livez' || fail "liveness probe missing"
matches 'http://127.0.0.1:8000/readyz' || fail "readiness probe missing"
matches 'mountPath: /tmp' || fail "writable /tmp mount missing"
matches 'terminationGracePeriodSeconds: 30' || fail "termination grace period missing"
matches 'requests:' || fail "resource requests missing"
matches 'limits:' || fail "resource limits missing"

# provider=static: AUTH_PROVIDER + MCP_STATIC_TOKEN from the chart Secret.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=static --set mcp.auth.staticToken=s3cret)
matches 'name: AUTH_PROVIDER' || fail "AUTH_PROVIDER not rendered"
matches 'value: "static"' || fail "AUTH_PROVIDER value wrong"
matches 'name: MCP_STATIC_TOKEN' || fail "MCP_STATIC_TOKEN not rendered"
matches 'key: static-token' || fail "static-token secretKeyRef missing"
matches "static-token: \"$(printf s3cret | base64)\"" || fail "static-token not in Secret"

# provider=static with no staticToken: no chart-managed MCP_STATIC_TOKEN, so a
# token supplied through env.additional is not declared twice.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=static)
matches 'name: MCP_STATIC_TOKEN' && fail "MCP_STATIC_TOKEN rendered without a token"
matches "static-token:" && fail "empty static-token key rendered"

# provider=oauth alone wires the full OAuth env block and Secret key.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=oauth --set mcp.oauth.clientSecret=cs3cret)
matches 'value: "oauth"' || fail "AUTH_PROVIDER=oauth not rendered"
matches 'name: OAUTH_ISSUER' || fail "OAUTH_* block missing for provider=oauth"
matches 'key: oauth-client-secret' || fail "oauth-client-secret ref missing"
matches "oauth-client-secret: \"$(printf cs3cret | base64)\"" || fail "oauth-client-secret not in Secret"

# Legacy oauth.enabled=true still wires the same block, with no AUTH_PROVIDER.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.oauth.enabled=true)
matches 'name: OAUTH_ISSUER' || fail "OAUTH_* block missing for legacy flag"
matches 'name: AUTH_PROVIDER' && fail "legacy flag rendered AUTH_PROVIDER"

# AUTH_PROVIDER renders normalized (trim + lowercase), matching the server's
# _resolve_auth_provider; a whitespace-only value renders no env var at all.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=" OAuth ")
matches 'value: "oauth"' || fail "AUTH_PROVIDER not normalized"
out=$(render --set mcp.auth.provider="   ")
matches 'name: AUTH_PROVIDER' && fail "whitespace-only provider rendered AUTH_PROVIDER"

# Non-loopback with no auth provider (or an explicit "none") must fail to render.
if render --set service.enabled=true --set mcp.host=0.0.0.0 >/dev/null 2>&1; then
  fail "non-loopback bind allowed without an auth provider"
fi
if render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=none >/dev/null 2>&1; then
  fail "non-loopback bind allowed with provider=none"
fi

# User-supplied volumes and mounts are preserved alongside the chart's /tmp
# volume.
out=$(render \
  --set 'volumeMounts.additional[0].name=work' \
  --set 'volumeMounts.additional[0].mountPath=/work' \
  --set 'volumes.additional[0].name=work' \
  --set 'volumes.additional[0].emptyDir.sizeLimit=8Mi')
matches 'mountPath: /work' || fail "additional volume mount missing"
matches 'sizeLimit: 8Mi' || fail "additional volume missing"

# The Deployment uses the same generated TLS secret name as the Certificate
# when certificate.secretName is omitted.
out=$(render --set certificate.enabled=true --set certificate.issuer=internal-ca)
matches 'kind: Certificate' || fail "Certificate missing"
[[ $(count_ere 'secretName: "?smoke-mcp-pinot-tls"?') -eq 2 ]] || \
  fail "generated certificate secret name is inconsistent"

# Optional availability and network-isolation resources render on demand.
out=$(render --set networkPolicy.enabled=true \
  --set podDisruptionBudget.enabled=true)
matches 'kind: NetworkPolicy' || fail "NetworkPolicy missing"
matches 'kind: PodDisruptionBudget' || fail "PodDisruptionBudget missing"

if render --set podDisruptionBudget.enabled=true \
  --set podDisruptionBudget.maxUnavailable=1 >/dev/null 2>&1; then
  fail "PDB rendered with both minAvailable and maxUnavailable"
fi

echo "OK"
