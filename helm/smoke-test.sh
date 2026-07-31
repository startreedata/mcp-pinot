#!/usr/bin/env bash
# `helm template` smoke checks for the auth-provider wiring and exposure gate.
set -euo pipefail

CHART="$(dirname "$0")/mcp-pinot"

render() { helm template smoke "$CHART" "$@"; }

# Exposed renders need a Host allowlist (see the wildcard-bind guard below); this
# keeps each case focused on the behaviour it is named for.
render_exposed() {
  render --set service.enabled=true --set mcp.host=0.0.0.0 \
    --set 'mcp.allowedHosts={mcp.example.com}' "$@"
}

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
matches 'maxUnavailable: 1' || fail "single-process rolling strategy missing"
matches 'maxSurge: 0' || fail "rolling strategy permits overlapping processes"
matches 'requests:' || fail "resource requests missing"
matches 'limits:' || fail "resource limits missing"

# provider=static: AUTH_PROVIDER + MCP_STATIC_TOKEN from the chart Secret.
out=$(render_exposed \
  --set mcp.auth.provider=static --set mcp.auth.staticToken=s3cret)
matches 'name: AUTH_PROVIDER' || fail "AUTH_PROVIDER not rendered"
matches 'value: "static"' || fail "AUTH_PROVIDER value wrong"
matches 'name: MCP_STATIC_TOKEN' || fail "MCP_STATIC_TOKEN not rendered"
matches 'name: MCP_STATIC_SCOPES' || fail "MCP_STATIC_SCOPES not rendered"
matches 'value: "pinot:read pinot:write pinot:admin"' || fail "static scopes value wrong"
matches 'key: static-token' || fail "static-token secretKeyRef missing"
matches "static-token: \"$(printf s3cret | base64)\"" || fail "static-token not in Secret"

# provider=static with no staticToken: the chart mints and persists one, so the
# install is usable without an operator choosing or pasting a secret.
out=$(render_exposed \
  --set mcp.auth.provider=static)
matches 'name: MCP_STATIC_TOKEN' || fail "auto-generated MCP_STATIC_TOKEN not wired"
matches 'key: static-token' || fail "auto-generated static-token ref missing"
matches 'static-token: "[A-Za-z0-9+/=]\{32,\}"' || fail "auto-generated static-token not in Secret"

# provider=static with the token supplied through env.additional: the chart stays
# out of the way, so MCP_STATIC_TOKEN is declared exactly once and no unused
# secret material is stored.
out=$(render_exposed \
  --set mcp.auth.provider=static \
  --set 'env.additional[0].name=MCP_STATIC_TOKEN' \
  --set 'env.additional[0].value=external' )
matches 'key: static-token' && fail "chart minted a token despite an external one"
matches "static-token:" && fail "unused static-token key rendered"
[ "$(printf '%s' "$out" | grep -c 'name: MCP_STATIC_TOKEN')" = "1" ] \
  || fail "MCP_STATIC_TOKEN not declared exactly once"

# Health probes must speak the scheme the server actually serves; an http:// probe
# against a TLS listener fails forever.
out=$(render_exposed --set mcp.auth.provider=static --set mcp.ssl.enabled=true)
matches 'https://127.0.0.1:8000/livez' || fail "liveness probe not using https with mcp.ssl.enabled"
matches 'https://127.0.0.1:8000/readyz' || fail "readiness probe not using https with mcp.ssl.enabled"
matches '_create_unverified_context' || fail "loopback https probe must skip cert verification"
out=$(render_exposed --set mcp.auth.provider=static)
matches 'http://127.0.0.1:8000/livez' || fail "liveness probe not using http without TLS"
matches 'https://127.0.0.1' && fail "https probe rendered without mcp.ssl.enabled"

# A wildcard bind needs an explicit Host allowlist: the chart refuses to render
# rather than letting the server exit at startup.
if render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=static >/dev/null 2>&1; then
  fail "wildcard bind rendered without mcp.allowedHosts"
fi

# ... and renders MCP_ALLOWED_HOSTS/ORIGINS once supplied.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=static \
  --set 'mcp.allowedHosts={mcp.example.com,mcp.example.com:443}' \
  --set 'mcp.allowedOrigins={https://mcp.example.com}')
matches 'name: MCP_ALLOWED_HOSTS' || fail "MCP_ALLOWED_HOSTS not rendered"
matches 'value: "mcp.example.com,mcp.example.com:443"' || fail "allowed hosts value wrong"
matches 'name: MCP_ALLOWED_ORIGINS' || fail "MCP_ALLOWED_ORIGINS not rendered"

# An allowlist supplied through env.additional satisfies the guard too.
out=$(render --set service.enabled=true --set mcp.host=0.0.0.0 \
  --set mcp.auth.provider=static \
  --set 'env.additional[0].name=MCP_ALLOWED_HOSTS' \
  --set 'env.additional[0].value=mcp.example.com')
matches 'name: MCP_ALLOWED_HOSTS' || fail "external MCP_ALLOWED_HOSTS missing"

# provider=oauth+static wires BOTH credential types on one deployment.
out=$(render_exposed \
  --set mcp.auth.provider=oauth+static --set mcp.oauth.clientSecret=cs3cret \
  --set 'mcp.auth.staticScopes={pinot:read}' \
  --set 'mcp.oauth.grantedScopes={pinot:read,pinot:write}')
matches 'value: "oauth+static"' || fail "composite AUTH_PROVIDER not rendered"
matches 'name: MCP_STATIC_TOKEN' || fail "static token missing under oauth+static"
matches 'name: OAUTH_ISSUER' || fail "OAuth block missing under oauth+static"
matches 'value: "pinot:read pinot:write"' || fail "granted scopes missing under oauth+static"
matches 'key: static-token' || fail "static-token ref missing under oauth+static"
matches 'key: oauth-client-secret' || fail "oauth-client-secret ref missing under oauth+static"

# provider=oauth with a narrowed grant renders the read-only scope set.
out=$(render_exposed \
  --set mcp.auth.provider=oauth --set mcp.oauth.clientSecret=cs3cret \
  --set 'mcp.oauth.grantedScopes={pinot:read}')
matches 'name: OAUTH_GRANTED_SCOPES' || fail "OAUTH_GRANTED_SCOPES not rendered"
matches 'value: "pinot:read"' || fail "granted scopes value wrong"

# ... and omits it entirely when unset, leaving the server default in place.
out=$(render_exposed \
  --set mcp.auth.provider=oauth --set mcp.oauth.clientSecret=cs3cret)
matches 'name: OAUTH_GRANTED_SCOPES' && fail "OAUTH_GRANTED_SCOPES rendered when unset"

# provider=oauth alone wires the full OAuth env block and Secret key.
out=$(render_exposed \
  --set mcp.auth.provider=oauth --set mcp.oauth.clientSecret=cs3cret)
matches 'value: "oauth"' || fail "AUTH_PROVIDER=oauth not rendered"
matches 'name: OAUTH_ISSUER' || fail "OAUTH_* block missing for provider=oauth"
matches 'key: oauth-client-secret' || fail "oauth-client-secret ref missing"
matches "oauth-client-secret: \"$(printf cs3cret | base64)\"" || fail "oauth-client-secret not in Secret"

# Legacy oauth.enabled=true still wires the same block, with no AUTH_PROVIDER.
out=$(render_exposed \
  --set mcp.oauth.enabled=true)
matches 'name: OAUTH_ISSUER' || fail "OAUTH_* block missing for legacy flag"
matches 'name: AUTH_PROVIDER' && fail "legacy flag rendered AUTH_PROVIDER"

# AUTH_PROVIDER renders normalized (trim + lowercase), matching the server's
# _resolve_auth_provider; a whitespace-only value renders no env var at all.
out=$(render_exposed \
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
  --set podDisruptionBudget.minAvailable=1 >/dev/null 2>&1; then
  fail "PDB rendered with both minAvailable and maxUnavailable"
fi

if render --set replicas=2 >/dev/null 2>&1; then
  fail "multi-replica deployment rendered despite process-local security state"
fi

echo "OK"
