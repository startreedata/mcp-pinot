{{/*
Expand the name of the chart.
*/}}
{{- define "mcp-pinot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "mcp-pinot.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "mcp-pinot.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "mcp-pinot.labels" -}}
helm.sh/chart: {{ include "mcp-pinot.chart" . }}
{{ include "mcp-pinot.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "mcp-pinot.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mcp-pinot.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "mcp-pinot.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "mcp-pinot.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Resolve the effective auth provider, mirroring config.py::_resolve_auth_provider:
mcp.auth.provider wins, else the legacy mcp.oauth.enabled maps to "oauth".
Returns "none" when no provider is selected.
*/}}
{{- define "mcp-pinot.authProvider" -}}
{{- $provider := lower (trim (toString (.Values.mcp.auth.provider | default ""))) -}}
{{- if $provider -}}
{{- $provider -}}
{{- else if .Values.mcp.oauth.enabled -}}
oauth
{{- else -}}
none
{{- end -}}
{{- end }}

{{/*
Whether a given credential type is accepted, given that mcp.auth.provider may name
several (e.g. "oauth+static" for interactive users plus one trusted backend).

Call as: include "mcp-pinot.authHas" (dict "root" . "name" "static")
*/}}
{{- define "mcp-pinot.authHas" -}}
{{- $want := .name -}}
{{- range splitList "+" (include "mcp-pinot.authProvider" .root) -}}
{{- if eq (trim .) $want -}}true{{- end -}}
{{- end -}}
{{- end }}

{{/*
Whether env.additional already declares a given variable.

Call as: include "mcp-pinot.envHas" (dict "root" . "name" "MCP_STATIC_TOKEN")
Used so the chart never fights a caller who supplies a value itself: it neither
mints a token nor declares a duplicate variable, and the exposure guards trust an
externally supplied allowlist.
*/}}
{{- define "mcp-pinot.envHas" -}}
{{- $name := .name -}}
{{- $found := false -}}
{{- range .root.Values.env.additional -}}
{{- if eq (.name | default "") $name -}}{{- $found = true -}}{{- end -}}
{{- end -}}
{{- if $found -}}true{{- end -}}
{{- end }}

{{/*
Whether the caller supplies MCP_STATIC_TOKEN themselves through env.additional.
*/}}
{{- define "mcp-pinot.staticTokenFromEnv" -}}
{{- include "mcp-pinot.envHas" (dict "root" . "name" "MCP_STATIC_TOKEN") -}}
{{- end }}

{{/*
Resolve the static shared bearer token for provider=static.

This helper is used only when MCP_STATIC_TOKEN is not supplied through
env.additional. In that chart-managed path, an explicit mcp.auth.staticToken wins.
Otherwise the token is auto-generated ONCE per environment and persisted in this
chart's Secret: on upgrades `lookup` finds the existing Secret and reuses its
`static-token` key, so the value is stable across releases, and only the very first
install mints a fresh randAlphaNum. This makes provider=static zero-touch — no
operator ever has to pick, paste, or distribute a token, and each environment gets
a distinct one.

During `helm template`/`--dry-run` there is no cluster to look up, so a throwaway
token is rendered; that output is never applied. Consumers must therefore read
the token from the Secret (key `static-token`), never from rendered manifests.
*/}}
{{- define "mcp-pinot.staticToken" -}}
{{- if .Values.mcp.auth.staticToken -}}
{{- .Values.mcp.auth.staticToken -}}
{{- else -}}
{{- $secretName := printf "%s-secrets" (include "mcp-pinot.fullname" .) -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace $secretName -}}
{{- $prior := "" -}}
{{- if $existing -}}{{- $prior = index (default dict $existing.data) "static-token" -}}{{- end -}}
{{- if $prior -}}
{{- $prior | b64dec -}}
{{- else -}}
{{- randAlphaNum 48 -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Health-probe command. The server terminates TLS itself when mcp.ssl.enabled is set,
so the probe has to speak the same scheme — an http:// probe against a TLS listener
fails forever, which is why probes used to have to be disabled for any deployment
serving HTTPS. The certificate is issued for the service's public names, never for
127.0.0.1, so a loopback probe cannot verify it; verification is therefore skipped
for the local check only. /livez and /readyz sit outside the MCP path, so the Host
allowlist and the auth provider do not apply to them.

Call as: include "mcp-pinot.probeCommand" (dict "root" . "check" .Values.healthCheck.liveness)
*/}}
{{- define "mcp-pinot.probeCommand" -}}
{{- $scheme := ternary "https" "http" .root.Values.mcp.ssl.enabled -}}
- python
- -c
- "import ssl, sys, urllib.request; ctx = ssl._create_unverified_context() if sys.argv[1].startswith('https') else None; sys.exit(0 if urllib.request.urlopen(sys.argv[1], timeout=float(sys.argv[2]), context=ctx).status == 200 else 1)"
- {{ printf "%s://127.0.0.1:%v%s" $scheme .check.port .check.path | quote }}
- {{ .check.timeoutSeconds | quote }}
{{- end }}

{{/*
Validate MCP HTTP exposure settings.
*/}}
{{- define "mcp-pinot.isLoopbackHost" -}}
{{- $host := lower (toString .Values.mcp.host) -}}
{{- if or (eq $host "localhost") (eq $host "::1") (hasPrefix "127." $host) -}}true{{- else -}}false{{- end -}}
{{- end }}

{{- define "mcp-pinot.validateExposure" -}}
{{- $host := lower (toString .Values.mcp.host) -}}
{{- $isLoopback := eq (include "mcp-pinot.isLoopbackHost" .) "true" -}}
{{- $serviceEnabled := .Values.service.enabled -}}
{{- $traefikEnabled := .Values.traefik.enabled -}}
{{- if and $traefikEnabled (not $serviceEnabled) -}}
{{- fail "traefik.enabled=true requires service.enabled=true" -}}
{{- end -}}
{{- $authEnabled := ne (include "mcp-pinot.authProvider" .) "none" -}}
{{- if and (or $serviceEnabled $traefikEnabled) $isLoopback -}}
{{- fail "service and Traefik exposure require mcp.host to be non-loopback; set mcp.host=0.0.0.0 and an auth provider (mcp.auth.provider=oauth|static, or mcp.oauth.enabled=true)" -}}
{{- end -}}
{{- if and (not $isLoopback) (not $authEnabled) -}}
{{- fail "mcp.host is non-loopback, so an auth provider is required; set mcp.auth.provider=oauth|static (or the legacy mcp.oauth.enabled=true)" -}}
{{- end -}}
{{- $hostAllowlisted := or .Values.mcp.allowedHosts (eq (include "mcp-pinot.envHas" (dict "root" . "name" "MCP_ALLOWED_HOSTS")) "true") -}}
{{- $isWildcard := or (eq $host "0.0.0.0") (eq $host "::") (eq $host "[::]") -}}
{{- if and $isWildcard (not $hostAllowlisted) -}}
{{- fail "mcp.host is a wildcard bind, which does not identify a public authority: set mcp.allowedHosts to the exact Host authorities clients use (e.g. [mcp.example.com, mcp.example.com:443]). Without it the server exits at startup instead of serving." -}}
{{- end -}}
{{- end }}

{{/*
Certificate template for internal certificates
*/}}
{{- define "mcp-pinot.certificate" -}}
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {{ .name }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "mcp-pinot.labels" . | nindent 4 }}
spec:
  commonName: {{ .commonName | default .name }}
  secretName: {{ .secretName | default (printf "%s-tls" .name) }}
  dnsNames:
    {{- if .dnsNames }}
    {{- range .dnsNames }}
    - {{ . | quote }}
    {{- end }}
    {{- else }}
    - "{{ .name }}.{{ .Release.Namespace }}.svc.cluster.local"
    - "{{ .name }}.{{ .Release.Namespace }}.svc.cluster"
    - "{{ .name }}.{{ .Release.Namespace }}.svc"
    - "{{ .name }}.{{ .Release.Namespace }}"
    - "{{ .name }}"
    - localhost
    {{- end }}
  issuerRef:
    name: {{ .issuer }}
    kind: {{ .issuerKind | default "ClusterIssuer" }}
    group: {{ .issuerGroup | default "cert-manager.io" }}
{{- end }}
