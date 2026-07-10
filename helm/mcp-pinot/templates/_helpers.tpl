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
Validate MCP HTTP exposure settings.
*/}}
{{- define "mcp-pinot.isLoopbackHost" -}}
{{- $host := lower (toString .Values.mcp.host) -}}
{{- if or (eq $host "localhost") (eq $host "::1") (hasPrefix "127." $host) -}}true{{- else -}}false{{- end -}}
{{- end }}

{{- define "mcp-pinot.validateExposure" -}}
{{- $isLoopback := eq (include "mcp-pinot.isLoopbackHost" .) "true" -}}
{{- $serviceEnabled := .Values.service.enabled -}}
{{- $traefikEnabled := .Values.traefik.enabled -}}
{{- $healthCheckEnabled := or .Values.healthCheck.liveness.enabled .Values.healthCheck.readiness.enabled -}}
{{- if and $traefikEnabled (not $serviceEnabled) -}}
{{- fail "traefik.enabled=true requires service.enabled=true" -}}
{{- end -}}
{{- $authEnabled := or .Values.mcp.auth.provider .Values.mcp.oauth.enabled -}}
{{- if and (or $serviceEnabled $traefikEnabled $healthCheckEnabled) $isLoopback -}}
{{- fail "service, Traefik, and HTTP health checks require mcp.host to be non-loopback; set mcp.host=0.0.0.0 and an auth provider (mcp.auth.provider=oauth|static, or mcp.oauth.enabled=true)" -}}
{{- end -}}
{{- if and (not $isLoopback) (not $authEnabled) -}}
{{- fail "mcp.host is non-loopback, so an auth provider is required; set mcp.auth.provider=oauth|static (or the legacy mcp.oauth.enabled=true)" -}}
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
