{{/*
Expand the name of the chart.
*/}}
{{- define "network-inventory-planning.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "network-inventory-planning.fullname" -}}
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
Chart name and version as used by the chart label.
*/}}
{{- define "network-inventory-planning.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "network-inventory-planning.labels" -}}
helm.sh/chart: {{ include "network-inventory-planning.chart" . }}
{{ include "network-inventory-planning.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "network-inventory-planning.selectorLabels" -}}
app.kubernetes.io/name: {{ include "network-inventory-planning.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "network-inventory-planning.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "network-inventory-planning.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding DATABASE_URL, when the chart creates its own.
*/}}
{{- define "network-inventory-planning.databaseSecretName" -}}
{{- if .Values.database.existingSecret }}
{{- .Values.database.existingSecret }}
{{- else }}
{{- include "network-inventory-planning.fullname" . }}-database
{{- end }}
{{- end }}

{{/*
Fully qualified name of the MCP server deployment/service.
*/}}
{{- define "network-inventory-planning.mcpFullname" -}}
{{- include "network-inventory-planning.fullname" . }}-mcp
{{- end }}

{{/*
Fully qualified name of the console-remote frontend deployment/service
(the nginx pod that serves nip_mfe, ADR 0010).
*/}}
{{- define "network-inventory-planning.frontendFullname" -}}
{{- include "network-inventory-planning.fullname" . }}-frontend
{{- end }}

{{/*
Fully qualified name of the analytics projector deployment (ADR 0009).
*/}}
{{- define "network-inventory-planning.projectorFullname" -}}
{{- include "network-inventory-planning.fullname" . }}-projector
{{- end }}

{{/*
Fully qualified name of the analytics reports deployment/service (ADR 0009).
The reports Service is cluster-internal (component=analytics-reports); nothing
in this chart routes external traffic to it.
*/}}
{{- define "network-inventory-planning.reportsFullname" -}}
{{- include "network-inventory-planning.fullname" . }}-reports
{{- end }}

{{/*
Name of the Secret holding the analytical DSNs: the operator's own
(analytics.database.existingSecret, keys ANALYTICS_DATABASE_URL and
ANALYTICS_READER_DATABASE_URL) or the one this chart creates.
*/}}
{{- define "network-inventory-planning.analyticsSecretName" -}}
{{- if .Values.analytics.database.existingSecret }}
{{- .Values.analytics.database.existingSecret }}
{{- else }}
{{- include "network-inventory-planning.fullname" . }}-analytics
{{- end }}
{{- end }}

{{/*
analytics.enabled needs an analytical DSN source (the projector and reports
refuse to boot without one) and a broker (KAFKA_BROKERS is only rendered into
the api when kafka.enabled; the projector reads kafka.brokers). Fail at render
instead of crash-looping.
*/}}
{{- define "network-inventory-planning.requireAnalyticsConfig" -}}
{{- if not (or .Values.analytics.database.projectorUrl .Values.analytics.database.existingSecret) -}}
{{- fail "analytics.enabled is true but neither analytics.database.projectorUrl nor analytics.database.existingSecret is set — the projector and reports binaries refuse to boot without an analytical database URL." -}}
{{- end -}}
{{- if not .Values.kafka.enabled -}}
{{- fail "analytics.enabled is true but kafka.enabled is false — the projector consumes warehouse.network-inventory-planning.analytics and needs kafka.brokers. Set kafka.enabled=true." -}}
{{- end -}}
{{- end -}}
