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
Without DATABASE_URL the binary boots in its zero-config diagnostic mode (no
read models, no consumers, no saga): fine for `go run`, never a deployable
state. Fail at render instead of shipping a pod that looks healthy and does
nothing.
*/}}
{{- define "network-inventory-planning.requireDatabase" -}}
{{- if not (or .Values.database.url .Values.database.existingSecret) -}}
{{- fail "network-inventory-planning requires database.url or database.existingSecret to be set — without DATABASE_URL the binary runs in diagnostic mode with no read models, consumers or saga, which is not a deployable state." -}}
{{- end -}}
{{- end -}}

{{/*
Every Kafka-dependent feature (the five consumers, the outbox relay) is gated on
KAFKA_BROKERS, which is only rendered when kafka.enabled. An enabled relay or a
non-empty consumer group with kafka.enabled=false would be silently inert, so
refuse to render instead.
*/}}
{{- define "network-inventory-planning.requireKafka" -}}
{{- if not .Values.kafka.enabled -}}
{{- if .Values.outbox.relayEnabled -}}
{{- fail "outbox.relayEnabled is true but kafka.enabled is false — the outbox relay needs KAFKA_BROKERS and would be silently disabled. Set kafka.enabled=true and kafka.brokers." -}}
{{- end -}}
{{- end -}}
{{- end -}}
