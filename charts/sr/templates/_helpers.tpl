{{- define "sr.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sr.fullname" -}}
{{- $name := include "sr.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "sr.labels" -}}
app.kubernetes.io/name: {{ include "sr.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "sr.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sr.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
