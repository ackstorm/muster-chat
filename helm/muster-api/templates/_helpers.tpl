{{- define "muster-api.labels" -}}
app.kubernetes.io/name: muster-api
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}

{{- define "muster-api.selectorLabels" -}}
app.kubernetes.io/name: muster-api
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "muster-api.valkeyLabels" -}}
app.kubernetes.io/name: muster-api-valkey
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "muster-api.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{ .Values.serviceAccount.name | default .Release.Name }}
{{- else -}}
{{ .Values.serviceAccount.name | default "default" }}
{{- end -}}
{{- end }}
