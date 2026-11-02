{{- define "ffkv.name" -}}
{{- printf "%s-ffkv" .Release.Name | trunc 54 | trimSuffix "-" -}}
{{- end -}}
{{- define "ffkv.labels" -}}
app.kubernetes.io/name: ffkv
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}
{{- define "ffkv.probe" -}}
exec:
  command:
    - python3
    - /usr/local/bin/ffkv-probe
    - --host
    - 127.0.0.1
    - --port
    - '6379'
    - --username
    - {{ .Values.healthUsername | quote }}
    - --password-file
    - /run/ffkv/config/health-password
    - --tls-ca
    - /run/ffkv/config/ca.crt
    - --tls-server-name
    - localhost
timeoutSeconds: 5
periodSeconds: 15
{{- end -}}
