{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "sga.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
    CONTROL PLANE
*/}}

{{- define "sga.controlPlaneName" -}}
{{- default .Chart.Name .Values.controlPlane.nameOverride | trunc 63 | trimSuffix "-" }}-control-plane
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "sga.controlPlaneFullname" -}}
{{- if .Values.controlPlane.fullnameOverride }}
{{- .Values.controlPlane.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.controlPlane.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}-control-plane
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}-control-plane
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "sga.controlPlaneLabels" -}}
helm.sh/chart: {{ include "sga.chart" . }}
{{ include "sga.controlPlaneSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}


{{- define "sga.controlPlaneSelectorLabels" -}}
app.kubernetes.io/name: {{ include "sga.controlPlaneName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "sga.controlPlaneServiceAccountName" -}}
{{- if .Values.controlPlane.serviceAccount.create }}
{{- default (include "sga.controlPlaneFullname" .) .Values.controlPlane.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.controlPlane.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "sga.controlPlaneRoleName" -}}
{{- if .Values.controlPlane.serviceAccount.create }}
{{- default (include "sga.controlPlaneFullname" .) .Values.controlPlane.serviceAccount.role.name }}
{{- else }}
{{- default "default" .Values.controlPlane.serviceAccount.role.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role binding to use
*/}}
{{- define "sga.controlPlaneRoleBindingName" -}}
{{- if .Values.controlPlane.serviceAccount.create }}
{{- default (include "sga.controlPlaneFullname" .) .Values.controlPlane.serviceAccount.roleBinding.name }}
{{- else }}
{{- default "default" .Values.controlPlane.serviceAccount.roleBinding.name }}
{{- end }}
{{- end }}




{{/*
    DEPLOYER
*/}}

{{- define "sga.deployerName" -}}
{{- default .Chart.Name .Values.deployer.nameOverride | trunc 63 | trimSuffix "-" }}-deployer
{{- end }}


{{- define "sga.deployerFullname" -}}
{{- if .Values.deployer.fullnameOverride }}
{{- .Values.deployer.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.deployer.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}-deployer
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}-deployer
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "sga.deployerLabels" -}}
helm.sh/chart: {{ include "sga.chart" . }}
{{ include "sga.deployerSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}


{{- define "sga.deployerSelectorLabels" -}}
app.kubernetes.io/name: {{ include "sga.deployerName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "sga.deployerServiceAccountName" -}}
{{- if .Values.deployer.serviceAccount.create }}
{{- default (include "sga.deployerFullname" .) .Values.deployer.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.deployer.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "sga.deployerRoleName" -}}
{{- if .Values.deployer.serviceAccount.create }}
{{- default (include "sga.deployerFullname" .) .Values.deployer.serviceAccount.role.name }}
{{- else }}
{{- default "default" .Values.deployer.serviceAccount.role.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role binding to use
*/}}
{{- define "sga.deployerRoleBindingName" -}}
{{- if .Values.deployer.serviceAccount.create }}
{{- default (include "sga.deployerFullname" .) .Values.deployer.serviceAccount.roleBinding.name }}
{{- else }}
{{- default "default" .Values.deployer.serviceAccount.roleBinding.name }}
{{- end }}
{{- end }}



{{/*
    API GATEWAY
*/}}

{{- define "sga.apiGatewayName" -}}
{{- default .Chart.Name .Values.apiGateway.nameOverride | trunc 63 | trimSuffix "-" }}-api-gateway
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "sga.apiGatewayFullname" -}}
{{- if .Values.apiGateway.fullnameOverride }}
{{- .Values.apiGateway.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.apiGateway.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}-api-gateway
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}-api-gateway
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "sga.apiGatewayLabels" -}}
helm.sh/chart: {{ include "sga.chart" . }}
{{ include "sga.apiGatewaySelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}


{{- define "sga.apiGatewaySelectorLabels" -}}
app.kubernetes.io/name: {{ include "sga.apiGatewayName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "sga.apiGatewayServiceAccountName" -}}
{{- if .Values.apiGateway.serviceAccount.create }}
{{- default (include "sga.apiGatewayFullname" .) .Values.apiGateway.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.apiGateway.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "sga.apiGatewayRoleName" -}}
{{- if .Values.apiGateway.serviceAccount.create }}
{{- default (include "sga.apiGatewayFullname" .) .Values.apiGateway.serviceAccount.role.name }}
{{- else }}
{{- default "default" .Values.apiGateway.serviceAccount.role.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role binding to use
*/}}
{{- define "sga.apiGatewayRoleBindingName" -}}
{{- if .Values.apiGateway.serviceAccount.create }}
{{- default (include "sga.apiGatewayFullname" .) .Values.apiGateway.serviceAccount.roleBinding.name }}
{{- else }}
{{- default "default" .Values.apiGateway.serviceAccount.roleBinding.name }}
{{- end }}
{{- end }}




