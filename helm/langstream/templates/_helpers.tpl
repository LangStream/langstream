{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "langstream.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
    CONTROL PLANE
*/}}

{{- define "langstream.controlPlaneName" -}}
{{- default .Chart.Name .Values.controlPlane.nameOverride | trunc 63 | trimSuffix "-" }}-control-plane
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "langstream.controlPlaneFullname" -}}
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
{{- define "langstream.controlPlaneLabels" -}}
helm.sh/chart: {{ include "langstream.chart" . }}
{{ include "langstream.controlPlaneSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}


{{- define "langstream.controlPlaneSelectorLabels" -}}
app.kubernetes.io/name: {{ include "langstream.controlPlaneName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "langstream.controlPlaneServiceAccountName" -}}
{{- if .Values.controlPlane.serviceAccount.create }}
{{- default (include "langstream.controlPlaneFullname" .) .Values.controlPlane.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.controlPlane.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "langstream.controlPlaneRoleName" -}}
{{- if .Values.controlPlane.serviceAccount.create }}
{{- default (include "langstream.controlPlaneFullname" .) .Values.controlPlane.serviceAccount.role.name }}
{{- else }}
{{- default "default" .Values.controlPlane.serviceAccount.role.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role binding to use
*/}}
{{- define "langstream.controlPlaneRoleBindingName" -}}
{{- if .Values.controlPlane.serviceAccount.create }}
{{- default (include "langstream.controlPlaneFullname" .) .Values.controlPlane.serviceAccount.roleBinding.name }}
{{- else }}
{{- default "default" .Values.controlPlane.serviceAccount.roleBinding.name }}
{{- end }}
{{- end }}




{{/*
    DEPLOYER
*/}}

{{- define "langstream.deployerName" -}}
{{- default .Chart.Name .Values.deployer.nameOverride | trunc 63 | trimSuffix "-" }}-deployer
{{- end }}


{{- define "langstream.deployerFullname" -}}
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
{{- define "langstream.deployerLabels" -}}
helm.sh/chart: {{ include "langstream.chart" . }}
{{ include "langstream.deployerSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}


{{- define "langstream.deployerSelectorLabels" -}}
app.kubernetes.io/name: {{ include "langstream.deployerName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "langstream.deployerServiceAccountName" -}}
{{- if .Values.deployer.serviceAccount.create }}
{{- default (include "langstream.deployerFullname" .) .Values.deployer.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.deployer.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "langstream.deployerRoleName" -}}
{{- if .Values.deployer.serviceAccount.create }}
{{- default (include "langstream.deployerFullname" .) .Values.deployer.serviceAccount.role.name }}
{{- else }}
{{- default "default" .Values.deployer.serviceAccount.role.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role binding to use
*/}}
{{- define "langstream.deployerRoleBindingName" -}}
{{- if .Values.deployer.serviceAccount.create }}
{{- default (include "langstream.deployerFullname" .) .Values.deployer.serviceAccount.roleBinding.name }}
{{- else }}
{{- default "default" .Values.deployer.serviceAccount.roleBinding.name }}
{{- end }}
{{- end }}


{{/*
    CLIENT
*/}}

{{- define "langstream.clientName" -}}
{{- default .Chart.Name .Values.client.nameOverride | trunc 63 | trimSuffix "-" }}-client
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "langstream.clientFullname" -}}
{{- if .Values.client.fullnameOverride }}
{{- .Values.client.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.client.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}-client
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}-client
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "langstream.clientLabels" -}}
helm.sh/chart: {{ include "langstream.chart" . }}
{{ include "langstream.clientSelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}


{{- define "langstream.clientSelectorLabels" -}}
app.kubernetes.io/name: {{ include "langstream.clientName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}



{{/*
    API GATEWAY
*/}}

{{- define "langstream.apiGatewayName" -}}
{{- default .Chart.Name .Values.apiGateway.nameOverride | trunc 63 | trimSuffix "-" }}-api-gateway
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "langstream.apiGatewayFullname" -}}
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
{{- define "langstream.apiGatewayLabels" -}}
helm.sh/chart: {{ include "langstream.chart" . }}
{{ include "langstream.apiGatewaySelectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}


{{- define "langstream.apiGatewaySelectorLabels" -}}
app.kubernetes.io/name: {{ include "langstream.apiGatewayName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "langstream.apiGatewayServiceAccountName" -}}
{{- if .Values.apiGateway.serviceAccount.create }}
{{- default (include "langstream.apiGatewayFullname" .) .Values.apiGateway.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.apiGateway.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "langstream.apiGatewayRoleName" -}}
{{- if .Values.apiGateway.serviceAccount.create }}
{{- default (include "langstream.apiGatewayFullname" .) .Values.apiGateway.serviceAccount.role.name }}
{{- else }}
{{- default "default" .Values.apiGateway.serviceAccount.role.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role binding to use
*/}}
{{- define "langstream.apiGatewayRoleBindingName" -}}
{{- if .Values.apiGateway.serviceAccount.create }}
{{- default (include "langstream.apiGatewayFullname" .) .Values.apiGateway.serviceAccount.roleBinding.name }}
{{- else }}
{{- default "default" .Values.apiGateway.serviceAccount.roleBinding.name }}
{{- end }}
{{- end }}




