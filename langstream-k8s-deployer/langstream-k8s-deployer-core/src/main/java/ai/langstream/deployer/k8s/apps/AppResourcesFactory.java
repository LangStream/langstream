/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.deployer.k8s.apps;

import static ai.langstream.deployer.k8s.CRDConstants.DEPLOYER_JOB_PREFIX_CLEANUP;
import static ai.langstream.deployer.k8s.CRDConstants.DEPLOYER_JOB_PREFIX_DEPLOYER;
import static ai.langstream.deployer.k8s.CRDConstants.MAX_APPLICATION_ID_LENGTH;
import static ai.langstream.deployer.k8s.CRDConstants.SETUP_JOB_PREFIX_CLEANUP;
import static ai.langstream.deployer.k8s.CRDConstants.SETUP_JOB_PREFIX_DEPLOYER;

import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.deployer.k8s.CRDConstants;
import ai.langstream.deployer.k8s.PodTemplate;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpec;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpecOptions;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.runtime.api.application.ApplicationSetupConfiguration;
import ai.langstream.runtime.api.application.ApplicationSetupConstants;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConfiguration;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConstants;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

public class AppResourcesFactory {

    @Builder
    @Getter
    public static class GenerateJobParams {
        private ApplicationCustomResource applicationCustomResource;
        private boolean deleteJob;
        @Builder.Default private Map<String, Object> clusterRuntimeConfiguration = Map.of();
        private String image;
        private String imagePullPolicy;
        private PodTemplate podTemplate;
    }

    @SneakyThrows
    public static Job generateDeployerJob(GenerateJobParams params) {
        final ApplicationCustomResource applicationCustomResource =
                Objects.requireNonNull(params.getApplicationCustomResource());
        final boolean isDeleteJob = params.isDeleteJob();
        final Map<String, Object> clusterRuntimeConfiguration =
                params.getClusterRuntimeConfiguration();
        final String image = params.getImage();
        final String imagePullPolicy = params.getImagePullPolicy();

        final String applicationId = applicationCustomResource.getMetadata().getName();
        final ApplicationSpec spec = applicationCustomResource.getSpec();
        final String tenant = spec.getTenant();
        final ApplicationSpecOptions applicationSpecOptions =
                ApplicationSpec.deserializeOptions(spec.getOptions());

        final String clusterRuntimeConfigVolumeName = "cluster-runtime-config";
        final String appConfigVolumeName = "app-config";

        final String containerImage = resolveContainerImage(image, spec);
        final String containerImagePullPolicy =
                resolveContainerImagePullPolicy(imagePullPolicy, spec);

        RuntimeDeployerConfiguration.DeployFlags deployFlags =
                new RuntimeDeployerConfiguration.DeployFlags();
        deployFlags.setAutoUpgradeRuntimeImage(applicationSpecOptions.isAutoUpgradeRuntimeImage());
        deployFlags.setAutoUpgradeRuntimeImagePullPolicy(
                applicationSpecOptions.isAutoUpgradeRuntimeImagePullPolicy());
        deployFlags.setAutoUpgradeAgentResources(
                applicationSpecOptions.isAutoUpgradeAgentResources());
        deployFlags.setAutoUpgradeAgentPodTemplate(
                applicationSpecOptions.isAutoUpgradeAgentPodTemplate());
        deployFlags.setSeed(applicationSpecOptions.getSeed());
        final RuntimeDeployerConfiguration config =
                new RuntimeDeployerConfiguration(
                        applicationId,
                        tenant,
                        spec.getApplication(),
                        spec.getCodeArchiveId(),
                        deployFlags);

        Map<String, Object> initContainerConfigs = new LinkedHashMap<>();
        initContainerConfigs.put(appConfigVolumeName, config);
        initContainerConfigs.put(clusterRuntimeConfigVolumeName, clusterRuntimeConfiguration);
        final Container initContainer =
                createInitContainerInjectingConfig(
                        "deployer-init-config",
                        containerImage,
                        containerImagePullPolicy,
                        initContainerConfigs);
        final String command = isDeleteJob ? "delete" : "deploy";
        final String clusterConfigVolume = "cluster-config";

        final List<VolumeMount> volumeMounts =
                List.of(
                        new VolumeMountBuilder()
                                .withName(appConfigVolumeName)
                                .withMountPath("/%s".formatted(appConfigVolumeName))
                                .build(),
                        new VolumeMountBuilder()
                                .withName("app-secrets")
                                .withMountPath("/app-secrets")
                                .build(),
                        new VolumeMountBuilder()
                                .withName(clusterRuntimeConfigVolumeName)
                                .withMountPath("/%s".formatted(clusterRuntimeConfigVolumeName))
                                .build(),
                        new VolumeMountBuilder()
                                .withName(clusterConfigVolume)
                                .withMountPath("/cluster-config")
                                .build());
        final List<EnvVar> envVars =
                List.of(
                        new EnvVarBuilder()
                                .withName(RuntimeDeployerConstants.APP_CONFIG_ENV)
                                .withValue("/%s/config".formatted(appConfigVolumeName))
                                .build(),
                        new EnvVarBuilder()
                                .withName(RuntimeDeployerConstants.CLUSTER_RUNTIME_CONFIG_ENV)
                                .withValue("/%s/config".formatted(clusterRuntimeConfigVolumeName))
                                .build(),
                        new EnvVarBuilder()
                                .withName(RuntimeDeployerConstants.APP_SECRETS_ENV)
                                .withValue("/app-secrets/secrets")
                                .build(),
                        new EnvVarBuilder()
                                .withName(RuntimeDeployerConstants.CLUSTER_CONFIG_ENV)
                                .withValue("/cluster-config/config")
                                .build(),
                        new EnvVarBuilder()
                                .withName(RuntimeDeployerConstants.TOKEN_ENV)
                                .withValue("/var/run/secrets/kubernetes.io/serviceaccount/token")
                                .build());
        // keep args for backward compatibility
        final List<String> args =
                List.of(
                        "deployer-runtime",
                        command,
                        "/%s/config".formatted(clusterRuntimeConfigVolumeName),
                        "/%s/config".formatted(appConfigVolumeName),
                        "/app-secrets/secrets");
        final String containerName = "deployer";
        final Container container =
                createContainer(
                        containerName,
                        containerImage,
                        containerImagePullPolicy,
                        volumeMounts,
                        envVars,
                        args);

        final Map<String, String> labels = getLabelsForDeployer(isDeleteJob, applicationId);
        final List<Volume> volumes =
                List.of(
                        new VolumeBuilder()
                                .withName(appConfigVolumeName)
                                .withEmptyDir(new EmptyDirVolumeSource())
                                .build(),
                        new VolumeBuilder()
                                .withName("app-secrets")
                                .withNewSecret()
                                .withSecretName(applicationId)
                                .endSecret()
                                .build(),
                        new VolumeBuilder()
                                .withName(clusterRuntimeConfigVolumeName)
                                .withEmptyDir(new EmptyDirVolumeSource())
                                .build(),
                        new VolumeBuilder()
                                .withName(clusterConfigVolume)
                                .withNewSecret()
                                .withSecretName(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET)
                                .withItems(
                                        new KeyToPathBuilder()
                                                .withKey(
                                                        CRDConstants
                                                                .TENANT_CLUSTER_CONFIG_SECRET_KEY)
                                                .withPath("config")
                                                .build())
                                .endSecret()
                                .build());
        final String jobName = getDeployerJobName(applicationId, isDeleteJob);

        final String serviceAccountName =
                CRDConstants.computeDeployerServiceAccountForTenant(tenant);
        return generateJob(
                params,
                jobName,
                labels,
                List.of(initContainer),
                container,
                volumes,
                serviceAccountName);
    }

    @SneakyThrows
    public static Job generateSetupJob(GenerateJobParams params) {
        final ApplicationCustomResource applicationCustomResource =
                Objects.requireNonNull(params.getApplicationCustomResource());
        final boolean isDeleteJob = params.isDeleteJob();
        final Map<String, Object> clusterRuntimeConfiguration =
                params.getClusterRuntimeConfiguration();
        final String image = params.getImage();
        final String imagePullPolicy = params.getImagePullPolicy();

        final String applicationId = applicationCustomResource.getMetadata().getName();
        final ApplicationSpec spec = applicationCustomResource.getSpec();
        final String tenant = spec.getTenant();

        final String clusterRuntimeConfigVolumeName = "cluster-runtime-config";
        final String appConfigVolumeName = "app-config";
        final String clusterConfigVolume = "cluster-config";

        final String containerImage = resolveContainerImage(image, spec);
        final String containerImagePullPolicy =
                resolveContainerImagePullPolicy(imagePullPolicy, spec);

        final ApplicationSetupConfiguration config =
                new ApplicationSetupConfiguration(
                        applicationId, tenant, spec.getApplication(), spec.getCodeArchiveId());

        Map<String, Object> initContainerConfigs = new LinkedHashMap<>();
        initContainerConfigs.put(appConfigVolumeName, config);
        initContainerConfigs.put(clusterRuntimeConfigVolumeName, clusterRuntimeConfiguration);
        final Container initContainer =
                createInitContainerInjectingConfig(
                        "setup-init-config",
                        containerImage,
                        containerImagePullPolicy,
                        initContainerConfigs);
        final List<VolumeMount> volumeMounts =
                List.of(
                        new VolumeMountBuilder()
                                .withName(appConfigVolumeName)
                                .withMountPath("/%s".formatted(appConfigVolumeName))
                                .build(),
                        new VolumeMountBuilder()
                                .withName("app-secrets")
                                .withMountPath("/app-secrets")
                                .build(),
                        new VolumeMountBuilder()
                                .withName(clusterRuntimeConfigVolumeName)
                                .withMountPath("/%s".formatted(clusterRuntimeConfigVolumeName))
                                .build(),
                        new VolumeMountBuilder()
                                .withName(clusterConfigVolume)
                                .withMountPath("/cluster-config")
                                .build());
        final List<EnvVar> envVars =
                List.of(
                        new EnvVarBuilder()
                                .withName(ApplicationSetupConstants.APP_CONFIG_ENV)
                                .withValue("/%s/config".formatted(appConfigVolumeName))
                                .build(),
                        new EnvVarBuilder()
                                .withName(ApplicationSetupConstants.CLUSTER_RUNTIME_CONFIG_ENV)
                                .withValue("/%s/config".formatted(clusterRuntimeConfigVolumeName))
                                .build(),
                        new EnvVarBuilder()
                                .withName(ApplicationSetupConstants.APP_SECRETS_ENV)
                                .withValue("/app-secrets/secrets")
                                .build(),
                        new EnvVarBuilder()
                                .withName(ApplicationSetupConstants.CLUSTER_CONFIG_ENV)
                                .withValue("/cluster-config/config")
                                .build(),
                        new EnvVarBuilder()
                                .withName(ApplicationSetupConstants.TOKEN_ENV)
                                .withValue("/var/run/secrets/kubernetes.io/serviceaccount/token")
                                .build());
        final String cmd = isDeleteJob ? "cleanup" : "deploy";

        final List<String> args = List.of("application-setup", cmd);
        final String containerName = "setup";
        final Container container =
                createContainer(
                        containerName,
                        containerImage,
                        containerImagePullPolicy,
                        volumeMounts,
                        envVars,
                        args);

        final Map<String, String> labels = getLabelsForSetup(isDeleteJob, applicationId);
        final List<Volume> volumes =
                List.of(
                        new VolumeBuilder()
                                .withName(appConfigVolumeName)
                                .withEmptyDir(new EmptyDirVolumeSource())
                                .build(),
                        new VolumeBuilder()
                                .withName("app-secrets")
                                .withNewSecret()
                                .withSecretName(applicationId)
                                .endSecret()
                                .build(),
                        new VolumeBuilder()
                                .withName(clusterRuntimeConfigVolumeName)
                                .withEmptyDir(new EmptyDirVolumeSource())
                                .build(),
                        new VolumeBuilder()
                                .withName(clusterConfigVolume)
                                .withNewSecret()
                                .withSecretName(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET)
                                .withItems(
                                        new KeyToPathBuilder()
                                                .withKey(
                                                        CRDConstants
                                                                .TENANT_CLUSTER_CONFIG_SECRET_KEY)
                                                .withPath("config")
                                                .build())
                                .endSecret()
                                .build());
        final String jobName = getSetupJobName(applicationId, isDeleteJob);

        final String serviceAccountName =
                CRDConstants.computeRuntimeServiceAccountForTenant(tenant);
        return generateJob(
                params,
                jobName,
                labels,
                List.of(initContainer),
                container,
                volumes,
                serviceAccountName);
    }

    private static Container createContainer(
            String containerName,
            String containerImage,
            String containerImagePullPolicy,
            List<VolumeMount> volumeMounts,
            List<EnvVar> envVars,
            List<String> args) {
        return new ContainerBuilder()
                .withName(containerName)
                .withImage(containerImage)
                .withImagePullPolicy(containerImagePullPolicy)
                .withEnv(envVars)
                .withArgs(args)
                .withVolumeMounts(volumeMounts)
                .withNewResources()
                .withRequests(
                        Map.of("cpu", Quantity.parse("100m"), "memory", Quantity.parse("128Mi")))
                .endResources()
                .withTerminationMessagePolicy("FallbackToLogsOnError")
                .build();
    }

    @SneakyThrows
    private static Job generateJob(
            GenerateJobParams params,
            String jobName,
            final Map<String, String> labels,
            List<Container> initContainers,
            Container container,
            List<Volume> volumes,
            String serviceAccountName) {
        final ApplicationCustomResource applicationCustomResource =
                Objects.requireNonNull(params.getApplicationCustomResource());
        final PodTemplate podTemplate = params.getPodTemplate();
        return new JobBuilder()
                .withNewMetadata()
                .withName(jobName)
                .withNamespace(applicationCustomResource.getMetadata().getNamespace())
                .withLabels(labels)
                .withOwnerReferences(
                        KubeUtil.getOwnerReferenceForResource(applicationCustomResource))
                .endMetadata()
                .withNewSpec()
                // only 1 attempt but keep the pod so we debug the logs
                .withBackoffLimit(0)
                .withNewTemplate()
                .withNewMetadata()
                .withAnnotations(getPodAnnotations(podTemplate))
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withTolerations(podTemplate != null ? podTemplate.tolerations() : null)
                .withNodeSelector(podTemplate != null ? podTemplate.nodeSelector() : null)
                .withServiceAccountName(serviceAccountName)
                .withVolumes(volumes)
                .withInitContainers(initContainers)
                .withContainers(List.of(container))
                .withRestartPolicy("Never")
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }

    private static String resolveContainerImagePullPolicy(
            String imagePullPolicy, ApplicationSpec spec) {
        final String containerImagePullPolicy =
                imagePullPolicy != null && !imagePullPolicy.isBlank()
                        ? imagePullPolicy
                        : spec.getImagePullPolicy();
        if (containerImagePullPolicy == null) {
            throw new IllegalStateException(
                    "Runtime image pull policy is not specified, neither in the resource and in the deployer "
                            + "configuration.");
        }
        return containerImagePullPolicy;
    }

    private static String resolveContainerImage(String image, ApplicationSpec spec) {
        final String containerImage = image != null && !image.isBlank() ? image : spec.getImage();

        if (containerImage == null) {
            throw new IllegalStateException(
                    "Runtime image is not specified, neither in the resource and in the deployer configuration.");
        }
        return containerImage;
    }

    private static Container createInitContainerInjectingConfig(
            String containerName,
            String containerImage,
            String containerImagePullPolicy,
            Map<String, Object> configs) {
        final ContainerBuilder builder =
                new ContainerBuilder()
                        .withName(containerName)
                        .withImage(containerImage)
                        .withImagePullPolicy(containerImagePullPolicy)
                        .withCommand("bash", "-c");

        List<String> cmds = new ArrayList<>();
        List<VolumeMount> volumeMounts = new ArrayList<>();
        for (Map.Entry<String, Object> entry : configs.entrySet()) {
            final String jsonValue = SerializationUtil.writeInlineBashJson(entry.getValue());
            cmds.add("echo '%s' > /%s/config".formatted(jsonValue, entry.getKey()));
            volumeMounts.add(
                    new VolumeMountBuilder()
                            .withName(entry.getKey())
                            .withMountPath("/%s".formatted(entry.getKey()))
                            .build());
        }
        return builder.withArgs(cmds.stream().collect(Collectors.joining(" && ")))
                .withVolumeMounts(volumeMounts)
                .build();
    }

    private static Map<String, String> getPodAnnotations(PodTemplate podTemplate) {
        final Map<String, String> annotations = new HashMap<>();
        if (podTemplate != null && podTemplate.annotations() != null) {
            annotations.putAll(podTemplate.annotations());
        }
        return annotations;
    }

    public static Map<String, String> getLabelsForDeployer(boolean delete, String applicationId) {
        return Map.of(
                CRDConstants.COMMON_LABEL_APP,
                "langstream-deployer",
                CRDConstants.APP_LABEL_APPLICATION,
                applicationId,
                CRDConstants.APP_LABEL_SCOPE,
                delete ? CRDConstants.APP_LABEL_SCOPE_DELETE : CRDConstants.APP_LABEL_SCOPE_DEPLOY);
    }

    public static Map<String, String> getLabelsForSetup(boolean delete, String applicationId) {
        return Map.of(
                CRDConstants.COMMON_LABEL_APP,
                "langstream-setup",
                CRDConstants.APP_LABEL_APPLICATION,
                applicationId,
                CRDConstants.APP_LABEL_SCOPE,
                delete ? CRDConstants.APP_LABEL_SCOPE_DELETE : CRDConstants.APP_LABEL_SCOPE_DEPLOY);
    }

    public static String getDeployerJobName(String applicationId, boolean delete) {
        if (delete) {
            return DEPLOYER_JOB_PREFIX_CLEANUP + applicationId;
        } else {
            return DEPLOYER_JOB_PREFIX_DEPLOYER + applicationId;
        }
    }

    public static String getSetupJobName(String applicationId, boolean delete) {
        if (delete) {
            return SETUP_JOB_PREFIX_CLEANUP + applicationId;
        } else {
            return SETUP_JOB_PREFIX_DEPLOYER + applicationId;
        }
    }

    public static ApplicationLifecycleStatus computeApplicationStatus(
            KubernetesClient client, ApplicationCustomResource customResource) {

        return switch (customResource.getStatus().getStatus().getStatus()) {
            case CREATED, DEPLOYED, ERROR_DEPLOYING, ERROR_DELETING -> customResource
                    .getStatus()
                    .getStatus();
            case DEPLOYING, DELETING -> getStatusFromJob(client, customResource);
        };
    }

    private static ApplicationLifecycleStatus getStatusFromJob(
            KubernetesClient client, ApplicationCustomResource customResource) {
        boolean delete =
                customResource.getStatus().getStatus().getStatus()
                        == ApplicationLifecycleStatus.Status.DELETING;
        Pod pod = getDeployerPod(client, customResource, delete);
        if (pod == null) {
            pod = getSetupPod(client, customResource, delete);
        }
        if (pod == null) {
            return customResource.getStatus().getStatus();
        }
        final KubeUtil.PodStatus podStatus =
                KubeUtil.getPodsStatuses(List.of(pod)).values().iterator().next();

        return switch (podStatus.getState()) {
            case RUNNING, WAITING, COMPLETED -> customResource.getStatus().getStatus();
            case ERROR -> delete
                    ? ApplicationLifecycleStatus.errorDeleting(podStatus.getMessage())
                    : ApplicationLifecycleStatus.errorDeploying(podStatus.getMessage());
        };
    }

    private static Pod getSetupPod(
            KubernetesClient client, ApplicationCustomResource customResource, boolean delete) {
        return getPod(
                client,
                customResource,
                getLabelsForSetup(delete, customResource.getMetadata().getName()));
    }

    private static Pod getDeployerPod(
            KubernetesClient client, ApplicationCustomResource customResource, boolean delete) {
        final Map<String, String> labels =
                getLabelsForDeployer(delete, customResource.getMetadata().getName());
        return getPod(client, customResource, labels);
    }

    private static Pod getPod(
            KubernetesClient client,
            ApplicationCustomResource customResource,
            Map<String, String> labels) {
        final List<Pod> pods =
                client.resources(Pod.class)
                        .inNamespace(customResource.getMetadata().getNamespace())
                        .withLabels(labels)
                        .list()
                        .getItems();
        if (pods.isEmpty()) {
            return null;
        }
        final Pod pod = pods.get(0);
        return pod;
    }

    public static void validateApplicationId(String applicationId) throws IllegalArgumentException {
        if (applicationId.length() <= 1) {
            throw new IllegalArgumentException(
                    ("Application id '%s' is too short. Must be at least 2 characters long.")
                            .formatted(applicationId));
        }
        if (!CRDConstants.RESOURCE_NAME_PATTERN.matcher(applicationId).matches()) {
            throw new IllegalArgumentException(
                    ("Application id '%s' contains illegal characters. Allowed characters are alphanumeric and "
                                    + "dash.")
                            .formatted(applicationId));
        }

        if (applicationId.length() > MAX_APPLICATION_ID_LENGTH) {
            throw new IllegalArgumentException(
                    "Application id '%s' is too long, max length is %d"
                            .formatted(applicationId, MAX_APPLICATION_ID_LENGTH));
        }
    }
}
