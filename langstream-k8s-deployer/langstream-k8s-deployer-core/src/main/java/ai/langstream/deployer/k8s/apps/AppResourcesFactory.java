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

import static ai.langstream.deployer.k8s.CRDConstants.JOB_PREFIX_CLEANUP;
import static ai.langstream.deployer.k8s.CRDConstants.JOB_PREFIX_DEPLOYER;
import static ai.langstream.deployer.k8s.CRDConstants.MAX_APPLICATION_ID_LENGTH;
import ai.langstream.api.model.ApplicationLifecycleStatus;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.deployer.k8s.CRDConstants;
import ai.langstream.deployer.k8s.PodTemplate;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import ai.langstream.deployer.k8s.api.crds.apps.ApplicationSpec;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConfiguration;
import ai.langstream.runtime.api.deployer.RuntimeDeployerConstants;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

public class AppResourcesFactory {

    @Builder
    @Getter
    public static class GenerateJobParams {
        private ApplicationCustomResource applicationCustomResource;
        private boolean deleteJob;
        @Builder.Default
        private Map<String, Object> clusterRuntimeConfiguration = Map.of();
        private String image;
        private String imagePullPolicy;
        private PodTemplate podTemplate;

    }


    @SneakyThrows
    public static Job generateJob(GenerateJobParams params) {
        final ApplicationCustomResource applicationCustomResource = Objects.requireNonNull(params.getApplicationCustomResource());
        final boolean isDeleteJob = params.isDeleteJob();
        final Map<String, Object> clusterRuntimeConfiguration = params.getClusterRuntimeConfiguration();
        final String image = params.getImage();
        final String imagePullPolicy = params.getImagePullPolicy();
        final PodTemplate podTemplate = params.getPodTemplate();

        final String applicationId = applicationCustomResource.getMetadata().getName();
        final ApplicationSpec spec = applicationCustomResource.getSpec();
        final String tenant = spec.getTenant();


        final RuntimeDeployerConfiguration config = new RuntimeDeployerConfiguration(
                applicationId,
                tenant,
                spec.getApplication(),
                spec.getCodeArchiveId()
        );

        final String containerImage = image != null && !image.isBlank() ? image: spec.getImage();
        final String containerImagePullPolicy = imagePullPolicy != null && !imagePullPolicy.isBlank() ? imagePullPolicy: spec.getImagePullPolicy();
        if (containerImage == null) {
            throw new IllegalStateException("Runtime image is not specified, neither in the resource and in the deployer configuration.");
        }
        if (containerImagePullPolicy == null) {
            throw new IllegalStateException("Runtime image pull policy is not specified, neither in the resource and in the deployer configuration.");
        }
        final Container initContainer = new ContainerBuilder()
                .withName("deployer-init-config")
                .withImage(containerImage)
                .withImagePullPolicy(containerImagePullPolicy)
                .withCommand("bash", "-c")
                .withArgs("echo '%s' > /app-config/config && echo '%s' > /cluster-runtime-config/config".formatted(
                        SerializationUtil.writeAsJson(config).replace("'", "'\"'\"'"),
                        SerializationUtil.writeAsJson(clusterRuntimeConfiguration).replace("'", "'\"'\"'")))
                .withVolumeMounts(new VolumeMountBuilder()
                                .withName("app-config")
                                .withMountPath("/app-config")
                                .build(),
                        new VolumeMountBuilder()
                                .withName("cluster-runtime-config")
                                .withMountPath("/cluster-runtime-config")
                                .build())
                .build();
        final String command = isDeleteJob ? "delete" : "deploy";

        final Container container = new ContainerBuilder()
                .withName("deployer")
                .withImage(containerImage)
                .withImagePullPolicy(containerImagePullPolicy)
                .withEnv(new EnvVarBuilder()
                        .withName(RuntimeDeployerConstants.APP_CONFIG_ENV)
                        .withValue("/app-config/config")
                        .build(),
                        new EnvVarBuilder()
                                .withName(RuntimeDeployerConstants.CLUSTER_RUNTIME_CONFIG_ENV)
                                .withValue("/cluster-runtime-config/config")
                                .build(),
                        new EnvVarBuilder()
                                .withName(RuntimeDeployerConstants.APP_SECRETS_ENV)
                                .withValue("/app-secrets/secrets")
                                .build()
                        )
                // keep args for backward compatibility
                .withArgs("deployer-runtime", command, "/cluster-runtime-config/config", "/app-config/config",
                        "/app-secrets/secrets")
                .withVolumeMounts(new VolumeMountBuilder()
                                .withName("app-config")
                                .withMountPath("/app-config")
                                .build(),
                        new VolumeMountBuilder()
                                .withName("app-secrets")
                                .withMountPath("/app-secrets")
                                .build(),
                        new VolumeMountBuilder()
                                .withName("cluster-runtime-config")
                                .withMountPath("/cluster-runtime-config")
                                .build())
                .withNewResources()
                .withRequests(Map.of("cpu", Quantity.parse("100m"), "memory", Quantity.parse("128Mi")))
                .endResources()
                .withTerminationMessagePolicy("FallbackToLogsOnError")
                .build();


        final Map<String, String> labels = getLabels(isDeleteJob, applicationId);
        return new JobBuilder()
                .withNewMetadata()
                .withName(getJobName(applicationId, isDeleteJob))
                .withNamespace(applicationCustomResource.getMetadata().getNamespace())
                .withLabels(labels)
                .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(applicationCustomResource))
                .endMetadata()
                .withNewSpec()
                // only 1 attempt but keep the pod so we debug the logs
                .withBackoffLimit(1)
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withTolerations(podTemplate != null ? podTemplate.tolerations() : null)
                .withNodeSelector(podTemplate != null ? podTemplate.nodeSelector() : null)
                .withServiceAccountName(tenant)
                .withVolumes(new VolumeBuilder()
                                .withName("app-config")
                                .withEmptyDir(new EmptyDirVolumeSource())
                                .build(),
                        new VolumeBuilder()
                                .withName("app-secrets")
                                .withNewSecret()
                                .withSecretName(applicationId)
                                .endSecret()
                                .build(),
                        new VolumeBuilder()
                                .withName("cluster-runtime-config")
                                .withEmptyDir(new EmptyDirVolumeSource())
                                .build()
                )
                .withInitContainers(List.of(initContainer))
                .withContainers(List.of(container))
                .withRestartPolicy("Never")
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }

    public static Map<String, String> getLabels(boolean delete, String applicationId) {
        return Map.of(
                CRDConstants.COMMON_LABEL_APP, "langstream-deployer",
                CRDConstants.APP_LABEL_APPLICATION, applicationId,
                CRDConstants.APP_LABEL_SCOPE, delete ? CRDConstants.APP_LABEL_SCOPE_DELETE : CRDConstants.APP_LABEL_SCOPE_DEPLOY);
    }

    public static String getJobName(String applicationId, boolean delete) {
        if (delete) {
            return JOB_PREFIX_CLEANUP + applicationId;
        } else {
            return JOB_PREFIX_DEPLOYER + applicationId;
        }
    }

    public static ApplicationLifecycleStatus computeApplicationStatus(KubernetesClient client,
                                                                      ApplicationCustomResource customResource) {

        return switch (customResource.getStatus().getStatus().getStatus()) {
            case CREATED, DEPLOYED, ERROR_DEPLOYING, ERROR_DELETING -> customResource.getStatus().getStatus();
            case DEPLOYING, DELETING -> getStatusFromJob(client, customResource);
        };
    }


    private static ApplicationLifecycleStatus getStatusFromJob(KubernetesClient client, ApplicationCustomResource customResource) {
        boolean delete = customResource.getStatus().getStatus().getStatus() == ApplicationLifecycleStatus.Status.DELETING;
        final List<Pod> pods = client.resources(Pod.class)
                .inNamespace(customResource.getMetadata().getNamespace())
                .withLabels(getLabels(delete, customResource.getMetadata().getName()))
                .list()
                .getItems();
        if (pods.isEmpty()) {
            // no job started yet
            return customResource.getStatus().getStatus();
        }
        final KubeUtil.PodStatus podStatus = KubeUtil.getPodsStatuses(pods).values().iterator().next();

        return switch (podStatus.getState()) {
            case RUNNING, WAITING -> customResource.getStatus().getStatus();
            case ERROR -> delete ? ApplicationLifecycleStatus.errorDeleting(podStatus.getMessage()) :
                ApplicationLifecycleStatus.errorDeploying(podStatus.getMessage());
        };
    }


    public static void validateApplicationId(String applicationId) throws IllegalArgumentException {
        if (!CRDConstants.RESOURCE_NAME_PATTERN.matcher(applicationId).matches()) {
            throw new IllegalArgumentException(("Application id '%s' contains illegal characters. Allowed characters are alphanumeric and "
                    + "dash.").formatted(applicationId));
        }

        if (applicationId.length() > MAX_APPLICATION_ID_LENGTH) {
            throw new IllegalArgumentException("Application id '%s' is too long, max length is %d".formatted(applicationId, MAX_APPLICATION_ID_LENGTH));
        }
    }


}
