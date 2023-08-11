/**
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
package com.datastax.oss.sga.deployer.k8s.apps;

import static com.datastax.oss.sga.deployer.k8s.CRDConstants.JOB_PREFIX_CLEANUP;
import static com.datastax.oss.sga.deployer.k8s.CRDConstants.JOB_PREFIX_DEPLOYER;
import static com.datastax.oss.sga.deployer.k8s.CRDConstants.MAX_APPLICATION_ID_LENGTH;
import com.datastax.oss.sga.api.model.ApplicationLifecycleStatus;
import com.datastax.oss.sga.deployer.k8s.CRDConstants;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.apps.ApplicationSpec;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.runtime.api.deployer.RuntimeDeployerConfiguration;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;

public class AppResourcesFactory {



    @SneakyThrows
    public static Job generateJob(ApplicationCustomResource applicationCustomResource, final Map<String, Object> clusterRuntimeConfiguration, boolean delete) {

        final String applicationId = applicationCustomResource.getMetadata().getName();
        final ApplicationSpec spec = applicationCustomResource.getSpec();
        final String tenant = spec.getTenant();


        final RuntimeDeployerConfiguration config = new RuntimeDeployerConfiguration(
                applicationId,
                tenant,
                spec.getApplication(),
                spec.getCodeArchiveId()
        );

        final Container initContainer = new ContainerBuilder()
                .withName("deployer-init-config")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
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
        final String command = delete ? "delete" : "deploy";

        final Container container = new ContainerBuilder()
                .withName("deployer")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
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


        final Map<String, String> labels = getLabels(delete, applicationId);
        final Job job = new JobBuilder()
                .withNewMetadata()
                .withName(getJobName(applicationId, delete))
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
                .withServiceAccount(tenant)
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
        return job;
    }

    public static Map<String, String> getLabels(boolean delete, String applicationId) {
        final Map<String, String> labels = Map.of(
                CRDConstants.COMMON_LABEL_APP, "sga-deployer",
                CRDConstants.APP_LABEL_APPLICATION, applicationId,
                CRDConstants.APP_LABEL_SCOPE, delete ? CRDConstants.APP_LABEL_SCOPE_DELETE : CRDConstants.APP_LABEL_SCOPE_DEPLOY);
        return labels;
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

        switch (customResource.getStatus().getStatus().getStatus()) {
            case CREATED:
            case DEPLOYED:
            case ERROR_DEPLOYING:
            case ERROR_DELETING:
                return customResource.getStatus().getStatus();
            case DEPLOYING:
            case DELETING:
                return getStatusFromJob(client, customResource);
            default:
                throw new IllegalStateException("Unknown status " + customResource.getStatus().getStatus().getStatus());
        }
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

        switch (podStatus.getState()) {
            case RUNNING:
            case WAITING:
                return customResource.getStatus().getStatus();
            case ERROR:
            default:
                return delete ? ApplicationLifecycleStatus.errorDeleting(podStatus.getMessage()) : ApplicationLifecycleStatus.errorDeploying(podStatus.getMessage());
        }
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
