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
package ai.langstream.deployer.k8s.agents;

import static ai.langstream.deployer.k8s.CRDConstants.MAX_AGENT_ID_LENGTH;

import ai.langstream.api.model.ApplicationStatus;
import ai.langstream.api.runner.code.AgentStatusResponse;
import ai.langstream.deployer.k8s.CRDConstants;
import ai.langstream.deployer.k8s.PodTemplate;
import ai.langstream.deployer.k8s.api.crds.agents.AgentCustomResource;
import ai.langstream.deployer.k8s.api.crds.agents.AgentSpec;
import ai.langstream.deployer.k8s.util.KubeUtil;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.runtime.api.agent.AgentCodeDownloaderConstants;
import ai.langstream.runtime.api.agent.AgentRunnerConstants;
import ai.langstream.runtime.api.agent.DownloadAgentCodeConfiguration;
import ai.langstream.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AgentResourcesFactory {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    protected static final String AGENT_SECRET_DATA_APP = "app-config";

    public static Service generateHeadlessService(AgentCustomResource agentCustomResource) {
        final Map<String, String> agentLabels =
                getAgentLabels(
                        agentCustomResource.getSpec().getAgentId(),
                        agentCustomResource.getSpec().getApplicationId());
        return new ServiceBuilder()
                .withNewMetadata()
                .withName(agentCustomResource.getMetadata().getName())
                .withNamespace(agentCustomResource.getMetadata().getNamespace())
                .withLabels(agentLabels)
                .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(agentCustomResource))
                .endMetadata()
                .withNewSpec()
                .withPorts(
                        List.of(
                                new ServicePortBuilder().withName("http").withPort(8080).build(),
                                new ServicePortBuilder()
                                        .withName("service")
                                        .withPort(8000)
                                        .build()))
                .withSelector(agentLabels)
                .withClusterIP("None")
                .endSpec()
                .build();
    }

    @Builder
    @Getter
    public static class GenerateStatefulsetParams {
        private AgentCustomResource agentCustomResource;

        @Builder.Default
        private AgentResourceUnitConfiguration agentResourceUnitConfiguration =
                new AgentResourceUnitConfiguration();

        private String image;
        private String imagePullPolicy;
        private PodTemplate podTemplate;
    }

    public static StatefulSet generateStatefulSet(GenerateStatefulsetParams params) {
        final AgentCustomResource agentCustomResource =
                Objects.requireNonNull(params.getAgentCustomResource());
        final AgentResourceUnitConfiguration agentResourceUnitConfiguration =
                Objects.requireNonNull(params.getAgentResourceUnitConfiguration());
        final PodTemplate podTemplate = params.getPodTemplate();

        final AgentSpec spec = agentCustomResource.getSpec();
        final String image = getStsImage(params);
        final String imagePullPolicy = getStsImagePullPolicy(params);

        final String appConfigVolume = "app-config";
        final String clusterConfigVolume = "cluster-config";
        final String downloadConfigVolume = "download-config";

        final String downloadCodeVolume = "code-download";
        final String downloadCodePath = "/app-code-download";

        final DownloadAgentCodeConfiguration downloadAgentCodeConfiguration =
                new DownloadAgentCodeConfiguration(
                        downloadCodePath,
                        spec.getTenant(),
                        spec.getApplicationId(),
                        spec.getCodeArchiveId());

        final Container injectConfigForDownloadCodeInitContainer =
                new ContainerBuilder()
                        .withName("code-download-init")
                        .withImage(image)
                        .withImagePullPolicy(imagePullPolicy)
                        .withResources(
                                new ResourceRequirementsBuilder()
                                        .withRequests(
                                                Map.of(
                                                        "cpu",
                                                        Quantity.parse("100m"),
                                                        "memory",
                                                        Quantity.parse("100Mi")))
                                        .build())
                        .withCommand("bash", "-c")
                        .withArgs(
                                "echo '%s' > /download-config/config"
                                        .formatted(
                                                SerializationUtil.writeInlineBashJson(
                                                        downloadAgentCodeConfiguration)))
                        .withVolumeMounts(
                                new VolumeMountBuilder()
                                        .withName(downloadConfigVolume)
                                        .withMountPath("/download-config")
                                        .build())
                        .withTerminationMessagePolicy("FallbackToLogsOnError")
                        .build();

        ;

        final AgentSpec.Resources resources = spec.getResources();
        final List<AgentSpec.Disk> disks = spec.getDisks();

        final Container downloadCodeInitContainer =
                new ContainerBuilder()
                        .withName("code-download")
                        .withImage(image)
                        .withImagePullPolicy(imagePullPolicy)
                        .withArgs("agent-code-download")
                        .withEnv(
                                new EnvVarBuilder()
                                        .withName(AgentCodeDownloaderConstants.CLUSTER_CONFIG_ENV)
                                        .withValue("/cluster-config/config")
                                        .build(),
                                new EnvVarBuilder()
                                        .withName(AgentCodeDownloaderConstants.DOWNLOAD_CONFIG_ENV)
                                        .withValue("/download-config/config")
                                        .build(),
                                new EnvVarBuilder()
                                        .withName(AgentCodeDownloaderConstants.TOKEN_ENV)
                                        .withValue(
                                                "/var/run/secrets/kubernetes.io/serviceaccount/token")
                                        .build())
                        .withVolumeMounts(
                                List.of(
                                        new VolumeMountBuilder()
                                                .withName(clusterConfigVolume)
                                                .withMountPath("/cluster-config")
                                                .build(),
                                        new VolumeMountBuilder()
                                                .withName(downloadConfigVolume)
                                                .withMountPath("/download-config")
                                                .build(),
                                        new VolumeMountBuilder()
                                                .withName(downloadCodeVolume)
                                                .withMountPath(downloadCodePath)
                                                .build()))
                        .withTerminationMessagePolicy("FallbackToLogsOnError")
                        .build();

        final ContainerPort portHttp =
                new ContainerPortBuilder().withName("http").withContainerPort(8080).build();

        final ContainerPort portCustomServiceHttp =
                new ContainerPortBuilder().withName("service").withContainerPort(8000).build();

        final List<VolumeMount> containerMounts = new ArrayList<>();
        containerMounts.add(
                new VolumeMountBuilder()
                        .withName(appConfigVolume)
                        .withMountPath("/app-config")
                        .build());
        containerMounts.add(
                new VolumeMountBuilder()
                        .withName(downloadCodeVolume)
                        .withMountPath(downloadCodePath)
                        .build());
        final List<PersistentVolumeClaim> persistentVolumeClaims = new ArrayList<>();
        handleContainerDisks(
                agentCustomResource,
                spec,
                persistentVolumeClaims,
                disks,
                containerMounts,
                agentResourceUnitConfiguration);
        final Container container =
                new ContainerBuilder()
                        .withName("runtime")
                        .withImage(image)
                        .withImagePullPolicy(imagePullPolicy)
                        .withPorts(portHttp, portCustomServiceHttp)
                        .withLivenessProbe(createLivenessProbe(agentResourceUnitConfiguration))
                        .withReadinessProbe(createReadinessProbe(agentResourceUnitConfiguration))
                        .withResources(convertResources(resources, agentResourceUnitConfiguration))
                        .withEnv(
                                new EnvVarBuilder()
                                        .withName(AgentRunnerConstants.POD_CONFIG_ENV)
                                        .withValue("/app-config/config")
                                        .build(),
                                new EnvVarBuilder()
                                        .withName(AgentRunnerConstants.DOWNLOADED_CODE_PATH_ENV)
                                        .withValue(downloadCodePath)
                                        .build())
                        // keep args for backward compatibility
                        .withArgs("agent-runtime", "/app-config/config", downloadCodePath)
                        .withVolumeMounts(containerMounts)
                        .withTerminationMessagePolicy("FallbackToLogsOnError")
                        .build();

        final Map<String, String> labels =
                getAgentLabels(spec.getAgentId(), spec.getApplicationId());

        final String name = agentCustomResource.getMetadata().getName();
        return new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(agentCustomResource.getMetadata().getNamespace())
                .withLabels(labels)
                .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(agentCustomResource))
                .endMetadata()
                .withNewSpec()
                .withServiceName(name)
                .withReplicas(computeReplicas(agentResourceUnitConfiguration, resources))
                .withNewSelector()
                .withMatchLabels(labels)
                .endSelector()
                .withPodManagementPolicy("Parallel")
                .withNewTemplate()
                .withNewMetadata()
                .withAnnotations(getPodAnnotations(spec, podTemplate))
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withTolerations(podTemplate != null ? podTemplate.tolerations() : null)
                .withNodeSelector(podTemplate != null ? podTemplate.nodeSelector() : null)
                .withTerminationGracePeriodSeconds(60L)
                .withSecurityContext(getPodSecurityContext())
                .withInitContainers(
                        List.of(
                                injectConfigForDownloadCodeInitContainer,
                                downloadCodeInitContainer))
                .withContainers(List.of(container))
                .withVolumes(
                        new VolumeBuilder()
                                .withName(appConfigVolume)
                                .withNewSecret()
                                .withSecretName(spec.getAgentConfigSecretRef())
                                .withItems(
                                        new io.fabric8.kubernetes.api.model.KeyToPathBuilder()
                                                .withKey(AGENT_SECRET_DATA_APP)
                                                .withPath("config")
                                                .build())
                                .endSecret()
                                .build(),
                        new VolumeBuilder()
                                .withName(clusterConfigVolume)
                                .withNewSecret()
                                .withSecretName(CRDConstants.TENANT_CLUSTER_CONFIG_SECRET)
                                .withItems(
                                        new io.fabric8.kubernetes.api.model.KeyToPathBuilder()
                                                .withKey(
                                                        CRDConstants
                                                                .TENANT_CLUSTER_CONFIG_SECRET_KEY)
                                                .withPath("config")
                                                .build())
                                .endSecret()
                                .build(),
                        new VolumeBuilder()
                                .withName(downloadCodeVolume)
                                .withNewEmptyDir()
                                .endEmptyDir()
                                .build(),
                        new VolumeBuilder()
                                .withName(downloadConfigVolume)
                                .withNewEmptyDir()
                                .endEmptyDir()
                                .build())
                .withServiceAccountName(
                        CRDConstants.computeRuntimeServiceAccountForTenant(spec.getTenant()))
                .endSpec()
                .endTemplate()
                .withVolumeClaimTemplates(persistentVolumeClaims)
                .endSpec()
                .build();
    }

    private static void handleContainerDisks(
            AgentCustomResource agentCustomResource,
            AgentSpec spec,
            List<PersistentVolumeClaim> persistentVolumeClaims,
            List<AgentSpec.Disk> disks,
            List<VolumeMount> containerMounts,
            AgentResourceUnitConfiguration agentResourceUnitConfiguration) {
        if (disks != null) {
            disks.forEach(
                    (disk) -> {
                        String agentId = disk.agentId();
                        String pvcName = spec.getApplicationId() + "-" + agentId;
                        String path = "/persistent-state/" + agentId;
                        containerMounts.add(
                                new VolumeMountBuilder()
                                        .withName(pvcName)
                                        .withMountPath(path)
                                        .withReadOnly(false)
                                        .build());

                        final Quantity size;
                        if (disk.size() <= 0) {
                            size =
                                    Quantity.parse(
                                            agentResourceUnitConfiguration
                                                    .getDefaultStorageDiskSize());
                        } else {
                            size = Quantity.parse(disk.size() + "");
                        }
                        final Map<String, String> mapping =
                                agentResourceUnitConfiguration.getStorageClassesMapping();
                        String storageClass = null;
                        if (disk.type() != null && mapping != null) {
                            storageClass = mapping.get(disk.type());
                        }
                        if (storageClass == null) {
                            log.info(
                                    "Disk type '{}' not found in mapping ({}), using default storage class '{}'",
                                    disk.type(),
                                    mapping,
                                    agentResourceUnitConfiguration.getDefaultStorageClass());
                            storageClass = agentResourceUnitConfiguration.getDefaultStorageClass();
                        }

                        persistentVolumeClaims.add(
                                new PersistentVolumeClaimBuilder()
                                        .withNewMetadata()
                                        .withName(pvcName)
                                        .withNamespace(
                                                agentCustomResource.getMetadata().getNamespace())
                                        .endMetadata()
                                        .withNewSpec()
                                        .withAccessModes("ReadWriteOnce")
                                        .withStorageClassName(storageClass)
                                        .withNewResources()
                                        .withRequests(Map.of("storage", size))
                                        .endResources()
                                        .endSpec()
                                        .build());
                    });
        }
    }

    private static Probe createLivenessProbe(
            AgentResourceUnitConfiguration agentResourceUnitConfiguration) {
        if (!agentResourceUnitConfiguration.isEnableLivenessProbe()) {
            return null;
        }
        return new ProbeBuilder()
                .withNewHttpGet()
                .withNewPort()
                .withValue("http")
                .endPort()
                .withPath("/metrics")
                .endHttpGet()
                .withTimeoutSeconds(agentResourceUnitConfiguration.getLivenessProbeTimeoutSeconds())
                .withInitialDelaySeconds(
                        agentResourceUnitConfiguration.getLivenessProbeInitialDelaySeconds())
                .withPeriodSeconds(agentResourceUnitConfiguration.getLivenessProbePeriodSeconds())
                .build();
    }

    private static Probe createReadinessProbe(
            AgentResourceUnitConfiguration agentResourceUnitConfiguration) {
        if (!agentResourceUnitConfiguration.isEnableReadinessProbe()) {
            return null;
        }
        return new ProbeBuilder()
                .withNewHttpGet()
                .withNewPort()
                .withValue("http")
                .endPort()
                .withPath("/metrics")
                .endHttpGet()
                .withTimeoutSeconds(
                        agentResourceUnitConfiguration.getReadinessProbeTimeoutSeconds())
                .withInitialDelaySeconds(
                        agentResourceUnitConfiguration.getReadinessProbeInitialDelaySeconds())
                .withPeriodSeconds(agentResourceUnitConfiguration.getReadinessProbePeriodSeconds())
                .build();
    }

    private static Map<String, String> getPodAnnotations(AgentSpec spec, PodTemplate podTemplate) {
        final Map<String, String> annotations = new HashMap<>();
        annotations.put("ai.langstream/config-checksum", spec.getAgentConfigSecretRefChecksum());
        annotations.put("ai.langstream/application-seed", spec.getApplicationSeed() + "");
        if (podTemplate != null && podTemplate.annotations() != null) {
            annotations.putAll(podTemplate.annotations());
        }
        return annotations;
    }

    private static PodSecurityContext getPodSecurityContext() {
        return new PodSecurityContextBuilder().withFsGroup(10_000L).build();
    }

    private static String getStsImagePullPolicy(GenerateStatefulsetParams params) {
        final String imagePullPolicy = params.getImagePullPolicy();
        final String containerImagePullPolicy =
                imagePullPolicy != null && !imagePullPolicy.isBlank()
                        ? imagePullPolicy
                        : params.getAgentCustomResource().getSpec().getImagePullPolicy();
        if (containerImagePullPolicy == null) {
            throw new IllegalStateException(
                    "Runtime image pull policy is not specified, neither in the resource and in the deployer configuration.");
        }
        return containerImagePullPolicy;
    }

    private static String getStsImage(GenerateStatefulsetParams params) {
        final String image = params.getImage();

        final String containerImage =
                image != null && !image.isBlank()
                        ? image
                        : params.getAgentCustomResource().getSpec().getImage();
        if (containerImage == null) {
            throw new IllegalStateException(
                    "Runtime image is not specified, neither in the resource and in the deployer configuration.");
        }
        return containerImage;
    }

    public static Secret generateAgentSecret(
            String agentFullName, RuntimePodConfiguration runtimePodConfiguration) {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(agentFullName)
                .endMetadata()
                .withData(
                        Map.of(
                                AGENT_SECRET_DATA_APP,
                                Base64.getEncoder()
                                        .encodeToString(
                                                SerializationUtil.writeAsJsonBytes(
                                                        runtimePodConfiguration))))
                .build();
    }

    public static RuntimePodConfiguration readRuntimePodConfigurationFromSecret(Secret secret) {
        final String appConfig = secret.getData().get(AGENT_SECRET_DATA_APP);
        return SerializationUtil.readJson(
                new String(Base64.getDecoder().decode(appConfig), StandardCharsets.UTF_8),
                RuntimePodConfiguration.class);
    }

    private static int computeReplicas(
            AgentResourceUnitConfiguration agentResourceUnitConfiguration,
            AgentSpec.Resources resources) {
        Integer requestedParallelism = resources == null ? null : resources.parallelism();
        if (requestedParallelism == null) {
            requestedParallelism = agentResourceUnitConfiguration.getDefaultInstanceUnits();
        }
        if (requestedParallelism > agentResourceUnitConfiguration.getMaxInstanceUnits()) {
            throw new IllegalArgumentException(
                    "Requested %d instances, max is %d"
                            .formatted(
                                    requestedParallelism,
                                    agentResourceUnitConfiguration.getMaxInstanceUnits()));
        }
        return requestedParallelism;
    }

    private static ResourceRequirements convertResources(
            AgentSpec.Resources resources,
            AgentResourceUnitConfiguration agentResourceUnitConfiguration) {
        Integer memCpuUnits = resources == null ? null : resources.size();
        if (memCpuUnits == null) {
            memCpuUnits = agentResourceUnitConfiguration.getDefaultCpuMemUnits();
        }

        Integer instances = resources == null ? null : resources.parallelism();
        if (instances == null) {
            instances = agentResourceUnitConfiguration.getDefaultInstanceUnits();
        }

        if (memCpuUnits > agentResourceUnitConfiguration.getMaxCpuMemUnits()) {
            throw new IllegalArgumentException(
                    "Requested %d cpu/mem units, max is %d"
                            .formatted(
                                    memCpuUnits,
                                    agentResourceUnitConfiguration.getMaxCpuMemUnits()));
        }
        if (instances > agentResourceUnitConfiguration.getMaxInstanceUnits()) {
            throw new IllegalArgumentException(
                    "Requested %d instance units, max is %d"
                            .formatted(
                                    instances,
                                    agentResourceUnitConfiguration.getMaxInstanceUnits()));
        }

        final Map<String, Quantity> quantities = new HashMap<>();
        quantities.put(
                "cpu",
                Quantity.parse(
                        "%f"
                                .formatted(
                                        memCpuUnits
                                                * agentResourceUnitConfiguration.getCpuPerUnit())));
        quantities.put(
                "memory",
                Quantity.parse(
                        "%dM"
                                .formatted(
                                        memCpuUnits
                                                * agentResourceUnitConfiguration.getMemPerUnit())));

        return new ResourceRequirementsBuilder()
                .withRequests(quantities)
                .withLimits(quantities)
                .build();
    }

    public static Map<String, String> getAgentLabels(String agentId, String applicationId) {
        return Map.of(
                CRDConstants.COMMON_LABEL_APP, "langstream-runtime",
                CRDConstants.AGENT_LABEL_AGENT_ID, agentId,
                CRDConstants.AGENT_LABEL_APPLICATION, applicationId);
    }

    public static AgentCustomResource generateAgentCustomResource(
            final String applicationId, final String agentId, final AgentSpec agentSpec) {
        final AgentCustomResource agentCR = new AgentCustomResource();
        final String agentName = getAgentCustomResourceName(applicationId, agentId);
        agentCR.setMetadata(
                new ObjectMetaBuilder()
                        .withName(agentName)
                        .withLabels(getAgentLabels(agentId, applicationId))
                        .build());
        agentSpec.setApplicationId(applicationId);
        agentSpec.setAgentId(agentId);
        agentCR.setSpec(agentSpec);
        return agentCR;
    }

    public static String getAgentCustomResourceName(String applicationId, String agentId) {
        return "%s-%s".formatted(applicationId, agentId);
    }

    @Data
    @Builder
    public static class AgentRunnerSpec {
        private String agentId;
        private String agentType;
        private String componentType;
        private String inputTopic;
        private String outputTopic;
        private Map<String, Object> configuration;
    }

    public static Map<String, ApplicationStatus.AgentStatus> aggregateAgentsStatus(
            final KubernetesClient client,
            final String namespace,
            final String applicationId,
            final List<String> declaredAgents,
            final Map<String, AgentRunnerSpec> agentRunners,
            boolean queryPods) {
        final Map<String, AgentCustomResource> agentCustomResources =
                client
                        .resources(AgentCustomResource.class)
                        .inNamespace(namespace)
                        .withLabel(CRDConstants.AGENT_LABEL_APPLICATION, applicationId)
                        .list()
                        .getItems()
                        .stream()
                        .collect(
                                Collectors.toMap(
                                        a ->
                                                a.getMetadata()
                                                        .getLabels()
                                                        .get(CRDConstants.AGENT_LABEL_AGENT_ID),
                                        Function.identity()));

        Map<String, ApplicationStatus.AgentStatus> agents = new HashMap<>();

        for (String declaredAgent : declaredAgents) {

            final AgentCustomResource cr = agentCustomResources.get(declaredAgent);
            if (cr == null) {
                continue;
            }
            ApplicationStatus.AgentStatus agentStatus = new ApplicationStatus.AgentStatus();
            agentStatus.setStatus(cr.getStatus().getStatus());
            AgentRunnerSpec agentRunnerSpec =
                    agentRunners.values().stream()
                            .filter(a -> a.getAgentId().equals(declaredAgent))
                            .findFirst()
                            .orElse(null);
            Map<String, ApplicationStatus.AgentWorkerStatus> podStatuses =
                    getPodStatuses(
                            client, applicationId, namespace, declaredAgent, agentRunnerSpec);
            agentStatus.setWorkers(podStatuses);

            agents.put(declaredAgent, agentStatus);
        }

        if (queryPods) {
            queryPodsStatus(agents, applicationId);
        }

        return agents;
    }

    private static void queryPodsStatus(
            Map<String, ApplicationStatus.AgentStatus> agents, String applicationId) {
        if (agents.isEmpty()) {
            return;
        }
        HttpClient httpClient = HttpClient.newHttpClient();
        ExecutorService threadPool = Executors.newFixedThreadPool(Math.min(agents.size(), 4));
        try {
            Map<String, CompletableFuture<ApplicationStatus.AgentWorkerStatus>> futures =
                    new HashMap<>();

            for (Map.Entry<String, ApplicationStatus.AgentStatus> agentEntries :
                    agents.entrySet()) {
                String declaredAgent = agentEntries.getKey();
                ApplicationStatus.AgentStatus agentStatus = agentEntries.getValue();

                for (Map.Entry<String, ApplicationStatus.AgentWorkerStatus> entry :
                        agentStatus.getWorkers().entrySet()) {
                    ApplicationStatus.AgentWorkerStatus agentWorkerStatus = entry.getValue();
                    String url = agentWorkerStatus.getUrl();
                    CompletableFuture<ApplicationStatus.AgentWorkerStatus> result =
                            new CompletableFuture<>();
                    String podId = declaredAgent + "##" + entry.getKey();
                    futures.put(podId, result);
                    threadPool.submit(
                            () -> {
                                try {
                                    if (url != null) {
                                        log.info(
                                                "Querying pod {} for agent {} in application {}",
                                                url,
                                                declaredAgent,
                                                applicationId);
                                        List<AgentStatusResponse> agentsStatus =
                                                queryAgentStatus(url, httpClient);
                                        log.info(
                                                "Info for pod {}: {}",
                                                entry.getKey(),
                                                agentsStatus);
                                        ApplicationStatus.AgentWorkerStatus
                                                agentWorkerStatusWithInfo =
                                                        agentWorkerStatus.applyAgentStatus(
                                                                agentsStatus);
                                        result.complete(agentWorkerStatusWithInfo);
                                    } else {
                                        log.warn(
                                                "No URL for pod {} for agent {} in application {}",
                                                entry.getKey(),
                                                declaredAgent,
                                                applicationId);
                                        result.complete(agentWorkerStatus);
                                    }
                                } catch (Throwable error) {
                                    result.completeExceptionally(error);
                                }
                            });
                }
            }

            // wait for all futures to complete
            try {
                CompletableFuture.allOf(futures.values().toArray(CompletableFuture[]::new)).join();
            } catch (CancellationException | CompletionException ignore) {
            }

            for (Map.Entry<String, ApplicationStatus.AgentStatus> agentEntries :
                    agents.entrySet()) {
                String declaredAgent = agentEntries.getKey();
                ApplicationStatus.AgentStatus agentStatus = agentEntries.getValue();
                for (Map.Entry<String, ApplicationStatus.AgentWorkerStatus> entry :
                        agentStatus.getWorkers().entrySet()) {
                    String podId = declaredAgent + "##" + entry.getKey();
                    CompletableFuture<ApplicationStatus.AgentWorkerStatus> future =
                            futures.get(podId);
                    try {
                        ApplicationStatus.AgentWorkerStatus newStatus = future.get();
                        entry.setValue(newStatus);
                    } catch (InterruptedException | ExecutionException impossibleToGetStatus) {
                        log.warn(
                                "Failed to query pod {} for agent {} in application {}: {}",
                                entry.getKey(),
                                declaredAgent,
                                applicationId,
                                impossibleToGetStatus + "");
                    }
                }
            }
        } finally {
            threadPool.shutdown();
        }
    }

    private static List<AgentStatusResponse> queryAgentStatus(String url, HttpClient httpClient) {
        try {
            String body =
                    httpClient
                            .send(
                                    HttpRequest.newBuilder()
                                            .uri(URI.create(url + "/info"))
                                            .GET()
                                            .timeout(Duration.ofSeconds(10))
                                            .build(),
                                    HttpResponse.BodyHandlers.ofString())
                            .body();
            return MAPPER.readValue(body, new TypeReference<>() {});
        } catch (IOException | InterruptedException e) {
            log.warn("Failed to query agent info from {}", url, e);
            return List.of();
        }
    }

    private static Map<String, ApplicationStatus.AgentWorkerStatus> getPodStatuses(
            KubernetesClient client,
            String applicationId,
            final String namespace,
            final String agent,
            final AgentRunnerSpec agentRunnerSpec) {
        final List<Pod> pods =
                client.resources(Pod.class)
                        .inNamespace(namespace)
                        .withLabels(getAgentLabels(agent, applicationId))
                        .list()
                        .getItems();

        return KubeUtil.getPodsStatuses(pods).entrySet().stream()
                .map(
                        e -> {
                            KubeUtil.PodStatus podStatus = e.getValue();
                            ApplicationStatus.AgentWorkerStatus status =
                                    switch (podStatus.getState()) {
                                        case RUNNING -> ApplicationStatus.AgentWorkerStatus.RUNNING(
                                                podStatus.getUrl());
                                        case WAITING -> ApplicationStatus.AgentWorkerStatus
                                                .INITIALIZING();
                                        case ERROR -> ApplicationStatus.AgentWorkerStatus.ERROR(
                                                podStatus.getUrl(), podStatus.getMessage());
                                        default -> throw new RuntimeException(
                                                "Unexpected pod state: "
                                                        + podStatus.getState()
                                                        + " "
                                                        + e.getKey());
                                    };
                            if (agentRunnerSpec != null) {
                                status =
                                        status.withAgentSpec(
                                                agentRunnerSpec.getAgentId(),
                                                agentRunnerSpec.getAgentType(),
                                                agentRunnerSpec.getComponentType(),
                                                agentRunnerSpec.getConfiguration(),
                                                agentRunnerSpec.getInputTopic(),
                                                agentRunnerSpec.getOutputTopic());
                            }
                            return new AbstractMap.SimpleEntry<>(e.getKey(), status);
                        })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static List<String> getAgentPods(
            KubernetesClient client, String namespace, String applicationId) {
        return client
                .pods()
                .inNamespace(namespace)
                .withLabels(
                        new TreeMap<>(
                                Map.of(
                                        CRDConstants.COMMON_LABEL_APP,
                                        "langstream-runtime",
                                        CRDConstants.AGENT_LABEL_APPLICATION,
                                        applicationId)))
                .list()
                .getItems()
                .stream()
                .map(pod -> pod.getMetadata().getName())
                .sorted()
                .collect(Collectors.toList());
    }

    public static void validateAgentId(String agentId, String applicationId)
            throws IllegalArgumentException {
        final String fullAgentId = getAgentCustomResourceName(applicationId, agentId);
        if (!CRDConstants.RESOURCE_NAME_PATTERN.matcher(fullAgentId).matches()) {
            throw new IllegalArgumentException(
                    ("Agent id '%s' (computed as '%s') contains illegal characters. "
                                    + "Allowed characters are alphanumeric and dash. To fully control the agent id, you can set the 'id' field.")
                            .formatted(agentId, fullAgentId));
        }
        if (agentId.length() > MAX_AGENT_ID_LENGTH) {
            throw new IllegalArgumentException(
                    "Agent id '%s' is too long, max length is %d. To fully control the agent id, you can set the 'id' field."
                            .formatted(agentId, MAX_AGENT_ID_LENGTH));
        }
    }
}
