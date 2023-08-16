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
package com.datastax.oss.sga.deployer.k8s.agents;

import static com.datastax.oss.sga.deployer.k8s.CRDConstants.MAX_AGENT_ID_LENGTH;
import com.datastax.oss.sga.api.model.ApplicationStatus;
import com.datastax.oss.sga.api.runner.code.AgentStatusResponse;
import com.datastax.oss.sga.deployer.k8s.CRDConstants;
import com.datastax.oss.sga.deployer.k8s.PodTemplate;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentSpec;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.runtime.api.agent.CodeStorageConfig;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class AgentResourcesFactory {

    private static ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    protected static final String AGENT_SECRET_DATA_APP = "app-config";

    public static Service generateHeadlessService(AgentCustomResource agentCustomResource) {
        final Map<String, String> agentLabels = getAgentLabels(agentCustomResource.getSpec().getAgentId(),
                agentCustomResource.getSpec().getApplicationId());
        return new ServiceBuilder()
                .withNewMetadata()
                .withName(agentCustomResource.getMetadata().getName())
                .withNamespace(agentCustomResource.getMetadata().getNamespace())
                .withLabels(agentLabels)
                .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(agentCustomResource))
                .endMetadata()
                .withNewSpec()
                .withPorts(List.of(
                        new ServicePortBuilder()
                                .withName("http")
                                .withPort(8080)
                                .build()
                        )
                )
                .withSelector(agentLabels)
                .withClusterIP("None")
                .endSpec()
                .build();
    }

    public static StatefulSet generateStatefulSet(AgentCustomResource agentCustomResource,
                                                  Map<String, Object> codeStoreConfiguration,
                                                  AgentResourceUnitConfiguration agentResourceUnitConfiguration) {
        return generateStatefulSet(agentCustomResource, codeStoreConfiguration, agentResourceUnitConfiguration, null);
    }


    public static StatefulSet generateStatefulSet(AgentCustomResource agentCustomResource,
                                                  Map<String, Object> codeStoreConfiguration,
                                                  AgentResourceUnitConfiguration agentResourceUnitConfiguration,
                                                  PodTemplate podTemplate) {

        final AgentSpec spec = agentCustomResource.getSpec();

        final CodeStorageConfig codeStorageConfig =
                new CodeStorageConfig(spec.getTenant(), codeStoreConfiguration.getOrDefault("type", "none").toString(),
                        spec.getCodeArchiveId(),
                        codeStoreConfiguration);

        final String appConfigVolume = "app-config";
        final String codeConfigVolume = "code-config";
        final String downloadedCodeVolume = "code-download";
        final Container injectConfigForDownloadCodeInitContainer = new ContainerBuilder()
                .withName("code-download-init")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", Quantity.parse("100m"),
                        "memory", Quantity.parse("100Mi"))).build())
                .withCommand("bash", "-c")
                .withArgs("echo '%s' > /code-config/config".formatted(SerializationUtil.writeAsJson(codeStorageConfig).replace("'", "'\"'\"'")))
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName(codeConfigVolume)
                        .withMountPath("/code-config")
                        .build()
                )
                .withTerminationMessagePolicy("FallbackToLogsOnError")
                .build();

        final Container downloadCodeInitContainer = new ContainerBuilder()
                .withName("code-download")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withArgs("agent-code-download", "/code-config/config", "/app-code-download")
                .withVolumeMounts(
                        new VolumeMountBuilder()
                        .withName(codeConfigVolume)
                        .withMountPath("/code-config")
                        .build(),
                        new VolumeMountBuilder()
                                .withName(downloadedCodeVolume)
                                .withMountPath("/app-code-download")
                                .build()
                )
                .withTerminationMessagePolicy("FallbackToLogsOnError")
                .build();

        final Container container = new ContainerBuilder()
                .withName("runtime")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withPorts(List.of(new ContainerPort(8080, null, null, "http", "TCP")))
//                .withLivenessProbe(createLivenessProbe())
//                .withReadinessProbe(createReadinessProbe())
                .withResources(convertResources(spec.getResources(), agentResourceUnitConfiguration))
                .withArgs("agent-runtime", "/app-config/config", "/app-code-download")
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName(appConfigVolume)
                        .withMountPath("/app-config")
                        .build(),
                        new VolumeMountBuilder()
                                .withName(downloadedCodeVolume)
                                .withMountPath("/app-code-download")
                                .build()
                )
                .withTerminationMessagePolicy("FallbackToLogsOnError")
                .build();

        final Map<String, String> labels = getAgentLabels(spec.getAgentId(), spec.getApplicationId());

        final String name = agentCustomResource.getMetadata().getName();
        final StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(agentCustomResource.getMetadata().getNamespace())
                .withLabels(labels)
                .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(agentCustomResource))
                .endMetadata()
                .withNewSpec()
                .withServiceName(name)
                .withReplicas(computeReplicas(agentResourceUnitConfiguration, spec.getResources()))
                .withNewSelector()
                .withMatchLabels(labels)
                .endSelector()
                .withPodManagementPolicy("Parallel")
                .withNewTemplate()
                .withNewMetadata()
                .withAnnotations(Map.of("sga.com.datastax.oss/config-checksum", spec.getAgentConfigSecretRefChecksum()))
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withTolerations(podTemplate != null ? podTemplate.tolerations() : null)
                .withNodeSelector(podTemplate != null ? podTemplate.nodeSelector() : null)
                .withTerminationGracePeriodSeconds(60L)
                .withInitContainers(List.of(injectConfigForDownloadCodeInitContainer, downloadCodeInitContainer))
                .withContainers(List.of(container))
                .withVolumes(new VolumeBuilder()
                        .withName(appConfigVolume)
                        .withNewSecret()
                        .withSecretName(spec.getAgentConfigSecretRef())
                        .withItems(new io.fabric8.kubernetes.api.model.KeyToPathBuilder()
                                .withKey(AGENT_SECRET_DATA_APP)
                                .withPath("config")
                                .build()
                        )
                        .endSecret()
                        .build(),
                        new VolumeBuilder()
                                .withName(codeConfigVolume)
                                .withNewEmptyDir().endEmptyDir()
                                .build(),
                        new VolumeBuilder()
                                .withName(downloadedCodeVolume)
                                .withNewEmptyDir().endEmptyDir()
                                .build()
                )
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
        return statefulSet;
    }

    public static Secret generateAgentSecret(String agentFullName, RuntimePodConfiguration runtimePodConfiguration) {
        return new SecretBuilder()
                .withNewMetadata()
                .withName(agentFullName)
                .endMetadata()
                .withData(Map.of(AGENT_SECRET_DATA_APP,
                                Base64.getEncoder().encodeToString(SerializationUtil.writeAsJsonBytes(runtimePodConfiguration))
                        )
                )
                .build();
    }

    public static RuntimePodConfiguration readRuntimePodConfigurationFromSecret(Secret secret) {
        final String appConfig = secret.getData()
                .get(AGENT_SECRET_DATA_APP);
        return SerializationUtil.readJson(new String(Base64.getDecoder().decode(appConfig), StandardCharsets.UTF_8),
                RuntimePodConfiguration.class);
    }

    private static int computeReplicas(AgentResourceUnitConfiguration agentResourceUnitConfiguration,
                                       AgentSpec.Resources resources) {
        Integer requestedParallelism = resources == null ? null : resources.parallelism();
        if (requestedParallelism == null) {
            requestedParallelism = agentResourceUnitConfiguration.getDefaultInstanceUnits();
        }
        if (requestedParallelism > agentResourceUnitConfiguration.getMaxInstanceUnits()) {
            throw new IllegalArgumentException("Requested %d instances, max is %d".formatted(requestedParallelism,
                    agentResourceUnitConfiguration.getMaxInstanceUnits()));
        }
        return requestedParallelism;
    }

    private static ResourceRequirements convertResources(AgentSpec.Resources resources,
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
            throw new IllegalArgumentException("Requested %d cpu/mem units, max is %d".formatted(memCpuUnits,
                    agentResourceUnitConfiguration.getMaxCpuMemUnits()));
        }
        if (instances > agentResourceUnitConfiguration.getMaxInstanceUnits()) {
            throw new IllegalArgumentException("Requested %d instance units, max is %d".formatted(instances,
                    agentResourceUnitConfiguration.getMaxInstanceUnits()));
        }

        final Map<String, Quantity> requests = new HashMap<>();
        requests.put("cpu",
                Quantity.parse("%f".formatted(memCpuUnits * agentResourceUnitConfiguration.getCpuPerUnit())));
        requests.put("memory",
                Quantity.parse("%dM".formatted(memCpuUnits * agentResourceUnitConfiguration.getMemPerUnit())));

        return new ResourceRequirementsBuilder()
                .withRequests(requests)
                .build();
    }

    public static Map<String, String> getAgentLabels(String agentId, String applicationId) {
        final Map<String, String> labels = Map.of(
                CRDConstants.COMMON_LABEL_APP, "sga-runtime",
                CRDConstants.AGENT_LABEL_AGENT_ID, agentId,
                CRDConstants.AGENT_LABEL_APPLICATION, applicationId);
        return labels;
    }

    public static AgentCustomResource generateAgentCustomResource(final String applicationId,
                                                                  final String agentId,
                                                                  final AgentSpec agentSpec) {
        final AgentCustomResource agentCR = new AgentCustomResource();
        final String agentName = getAgentCustomResourceName(applicationId, agentId);
        agentCR.setMetadata(new ObjectMetaBuilder()
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
        private Map<String, Object> configuration;
    }


    public static Map<String, ApplicationStatus.AgentStatus> aggregateAgentsStatus(
            final KubernetesClient client,
            final String namespace,
            final String applicationId,
            final List<String> declaredAgents,
            final Map<String, AgentRunnerSpec> agentRunners,
            boolean queryPods) {
        final Map<String, AgentCustomResource> agentCustomResources = client.resources(AgentCustomResource.class)
                .inNamespace(namespace)
                .withLabel(CRDConstants.AGENT_LABEL_APPLICATION, applicationId)
                .list()
                .getItems()
                .stream().collect(Collectors.toMap(
                        a -> a.getMetadata().getLabels().get(CRDConstants.AGENT_LABEL_AGENT_ID),
                        Function.identity()
                ));

        Map<String, ApplicationStatus.AgentStatus> agents = new HashMap<>();

        for (String declaredAgent : declaredAgents) {

            final AgentCustomResource cr = agentCustomResources.get(declaredAgent);
            if (cr == null) {
                continue;
            }
            ApplicationStatus.AgentStatus agentStatus = new ApplicationStatus.AgentStatus();
            agentStatus.setStatus(cr.getStatus().getStatus());
            AgentRunnerSpec agentRunnerSpec = agentRunners
                    .values()
                    .stream()
                    .filter(a-> a.getAgentId().equals(declaredAgent))
                    .findFirst()
                    .orElse(null);
            Map<String, ApplicationStatus.AgentWorkerStatus> podStatuses =
                    getPodStatuses(client, applicationId, namespace, declaredAgent, agentRunnerSpec);
            agentStatus.setWorkers(podStatuses);

            agents.put(declaredAgent, agentStatus);
        }

        if (queryPods) {
            queryPodsStatus(agents, applicationId);
        }

        return agents;
    }

    private static void queryPodsStatus(Map<String, ApplicationStatus.AgentStatus> agents,
                                        String applicationId) {
        if (agents.isEmpty()) {
            return;
        }
        HttpClient httpClient = HttpClient.newHttpClient();
        ExecutorService threadPool = Executors.newFixedThreadPool(Math.min(agents.size(), 4));
        try {
            Map<String, CompletableFuture<ApplicationStatus.AgentWorkerStatus>> futures = new HashMap<>();

            for (Map.Entry<String, ApplicationStatus.AgentStatus> agentEntries : agents.entrySet()) {
                String declaredAgent = agentEntries.getKey();
                ApplicationStatus.AgentStatus agentStatus = agentEntries.getValue();

                for (Map.Entry<String, ApplicationStatus.AgentWorkerStatus> entry : agentStatus.getWorkers().entrySet()) {
                    ApplicationStatus.AgentWorkerStatus agentWorkerStatus = entry.getValue();
                    String url = agentWorkerStatus.getUrl();
                    CompletableFuture<ApplicationStatus.AgentWorkerStatus> result = new CompletableFuture<>();
                    String podId = declaredAgent + "##" + entry.getKey();
                    futures.put(podId, result);
                    threadPool.submit(() -> {
                        try {
                            if (url != null) {
                                log.info("Querying pod {} for agent {} in application {}",
                                        url, declaredAgent, applicationId);
                                List<AgentStatusResponse> agentsStatus = queryAgentStatus(url, httpClient);
                                log.info("Info for pod {}: {}", entry.getKey(), agentsStatus);
                                ApplicationStatus.AgentWorkerStatus agentWorkerStatusWithInfo =
                                        agentWorkerStatus.applyAgentStatus(agentsStatus);
                                result.complete(agentWorkerStatusWithInfo);
                            } else {
                                log.warn("No URL for pod {} for agent {} in application {}",
                                        entry.getKey(), declaredAgent, applicationId);
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
                CompletableFuture
                        .allOf(futures.values().toArray(CompletableFuture[]::new))
                        .join();
            } catch (CancellationException | CompletionException ignore) {
            }

            for (Map.Entry<String, ApplicationStatus.AgentStatus> agentEntries : agents.entrySet()) {
                String declaredAgent = agentEntries.getKey();
                ApplicationStatus.AgentStatus agentStatus = agentEntries.getValue();
                for (Map.Entry<String, ApplicationStatus.AgentWorkerStatus> entry : agentStatus.getWorkers().entrySet()) {
                    String podId = declaredAgent + "##" + entry.getKey();
                    CompletableFuture<ApplicationStatus.AgentWorkerStatus> future = futures.get(podId);
                    try {
                        ApplicationStatus.AgentWorkerStatus newStatus = future.get();
                        entry.setValue(newStatus);
                    } catch (InterruptedException | ExecutionException impossibleToGetStatus) {
                        log.warn("Failed to query pod {} for agent {} in application {}: {}",
                                entry.getKey(), declaredAgent, applicationId, impossibleToGetStatus + "");
                    }
                }
            }
        } finally {
            threadPool.shutdown();
        }
    }

    private static List<AgentStatusResponse> queryAgentStatus(String url, HttpClient httpClient) {
        try {
            String body = httpClient.send(HttpRequest.newBuilder()
                                    .uri(URI.create(url + "/info"))
                                    .GET()
                                    .timeout(Duration.ofSeconds(10))
                                    .build(),
                            HttpResponse.BodyHandlers.ofString())
                    .body();
            return MAPPER.readValue(body, new TypeReference<List<AgentStatusResponse>>() {});
        } catch (IOException | InterruptedException e) {
            log.warn("Failed to query agent info from {}", url, e);
            return List.of();
        }
    }

    private static Map<String, ApplicationStatus.AgentWorkerStatus> getPodStatuses(
            KubernetesClient client, String applicationId, final String namespace,
            final String agent, final AgentRunnerSpec agentRunnerSpec) {
        final List<Pod> pods = client.resources(Pod.class)
                .inNamespace(namespace)
                .withLabels(getAgentLabels(agent, applicationId))
                .list()
                .getItems();

        return KubeUtil.getPodsStatuses(pods)
                .entrySet()
                .stream()
                .map(e -> {
                    KubeUtil.PodStatus podStatus = e.getValue();
                    log.info("Pod {} status: {} url: {} agentRunnerSpec {}", e.getKey(), podStatus.getState(), podStatus.getUrl(), agentRunnerSpec);
                    ApplicationStatus.AgentWorkerStatus status = switch (podStatus.getState()) {
                        case RUNNING -> ApplicationStatus.AgentWorkerStatus.RUNNING(podStatus.getUrl());
                        case WAITING -> ApplicationStatus.AgentWorkerStatus.INITIALIZING();
                        case ERROR -> ApplicationStatus.AgentWorkerStatus.ERROR(podStatus.getUrl(), podStatus.getMessage());
                        default -> throw new RuntimeException("Unknown pod state: " + podStatus.getState());
                    };
                    if (agentRunnerSpec != null) {
                        status = status.withAgentSpec(agentRunnerSpec.getAgentId(),
                                agentRunnerSpec.getAgentType(),
                                agentRunnerSpec.getConfiguration());
                    }
                    return new AbstractMap.SimpleEntry<>(e.getKey(), status);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }




    public static List<String> getAgentPods(KubernetesClient client, String namespace, String applicationId) {
        return client.pods().inNamespace(namespace)
                .withLabels(new TreeMap<>(Map.of(
                                CRDConstants.COMMON_LABEL_APP, "sga-runtime",
                                CRDConstants.AGENT_LABEL_APPLICATION, applicationId)
                        )
                )
                .list()
                .getItems()
                .stream()
                .map(pod -> pod.getMetadata().getName())
                .sorted()
                .collect(Collectors.toList());
    }

    public static void validateAgentId(String agentId, String applicationId) throws IllegalArgumentException{
        final String fullAgentId = getAgentCustomResourceName(applicationId, agentId);
        if (!CRDConstants.RESOURCE_NAME_PATTERN.matcher(fullAgentId).matches()) {
            throw new IllegalArgumentException(("Agent id '%s' (computed as '%s') contains illegal characters. "
                    + "Allowed characters are alphanumeric and dash. To fully control the agent id, you can set the 'id' field.").formatted(agentId, fullAgentId));
        }
        if (agentId.length() > MAX_AGENT_ID_LENGTH) {
            throw new IllegalArgumentException("Agent id '%s' is too long, max length is %d. To fully control the agent id, you can set the 'id' field.".formatted(agentId,
                    MAX_AGENT_ID_LENGTH));
        }
    }

}
