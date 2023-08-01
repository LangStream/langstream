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

import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.api.model.ApplicationStatus;
import com.datastax.oss.sga.deployer.k8s.CRDConstants;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentSpec;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.runtime.api.agent.CodeStorageConfig;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AgentResourcesFactory {

    protected static final String AGENT_SECRET_DATA_APP = "app-config";

    public static StatefulSet generateStatefulSet(AgentCustomResource agentCustomResource,
                                                  Map<String, Object> codeStoreConfiguration,
                                                  AgentResourceUnitConfiguration agentResourceUnitConfiguration) {

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

        final StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(sanitizeName(agentCustomResource.getMetadata().getName()))
                .withNamespace(agentCustomResource.getMetadata().getNamespace())
                .withLabels(labels)
                .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(agentCustomResource))
                .endMetadata()
                .withNewSpec()
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
        final String agentName = "%s-%s".formatted(applicationId, agentId);
        return sanitizeName(agentName);
    }


    public static Map<String, ApplicationStatus.AgentStatus> aggregateAgentsStatus(
            final KubernetesClient client,
            final String namespace,
            final String applicationId,
            final List<String> declaredAgents) {
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
                Map<String, ApplicationStatus.AgentWorkerStatus> podStatuses =
                        getPodStatuses(client, applicationId, namespace, declaredAgent);
                agentStatus.setWorkers(podStatuses);

            agents.put(declaredAgent, agentStatus);
        }
        return agents;
    }

    private static Map<String, ApplicationStatus.AgentWorkerStatus> getPodStatuses(
            KubernetesClient client, String applicationId, final String namespace,
            final String agent) {
        final List<Pod> pods = client.resources(Pod.class)
                .inNamespace(namespace)
                .withLabels(getAgentLabels(agent, applicationId))
                .list()
                .getItems();

        return KubeUtil.getPodsStatuses(pods)
                .entrySet()
                .stream()
                .map(e -> {
                    ApplicationStatus.AgentWorkerStatus status = switch (e.getValue().getState()) {
                        case RUNNING -> ApplicationStatus.AgentWorkerStatus.RUNNING;
                        case WAITING -> ApplicationStatus.AgentWorkerStatus.INITIALIZING;
                        case ERROR -> ApplicationStatus.AgentWorkerStatus.error(e.getValue().getMessage());
                        default -> throw new RuntimeException("Unknown pod state: " + e.getValue().getState());
                    };
                    return new AbstractMap.SimpleEntry<>(e.getKey(), status);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static String sanitizeName(String name) {
        // Define the regular expression pattern
        String pattern = "[^a-z0-9.-]";

        // Remove invalid characters from the name
        String sanitizedName = name.toLowerCase().replaceAll(pattern, "");

        // Check if the sanitized name starts or ends with a non-alphanumeric character
        if (!sanitizedName.matches("[a-z0-9].*[a-z0-9]")) {
            // Add a default prefix and suffix
            sanitizedName = "default-" + sanitizedName + "-default";
        }

        // Truncate the name to 63 characters
        sanitizedName = sanitizedName.substring(0, Math.min(sanitizedName.length(), 63));

        return sanitizedName;
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

}
