package com.datastax.oss.sga.deployer.k8s.agents;

import com.datastax.oss.sga.api.model.AgentLifecycleStatus;
import com.datastax.oss.sga.api.model.ApplicationStatus;
import com.datastax.oss.sga.api.model.ResourcesSpec;
import com.datastax.oss.sga.deployer.k8s.CRDConstants;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentSpec;
import com.datastax.oss.sga.deployer.k8s.util.KubeUtil;
import com.datastax.oss.sga.deployer.k8s.util.SerializationUtil;
import com.datastax.oss.sga.runtime.api.agent.CodeStorageConfig;
import com.datastax.oss.sga.runtime.api.agent.RuntimePodConfiguration;
import com.datastax.oss.sga.runtime.k8s.api.PodAgentConfiguration;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AgentResourcesFactory {

    public static StatefulSet generateStatefulSet(AgentCustomResource agentCustomResource,
                                                  Map<String, Object> codeStoreConfiguration,
                                                  AgentResourceUnitConfiguration agentResourceUnitConfiguration) {

        final AgentSpec spec = agentCustomResource.getSpec();
        final PodAgentConfiguration podAgentConfiguration =
                SerializationUtil.readJson(spec.getConfiguration(), PodAgentConfiguration.class);
        final String agentId = podAgentConfiguration.agentConfiguration().agentId();
        final String applicationId = spec.getApplicationId();
        RuntimePodConfiguration podConfig = new RuntimePodConfiguration(
                podAgentConfiguration.input(),
                podAgentConfiguration.output(),
                new com.datastax.oss.sga.runtime.api.agent.AgentSpec(
                        com.datastax.oss.sga.runtime.api.agent.AgentSpec.ComponentType.valueOf(
                                podAgentConfiguration.agentConfiguration().componentType()),
                        spec.getTenant(),
                        agentId,
                        applicationId,
                        podAgentConfiguration.agentConfiguration().agentType(),
                        podAgentConfiguration.agentConfiguration().configuration()
                ),
                podAgentConfiguration.streamingCluster(),
                new CodeStorageConfig(codeStoreConfiguration.getOrDefault("type", "none").toString(),
                        podAgentConfiguration.codeStorage().codeStorageArchiveId(), codeStoreConfiguration)
        );


        final Container container = new ContainerBuilder()
                .withName("runtime")
                .withImage(podAgentConfiguration.image())
                .withImagePullPolicy(podAgentConfiguration.imagePullPolicy())
//                .withLivenessProbe(createLivenessProbe())
//                .withReadinessProbe(createReadinessProbe())
                .withResources(convertResources(podAgentConfiguration, agentResourceUnitConfiguration))
                .withArgs("agent-runtime", "/app-config/config")
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("app-config")
                        .withMountPath("/app-config")
                        .build()
                )
                .withTerminationMessagePolicy("FallbackToLogsOnError")
                .build();

        final Container initContainer = new ContainerBuilder()
                .withName("runtime-init-config")
                .withImage(podAgentConfiguration.image())
                .withImagePullPolicy(podAgentConfiguration.imagePullPolicy())
                .withResources(new ResourceRequirementsBuilder().withRequests(Map.of("cpu", Quantity.parse("100m"),
                        "memory", Quantity.parse("100Mi"))).build())
                .withCommand("bash", "-c")
                .withArgs("echo '%s' > /app-config/config".formatted(SerializationUtil.writeAsJson(podConfig)))
                .withVolumeMounts(new VolumeMountBuilder()
                        .withName("app-config")
                        .withMountPath("/app-config")
                        .build())
                .build();

        final String tenant = spec.getTenant();
        final Map<String, String> labels = getAgentLabels(agentId, applicationId);
        computeReplicas(agentResourceUnitConfiguration, podAgentConfiguration);

        final StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                .withName(sanitizeName(agentCustomResource.getMetadata().getName()))
                .withNamespace(agentCustomResource.getMetadata().getNamespace())
                .withLabels(labels)
                .withOwnerReferences(KubeUtil.getOwnerReferenceForResource(agentCustomResource))
                .endMetadata()
                .withNewSpec()
                .withReplicas(computeReplicas(agentResourceUnitConfiguration, podAgentConfiguration))
                .withNewSelector()
                .withMatchLabels(labels)
                .endSelector()
                .withPodManagementPolicy("Parallel")
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(labels)
                .endMetadata()
                .withNewSpec()
                .withServiceAccountName(tenant)
                .withTerminationGracePeriodSeconds(60L)
                .withInitContainers(List.of(initContainer))
                .withContainers(List.of(container))
                .withVolumes(new VolumeBuilder()
                        .withName("app-config")
                        .withEmptyDir(new EmptyDirVolumeSource())
                        .build()
                )
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
        return statefulSet;
    }

    private static int computeReplicas(AgentResourceUnitConfiguration agentResourceUnitConfiguration,
                                  PodAgentConfiguration podAgentConfiguration) {
        Integer requestedParallelism = podAgentConfiguration.resources() == null ? null : podAgentConfiguration.resources().parallelism();
        if (requestedParallelism == null) {
            requestedParallelism = agentResourceUnitConfiguration.getDefaultInstanceUnits();
        }
        if (requestedParallelism > agentResourceUnitConfiguration.getMaxInstanceUnits()) {
            throw new IllegalArgumentException("Requested %d instances, max is %d".formatted(requestedParallelism, agentResourceUnitConfiguration.getMaxInstanceUnits()));
        }
        return requestedParallelism;
    }

    private static ResourceRequirements convertResources(PodAgentConfiguration podAgentConfiguration, AgentResourceUnitConfiguration agentResourceUnitConfiguration) {
        Integer memCpuUnits = podAgentConfiguration.resources() == null ? null : podAgentConfiguration.resources().size();
        if (memCpuUnits == null) {
            memCpuUnits = agentResourceUnitConfiguration.getDefaultCpuMemUnits();
        }

        Integer instances = podAgentConfiguration.resources() == null ? null : podAgentConfiguration.resources().parallelism();
        if (instances == null) {
            instances = agentResourceUnitConfiguration.getDefaultInstanceUnits();
        }

        if (memCpuUnits > agentResourceUnitConfiguration.getMaxCpuMemUnits()) {
            throw new IllegalArgumentException("Requested %d cpu/mem units, max is %d".formatted(memCpuUnits, agentResourceUnitConfiguration.getMaxCpuMemUnits()));
        }
        if (instances > agentResourceUnitConfiguration.getMaxInstanceUnits()) {
            throw new IllegalArgumentException("Requested %d instance units, max is %d".formatted(instances, agentResourceUnitConfiguration.getMaxInstanceUnits()));
        }

        final Map<String, Quantity> requests = new HashMap<>();
        requests.put("cpu", Quantity.parse("%f".formatted(memCpuUnits * agentResourceUnitConfiguration.getCpuPerUnit())));
        requests.put("memory", Quantity.parse("%dM".formatted(memCpuUnits * agentResourceUnitConfiguration.getMemPerUnit())));

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
                                                                  final String tenant,
                                                                  final PodAgentConfiguration podAgentConfiguration) {
        final AgentCustomResource agentCR = new AgentCustomResource();
        final String agentName = getAgentCustomResourceName(applicationId, agentId);
        agentCR.setMetadata(new ObjectMetaBuilder()
                .withName(agentName)
                .withLabels(getAgentLabels(agentId, applicationId))
                .build());
        agentCR.setSpec(AgentSpec.builder()
                .tenant(tenant)
                .applicationId(applicationId)
                .configuration(SerializationUtil.writeAsJson(podAgentConfiguration))
                .build());
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

            ApplicationStatus.AgentStatus agentStatus = new ApplicationStatus.AgentStatus();
            final AgentCustomResource cr = agentCustomResources.get(declaredAgent);
            if (cr == null) {
                agentStatus.setStatus(AgentLifecycleStatus.DEPLOYING);
            } else {
                agentStatus.setStatus(cr.getStatus().getStatus());
                Map<String, ApplicationStatus.AgentWorkerStatus> podStatuses =
                        getPodStatuses(client, applicationId, namespace, declaredAgent);
                agentStatus.setWorkers(podStatuses);

            }

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
}
