package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.deployer.k8s.agents.AgentResourcesFactory;
import com.datastax.oss.sga.deployer.k8s.api.crds.agents.AgentCustomResource;
import com.datastax.oss.sga.impl.common.BasicClusterRuntime;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import com.datastax.oss.sga.impl.k8s.KubernetesClientFactory;
import com.datastax.oss.sga.runtime.k8s.api.PodAgentConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class KubernetesClusterRuntime extends BasicClusterRuntime {
    static final ObjectMapper mapper = new ObjectMapper()
            .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
    public static final String CLUSTER_TYPE = "kubernetes";

    private KubernetesClusterRuntimeConfiguration configuration;
    private KubernetesClient client;

    @Override
    public String getClusterType() {
        return CLUSTER_TYPE;
    }

    @Override
    @SneakyThrows
    public void initialize(Map<String, Object> configuration) {
        this.configuration = mapper.convertValue(configuration, KubernetesClusterRuntimeConfiguration.class);
        this.client = KubernetesClientFactory.get(null);
    }

    @Override
    @SneakyThrows
    public Object deploy(String tenant, ExecutionPlan applicationInstance,
                         StreamingClusterRuntime streamingClusterRuntime,
                         String codeStorageArchiveId) {
        streamingClusterRuntime.deploy(applicationInstance);

        List<PodAgentConfiguration> configs = buildPodAgentConfigurations(applicationInstance, streamingClusterRuntime, codeStorageArchiveId);
        final String namespace = configuration.getNamespacePrefix() + tenant;

        for (PodAgentConfiguration podAgentConfiguration : configs) {

            final AgentCustomResource agentCustomResource = AgentResourcesFactory.generateAgentCustomResource(
                    applicationInstance.getApplicationId(),
                    podAgentConfiguration.agentConfiguration().agentId(),
                    tenant,
                    configuration.getImage(),
                    configuration.getImagePullPolicy(),
                    podAgentConfiguration
            );
            client.resource(agentCustomResource).inNamespace(namespace).serverSideApply();
            log.info("Created CRD {} with spec {}",
                    agentCustomResource.getMetadata().getName(), agentCustomResource.getSpec());
        }
        return configs;
    }

    private static List<PodAgentConfiguration> buildPodAgentConfigurations(ExecutionPlan applicationInstance,
                                                                           StreamingClusterRuntime streamingClusterRuntime,
                                                                           String codeStorageArchiveId) {
        List<PodAgentConfiguration> agents = new ArrayList<>();
        for (AgentNode agentImplementation : applicationInstance.getAgents().values()) {
            buildPodAgentConfiguration(agents, agentImplementation, streamingClusterRuntime,
                    applicationInstance, codeStorageArchiveId);
        }
        return agents;
    }

    private static void buildPodAgentConfiguration(List<PodAgentConfiguration> agentsCustomResourceDefinitions,
                                                   AgentNode agent,
                                                   StreamingClusterRuntime streamingClusterRuntime,
                                                   ExecutionPlan applicationInstance,
                                                   String codeStorageArchiveId) {
        log.info("Building configuration for Agent {}, codeStorageArchiveId {}", agent, codeStorageArchiveId);
        if (!(agent instanceof DefaultAgentNode)) {
            throw new UnsupportedOperationException("Only default agent implementations are supported");
        }
        DefaultAgentNode defaultAgentImplementation = (DefaultAgentNode) agent;

        Map<String, Object> agentConfiguration = new HashMap<>();
        agentConfiguration.putAll(defaultAgentImplementation.getConfiguration());
        agentConfiguration.put("agentId", defaultAgentImplementation.getId());
        agentConfiguration.put("agentType", defaultAgentImplementation.getAgentType());

        if (defaultAgentImplementation.getCustomMetadata() != null) {
            agentConfiguration.put("metadata", defaultAgentImplementation.getCustomMetadata());
        }

        Map<String, Object> inputConfiguration = new HashMap<>();
        if (defaultAgentImplementation.getInputConnection() != null) {
            inputConfiguration = streamingClusterRuntime.createConsumerConfiguration(defaultAgentImplementation,
                    defaultAgentImplementation.getInputConnection());
        }
        Map<String, Object> outputConfiguration = new HashMap<>();
        if (defaultAgentImplementation.getOutputConnection() != null) {
            outputConfiguration = streamingClusterRuntime.createProducerConfiguration(defaultAgentImplementation,
                    defaultAgentImplementation.getOutputConnection());
        }

        PodAgentConfiguration crd = new PodAgentConfiguration(
                inputConfiguration,
                outputConfiguration,
                new PodAgentConfiguration.AgentConfiguration(defaultAgentImplementation.getId(),
                        defaultAgentImplementation.getAgentType(),
                        defaultAgentImplementation.getComponentType().name(),
                        defaultAgentImplementation.getConfiguration()),
                applicationInstance.getApplication().getInstance().streamingCluster(),
                new PodAgentConfiguration.CodeStorageConfiguration(codeStorageArchiveId)
        );

        agentsCustomResourceDefinitions.add(crd);
    }

    @Override
    public void delete(String tenant, ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime, String codeStorageArchiveId) {
        List<PodAgentConfiguration> agents = buildPodAgentConfigurations(applicationInstance, streamingClusterRuntime, codeStorageArchiveId);
        final String namespace = configuration.getNamespacePrefix() + tenant;
        for (PodAgentConfiguration agent : agents) {
            client.resources(AgentCustomResource.class).inNamespace(namespace).withName(agent.agentConfiguration().agentId())
                    .delete();

        }
    }
}
