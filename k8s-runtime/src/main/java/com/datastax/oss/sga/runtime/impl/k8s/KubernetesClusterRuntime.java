package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runtime.AgentImplementation;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.impl.common.BasicClusterRuntime;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class KubernetesClusterRuntime extends BasicClusterRuntime {
    static final ObjectMapper mapper = new ObjectMapper();
    public static final String CLUSTER_TYPE = "kubernetes";

    @Override
    public String getClusterType() {
        return CLUSTER_TYPE;
    }

    public static KubernetesClusterRuntimeConfiguration getKubernetesClusterRuntimeConfiguration(StreamingCluster streamingCluster) {
        final Map<String, Object> configuration = streamingCluster.configuration();
        return mapper.convertValue(configuration, KubernetesClusterRuntimeConfiguration.class);
    }

    @Override
    @SneakyThrows
    public void deploy(PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        streamingClusterRuntime.deploy(applicationInstance);

        List<PodAgentConfiguration> crds = buildCustomResourceDefinitions(applicationInstance, streamingClusterRuntime);
        for (PodAgentConfiguration crd : crds) {
            log.info("Created CRD: {}", crd);
        }
        // TODO: use K8S client to create CRDs
    }

    private static List<PodAgentConfiguration> buildCustomResourceDefinitions(PhysicalApplicationInstance applicationInstance,
                                                                              StreamingClusterRuntime streamingClusterRuntime) {
        List<PodAgentConfiguration> agentsCustomResourceDefinitions = new ArrayList<>();
        for (AgentImplementation agentImplementation : applicationInstance.getAgents().values()) {
            buildCRDForAgent(agentsCustomResourceDefinitions, agentImplementation, streamingClusterRuntime, applicationInstance);
        }
        return agentsCustomResourceDefinitions;
    }

    private static void buildCRDForAgent(List<PodAgentConfiguration> agentsCustomResourceDefinitions, AgentImplementation agent,
                                         StreamingClusterRuntime streamingClusterRuntime, PhysicalApplicationInstance applicationInstance) {
        log.info("Building CRD for Agent {}", agent);
        if (! (agent instanceof AbstractAgentProvider.DefaultAgentImplementation)) {
            throw new UnsupportedOperationException("Only default agent implementations are supported");
        }
        AbstractAgentProvider.DefaultAgentImplementation defaultAgentImplementation = (AbstractAgentProvider.DefaultAgentImplementation) agent;

        Map<String, Object> agentConfiguration = new HashMap<>();
        agentConfiguration.putAll(defaultAgentImplementation.getConfiguration());
        agentConfiguration.put("agentId", defaultAgentImplementation.getId());
        agentConfiguration.put("agentType", defaultAgentImplementation.getType());

        if (defaultAgentImplementation.getRuntimeMetadata() != null) {
            agentConfiguration.put("metadata", defaultAgentImplementation.getRuntimeMetadata());
        }

        Map<String, Object> inputConfiguration = new HashMap<>();
        if (defaultAgentImplementation.getInputConnection() != null) {
            inputConfiguration = streamingClusterRuntime.createConsumerConfiguration(defaultAgentImplementation, defaultAgentImplementation.getInputConnection());
        }
        Map<String, Object> outputConfiguration = new HashMap<>();
        if (defaultAgentImplementation.getOutputConnection() != null) {
            outputConfiguration = streamingClusterRuntime.createProducerConfiguration(defaultAgentImplementation, defaultAgentImplementation.getOutputConnection());
        }

        PodAgentConfiguration crd = new PodAgentConfiguration(
                inputConfiguration,
                outputConfiguration,
                agentConfiguration,
                applicationInstance.getApplicationInstance().getInstance().streamingCluster()
        );

        agentsCustomResourceDefinitions.add(crd);
    }

    @Override
    public void delete(PhysicalApplicationInstance applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        List<PodAgentConfiguration> crds = buildCustomResourceDefinitions(applicationInstance, streamingClusterRuntime);
        // TODO: delete CRDs
    }
}
