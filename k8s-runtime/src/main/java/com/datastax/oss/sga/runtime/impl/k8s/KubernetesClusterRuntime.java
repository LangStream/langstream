package com.datastax.oss.sga.runtime.impl.k8s;

import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
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
    public void deploy(ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        streamingClusterRuntime.deploy(applicationInstance);

        List<PodAgentConfiguration> crds = buildCustomResourceDefinitions(applicationInstance, streamingClusterRuntime);
        for (PodAgentConfiguration crd : crds) {
            log.info("Created CRD: {}", crd);
        }
        // TODO: use K8S client to create CRDs
    }

    private static List<PodAgentConfiguration> buildCustomResourceDefinitions(ExecutionPlan applicationInstance,
                                                                              StreamingClusterRuntime streamingClusterRuntime) {
        List<PodAgentConfiguration> agentsCustomResourceDefinitions = new ArrayList<>();
        for (AgentNode agentImplementation : applicationInstance.getAgents().values()) {
            buildCRDForAgent(agentsCustomResourceDefinitions, agentImplementation, streamingClusterRuntime, applicationInstance);
        }
        return agentsCustomResourceDefinitions;
    }

    private static void buildCRDForAgent(List<PodAgentConfiguration> agentsCustomResourceDefinitions, AgentNode agent,
                                         StreamingClusterRuntime streamingClusterRuntime, ExecutionPlan applicationInstance) {
        log.info("Building CRD for Agent {}", agent);
        if (! (agent instanceof AbstractAgentProvider.DefaultAgent)) {
            throw new UnsupportedOperationException("Only default agent implementations are supported");
        }
        AbstractAgentProvider.DefaultAgent defaultAgentImplementation = (AbstractAgentProvider.DefaultAgent) agent;

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
                applicationInstance.getApplication().getInstance().streamingCluster()
        );

        agentsCustomResourceDefinitions.add(crd);
    }

    @Override
    public void delete(ExecutionPlan applicationInstance, StreamingClusterRuntime streamingClusterRuntime) {
        List<PodAgentConfiguration> crds = buildCustomResourceDefinitions(applicationInstance, streamingClusterRuntime);
        // TODO: delete CRDs
    }
}
