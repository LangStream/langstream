package com.datastax.oss.sga.pulsar.agents.ai;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.AgentNodeMetadata;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.agents.ai.GenAIToolKitFunctionAgentProvider;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;
import com.datastax.oss.sga.pulsar.agents.AbstractPulsarAgentProvider;

public class PulsarGenAIToolKitFunctionAgentProvider extends GenAIToolKitFunctionAgentProvider {

    public PulsarGenAIToolKitFunctionAgentProvider() {
        super(PulsarClusterRuntime.CLUSTER_TYPE, "ai-tools");
    }

    @Override
    protected AgentNodeMetadata computeAgentMetadata(AgentConfiguration agentConfiguration,
                                                     ExecutionPlan physicalApplicationInstance,
                                                     ComputeClusterRuntime clusterRuntime,
                                                     StreamingClusterRuntime streamingClusterRuntime) {
        ComponentType componentType = getComponentType(agentConfiguration);
        String agentType = getAgentType(agentConfiguration);
        return AbstractPulsarAgentProvider.computePulsarMetadata(agentConfiguration, physicalApplicationInstance, componentType, agentType);
    }
}
