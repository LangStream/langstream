package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.AgentNodeMetadata;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntimeConfiguration;
import com.datastax.oss.sga.pulsar.PulsarName;

import java.util.List;
import java.util.Set;

import static com.datastax.oss.sga.pulsar.PulsarClientUtils.getPulsarClusterRuntimeConfiguration;

public abstract class AbstractPulsarAgentProvider extends AbstractAgentProvider {

    public AbstractPulsarAgentProvider(Set<String> supportedTypes,
                                       List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    public static PulsarName computePulsarName(ExecutionPlan instance, AgentConfiguration agentConfiguration) {
        PulsarClusterRuntimeConfiguration pulsarClusterRuntimeConfiguration = getPulsarClusterRuntimeConfiguration(instance.getApplication().getInstance().streamingCluster());
        return new PulsarName(pulsarClusterRuntimeConfiguration.getDefaultTenant(),
                pulsarClusterRuntimeConfiguration.getDefaultNamespace(), sanitizeName(agentConfiguration));
    }

    private static String sanitizeName(AgentConfiguration agentConfiguration) {
        return agentConfiguration.getName()
                .replace(" ", "-")
                .replace(".", "-")
                .toLowerCase();
    }

    @Override
    protected AgentNodeMetadata computeAgentMetadata(AgentConfiguration agentConfiguration, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime,
                                                     StreamingClusterRuntime streamingClusterRuntime) {
        ComponentType componentType = getComponentType(agentConfiguration);
        String agentType = getAgentType(agentConfiguration);
        return computePulsarMetadata(agentConfiguration, physicalApplicationInstance, componentType, agentType);
    }

    public static PulsarAgentNodeMetadata computePulsarMetadata(AgentConfiguration agentConfiguration, ExecutionPlan physicalApplicationInstance,
                                                                ComponentType componentType, String agentType) {
        PulsarName pulsarName = computePulsarName(physicalApplicationInstance, agentConfiguration);
        return new PulsarAgentNodeMetadata(pulsarName, componentType, agentType);
    }
}
