package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntimeConfiguration;
import com.datastax.oss.sga.pulsar.PulsarName;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

import static com.datastax.oss.sga.pulsar.PulsarClusterRuntime.getPulsarClusterRuntimeConfiguration;

public abstract class AbstractPulsarAgentProvider extends AbstractAgentProvider {

    public AbstractPulsarAgentProvider(List<String> supportedTypes,
                                       List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    public enum ComponentType {
        FUNCTION,
        SINK,
        SOURCE
    }

    @AllArgsConstructor
    @Data
    public static class PulsarAgentNodeMetadata {
        private final PulsarName pulsarName;
        private final ComponentType componentType;
        private final String agentType;
    }

    protected abstract ComponentType getComponentType(AgentConfiguration agentConfiguration);

    protected abstract String getAgentType(AgentConfiguration agentConfiguration);

    protected PulsarName computePulsarName(ExecutionPlan instance, AgentConfiguration agentConfiguration) {
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
    protected Object computeAgentMetadata(AgentConfiguration agentConfiguration, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime,
                                          StreamingClusterRuntime streamingClusterRuntime) {
        PulsarName pulsarName = computePulsarName(physicalApplicationInstance, agentConfiguration);
        return new PulsarAgentNodeMetadata(pulsarName, getComponentType(agentConfiguration), getAgentType(agentConfiguration));
    }
}
