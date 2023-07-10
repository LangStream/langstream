package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.api.runtime.StreamingClusterRuntime;
import com.datastax.oss.sga.pulsar.PulsarName;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

public abstract class AbstractPulsarSourceAgentProvider extends AbstractPulsarAgentProvider {

    public AbstractPulsarSourceAgentProvider(List<String> supportedTypes, List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    protected abstract String getSourceType(AgentConfiguration agentConfiguration);

    @AllArgsConstructor
    @Data
    public static class PulsarSourceMetadata {
        private final PulsarName pulsarName;
        private final String sourceType;
    }

    @Override
    protected Object computeAgentMetadata(AgentConfiguration agentConfiguration, PhysicalApplicationInstance physicalApplicationInstance,
                                          ClusterRuntime clusterRuntime, StreamingClusterRuntime streamingClusterRuntime) {
        PulsarName pulsarName = computePulsarName(physicalApplicationInstance, agentConfiguration);
        return new PulsarSourceMetadata(pulsarName, getSourceType(agentConfiguration));
    }


}
