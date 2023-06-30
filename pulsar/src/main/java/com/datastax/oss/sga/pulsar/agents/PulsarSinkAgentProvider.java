package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.impl.common.GenericSinkProvider;
import com.datastax.oss.sga.pulsar.PulsarName;
import com.datastax.oss.sga.pulsar.PulsarPhysicalApplicationInstance;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

public abstract class PulsarSinkAgentProvider extends GenericSinkProvider {

    public PulsarSinkAgentProvider(List<String> supportedTypes, List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    protected abstract String getSinkType(AgentConfiguration agentConfiguration);

    @AllArgsConstructor
    @Data
    public static class PulsarSinkMetadata {
        private final PulsarName pulsarName;
        private final String sinkType;
    }

    @Override
    protected Object computeAgentMetadata(AgentConfiguration agentConfiguration, PhysicalApplicationInstance physicalApplicationInstance, ClusterRuntime clusterRuntime) {
        PulsarPhysicalApplicationInstance pulsar = (PulsarPhysicalApplicationInstance) physicalApplicationInstance;
        PulsarName pulsarName = new PulsarName(pulsar.getDefaultTenant(), pulsar.getDefaultNamespace(), agentConfiguration.getName());
        return new PulsarSinkMetadata(pulsarName, getSinkType(agentConfiguration));
    }


}
