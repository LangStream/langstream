package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.pulsar.PulsarName;
import com.datastax.oss.sga.pulsar.PulsarPhysicalApplicationInstance;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

public abstract class AbstractPulsarFunctionAgentProvider extends AbstractAgentProvider {

    public AbstractPulsarFunctionAgentProvider(List<String> supportedTypes, List<String> supportedClusterTypes) {
        super(supportedTypes, supportedClusterTypes);
    }

    protected abstract String getFunctionType(AgentConfiguration agentConfiguration);

    protected abstract String getFunctionClassname(AgentConfiguration agentConfiguration);

    @AllArgsConstructor
    @Data
    public static class PulsarFunctionMetadata {
        private final PulsarName pulsarName;
        private final String functionType;

        private final String functionClassname;
    }

    @Override
    protected Object computeAgentMetadata(AgentConfiguration agentConfiguration, PhysicalApplicationInstance physicalApplicationInstance, ClusterRuntime clusterRuntime) {
        PulsarPhysicalApplicationInstance pulsar = (PulsarPhysicalApplicationInstance) physicalApplicationInstance;
        PulsarName pulsarName = new PulsarName(pulsar.getDefaultTenant(), pulsar.getDefaultNamespace(), agentConfiguration.getName());
        return new PulsarFunctionMetadata(pulsarName, getFunctionType(agentConfiguration), getFunctionClassname(agentConfiguration));
    }


}
