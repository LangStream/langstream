package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;

import java.util.List;
import java.util.Map;

public class GenericPulsarSourceAgentProvider extends AbstractPulsarSourceAgentProvider {

    public GenericPulsarSourceAgentProvider() {
        super(List.of("generic-pulsar-source"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getSourceType(AgentConfiguration configuration) {
        String sourceType = (String)configuration.getConfiguration()
                .get("sourceType");
        if (sourceType == null) {
            throw new IllegalArgumentException("For the generic pulsar-sink you must configured the sourceType configuration property");
        }
        return sourceType;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, PhysicalApplicationInstance physicalApplicationInstance, ClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        copy.remove("sourceType");
        return copy;
    }
}
