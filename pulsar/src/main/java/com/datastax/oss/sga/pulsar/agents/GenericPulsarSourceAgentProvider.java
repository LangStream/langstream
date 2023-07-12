package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;

import java.util.List;
import java.util.Map;

public class GenericPulsarSourceAgentProvider extends AbstractPulsarAgentProvider {

    public GenericPulsarSourceAgentProvider() {
        super(List.of("generic-pulsar-source"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getAgentType(AgentConfiguration configuration) {
        String sourceType = (String)configuration.getConfiguration()
                .get("sourceType");
        if (sourceType == null) {
            throw new IllegalArgumentException("For the generic pulsar-source you must configured the sourceType configuration property");
        }
        return sourceType;
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        copy.remove("sourceType");
        return copy;
    }
}
