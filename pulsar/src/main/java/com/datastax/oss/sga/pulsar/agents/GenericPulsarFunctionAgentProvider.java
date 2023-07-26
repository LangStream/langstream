package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GenericPulsarFunctionAgentProvider extends AbstractPulsarAgentProvider {

    public GenericPulsarFunctionAgentProvider() {
        super(Set.of("function"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getAgentType(AgentConfiguration configuration) {
        String functionType = (String)configuration.getConfiguration()
                .get("functionType");
        return functionType;
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.FUNCTION;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, Pipeline pipeline, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, pipeline, physicalApplicationInstance, clusterRuntime);
        copy.remove("functionType");
        return copy;
    }
}
