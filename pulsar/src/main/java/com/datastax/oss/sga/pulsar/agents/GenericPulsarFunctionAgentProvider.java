package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;

import java.util.List;
import java.util.Map;

public class GenericPulsarFunctionAgentProvider extends AbstractPulsarFunctionAgentProvider {

    public GenericPulsarFunctionAgentProvider() {
        super(List.of("generic-pulsar-function"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getFunctionType(AgentConfiguration configuration) {
        String functionType = (String)configuration.getConfiguration()
                .get("functionType");
        return functionType;
    }

    @Override
    protected String getFunctionClassname(AgentConfiguration configuration) {
        String functionClassname = (String)configuration.getConfiguration()
                .get("functionClassname");
        return functionClassname;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, PhysicalApplicationInstance physicalApplicationInstance, ClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        copy.remove("functionType");
        copy.remove("functionClassname");
        return copy;
    }
}
