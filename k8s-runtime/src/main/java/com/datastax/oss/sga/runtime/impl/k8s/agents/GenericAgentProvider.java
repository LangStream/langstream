package com.datastax.oss.sga.runtime.impl.k8s.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime;
import java.util.List;
import java.util.Map;

public class GenericAgentProvider extends AbstractAgentProvider {

    public GenericAgentProvider() {
        super(List.of("generic-agent"), List.of(KubernetesClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);
        // TODO.....
        return copy;
    }
}
