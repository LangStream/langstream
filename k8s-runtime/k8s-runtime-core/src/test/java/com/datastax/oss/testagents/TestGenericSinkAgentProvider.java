package com.datastax.oss.testagents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.Topic;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestGenericSinkAgentProvider extends AbstractAgentProvider {

    public TestGenericSinkAgentProvider() {
        super(Set.of("sink"), List.of("none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SINK;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);

        // we can auto-wire the "topics" configuration property
        Connection connection = physicalApplicationInstance.getConnectionImplementation(module, agentConfiguration.getInput());
        if (connection instanceof Topic topic) {
            copy.put("topics", topic.topicName());
        }
        return copy;
    }
}

