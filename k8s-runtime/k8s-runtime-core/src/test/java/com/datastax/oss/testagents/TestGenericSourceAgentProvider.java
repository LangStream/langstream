package com.datastax.oss.testagents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.Connection;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.Topic;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestGenericSourceAgentProvider extends AbstractAgentProvider {

    public TestGenericSourceAgentProvider() {
        super(Set.of("source"), List.of("none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SOURCE;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, Pipeline pipeline,  ExecutionPlan physicalApplicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> copy = super.computeAgentConfiguration(agentConfiguration, module, pipeline, physicalApplicationInstance, clusterRuntime);

        // we can auto-wire the "topic" configuration property
        Connection connection = physicalApplicationInstance.getConnectionImplementation(module, agentConfiguration.getOutput());
        if (connection instanceof Topic topic) {
            copy.put("topic", topic.topicName());
        }
        return copy;
    }
}

