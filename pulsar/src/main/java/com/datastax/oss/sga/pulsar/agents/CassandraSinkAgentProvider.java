package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ComputeClusterRuntime;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.Topic;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CassandraSinkAgentProvider extends AbstractPulsarAgentProvider {

    public CassandraSinkAgentProvider() {
        super(Set.of("cassandra-sink"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getAgentType(AgentConfiguration agentConfiguration) {
        return "cassandra-enhanced";
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.SINK;
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module, Pipeline pipeline,
                                                            ExecutionPlan applicationInstance, ComputeClusterRuntime clusterRuntime) {
        Map<String, Object> configuration = super.computeAgentConfiguration(agentConfiguration, module, pipeline, applicationInstance, clusterRuntime);

        // We have to automatically compute the list of topics (this is an additional configuration in the Sink that must match the input topics list)
        ConnectionImplementation connectionImplementation = applicationInstance.getConnectionImplementation(module, agentConfiguration.getInput());
        if (connectionImplementation instanceof Topic topic) {
            configuration.put("topics", topic.topicName());
        }
        return configuration;
    }
}
