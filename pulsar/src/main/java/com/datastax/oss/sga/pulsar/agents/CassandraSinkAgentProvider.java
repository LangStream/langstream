package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.ConnectionImplementation;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;
import com.datastax.oss.sga.pulsar.PulsarTopic;

import java.util.List;
import java.util.Map;

public class CassandraSinkAgentProvider extends AbstractPulsarSinkAgentProvider {

    public CassandraSinkAgentProvider() {
        super(List.of("cassandra-sink"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getSinkType(AgentConfiguration agentConfiguration) {
        return "cassandra-enhanced";
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, Module module,
                                                            PhysicalApplicationInstance physicalApplicationInstance, ClusterRuntime clusterRuntime) {
        Map<String, Object> configuration = super.computeAgentConfiguration(agentConfiguration, module, physicalApplicationInstance, clusterRuntime);

        // We have to automatically compute the list of topics (this is an additional configuration in the Sink that must match the input topics list)
        ConnectionImplementation connectionImplementation = physicalApplicationInstance.getConnectionImplementation(module, agentConfiguration.getInput());
        PulsarTopic pulsarTopic = (PulsarTopic) connectionImplementation;
        configuration.put("topics", pulsarTopic.name().toPulsarName());

        return configuration;
    }
}
