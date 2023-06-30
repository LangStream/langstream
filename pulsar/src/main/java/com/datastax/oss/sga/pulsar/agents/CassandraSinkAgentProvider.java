package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ClusterRuntime;
import com.datastax.oss.sga.api.runtime.PhysicalApplicationInstance;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;

import java.util.List;
import java.util.Map;

public class CassandraSinkAgentProvider extends PulsarSinkAgentProvider {

    public CassandraSinkAgentProvider() {
        super(List.of("cassandra-sink"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getSinkType(AgentConfiguration agentConfiguration) {
        return "cassandra-enhanced";
    }

    @Override
    protected Map<String, Object> computeAgentConfiguration(AgentConfiguration agentConfiguration, PhysicalApplicationInstance physicalApplicationInstance, ClusterRuntime clusterRuntime) {
        Map<String, Object> configuration = super.computeAgentConfiguration(agentConfiguration, physicalApplicationInstance, clusterRuntime);

        // TODO: automatically compute the list of topics (this is an additional configuration in the Sink that must match the input topics list), topic mappings


        return configuration;
    }
}
