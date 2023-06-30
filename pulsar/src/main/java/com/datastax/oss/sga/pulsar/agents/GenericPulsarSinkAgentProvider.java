package com.datastax.oss.sga.pulsar.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.pulsar.PulsarClusterRuntime;

import java.util.List;

public class GenericPulsarSinkAgentProvider extends PulsarSinkAgentProvider {

    public GenericPulsarSinkAgentProvider() {
        super(List.of("generic-pulsar-sink"), List.of(PulsarClusterRuntime.CLUSTER_TYPE));
    }

    @Override
    protected String getSinkType(AgentConfiguration configuration) {
        String sinkType = (String)configuration.getConfiguration()
                .get("sinkType");
        if (sinkType == null) {
            throw new IllegalArgumentException("For the generic pulsar-sink you must configured the sinkType configuration property");
        }
        return sinkType;
    }
}
