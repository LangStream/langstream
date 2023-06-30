package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.AgentConfiguration;

public interface AgentImplementationProvider {

    AgentImplementation createImplementation(AgentConfiguration agentConfiguration,
                                             PhysicalApplicationInstance physicalApplicationInstance,
                                             ClusterRuntime clusterRuntime,
                                             PluginsRegistry pluginsRegistry);

    boolean supports(String type, ClusterRuntime<?> clusterRuntime);
}
