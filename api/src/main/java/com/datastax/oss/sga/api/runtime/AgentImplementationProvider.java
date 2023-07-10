package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;

public interface AgentImplementationProvider {

    AgentImplementation createImplementation(AgentConfiguration agentConfiguration,
                                             Module module,
                                             PhysicalApplicationInstance physicalApplicationInstance,
                                             ClusterRuntime clusterRuntime,
                                             PluginsRegistry pluginsRegistry, StreamingClusterRuntime streamingClusterRuntime);

    boolean supports(String type, ClusterRuntime clusterRuntime);
}
