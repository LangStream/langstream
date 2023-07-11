package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;

public interface AgentImplementationProvider {

    /**
     * Create an Implementation of an Agent that can be deployed on the give runtimes.
     * @param agentConfiguration
     * @param module
     * @param physicalApplicationInstance
     * @param clusterRuntime
     * @param pluginsRegistry
     * @param streamingClusterRuntime
     * @return the Agent
     */
    AgentImplementation createImplementation(AgentConfiguration agentConfiguration,
                                             Module module,
                                             PhysicalApplicationInstance physicalApplicationInstance,
                                             ClusterRuntime clusterRuntime,
                                             PluginsRegistry pluginsRegistry,
                                             StreamingClusterRuntime streamingClusterRuntime);

    /**
     * Returns the ability of an Agent to be deployed on the give runtimes.
     * @param type
     * @param clusterRuntime
     * @return true if this provider that can create the implementation
     */
    boolean supports(String type, ClusterRuntime clusterRuntime);

    /**
     * Returns the ability of an Agent to be merged with the previous version.
     * @return true if the agents can be merged
     */
    default boolean canMerge(AgentImplementation previousAgent, AgentImplementation agentImplementation) {
        return false;
    }

    default AgentImplementation mergeAgents(AgentImplementation previousAgent, AgentImplementation agentImplementation, PhysicalApplicationInstance instance) {
        throw new UnsupportedOperationException("This provider cannot merge agents");
    }
}
