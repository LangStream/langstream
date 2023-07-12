package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;

public interface AgentNodeProvider {

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
    AgentNode createImplementation(AgentConfiguration agentConfiguration,
                                   Module module,
                                   ExecutionPlan physicalApplicationInstance,
                                   ComputeClusterRuntime clusterRuntime,
                                   PluginsRegistry pluginsRegistry,
                                   StreamingClusterRuntime streamingClusterRuntime);

    /**
     * Returns the ability of an Agent to be deployed on the give runtimes.
     * @param type
     * @param clusterRuntime
     * @return true if this provider that can create the implementation
     */
    boolean supports(String type, ComputeClusterRuntime clusterRuntime);

    /**
     * Returns the ability of an Agent to be merged with the previous version.
     * @return true if the agents can be merged
     */
    default boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation) {
        return false;
    }

    default AgentNode mergeAgents(AgentNode previousAgent, AgentNode agentImplementation, ExecutionPlan instance) {
        throw new UnsupportedOperationException("This provider cannot merge agents");
    }
}
