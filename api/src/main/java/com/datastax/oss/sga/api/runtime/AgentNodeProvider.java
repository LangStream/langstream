package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;

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
                                   Pipeline pipeline,
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

}
