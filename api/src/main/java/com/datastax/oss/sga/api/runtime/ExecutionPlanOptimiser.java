package com.datastax.oss.sga.api.runtime;

import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.Pipeline;

/**
 * Optimises the execution plan of a Pipeline.
 */
public interface ExecutionPlanOptimiser {
    /**
     * Returns the ability of an Agent to be merged with the previous version.
     * @return true if the agents can be merged
     */
    boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation);

    AgentNode mergeAgents(Module module, Pipeline pipeline, AgentNode previousAgent, AgentNode agentImplementation, ExecutionPlan instance);
}
