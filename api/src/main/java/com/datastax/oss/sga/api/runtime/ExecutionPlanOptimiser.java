package com.datastax.oss.sga.api.runtime;

/**
 * Optimises the execution plan of a Pipeline.
 */
public interface ExecutionPlanOptimiser {
    /**
     * Returns the ability of an Agent to be merged with the previous version.
     * @return true if the agents can be merged
     */
    boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation);

    AgentNode mergeAgents(AgentNode previousAgent, AgentNode agentImplementation, ExecutionPlan instance);
}
