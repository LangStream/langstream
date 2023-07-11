package com.datastax.oss.sga.api.runtime;

public interface AgentImplementation extends ConnectionImplementation {

    /**
     * The id of the agent. This can be used to compute subscriptions or consumer groups.
     * @return the id
     */
    String getId();
}
