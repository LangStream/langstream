package com.datastax.oss.sga.impl.noop;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;

import java.util.List;
import java.util.Set;

public class NoOpAgentNodeProvider extends AbstractAgentProvider {

    public NoOpAgentNodeProvider() {
        super(Set.of("noop"), List.of("none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.FUNCTION;
    }
}
