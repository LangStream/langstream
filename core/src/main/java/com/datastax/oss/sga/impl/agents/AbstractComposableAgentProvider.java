package com.datastax.oss.sga.impl.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.ExecutionPlanOptimiser;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implements support for agents that can be composed into a single Composable Agent.
 */
@Slf4j
public abstract class AbstractComposableAgentProvider extends AbstractAgentProvider {

    public AbstractComposableAgentProvider(Set<String> supportedTypes, List<String> supportedRuntimes) {
        super(supportedTypes, supportedRuntimes);
    }

    @Override
    protected boolean isComposable(AgentConfiguration agentConfiguration) {
        return true;
    }

}
