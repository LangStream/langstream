package com.datastax.oss.sga.runtime.impl.k8s.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.impl.common.AbstractAgentProvider;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;

import static com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime.CLUSTER_TYPE;

/**
 * Implements support for processors that can be executed in memory into a single pipeline.
 * This is a special processor that executes a pipeline of Agents in memory.
 * It is not expected to be exposed to the user.
 * It is created internally by the planner.
 */
@Slf4j
public class CompositeAgentProvider extends AbstractAgentProvider {

    public static final String AGENT_TYPE = "composite-agent";

    private static final Set<String> SUPPORTED_AGENT_TYPES = Set.of(AGENT_TYPE);

    public CompositeAgentProvider() {
        super(SUPPORTED_AGENT_TYPES, List.of(CLUSTER_TYPE, "none"));
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.FUNCTION;
    }

}
