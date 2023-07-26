package com.datastax.oss.sga.runtime.impl.k8s.agents;

import com.datastax.oss.sga.api.model.AgentConfiguration;
import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
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
public class AbstractComposableAgentProvider extends AbstractAgentProvider {


    public AbstractComposableAgentProvider(Set<String> supportedTypes, List<String> supportedRuntimes) {
        super(supportedTypes, supportedRuntimes);
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.FUNCTION;
    }

    @Override
    public boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation) {
        // all this kind of Agents can be merged
        return (getSupportedTypes().contains(previousAgent.getAgentType()) || previousAgent.getAgentType().equals(CompositeAgentProvider.AGENT_TYPE))
                && getSupportedTypes().contains(agentImplementation.getAgentType());
    }

    @Override
    public AgentNode mergeAgents(AgentNode previousAgent, AgentNode agentImplementation, ExecutionPlan instance) {
        if (previousAgent instanceof DefaultAgentNode agent1
                && agentImplementation instanceof DefaultAgentNode agent2) {

            if (agent1.getAgentType().equals(CompositeAgentProvider.AGENT_TYPE)) {
                // merge "in-memory-pipeline" with a Composable Agent
                Map<String, Object> configurationAgent2 = new HashMap<>();
                configurationAgent2.put("agentType", agent2.getAgentType());
                configurationAgent2.put("configuration", agent2.getConfiguration());
                Map<String, Object> newAgent1Configuration = new HashMap<>(agent1.getConfiguration());

                List<Map<String, Object>> agents = (List<Map<String, Object>>) newAgent1Configuration.get("agents");
                agents.add(configurationAgent2);
                log.info("Discarding topic {}", agent1.getOutputConnection());
                instance.discardTopic(agent1.getOutputConnection());
                agent1.overrideConfigurationAfterMerge(CompositeAgentProvider.AGENT_TYPE, newAgent1Configuration, agent2.getOutputConnection());
            } else {
                // merge two Composable Agents and build a "composite-agent"
                Map<String, Object> configurationAgent1 = new HashMap<>();
                configurationAgent1.put("agentType", agent1.getAgentType());
                configurationAgent1.put("configuration", agent1.getConfiguration());
                Map<String, Object> configurationAgent2 = new HashMap<>();
                configurationAgent2.put("agentType", agent2.getAgentType());
                configurationAgent2.put("configuration", agent2.getConfiguration());
                List<Map<String, Object>> agents = new ArrayList<>();
                agents.add(configurationAgent1);
                agents.add(configurationAgent2);
                Map<String, Object> result = new HashMap<>();
                result.put("agents", agents);
                log.info("Discarding topic {}", agent1.getOutputConnection());
                instance.discardTopic(agent1.getOutputConnection());
                agent1.overrideConfigurationAfterMerge(CompositeAgentProvider.AGENT_TYPE, result, agent2.getOutputConnection());
            }
            log.info("Agent 1 modified: {}", agent1);
            log.info("Discarding topic {}", agent2.getInputConnection());
            instance.discardTopic(agent2.getInputConnection());
            return previousAgent;
        }
        throw new IllegalStateException();
    }
}
