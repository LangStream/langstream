package com.datastax.oss.sga.impl.agents;

import com.datastax.oss.sga.api.runtime.AgentNode;
import com.datastax.oss.sga.api.runtime.ComponentType;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.ExecutionPlanOptimiser;
import com.datastax.oss.sga.impl.common.DefaultAgentNode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public final class ComposableAgentExecutionPlanOptimiser implements ExecutionPlanOptimiser {

    @Override
    public boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation) {
        boolean result = (previousAgent instanceof DefaultAgentNode agent1
                && agent1.isComposable()
                && agentImplementation instanceof DefaultAgentNode agent2
                && agent2.isComposable());
        log.info("canMerge {}", previousAgent);
        log.info("canMerge {}", agentImplementation);
        log.info("canMerge RESULT: {}", result);
        return result;
    }

    @Override
    public AgentNode mergeAgents(AgentNode previousAgent, AgentNode agentImplementation, ExecutionPlan instance) {
        if (previousAgent instanceof DefaultAgentNode agent1
                && agentImplementation instanceof DefaultAgentNode agent2) {

            if (agent1.getAgentType().equals(AbstractCompositeAgentProvider.AGENT_TYPE)) {
                // merge "composite-agent" with a Composable Agent

                Map<String, Object> configurationAgent2 = new HashMap<>();
                configurationAgent2.put("agentType", agent2.getAgentType());
                configurationAgent2.put("configuration", agent2.getConfiguration());

                Map<String, Object> newAgent1Configuration = new HashMap<>(agent1.getConfiguration());
                if (agent2.getComponentType() == ComponentType.FUNCTION) {
                    List<Map<String, Object>> processors = (List<Map<String, Object>>) newAgent1Configuration.get("processors");
                    processors.add(configurationAgent2);
                } else if (agent2.getComponentType() == ComponentType.SOURCE) {
                    Map<String, Object> currentSource = (Map<String, Object>) newAgent1Configuration.get("source");
                    if (!currentSource.isEmpty()) {
                        throw new IllegalStateException("Cannot merge two sources");
                    }
                    currentSource.putAll(configurationAgent2);
                } else if (agent2.getComponentType() == ComponentType.SINK) {
                    Map<String, Object> currentSink = (Map<String, Object>) newAgent1Configuration.get("sink");
                    if (!currentSink.isEmpty()) {
                        throw new IllegalStateException("Cannot merge two sinks");
                    }
                    currentSink.putAll(configurationAgent2);
                }

                log.info("Discarding topic {}", agent1.getOutputConnection());
                instance.discardTopic(agent1.getOutputConnection());
                agent1.overrideConfigurationAfterMerge(AbstractCompositeAgentProvider.AGENT_TYPE, newAgent1Configuration, agent2.getOutputConnection());
            } else {
                List<Map<String, Object>> processors = new ArrayList<>();
                Map<String, Object> source = new HashMap<>();
                Map<String, Object> sink = new HashMap<>();

                // merge two Composable Agents and build a "composite-agent"
                Map<String, Object> configurationAgent1 = new HashMap<>();
                configurationAgent1.put("agentType", agent1.getAgentType());
                configurationAgent1.put("configuration", agent1.getConfiguration());
                if (agent1.getComponentType() == ComponentType.SOURCE) {
                    source.putAll(configurationAgent1);
                } else if (agent1.getComponentType() == ComponentType.SINK) {
                    sink.putAll(configurationAgent1);
                } else if (agent1.getComponentType() == ComponentType.FUNCTION) {
                    processors.add(configurationAgent1);
                } else {
                    throw new IllegalStateException("Invalid agent type " + agent1.getComponentType());
                }

                Map<String, Object> configurationAgent2 = new HashMap<>();
                configurationAgent2.put("agentType", agent2.getAgentType());
                configurationAgent2.put("configuration", agent2.getConfiguration());
                if (agent2.getComponentType() == ComponentType.SOURCE) {
                    source.putAll(configurationAgent2);
                } else if (agent2.getComponentType() == ComponentType.SINK) {
                    sink.putAll(configurationAgent2);
                } else if (agent2.getComponentType() == ComponentType.FUNCTION) {
                    processors.add(configurationAgent2);
                } else {
                    throw new IllegalStateException("Invalid agent type " + agent2.getComponentType());
                }

                Map<String, Object> result = new HashMap<>();
                result.put("processors", processors);
                result.put("source", source);
                result.put("sink", sink);

                if (agent1.getOutputConnection() != null) {
                    log.info("Discarding topic {}", agent1.getOutputConnection());
                    instance.discardTopic(agent1.getOutputConnection());
                }
                agent1.overrideConfigurationAfterMerge(AbstractCompositeAgentProvider.AGENT_TYPE, result, agent2.getOutputConnection());
            }
            log.info("Agent 1 modified: {}", agent1);
            if (agent2.getInputConnection() != null) {
                log.info("Discarding topic {}", agent2.getInputConnection());
                instance.discardTopic(agent2.getInputConnection());
            }
            return previousAgent;
        }
        throw new IllegalStateException();
    }
}
