/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.impl.agents;

import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.model.ResourcesSpec;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.ExecutionPlanOptimiser;
import ai.langstream.impl.common.DefaultAgentNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ComposableAgentExecutionPlanOptimiser implements ExecutionPlanOptimiser {

    @Override
    public boolean supports(String clusterType) {
        return "kubernetes".equals(clusterType) || "none".equals(clusterType);
    }

    @Override
    public boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation) {
        boolean result =
                (previousAgent instanceof DefaultAgentNode agent1
                        && agent1.isComposable()
                        && agentImplementation instanceof DefaultAgentNode agent2
                        && agent2.isComposable()
                        && agent1.getComponentType() != ComponentType.SERVICE
                        && agent2.getComponentType() != ComponentType.SERVICE
                        && Objects.equals(
                                "true",
                                agent1.getConfiguration().getOrDefault("composable", "true") + "")
                        && Objects.equals(
                                "true",
                                agent2.getConfiguration().getOrDefault("composable", "true") + "")
                        && compareResourcesNoDisk(
                                agent1.getResourcesSpec(), agent2.getResourcesSpec())
                        && Objects.equals(agent1.getErrorsSpec(), agent2.getErrorsSpec()));
        if (log.isDebugEnabled()) {
            log.debug("canMerge {}", previousAgent);
            log.debug("canMerge {}", agentImplementation);
            log.debug("canMerge RESULT: {}", result);
        }
        return result;
    }

    private static boolean compareResourcesNoDisk(ResourcesSpec a, ResourcesSpec b) {
        Integer parallismA = a != null ? a.parallelism() : null;
        Integer parallismB = b != null ? b.parallelism() : null;
        Integer sizeA = a != null ? a.size() : null;
        Integer sizeB = b != null ? b.size() : null;
        return Objects.equals(parallismA, parallismB) && Objects.equals(sizeA, sizeB);
    }

    @Override
    public AgentNode mergeAgents(
            Module module,
            Pipeline pipeline,
            AgentNode previousAgent,
            AgentNode agentImplementation,
            ExecutionPlan instance) {
        if (previousAgent instanceof DefaultAgentNode agent1
                && agentImplementation instanceof DefaultAgentNode agent2) {

            if (agent1.getAgentType().equals(AbstractCompositeAgentProvider.AGENT_TYPE)) {
                // merge "composite-agent" with a Composable Agent

                Map<String, Object> configurationAgent2 = new HashMap<>();
                configurationAgent2.put("agentType", agent2.getAgentType());
                configurationAgent2.put("configuration", agent2.getConfiguration());
                configurationAgent2.put("agentId", agent2.getId());

                Map<String, Object> newAgent1Configuration =
                        new HashMap<>(agent1.getConfiguration());
                if (agent2.getComponentType() == ComponentType.PROCESSOR) {
                    List<Map<String, Object>> processors =
                            (List<Map<String, Object>>) newAgent1Configuration.get("processors");
                    processors.add(configurationAgent2);
                } else if (agent2.getComponentType() == ComponentType.SOURCE) {
                    Map<String, Object> currentSource =
                            (Map<String, Object>) newAgent1Configuration.get("source");
                    if (!currentSource.isEmpty()) {
                        throw new IllegalStateException("Cannot merge two sources");
                    }
                    currentSource.putAll(configurationAgent2);
                } else if (agent2.getComponentType() == ComponentType.SINK) {
                    Map<String, Object> currentSink =
                            (Map<String, Object>) newAgent1Configuration.get("sink");
                    if (!currentSink.isEmpty()) {
                        throw new IllegalStateException("Cannot merge two sinks");
                    }
                    currentSink.putAll(configurationAgent2);
                }

                log.info("Discarding topic {}", agent1.getOutputConnectionImplementation());
                instance.discardTopic(agent1.getOutputConnectionImplementation());
                agent1.overrideConfigurationAfterMerge(
                        AbstractCompositeAgentProvider.AGENT_TYPE,
                        newAgent1Configuration,
                        agent2.getOutputConnectionImplementation(),
                        agent2.getDisks());
            } else {
                List<Map<String, Object>> processors = new ArrayList<>();
                Map<String, Object> source = new HashMap<>();
                Map<String, Object> sink = new HashMap<>();

                // merge two Composable Agents and build a "composite-agent"
                Map<String, Object> configurationAgent1 = new HashMap<>();
                configurationAgent1.put("agentType", agent1.getAgentType());
                configurationAgent1.put("configuration", agent1.getConfiguration());
                configurationAgent1.put("agentId", agent1.getId());
                if (agent1.getComponentType() == ComponentType.SOURCE) {
                    source.putAll(configurationAgent1);
                } else if (agent1.getComponentType() == ComponentType.SINK) {
                    sink.putAll(configurationAgent1);
                } else if (agent1.getComponentType() == ComponentType.PROCESSOR) {
                    processors.add(configurationAgent1);
                } else {
                    throw new IllegalStateException(
                            "Invalid agent type " + agent1.getComponentType());
                }

                Map<String, Object> configurationAgent2 = new HashMap<>();
                configurationAgent2.put("agentType", agent2.getAgentType());
                configurationAgent2.put("configuration", agent2.getConfiguration());
                configurationAgent2.put("agentId", agent2.getId());
                if (agent2.getComponentType() == ComponentType.SOURCE) {
                    source.putAll(configurationAgent2);
                } else if (agent2.getComponentType() == ComponentType.SINK) {
                    sink.putAll(configurationAgent2);
                } else if (agent2.getComponentType() == ComponentType.PROCESSOR) {
                    processors.add(configurationAgent2);
                } else {
                    throw new IllegalStateException(
                            "Invalid agent type " + agent2.getComponentType());
                }

                Map<String, Object> result = new HashMap<>();
                result.put("processors", processors);
                result.put("source", source);
                result.put("sink", sink);

                if (agent1.getOutputConnectionImplementation() != null) {
                    log.info("Discarding topic {}", agent1.getOutputConnectionImplementation());
                    instance.discardTopic(agent1.getOutputConnectionImplementation());
                }
                agent1.overrideConfigurationAfterMerge(
                        AbstractCompositeAgentProvider.AGENT_TYPE,
                        result,
                        agent2.getOutputConnectionImplementation(),
                        agent2.getDisks());
            }
            log.info("Agent 1 modified: {}", agent1);
            if (agent2.getInputConnectionImplementation() != null) {
                log.info("Discarding topic {}", agent2.getInputConnectionImplementation());
                instance.discardTopic(agent2.getInputConnectionImplementation());
            }
            return previousAgent;
        }
        throw new IllegalStateException();
    }
}
