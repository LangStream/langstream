/**
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
package ai.langstream.impl.agents.ai;

import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.ExecutionPlanOptimiser;
import ai.langstream.impl.common.DefaultAgentNode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public final class GenAIToolKitExecutionPlanOptimizer implements ExecutionPlanOptimiser {

    @Override
    public boolean supports(String clusterType) {
        return "pulsar".equals(clusterType);
    }


    @Override
    public boolean canMerge(AgentNode previousAgent, AgentNode agentImplementation) {
        if (Objects.equals(previousAgent.getAgentType(), GenAIToolKitFunctionAgentProvider.AGENT_TYPE)
                && Objects.equals(previousAgent.getAgentType(), agentImplementation.getAgentType())
                && previousAgent instanceof DefaultAgentNode agent1
                && agentImplementation instanceof DefaultAgentNode agent2) {
            Map<String, Object> purgedConfiguration1 = new HashMap<>(agent1.getConfiguration());
            purgedConfiguration1.remove("steps");
            Map<String, Object> purgedConfiguration2 = new HashMap<>(agent2.getConfiguration());
            purgedConfiguration2.remove("steps");

            // test query steps with different datasources
            Object datasource1 = purgedConfiguration1.remove("datasource");
            Object datasource2 = purgedConfiguration2.remove("datasource");

            if (datasource1 != null && datasource2 != null) {
                log.info("Comparing datasources {} and {}", datasource1, datasource2);
                if (!Objects.equals(datasource1, datasource2)) {
                    log.info("Agents {} and {} cannot be merged (different datasources)",
                            previousAgent, agentImplementation);
                    return false;
                }
            }

            log.info("Comparing {} and {}", purgedConfiguration1, purgedConfiguration2);
            return purgedConfiguration1.equals(purgedConfiguration2);
        }
        log.info("Agents {} and {} cannot be merged", previousAgent, agentImplementation);
        return false;
    }

    @Override
    public AgentNode mergeAgents(Module module, Pipeline pipeline, AgentNode previousAgent, AgentNode agentImplementation,
                                 ExecutionPlan applicationInstance) {
        if (Objects.equals(previousAgent.getAgentType(), agentImplementation.getAgentType())
                && previousAgent instanceof DefaultAgentNode agent1
                && agentImplementation instanceof DefaultAgentNode agent2) {

            log.info("Merging agents");
            log.info("Agent 1: {}", agent1);
            log.info("Agent 2: {}", agent2);

            Map<String, Object> configurationWithoutSteps1 = new HashMap<>(agent1.getConfiguration());
            List<Map<String, Object>> steps1 = (List<Map<String, Object>>) configurationWithoutSteps1.remove("steps");
            Map<String, Object> configurationWithoutSteps2 = new HashMap<>(agent2.getConfiguration());
            List<Map<String, Object>> steps2 = (List<Map<String, Object>>) configurationWithoutSteps2.remove("steps");

            List<Map<String, Object>> mergedSteps = new ArrayList<>();
            mergedSteps.addAll(steps1);
            mergedSteps.addAll(steps2);

            Map<String, Object> result = new HashMap<>();
            result.putAll(configurationWithoutSteps1);
            result.putAll(configurationWithoutSteps2);
            result.put("steps", mergedSteps);

            log.info("Discarding topic {}", agent1.getInputConnectionImplementation());
            applicationInstance.discardTopic(agent1.getOutputConnectionImplementation());

            agent1.overrideConfigurationAfterMerge(agent1.getAgentType(), result, agent2.getOutputConnectionImplementation());

            log.info("Discarding topic {}", agent2.getInputConnectionImplementation());
            applicationInstance.discardTopic(agent2.getInputConnectionImplementation());
            return previousAgent;
        }
        throw new IllegalStateException();
    }
}
