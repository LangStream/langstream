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

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.common.AbstractAgentProvider;
import ai.langstream.impl.common.DefaultAgentNode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Implements support for processors that can be executed in memory into a single pipeline. This is
 * a special processor that executes a pipeline of Agents in memory. It is not expected to be
 * exposed to the user. It is created internally by the planner.
 */
@Slf4j
public abstract class AbstractCompositeAgentProvider extends AbstractAgentProvider {

    public static final String AGENT_TYPE = "composite-agent";

    private static final Set<String> SUPPORTED_AGENT_TYPES = Set.of(AGENT_TYPE);

    public AbstractCompositeAgentProvider(List<String> clusterRuntimes) {
        super(SUPPORTED_AGENT_TYPES, clusterRuntimes);
    }

    @Override
    protected final ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    @Override
    protected boolean isComposable(AgentConfiguration agentConfiguration) {
        return true;
    }

    public static Map<String, Object> getProcessorConfigurationAt(
            DefaultAgentNode composite, int index, String expectedType) {
        if (!Objects.equals(AGENT_TYPE, composite.getAgentType())) {
            throw new IllegalArgumentException("Not a composite agent: " + composite);
        }
        Map<String, Object> processorAgent =
                ((List<Map<String, Object>>) composite.getConfiguration().get("processors"))
                        .get(index);
        Objects.requireNonNull(processorAgent.get("agentId"));
        if (!expectedType.equals(processorAgent.get("agentType"))) {
            throw new IllegalArgumentException(
                    "Expected " + expectedType + " but got " + processorAgent.get("agentType"));
        }

        return (Map<String, Object>) processorAgent.get("configuration");
    }
}
