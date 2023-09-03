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
package ai.langstream.runtime.agent.python;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeProvider;
import ai.langstream.api.runtime.ComponentType;
import java.util.Set;

public class PythonCodeAgentProvider implements AgentCodeProvider {
    private static final Set<String> AGENT_TYPES =
            Set.of("python-source", "python-sink", "python-processor", "python-function");

    @Override
    public boolean supports(String agentType) {
        return AGENT_TYPES.contains(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new PythonAgentPlaceHolder(agentType);
    }

    private static class PythonAgentPlaceHolder extends AbstractAgentCode implements AgentCode {
        private final String type;

        public PythonAgentPlaceHolder(String type) {
            this.type = type;
        }

        @Override
        public ComponentType componentType() {
            return switch (type) {
                case "python-source" -> ComponentType.SOURCE;
                case "python-sink" -> ComponentType.SINK;
                case "python-processor", "python-function" -> ComponentType.PROCESSOR;
                default -> throw new IllegalArgumentException("Unknown agent type: " + type);
            };
        }
    }

    public static boolean isPythonCodeAgent(AgentCode agentCode) {
        return agentCode instanceof PythonAgentPlaceHolder;
    }
}
