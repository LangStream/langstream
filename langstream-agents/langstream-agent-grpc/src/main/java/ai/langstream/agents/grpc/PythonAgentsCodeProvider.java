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
package ai.langstream.agents.grpc;

import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeProvider;
import java.util.Set;

public class PythonAgentsCodeProvider implements AgentCodeProvider {

    private static final Set<String> SUPPORTED_AGENT_TYPES =
            Set.of("python-source", "python-processor", "python-sink", "python-service");

    @Override
    public boolean supports(String agentType) {
        return SUPPORTED_AGENT_TYPES.contains(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return switch (agentType) {
            case "python-source" -> new PythonGrpcAgentSource();
            case "python-processor" -> new PythonGrpcAgentProcessor();
            case "python-sink" -> new PythonGrpcAgentSink();
            case "python-service" -> new PythonGrpcAgentService();
            default -> throw new IllegalStateException("Unexpected agent type: " + agentType);
        };
    }
}
