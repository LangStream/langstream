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
package ai.langstream.ai.agents;

import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeProvider;

import java.util.Set;

public class GenAIAgentCodeProvider implements AgentCodeProvider {

    private static final Set<String> STEP_TYPES = Set.of(
            "drop-fields",
            "merge-key-value",
            "unwrap-key-value",
            "cast",
            "flatten",
            "drop",
            "compute",
            "compute-ai-embeddings",
            "query",
            "ai-chat-completions",
            "ai-tools" // legacy
    );

    @Override
    public boolean supports(String agentType) {
        return STEP_TYPES.contains(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        return new GenAIToolKitAgent();
    }
}
