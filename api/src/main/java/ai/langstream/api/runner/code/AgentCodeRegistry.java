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
package ai.langstream.api.runner.code;

import java.util.Objects;
import java.util.ServiceLoader;

/**
 * The runtime registry is a singleton that holds all the runtime information about the
 * possible implementations of the SGA API.
 */
public class AgentCodeRegistry {

    public AgentCode getAgentCode(String agentType) {
        Objects.requireNonNull(agentType, "agentType cannot be null");
        ServiceLoader<AgentCodeProvider> loader = ServiceLoader.load(AgentCodeProvider.class);
        ServiceLoader.Provider<AgentCodeProvider> agentCodeProviderProvider = loader
                .stream()
                .filter(p -> p.get().supports(agentType))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No AgentCodeProvider found for type " + agentType));

        return agentCodeProviderProvider.get().createInstance(agentType);
    }

}
