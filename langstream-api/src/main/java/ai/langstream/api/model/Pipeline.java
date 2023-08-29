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
package ai.langstream.api.model;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Pipeline {
    private String id;
    private String module;
    private String name;
    // defaults for all the agents in the pipeline
    private ResourcesSpec resources;
    // defaults for all the agents in the pipeline
    private ErrorsSpec errors;

    public Pipeline(String id, String module) {
        this.id = id;
        this.module = module;
    }

    private List<AgentConfiguration> agents = new ArrayList<>();

    public void addAgentConfiguration(AgentConfiguration a) {
        agents.add(a);
    }

    public AgentConfiguration getAgent(String definition) {
        return agents.stream()
                .filter(a -> a.getId().equals(definition))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Agent not found: " + definition));
    }
}
