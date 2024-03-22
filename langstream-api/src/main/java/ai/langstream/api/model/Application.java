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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

@Data
public class Application {

    private Map<String, Resource> resources = new HashMap<>();
    private Map<String, Module> modules = new HashMap<>();
    private List<Dependency> dependencies = new ArrayList<>();
    private Gateways gateways;

    private Instance instance;
    private Secrets secrets;

    @JsonIgnore
    public Module getModule(String module) {
        return modules.computeIfAbsent(module, Module::new);
    }

    @JsonIgnore
    public TopicDefinition resolveTopic(String input) {
        for (Module module : modules.values()) {
            if (module.getTopics().containsKey(input)) {
                return module.resolveTopic(input);
            }
        }
        throw new IllegalArgumentException(
                "Topic " + input + " is not defined in any module of the application");
    }
}
