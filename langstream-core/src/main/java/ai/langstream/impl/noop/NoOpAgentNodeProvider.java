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
package ai.langstream.impl.noop;

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.impl.common.AbstractAgentProvider;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NoOpAgentNodeProvider extends AbstractAgentProvider {

    public NoOpAgentNodeProvider() {
        super(Set.of("noop"), List.of("none"));
    }

    @Override
    protected ComponentType getComponentType(AgentConfiguration agentConfiguration) {
        return ComponentType.PROCESSOR;
    }

    @Override
    public Map<String, AgentConfigurationModel> generateSupportedTypesDocumentation() {
        return Map.of();
    }
}
