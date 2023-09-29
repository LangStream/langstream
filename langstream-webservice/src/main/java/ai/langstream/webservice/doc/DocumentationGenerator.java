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
package ai.langstream.webservice.doc;

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.doc.ApiConfigurationModel;
import ai.langstream.api.runtime.AgentNodeProvider;
import ai.langstream.api.runtime.PluginsRegistry;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentationGenerator {

    public static ApiConfigurationModel generateDocs(String version) {
        Map<String, AgentConfigurationModel> agents = new TreeMap<>();
        final List<AgentNodeProvider> nodes =
                new PluginsRegistry().lookupAvailableAgentImplementations(null);
        for (AgentNodeProvider node : nodes) {
            agents.putAll(node.generateSupportedTypesDocumentation());
        }
        return new ApiConfigurationModel(version, agents);
    }
}
