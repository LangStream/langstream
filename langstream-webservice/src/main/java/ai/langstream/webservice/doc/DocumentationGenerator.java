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
import ai.langstream.api.doc.AssetConfigurationModel;
import ai.langstream.api.doc.ResourceConfigurationModel;
import ai.langstream.api.runtime.AgentNodeProvider;
import ai.langstream.api.runtime.AssetNodeProvider;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DocumentationGenerator {

    public static ApiConfigurationModel generateDocs(String version) {
        Map<String, AgentConfigurationModel> agents = new TreeMap<>();
        Map<String, ResourceConfigurationModel> resources = new TreeMap<>();
        Map<String, AssetConfigurationModel> assets = new TreeMap<>();
        final PluginsRegistry registry = new PluginsRegistry();
        final List<AgentNodeProvider> nodes = registry.lookupAvailableAgentImplementations();
        for (AgentNodeProvider node : nodes) {
            agents.putAll(node.generateSupportedTypesDocumentation());
        }

        final List<ResourceNodeProvider> resourceNodeProviders =
                registry.lookupAvailableResourceImplementations();
        for (ResourceNodeProvider resourceNodeProvider : resourceNodeProviders) {
            resources.putAll(resourceNodeProvider.generateSupportedTypesDocumentation());
        }

        final List<AssetNodeProvider> assetNodeProviders =
                registry.lookupAvailableAssetImplementations();
        for (AssetNodeProvider assetProvider : assetNodeProviders) {
            assets.putAll(assetProvider.generateSupportedTypesDocumentation());
        }
        return new ApiConfigurationModel(version, agents, resources, assets);
    }
}
