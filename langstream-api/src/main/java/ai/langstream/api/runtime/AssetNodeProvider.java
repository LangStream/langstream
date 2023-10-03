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
package ai.langstream.api.runtime;

import ai.langstream.api.doc.AssetConfigurationModel;
import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.model.Module;
import java.util.Map;

public interface AssetNodeProvider {

    /**
     * Create an Implementation of an Asset that can be deployed on the give runtimes. This is also
     * useful to perform validation of the configuration.
     *
     * @param module the module
     * @param executionPlan the physical application instance
     * @param clusterRuntime the cluster runtime
     * @param pluginsRegistry the plugins registry
     * @return the Agent
     */
    AssetNode createImplementation(
            AssetDefinition assetDefinition,
            Module module,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry);

    /**
     * Returns the ability of an Asset to be deployed on the give runtimes.
     *
     * @param type the type of implementation
     * @param clusterRuntime the compute cluster runtime
     * @return true if this provider can create the implementation
     */
    boolean supports(String type, ComputeClusterRuntime clusterRuntime);

    default Map<String, AssetConfigurationModel> generateSupportedTypesDocumentation() {
        return Map.of();
    }
}
