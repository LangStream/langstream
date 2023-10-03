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

import ai.langstream.api.doc.ResourceConfigurationModel;
import ai.langstream.api.model.Resource;
import java.util.Map;

public interface ResourceNodeProvider {

    /**
     * Create an Implementation of a Resource.
     *
     * @param pluginsRegistry the plugins registry
     * @return the implementation
     */
    Map<String, Object> createImplementation(Resource resource, PluginsRegistry pluginsRegistry);

    /**
     * Returns the ability of a Resource to be deployed on the give runtimes.
     *
     * @param type the type of implementation
     * @param clusterRuntime the compute cluster runtime
     * @return true if this provider can create the implementation
     */
    boolean supports(String type, ComputeClusterRuntime clusterRuntime);

    default Map<String, ResourceConfigurationModel> generateSupportedTypesDocumentation() {
        return Map.of();
    }
}
