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
package ai.langstream.impl.resources;

import ai.langstream.api.model.Module;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import java.util.Map;
import java.util.Set;

public class AIProvidersResourceProvider implements ResourceNodeProvider {

    private static final Set<String> SUPPORTED_TYPES =
            Set.of("open-ai-configuration", "hugging-face-configuration", "vertex-configuration");

    @Override
    public Map<String, Object> createImplementation(
            Resource resource,
            Module module,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        Map<String, Object> configuration = resource.configuration();
        return resource.configuration();
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return SUPPORTED_TYPES.contains(type);
    }
}
