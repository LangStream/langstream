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
import ai.langstream.api.util.ConfigurationUtils;
import java.util.Map;

public class VectorDatabaseResourceProvider implements ResourceNodeProvider {
    @Override
    public Map<String, Object> createImplementation(
            Resource resource,
            Module module,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        Map<String, Object> configuration = resource.configuration();
        String service = ConfigurationUtils.getString("service", "", configuration);
        if (service.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing required field 'service' in a vector-database resource definition");
        }
        return resource.configuration();
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return "vector-database".equals(type);
    }
}
