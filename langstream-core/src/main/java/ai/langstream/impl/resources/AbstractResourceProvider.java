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

import ai.langstream.api.doc.ResourceConfigurationModel;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractResourceProvider implements ResourceNodeProvider {

    private final Set<String> supportedTypes;

    public AbstractResourceProvider(Set<String> supportedTypes) {
        this.supportedTypes = supportedTypes;
    }

    protected Class getAgentConfigModelClass(String type) {
        return null;
    }

    protected boolean isAgentConfigModelAllowUnknownProperties(String type) {
        return false;
    }

    @Override
    public Map<String, Object> createImplementation(
            Resource resource,
            Module module,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        return computeResourceConfiguration(
                resource, module, executionPlan, clusterRuntime, pluginsRegistry);
    }

    protected Map<String, Object> computeResourceConfiguration(
            Resource resource,
            Module module,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        final String type = resource.type();
        final Class modelClass = getAgentConfigModelClass(type);
        if (modelClass != null) {
            ClassConfigValidator.validateResourceModelFromClass(
                    resource,
                    modelClass,
                    resource.configuration(),
                    isAgentConfigModelAllowUnknownProperties(type));
        }
        return new HashMap<>(resource.configuration());
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return supportedTypes.contains(type);
    }

    @Override
    public Map<String, ResourceConfigurationModel> generateSupportedTypesDocumentation() {
        Map<String, ResourceConfigurationModel> result = new LinkedHashMap<>();
        for (String supportedType : supportedTypes) {
            final Class modelClass = getAgentConfigModelClass(supportedType);
            if (modelClass == null) {
                result.put(supportedType, new ResourceConfigurationModel());
            } else {
                result.put(
                        supportedType,
                        ClassConfigValidator.generateResourceModelFromClass(modelClass));
            }
        }
        return result;
    }
}
