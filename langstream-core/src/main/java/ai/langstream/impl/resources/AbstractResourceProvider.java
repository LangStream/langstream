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
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
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

    protected Class getResourceConfigModelClass(String type) {
        return null;
    }

    protected boolean isResourceConfigModelAllowUnknownProperties(String type) {
        return false;
    }

    @Override
    public Map<String, Object> createImplementation(
            Resource resource, PluginsRegistry pluginsRegistry) {
        return computeResourceConfiguration(resource, pluginsRegistry);
    }

    protected Map<String, Object> computeResourceConfiguration(
            Resource resource, PluginsRegistry pluginsRegistry) {
        final String type = resource.type();
        final Class modelClass = getResourceConfigModelClass(type);
        if (modelClass != null) {
            ClassConfigValidator.validateResourceModelFromClass(
                    resource,
                    modelClass,
                    resource.configuration(),
                    isResourceConfigModelAllowUnknownProperties(type));
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
            final Class modelClass = getResourceConfigModelClass(supportedType);
            if (modelClass == null) {
                final ResourceConfigurationModel model = new ResourceConfigurationModel();
                model.setType(supportedType);
                result.put(supportedType, model);
            } else {
                final ResourceConfigurationModel value =
                        ClassConfigValidator.generateResourceModelFromClass(modelClass);
                value.setType(supportedType);
                result.put(supportedType, value);
            }
        }
        return result;
    }
}
