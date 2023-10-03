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

import static ai.langstream.api.util.ConfigurationUtils.getString;

import ai.langstream.api.doc.ConfigPropertyModel;
import ai.langstream.api.doc.ResourceConfigurationModel;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BaseDataSourceResourceProvider implements ResourceNodeProvider {

    private final String resourceType;
    private final Map<String, DatasourceConfig> supportedServices;

    public interface DatasourceConfig {
        void validate(Resource resource);

        Class getResourceConfigModelClass();
    }

    @Override
    public Map<String, Object> createImplementation(
            Resource resource, PluginsRegistry pluginsRegistry) {
        Map<String, Object> configuration = resource.configuration();

        final String service = getString("service", null, configuration);
        if (service == null) {
            throw new IllegalArgumentException(
                    ClassConfigValidator.formatErrString(
                            new ClassConfigValidator.ResourceEntityRef(resource),
                            "service",
                            "service must be set to one of: " + supportedServices.keySet()));
        }
        if (!supportedServices.keySet().contains(service)) {
            throw new IllegalArgumentException(
                    ClassConfigValidator.formatErrString(
                            new ClassConfigValidator.ResourceEntityRef(resource),
                            "service",
                            "service must be set to one of: " + supportedServices.keySet()));
        }
        supportedServices.get(service).validate(resource);
        return resource.configuration();
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return resourceType.equals(type);
    }

    @Override
    public Map<String, ResourceConfigurationModel> generateSupportedTypesDocumentation() {
        Map<String, ResourceConfigurationModel> result = new LinkedHashMap<>();
        for (Map.Entry<String, DatasourceConfig> datasource : supportedServices.entrySet()) {
            final String service = datasource.getKey();
            final ResourceConfigurationModel value =
                    ClassConfigValidator.generateResourceModelFromClass(
                            datasource.getValue().getResourceConfigModelClass());
            value.getProperties()
                    .put(
                            "service",
                            ConfigPropertyModel.builder()
                                    .type("string")
                                    .required(true)
                                    .description("Service type. Set to '" + service + "'")
                                    .build());

            value.setType(resourceType);
            result.put(resourceType + "_" + service, value);
        }
        return result;
    }
}
