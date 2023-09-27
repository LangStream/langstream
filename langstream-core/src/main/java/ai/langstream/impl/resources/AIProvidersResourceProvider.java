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

import static ai.langstream.api.util.ConfigurationUtils.getMap;
import static ai.langstream.api.util.ConfigurationUtils.getString;
import static ai.langstream.api.util.ConfigurationUtils.requiredField;
import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;
import static ai.langstream.api.util.ConfigurationUtils.validateEnumField;

import ai.langstream.api.model.Module;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class AIProvidersResourceProvider implements ResourceNodeProvider {

    private static final Set<String> SUPPORTED_TYPES =
            Set.of("open-ai-configuration", "hugging-face-configuration", "vertex-configuration");
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Map<String, Object> createImplementation(
            Resource resource,
            Module module,
            ExecutionPlan executionPlan,
            ComputeClusterRuntime clusterRuntime,
            PluginsRegistry pluginsRegistry) {
        switch (resource.type()) {
            case "open-ai-configuration" -> {
                validateOpenAIConfigurationResource(resource);
            }
            case "hugging-face-configuration" -> {
                validateHuggingFaceConfigurationResource(resource);
            }
            case "vertex-configuration" -> {
                validateVertexConfigurationResource(resource);
            }
            default -> throw new IllegalStateException();
        }
        return resource.configuration();
    }

    private void validateVertexConfigurationResource(Resource resource) {
        Map<String, Object> configuration = resource.configuration();
        requiredNonEmptyField(configuration, "url", describe(resource));
        requiredNonEmptyField(configuration, "region", describe(resource));
        requiredNonEmptyField(configuration, "project", describe(resource));

        String token = getString("token", "", configuration);
        String serviceAccountJson = getString("serviceAccountJson", "", configuration);
        if (!token.isEmpty() && !serviceAccountJson.isEmpty()) {
            throw new IllegalArgumentException(
                    "Only one of token and serviceAccountJson should be provided in "
                            + describe(resource).get());
        }
        if (token.isEmpty()) {
            requiredNonEmptyField(configuration, "serviceAccountJson", describe(resource));
        }
        if (!serviceAccountJson.isEmpty()) {
            try {
                MAPPER.readValue(serviceAccountJson, Map.class);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Invalid JSON for field serviceAccountJson in " + describe(resource).get(),
                        e);
            }
        }
    }

    private void validateHuggingFaceConfigurationResource(Resource resource) {
        Map<String, Object> configuration = resource.configuration();
        validateEnumField(configuration, "provider", Set.of("local", "api"), describe(resource));
        requiredField(configuration, "model", describe(resource));
        getMap("options", Map.of(), configuration);
        getMap("arguments", Map.of(), configuration);
    }

    private void validateOpenAIConfigurationResource(Resource resource) {
        Map<String, Object> configuration = resource.configuration();
        validateEnumField(configuration, "provider", Set.of("azure", "openai"), describe(resource));
        String provider = getString("provider", "openai", configuration);
        if (provider.equals("azure")) {
            requiredField(configuration, "url", describe(resource));
        }
        requiredField(configuration, "access-key", describe(resource));
    }

    @Override
    public boolean supports(String type, ComputeClusterRuntime clusterRuntime) {
        return SUPPORTED_TYPES.contains(type);
    }

    protected static Supplier<String> describe(Resource resource) {
        return () -> "resource with id = " + resource.id() + " of type " + resource.type();
    }
}
