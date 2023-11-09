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
import static ai.langstream.api.util.ConfigurationUtils.requiredField;
import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;

import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.PluginsRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.Data;

public class AIProvidersResourceProvider extends AbstractResourceProvider {

    protected static final String OPEN_AI_CONFIGURATION = "open-ai-configuration";
    protected static final String HUGGING_FACE_CONFIGURATION = "hugging-face-configuration";
    protected static final String VERTEX_CONFIGURATION = "vertex-configuration";
    protected static final String BEDROCK_CONFIGURATION = "bedrock-configuration";

    protected static final String OLLAMA_CONFIGURATION = "ollama-configuration";
    private static final Set<String> SUPPORTED_TYPES =
            Set.of(
                    OPEN_AI_CONFIGURATION,
                    HUGGING_FACE_CONFIGURATION,
                    VERTEX_CONFIGURATION,
                    BEDROCK_CONFIGURATION,
                    OLLAMA_CONFIGURATION);
    protected static final ObjectMapper MAPPER = new ObjectMapper();

    public AIProvidersResourceProvider() {
        super(SUPPORTED_TYPES);
    }

    @Override
    protected Map<String, Object> computeResourceConfiguration(
            Resource resource, PluginsRegistry pluginsRegistry) {
        final Map<String, Object> copy =
                super.computeResourceConfiguration(resource, pluginsRegistry);
        // only dynamic checks, the rest is done in AbstractResourceProvider
        if (resource.type().equals(VERTEX_CONFIGURATION)) {
            validateVertexConfigurationResource(resource);
        } else if (resource.type().equals(OPEN_AI_CONFIGURATION)) {
            String provider = getString("provider", "openai", resource.configuration());
            if (provider.equals("azure")) {
                requiredField(resource.configuration(), "url", describe(resource));
            }
        }
        return copy;
    }

    private void validateVertexConfigurationResource(Resource resource) {
        Map<String, Object> configuration = resource.configuration();
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

    protected static Supplier<String> describe(Resource resource) {
        return () -> "resource with id = " + resource.id() + " of type " + resource.type();
    }

    @Override
    protected Class getResourceConfigModelClass(String type) {
        switch (type) {
            case OPEN_AI_CONFIGURATION -> {
                return OpenAIConfig.class;
            }
            case HUGGING_FACE_CONFIGURATION -> {
                return HuggingFaceConfig.class;
            }
            case VERTEX_CONFIGURATION -> {
                return VertexAIConfig.class;
            }
            case BEDROCK_CONFIGURATION -> {
                return BedrockConfig.class;
            }
            case OLLAMA_CONFIGURATION -> {
                return OllamaConfig.class;
            }
            default -> throw new IllegalStateException();
        }
    }

    @Data
    @ResourceConfig(name = "Open AI", description = "Connect to OpenAI API or Azure OpenAI API.")
    public static class OpenAIConfig {

        public enum Provider {
            openai,
            azure
        }

        @ConfigProperty(
                description =
                        """
                            The provider to use. Either "openai" or "azure".
                        """,
                defaultValue = "openai")
        private Provider provider;

        @ConfigProperty(
                description =
                        """
                            The access key to use.
                        """,
                required = true)
        @JsonProperty("access-key")
        private String accessKey;

        @ConfigProperty(
                description =
                        """
                            Url for Azure OpenAI API. Required only if provider is "azure".
                        """)
        private String url;
    }

    @Data
    @ResourceConfig(name = "Vertex AI", description = "Connect to VertexAI API.")
    public static class VertexAIConfig {

        @ConfigProperty(
                description =
                        """
                        URL connection for the Vertex API.
                        """,
                required = true)
        private String url;

        @ConfigProperty(
                description =
                        """
                        GCP region for the Vertex API.
                        """,
                required = true)
        private String region;

        @ConfigProperty(
                description =
                        """
                        GCP project name for the Vertex API.
                        """,
                required = true)
        private String project;

        @ConfigProperty(
                description =
                        """
                        Access key for the Vertex API.
                        """)
        private String token;

        @ConfigProperty(
                description =
                        """
                        Specify service account credentials. Refer to the GCP documentation on how to download it
                        """)
        private String serviceAccountJson;
    }

    @Data
    @ResourceConfig(name = "Hugging Face", description = "Connect to Hugging Face service.")
    public static class HuggingFaceConfig {

        public enum Provider {
            local,
            api
        }

        @ConfigProperty(
                description =
                        """
                            The provider to use. Either "local" or "api".
                        """,
                defaultValue = "api")
        private Provider provider;

        @JsonProperty("api-url")
        @ConfigProperty(
                description =
                        """
                        The URL of the Hugging Face API. Relevant only if provider is "api".
                        """,
                defaultValue = "https://api-inference.huggingface.co/pipeline/feature-extraction/")
        private String apiUrl;

        @ConfigProperty(
                description =
                        """
                        The model url to use. Relevant only if provider is "api".
                        """,
                defaultValue = "https://huggingface.co/api/models/")
        @JsonProperty("model-check-url")
        private String modelUrl;

        @ConfigProperty(
                description =
                        """
                            The access key to use for "api" provider.
                        """)
        @JsonProperty("access-key")
        private String accessKey;
    }

    @Data
    @ResourceConfig(name = "Ollama", description = "Connect to Ollama API.")
    public static class OllamaConfig {

        @ConfigProperty(
                description =
                        """
                            URL for the Ollama service.
                            """,
                required = true)
        @JsonProperty("url")
        private String url;
    }

    @Data
    @ResourceConfig(name = "AWS Bedrock", description = "Connect to AWS Bedrock API.")
    public static class BedrockConfig {

        @ConfigProperty(
                description =
                        """
                        Aws access key.
                        """,
                required = true)
        @JsonProperty("access-key")
        private String accessKey;

        @ConfigProperty(
                description =
                        """
                        Aws secret key.
                        """,
                required = true)
        @JsonProperty("secret-key")
        private String secretKey;

        @ConfigProperty(
                description =
                        """
                        AWS region for Bedrock endpoints.
                        """,
                defaultValue = "us-east-1")
        @JsonProperty("region")
        private String region;

        @ConfigProperty(
                description =
                        """
                        Override default AWS endpoint. Useful for testing and debugging. If using AWS, please only set the region.
                        """)
        @JsonProperty("endpoint-override")
        private String endpointOverride;
    }
}
