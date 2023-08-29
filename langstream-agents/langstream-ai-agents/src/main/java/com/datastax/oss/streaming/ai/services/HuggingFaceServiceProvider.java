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
package com.datastax.oss.streaming.ai.services;

import static com.datastax.oss.streaming.ai.embeddings.AbstractHuggingFaceEmbeddingService.DLJ_BASE_URL;

import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.AbstractHuggingFaceEmbeddingService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.embeddings.HuggingFaceEmbeddingService;
import com.datastax.oss.streaming.ai.embeddings.HuggingFaceRestEmbeddingService;
import com.datastax.oss.streaming.ai.model.config.ComputeProvider;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HuggingFaceServiceProvider implements ServiceProvider {

    private final Map<String, Object> providerConfiguration;

    public HuggingFaceServiceProvider(Map<String, Object> providerConfiguration) {
        this.providerConfiguration = providerConfiguration;
    }

    public HuggingFaceServiceProvider(TransformStepConfig tranformConfiguration) {
        this.providerConfiguration =
                new ObjectMapper().convertValue(tranformConfiguration.getHuggingface(), Map.class);
    }

    @Override
    public CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration) {
        throw new IllegalArgumentException("Completions is still not available for HuggingFace");
    }

    @Override
    public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration)
            throws Exception {
        String provider =
                additionalConfiguration
                        .getOrDefault("provider", ComputeProvider.API.name())
                        .toString()
                        .toUpperCase();
        String modelUrl = (String) additionalConfiguration.get("modelUrl");
        String model = (String) additionalConfiguration.get("model");
        Map<String, String> options = (Map<String, String>) additionalConfiguration.get("options");
        Map<String, String> arguments =
                (Map<String, String>) additionalConfiguration.get("arguments");
        switch (provider) {
            case "LOCAL":
                AbstractHuggingFaceEmbeddingService.HuggingFaceConfig.HuggingFaceConfigBuilder
                        builder =
                                AbstractHuggingFaceEmbeddingService.HuggingFaceConfig.builder()
                                        .options(options)
                                        .arguments(arguments);
                if (model != null && !model.isEmpty()) {
                    builder.modelName(model);

                    // automatically build the model URL if not provided
                    if (modelUrl == null || modelUrl.isEmpty()) {
                        modelUrl = DLJ_BASE_URL + model;
                        log.info("Automatically computed model URL {}", modelUrl);
                    }
                }
                builder.modelUrl(modelUrl);

                return new HuggingFaceEmbeddingService(builder.build());
            case "API":
                Objects.requireNonNull(model, "model name is required");
                HuggingFaceRestEmbeddingService.HuggingFaceApiConfig.HuggingFaceApiConfigBuilder
                        apiBuilder =
                                HuggingFaceRestEmbeddingService.HuggingFaceApiConfig.builder()
                                        .accessKey((String) providerConfiguration.get("access-key"))
                                        .model(model);

                String apiUurl = (String) providerConfiguration.get("api-url");
                if (apiUurl != null && !apiUurl.isEmpty()) {
                    apiBuilder.hfUrl(apiUurl);
                }
                String modelCheckUrl = (String) providerConfiguration.get("model-check-url");
                if (modelCheckUrl != null && !modelCheckUrl.isEmpty()) {
                    apiBuilder.hfCheckUrl(modelCheckUrl);
                }
                if (options != null && !options.isEmpty()) {
                    apiBuilder.options(options);
                } else {
                    apiBuilder.options(Map.of("wait_for_model", "true"));
                }

                return new HuggingFaceRestEmbeddingService(apiBuilder.build());
            default:
                throw new IllegalArgumentException(
                        "Unsupported HuggingFace service type: " + provider);
        }
    }

    @Override
    public void close() {}
}
