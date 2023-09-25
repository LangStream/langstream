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
package ai.langstream.ai.agents.services.impl;

import ai.langstream.ai.agents.services.ServiceProviderProvider;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.AbstractHuggingFaceEmbeddingService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.embeddings.HuggingFaceEmbeddingService;
import com.datastax.oss.streaming.ai.embeddings.HuggingFaceRestEmbeddingService;
import com.datastax.oss.streaming.ai.model.config.ComputeProvider;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HuggingFaceProvider implements ServiceProviderProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("huggingface");
    }

    @Override
    public ServiceProvider createImplementation(Map<String, Object> agentConfiguration) {
        return new HuggingFaceServiceProvider(
                (Map<String, Object>) agentConfiguration.get("huggingface"));
    }

    static class HuggingFaceServiceProvider implements ServiceProvider {
        private static final Logger log =
                LoggerFactory.getLogger(
                        com.datastax.oss.streaming.ai.services.HuggingFaceServiceProvider.class);
        private final Map<String, Object> providerConfiguration;

        public HuggingFaceServiceProvider(Map<String, Object> providerConfiguration) {
            this.providerConfiguration = providerConfiguration;
        }

        public HuggingFaceServiceProvider(TransformStepConfig tranformConfiguration) {
            this.providerConfiguration =
                    (Map)
                            (new ObjectMapper())
                                    .convertValue(
                                            tranformConfiguration.getHuggingface(), Map.class);
        }

        public CompletionsService getCompletionsService(
                Map<String, Object> additionalConfiguration) {
            String accessKey = (String) providerConfiguration.get("access-key");
            String url =
                    (String)
                            providerConfiguration.getOrDefault(
                                    "inference-url", "https://api-inference.huggingface.co");
            return new HuggingFaceCompletionsService(url, accessKey, additionalConfiguration);
        }

        public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration)
                throws Exception {
            String provider =
                    additionalConfiguration
                            .getOrDefault("provider", ComputeProvider.API.name())
                            .toString()
                            .toUpperCase();
            String modelUrl = (String) additionalConfiguration.get("modelUrl");
            String model = (String) additionalConfiguration.get("model");
            Map<String, String> options = (Map) additionalConfiguration.get("options");
            Map<String, String> arguments = (Map) additionalConfiguration.get("arguments");
            switch (provider) {
                case "LOCAL" -> {
                    AbstractHuggingFaceEmbeddingService.HuggingFaceConfig.HuggingFaceConfigBuilder
                            builder =
                                    AbstractHuggingFaceEmbeddingService.HuggingFaceConfig.builder()
                                            .options(options)
                                            .arguments(arguments);
                    if (model != null && !model.isEmpty()) {
                        builder.modelName(model);
                        if (modelUrl == null || modelUrl.isEmpty()) {
                            modelUrl = "djl://ai.djl.huggingface.pytorch" + model;
                            log.info("Automatically computed model URL {}", modelUrl);
                        }
                    }
                    builder.modelUrl(modelUrl);
                    return new HuggingFaceEmbeddingService(builder.build());
                }
                case "API" -> {
                    Objects.requireNonNull(model, "model name is required");
                    HuggingFaceRestEmbeddingService.HuggingFaceApiConfig.HuggingFaceApiConfigBuilder
                            apiBuilder =
                                    HuggingFaceRestEmbeddingService.HuggingFaceApiConfig.builder()
                                            .accessKey(
                                                    (String)
                                                            this.providerConfiguration.get(
                                                                    "access-key"))
                                            .model(model);
                    String apiUurl = (String) this.providerConfiguration.get("api-url");
                    if (apiUurl != null && !apiUurl.isEmpty()) {
                        apiBuilder.hfUrl(apiUurl);
                    }
                    String modelCheckUrl =
                            (String) this.providerConfiguration.get("model-check-url");
                    if (modelCheckUrl != null && !modelCheckUrl.isEmpty()) {
                        apiBuilder.hfCheckUrl(modelCheckUrl);
                    }
                    if (options != null && !options.isEmpty()) {
                        apiBuilder.options(options);
                    } else {
                        apiBuilder.options(Map.of("wait_for_model", "true"));
                    }
                    return new HuggingFaceRestEmbeddingService(apiBuilder.build());
                }
                default -> throw new IllegalArgumentException(
                        "Unsupported HuggingFace service type: " + provider);
            }
        }

        public void close() {}

        private static class HuggingFaceCompletionsService implements CompletionsService {

            final HttpClient httpClient;
            final String url;
            final String accessKey;

            public HuggingFaceCompletionsService(
                    String url, String accessKey, Map<String, Object> additionalConfiguration) {
                this.url = url;
                this.accessKey = accessKey;
                this.httpClient = HttpClient.newHttpClient();
            }

            @Override
            public CompletableFuture<String> getTextCompletions(
                    List<String> prompt,
                    StreamingChunksConsumer streamingChunksConsumer,
                    Map<String, Object> options) {
                throw new UnsupportedOperationException();
            }

            @Override
            @SneakyThrows
            public CompletableFuture<ChatCompletions> getChatCompletions(
                    List<ChatMessage> list,
                    StreamingChunksConsumer streamingChunksConsumer,
                    Map<String, Object> map) {

                String model = (String) map.get("model");
                // https://huggingface.co/docs/api-inference/quicktour
                String url = this.url + "/models/%s";
                String finalUrl = url.formatted(model);
                String request =
                        MAPPER.writeValueAsString(
                                list.stream()
                                        .map(ChatMessage::getContent)
                                        .collect(Collectors.toList()));
                log.info("URL: {}", finalUrl);
                log.info("Request: {}", request);
                CompletableFuture<HttpResponse<String>> responseHandle =
                        httpClient.sendAsync(
                                HttpRequest.newBuilder()
                                        .uri(URI.create(finalUrl))
                                        .header("Authorization", "Bearer " + accessKey)
                                        .header("Content-Type", "application/json")
                                        .POST(HttpRequest.BodyPublishers.ofString(request))
                                        .build(),
                                HttpResponse.BodyHandlers.ofString());
                return responseHandle.thenApply(
                        response -> {
                            ChatCompletions result = convertResponse(response);
                            return result;
                        });
            }

            @SneakyThrows
            private static ChatCompletions convertResponse(HttpResponse<String> response) {
                String body = response.body();
                if (log.isDebugEnabled()) {
                    log.debug("Response: {}", body);
                }
                List<ResponseBean> responseBeans = MAPPER.readValue(body, new TypeReference<>() {});
                ChatCompletions result = new ChatCompletions();
                result.setChoices(
                        responseBeans.stream()
                                .map(
                                        r ->
                                                new ChatChoice(
                                                        new ChatMessage("user")
                                                                .setContent(r.sequence)))
                                .collect(Collectors.toList()));
                return result;
            }
        }

        record ResponseBean(String score, String token_str, String sequence) {}
    }
}
