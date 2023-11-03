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
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.util.ConfigurationUtils;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.completions.TextCompletionResult;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.CredentialRefreshListener;
import com.google.api.client.auth.oauth2.TokenErrorResponse;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VertexAIProvider implements ServiceProviderProvider {

    private static final String VERTEX_URL_TEMPLATE =
            "%s/v1/projects/%s/locations/%s/publishers/google/models/%s:predict";

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("vertex");
    }

    @Override
    public ServiceProvider createImplementation(
            Map<String, Object> agentConfiguration, MetricsReporter metricsReporter) {

        Map<String, Object> config = (Map<String, Object>) agentConfiguration.get("vertex");
        String token = (String) config.get("token");
        String serviceAccountJson = (String) config.get("serviceAccountJson");
        String url = (String) config.get("url");
        String project = (String) config.get("project");
        String region = (String) config.get("region");

        return new VertexAIServiceProvider(url, project, region, token, serviceAccountJson);
    }

    private static class VertexAIServiceProvider implements ServiceProvider {

        final HttpClient httpClient;
        private final String url;
        private final String project;
        private final String region;

        private final String token;

        private final GoogleCredential googleCredential;
        private final ScheduledExecutorService refreshTokenExecutor;

        @SneakyThrows
        public VertexAIServiceProvider(
                String url,
                String project,
                String region,
                String token,
                String serviceAccountJson) {
            if (url == null || url.isEmpty()) {
                url = "https://" + region + "-aiplatform.googleapis.com";
            }
            this.url = url;

            this.project = project;
            this.region = region;

            if (token != null && !token.trim().isEmpty()) {
                log.info("Using static Access Token to connect to Vertex AI");
                this.token = token.trim();
                this.googleCredential = null;
                this.refreshTokenExecutor = null;
            } else if (serviceAccountJson != null && !serviceAccountJson.trim().isEmpty()) {
                log.info("Getting a token using OAuth2 from a Google Service Account");
                this.token = null;
                this.refreshTokenExecutor = Executors.newSingleThreadScheduledExecutor();
                this.googleCredential =
                        GoogleCredential.fromStream(
                                        new ByteArrayInputStream(
                                                serviceAccountJson.getBytes(
                                                        StandardCharsets.UTF_8)))
                                .createScoped(
                                        Set.of("https://www.googleapis.com/auth/cloud-platform"))
                                .toBuilder()
                                .addRefreshListener(
                                        new CredentialRefreshListener() {
                                            @Override
                                            public void onTokenResponse(
                                                    Credential credential,
                                                    TokenResponse tokenResponse)
                                                    throws IOException {
                                                log.error("Token refreshed {}", tokenResponse);
                                                Long expire = tokenResponse.getExpiresInSeconds();
                                                if (expire != null) {
                                                    long refresh = expire - 120;
                                                    log.info(
                                                            "Token will expire in {} seconds, scheduling refresh in {} seconds",
                                                            expire,
                                                            refresh);
                                                    scheduleRefreshToken(refresh);
                                                }
                                            }

                                            @Override
                                            public void onTokenErrorResponse(
                                                    Credential credential,
                                                    TokenErrorResponse tokenErrorResponse)
                                                    throws IOException {
                                                log.error(
                                                        "Error while refreshing token. {}",
                                                        tokenErrorResponse);
                                            }
                                        })
                                .build();

                // get the initial token
                // an error here fails the pod
                this.googleCredential.refreshToken();
            } else {
                throw new IllegalArgumentException(
                        "You have to pass the access token or the service account json file");
            }

            this.httpClient = HttpClient.newHttpClient();
        }

        private void scheduleRefreshToken(long refresh) {
            refreshTokenExecutor.schedule(
                    () -> {
                        doRefreshToken();
                    },
                    refresh,
                    java.util.concurrent.TimeUnit.SECONDS);
        }

        private void doRefreshToken() {
            try {
                log.info("Refreshing token");
                googleCredential.refreshToken();
            } catch (Throwable error) {
                log.error("Error while refreshing token", error);
                // schedule again in 60 seconds
                scheduleRefreshToken(60);
            }
        }

        protected String getCurrentToken() {
            if (token != null) {
                return token;
            }
            return googleCredential.getAccessToken();
        }

        @Override
        public CompletionsService getCompletionsService(Map<String, Object> map) throws Exception {
            String model = (String) map.get("model");
            if (model == null) {
                throw new IllegalArgumentException("'model' is required for completions service");
            }
            return new VertexAICompletionsService(model);
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> map) throws Exception {
            String model = (String) map.get("model");
            if (model == null) {
                throw new IllegalArgumentException("'model' is required for embeddings service");
            }
            return new VertexAIEmbeddingsService(model);
        }

        private <R, T> CompletableFuture<T> executeVertexCall(
                R requestEmbeddings, Class<T> responseType, String model) {
            String finalUrl = VERTEX_URL_TEMPLATE.formatted(url, project, region, model);
            String request;
            try {
                request = MAPPER.writeValueAsString(requestEmbeddings);
            } catch (JsonProcessingException e) {
                return CompletableFuture.failedFuture(e);
            }
            log.info("URL: {}", finalUrl);
            log.info("Request: {}", request);

            CompletableFuture<HttpResponse<String>> responseHandle =
                    httpClient.sendAsync(
                            HttpRequest.newBuilder()
                                    .uri(URI.create(finalUrl))
                                    .header("Authorization", "Bearer " + getCurrentToken())
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString(request))
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());

            return responseHandle.thenApply(
                    response -> {
                        return handleResponse(responseType, response);
                    });
        }

        @Override
        public void close() {
            if (refreshTokenExecutor != null) {
                refreshTokenExecutor.shutdownNow();
            }
        }

        private class VertexAICompletionsService implements CompletionsService {
            private final String model;

            public VertexAICompletionsService(String model) {
                this.model = model;
            }

            @Override
            public CompletableFuture<ChatCompletions> getChatCompletions(
                    List<ChatMessage> list,
                    StreamingChunksConsumer streamingChunksConsumer,
                    Map<String, Object> additionalConfiguration) {
                // https://cloud.google.com/vertex-ai/docs/generative-ai/chat/chat-prompts
                CompletionRequest.ChatInstance instance = new CompletionRequest.ChatInstance();
                instance.context = "";
                instance.examples = new ArrayList<>();
                instance.messages =
                        list.stream()
                                .map(
                                        m -> {
                                            CompletionRequest.ChatMessage message =
                                                    new CompletionRequest.ChatMessage();
                                            message.content = m.getContent();
                                            message.author = m.getRole();
                                            return message;
                                        })
                                .collect(Collectors.toList());
                CompletionRequest request = new CompletionRequest();
                request.instances.add(instance);
                appendRequestParameters(additionalConfiguration, request);

                CompletableFuture<ChatPredictions> predictionsResult =
                        executeVertexCall(request, ChatPredictions.class, model);
                return predictionsResult.thenApply(
                        predictions -> {
                            ChatCompletions completions = new ChatCompletions();
                            completions.setChoices(
                                    predictions.predictions.stream()
                                            .map(
                                                    p -> {
                                                        if (!p.candidates.isEmpty()) {
                                                            ChatChoice completion =
                                                                    new ChatChoice();
                                                            completion.setMessage(
                                                                    new ChatMessage(
                                                                                    p.candidates
                                                                                            .get(0)
                                                                                            .author)
                                                                            .setContent(
                                                                                    p.candidates
                                                                                            .get(0)
                                                                                            .content));
                                                            return completion;
                                                        } else {
                                                            ChatChoice completion =
                                                                    new ChatChoice();
                                                            completion.setMessage(
                                                                    new ChatMessage("")
                                                                            .setContent(""));
                                                            return completion;
                                                        }
                                                    })
                                            .collect(Collectors.toList()));
                            return completions;
                        });
            }

            private void appendRequestParameters(
                    Map<String, Object> additionalConfiguration, CompletionRequest request) {
                request.parameters = new HashMap<>();

                appendDoubleValue("temperature", "temperature", additionalConfiguration, request);
                appendIntValue("max-tokens", "maxOutputTokens", additionalConfiguration, request);
                appendDoubleValue("topP", "topP", additionalConfiguration, request);
                appendIntValue("topK", "topK", additionalConfiguration, request);
            }

            private void appendDoubleValue(
                    String key,
                    String toKey,
                    Map<String, Object> additionalConfiguration,
                    CompletionRequest request) {
                final Double typedValue =
                        ConfigurationUtils.getDouble(key, null, additionalConfiguration);
                if (typedValue != null) {
                    request.parameters.put(toKey, typedValue);
                }
            }

            private void appendIntValue(
                    String key,
                    String toKey,
                    Map<String, Object> additionalConfiguration,
                    CompletionRequest request) {
                final Integer typedValue =
                        ConfigurationUtils.getInteger(key, null, additionalConfiguration);
                if (typedValue != null) {
                    request.parameters.put(toKey, typedValue);
                }
            }

            @Data
            static class CompletionRequest {
                Map<String, Object> parameters;

                List<Object> instances = new ArrayList<>();

                @Data
                static class ChatInstance {
                    String context;
                    List<ChatInstanceExample> examples = new ArrayList<>();
                    List<ChatMessage> messages = new ArrayList<>();
                }

                @Data
                static class ChatInstanceExample {
                    Map<String, Object> input;
                    Map<String, Object> output;
                }

                @Data
                static class ChatMessage {
                    String author;
                    String content;
                }

                @Data
                static class TextInstance {
                    String prompt;
                }
            }

            @Override
            public CompletableFuture<TextCompletionResult> getTextCompletions(
                    List<String> prompt,
                    StreamingChunksConsumer streamingChunksConsumer,
                    Map<String, Object> options) {
                if (prompt.size() != 1) {
                    throw new IllegalArgumentException(
                            "Vertex AI only supports a single prompt for text completions.");
                }
                // https://cloud.google.com/vertex-ai/docs/generative-ai/chat/chat-prompts
                CompletionRequest.TextInstance instance = new CompletionRequest.TextInstance();
                instance.prompt = prompt.get(0);
                CompletionRequest request = new CompletionRequest();
                request.instances.add(instance);
                appendRequestParameters(options, request);

                CompletableFuture<TextPredictions> predictionsResult =
                        executeVertexCall(request, TextPredictions.class, model);
                return predictionsResult.thenApply(
                        predictions ->
                                new TextCompletionResult(
                                        predictions.predictions.get(0).content, null));
            }

            @Data
            static class ChatPredictions {

                List<Prediction> predictions;

                @Data
                static class Prediction {

                    List<Candidate> candidates = new ArrayList<>();

                    @Data
                    static class Candidate {
                        String author;
                        String content;
                    }
                }
            }

            @Data
            static class TextPredictions {

                List<Prediction> predictions;

                @Data
                static class Prediction {

                    String content;
                }
            }
        }

        private class VertexAIEmbeddingsService implements EmbeddingsService {
            private final String model;

            public VertexAIEmbeddingsService(String model) {
                this.model = model;
            }

            @Override
            @SneakyThrows
            public CompletableFuture<List<List<Double>>> computeEmbeddings(List<String> list) {
                // https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings#generative-ai-get-text-embedding-drest
                RequestEmbeddings requestEmbeddings = new RequestEmbeddings(list);
                CompletableFuture<Predictions> predictionsHandle =
                        executeVertexCall(requestEmbeddings, Predictions.class, model);
                return predictionsHandle.thenApply(
                        predictions ->
                                predictions.predictions.stream()
                                        .map(p -> p.embeddings.values)
                                        .collect(Collectors.toList()));
            }
        }

        @Data
        static class RequestEmbeddings {
            public RequestEmbeddings(List<String> instances) {
                this.instances = instances.stream().map(Instance::new).collect(Collectors.toList());
            }

            @Data
            @AllArgsConstructor
            static class Instance {
                String content;
            }

            final List<Instance> instances;
        }

        @Data
        static class Predictions {

            List<Prediction> predictions;

            @Data
            static class Prediction {

                Embeddings embeddings;

                @Data
                static class Embeddings {
                    List<Double> values;
                }
            }
        }
    }

    @SneakyThrows
    private static <T> T handleResponse(Class<T> responseType, HttpResponse<String> response) {
        if (response.statusCode() != 200) {
            throw new IllegalStateException(
                    "Unexpected status code: " + response.statusCode() + " " + response.body());
        }
        String body = response.body();
        log.info("Response: {}", body);
        return MAPPER.readValue(body, responseType);
    }
}
