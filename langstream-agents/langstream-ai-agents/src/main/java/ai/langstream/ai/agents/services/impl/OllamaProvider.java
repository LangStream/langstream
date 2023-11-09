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
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OllamaProvider implements ServiceProviderProvider {

    static ObjectMapper mapper =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("ollama");
    }

    @Override
    public ServiceProvider createImplementation(
            Map<String, Object> agentConfiguration, MetricsReporter metricsReporter) {

        Map<String, Object> config = (Map<String, Object>) agentConfiguration.get("ollama");
        String url = (String) config.get("url");
        return new OllamaServiceProvider(url);
    }

    private static class OllamaServiceProvider implements ServiceProvider {

        final HttpClient httpClient;
        private final String url;

        @SneakyThrows
        public OllamaServiceProvider(String url) {
            this.url = url;
            this.httpClient = HttpClient.newHttpClient();
        }

        @Override
        public CompletionsService getCompletionsService(Map<String, Object> map) throws Exception {
            String model = (String) map.get("model");
            if (model == null) {
                throw new IllegalArgumentException("'model' is required for completions service");
            }
            int minChunksPerMessage = ConfigurationUtils.getInt("min-chunks-per-message", 20, map);
            return new OllamaCompletionsService(model, minChunksPerMessage);
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> map) throws Exception {
            String model = (String) map.get("model");
            if (model == null) {
                throw new IllegalArgumentException("'model' is required for embedding service");
            }
            return new OllamaEmbeddingsService(model);
        }

        private class OllamaEmbeddingsService implements EmbeddingsService {
            private final String model;

            public OllamaEmbeddingsService(String model) {
                this.model = model;
            }

            @Override
            @SneakyThrows
            public CompletableFuture<List<List<Double>>> computeEmbeddings(List<String> list) {
                List<CompletableFuture<List<Double>>> futures = new ArrayList<>();
                for (String text : list) {
                    futures.add(computeEmbeddings(text));
                }
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .thenApply(
                                ___ ->
                                        futures.stream()
                                                .map(CompletableFuture::join)
                                                .collect(Collectors.toList()));
            }

            record EmbeddingResponse(List<Double> embedding, String error) {}

            private CompletableFuture<List<Double>> computeEmbeddings(String prompt) {
                String request;
                try {
                    request = MAPPER.writeValueAsString(new Request(model, prompt));
                    final HttpRequest.BodyPublisher bodyPublisher =
                            HttpRequest.BodyPublishers.ofString(request);

                    final HttpRequest.Builder requestBuilder =
                            HttpRequest.newBuilder()
                                    .uri(new URI(url + "/api/embeddings"))
                                    .version(HttpClient.Version.HTTP_1_1)
                                    .method("POST", bodyPublisher);
                    final HttpRequest httpRequest = requestBuilder.build();

                    return httpClient
                            .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                            .thenApply(
                                    response -> {
                                        if (response.statusCode() != 200) {
                                            throw new RuntimeException(
                                                    "HTTP Error: " + response.statusCode());
                                        }
                                        try {
                                            String body = response.body();
                                            EmbeddingResponse embeddings =
                                                    mapper.readValue(body, EmbeddingResponse.class);
                                            if (embeddings.error != null) {
                                                throw new RuntimeException(embeddings.error);
                                            }
                                            return embeddings.embedding();
                                        } catch (JsonProcessingException error) {
                                            throw new CompletionException(error);
                                        }
                                    });
                } catch (Throwable error) {
                    log.error("IO Error while calling Ollama", error);
                    return CompletableFuture.failedFuture(error);
                }
            }
        }

        private class StreamResponseProcessor extends CompletableFuture<String>
                implements Flow.Subscriber<String> {

            Flow.Subscription subscription;

            private final StringWriter totalAnswer = new StringWriter();

            private final StringWriter writer = new StringWriter();
            private final AtomicInteger numberOfChunks = new AtomicInteger();
            private final int minChunksPerMessage;

            private final AtomicInteger currentChunkSize = new AtomicInteger(1);
            private final AtomicInteger index = new AtomicInteger();

            private final CompletionsService.StreamingChunksConsumer streamingChunksConsumer;

            private final String answerId = java.util.UUID.randomUUID().toString();

            public StreamResponseProcessor(
                    int minChunksPerMessage,
                    CompletionsService.StreamingChunksConsumer streamingChunksConsumer) {
                this.minChunksPerMessage = minChunksPerMessage;
                this.streamingChunksConsumer = streamingChunksConsumer;
            }

            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscription.request(1);
            }

            record ResponseLine(
                    String model, String create_at, String response, boolean done, String error) {}

            @Override
            @SneakyThrows
            public synchronized void onNext(String body) {
                ResponseLine responseLine = mapper.readValue(body, ResponseLine.class);
                String content = responseLine.response();
                boolean last = responseLine.done();

                if (responseLine.error != null) {
                    log.error("Error: {}", responseLine.error);
                    this.completeExceptionally(new RuntimeException(responseLine.error));
                    return;
                }

                if (content != null && !content.isEmpty()) {
                    writer.write(content);
                    totalAnswer.write(content);
                    numberOfChunks.incrementAndGet();
                }

                // start from 1 chunk, then double the size until we reach the minChunksPerMessage
                // this gives better latencies for the first message
                int currentMinChunksPerMessage = currentChunkSize.get();

                if (numberOfChunks.get() >= currentMinChunksPerMessage || last) {
                    currentChunkSize.set(
                            Math.min(currentMinChunksPerMessage * 2, minChunksPerMessage));
                    streamingChunksConsumer.consumeChunk(
                            answerId,
                            index.incrementAndGet(),
                            new ChatChoice(new ChatMessage("system", writer.toString())),
                            last);
                    writer.getBuffer().setLength(0);
                    numberOfChunks.set(0);
                }
                if (last) {
                    this.complete(buildTotalAnswerMessage());
                }

                subscription.request(1);
            }

            @Override
            public void onError(Throwable error) {
                log.error("IO Error while calling Ollama", error);
                this.completeExceptionally(error);
            }

            @Override
            public void onComplete() {
                if (!this.isDone()) {
                    this.complete(buildTotalAnswerMessage());
                }
            }

            public String buildTotalAnswerMessage() {
                return totalAnswer.toString();
            }
        }

        record Request(String model, String prompt) {}

        @Override
        public void close() {}

        private class OllamaCompletionsService implements CompletionsService {
            private final String model;
            private final int minChunksPerMessage;

            public OllamaCompletionsService(String model, int minChunksPerMessage) {
                this.model = model;
                this.minChunksPerMessage = minChunksPerMessage;
            }

            @Override
            public CompletableFuture<ChatCompletions> getChatCompletions(
                    List<ChatMessage> list,
                    StreamingChunksConsumer streamingChunksConsumer,
                    Map<String, Object> additionalConfiguration) {

                String prompt =
                        list.stream().map(c -> c.getContent()).collect(Collectors.joining("\""));

                String request;
                try {
                    request = MAPPER.writeValueAsString(new Request(model, prompt));
                    final HttpRequest.BodyPublisher bodyPublisher =
                            HttpRequest.BodyPublishers.ofString(request);

                    final HttpRequest.Builder requestBuilder =
                            HttpRequest.newBuilder()
                                    .uri(new URI(url + "/api/generate"))
                                    .version(HttpClient.Version.HTTP_1_1)
                                    .method("POST", bodyPublisher);
                    final HttpRequest httpRequest = requestBuilder.build();

                    StreamResponseProcessor streamResponseProcessor =
                            new StreamResponseProcessor(
                                    minChunksPerMessage, streamingChunksConsumer);
                    httpClient.sendAsync(
                            httpRequest,
                            HttpResponse.BodyHandlers.fromLineSubscriber(streamResponseProcessor));

                    return streamResponseProcessor.thenApply(
                            s -> {
                                ChatCompletions result = new ChatCompletions();
                                result.setChoices(
                                        List.of(new ChatChoice(new ChatMessage("system", s))));
                                return result;
                            });
                } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
            }

            @Override
            public CompletableFuture<TextCompletionResult> getTextCompletions(
                    List<String> prompt,
                    StreamingChunksConsumer streamingChunksConsumer,
                    Map<String, Object> options) {
                return getChatCompletions(
                                prompt.stream().map(p -> new ChatMessage(null, p)).toList(),
                                streamingChunksConsumer,
                                options)
                        .thenApply(
                                c -> {
                                    return new TextCompletionResult(
                                            c.getChoices().get(0).content(), null);
                                });
            }
        }
    }
}
