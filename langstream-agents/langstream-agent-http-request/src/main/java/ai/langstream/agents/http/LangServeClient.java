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
package ai.langstream.agents.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.net.CookieManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LangServeClient {

    static final ObjectMapper mapper = new ObjectMapper();

    final HttpClient httpClient;

    final Options options;

    @Builder
    @Getter
    public static class Options {
        private String url;
        @Builder.Default private String method = "POST";
        @Builder.Default private boolean allowRedirects = true;
        private ExecutorService executorService;
        private CookieManager cookieManager;
        @Builder.Default private boolean debug = false;
        @Builder.Default private String contentField = "content";
        @Builder.Default private int minChunksPerMessage = 20;
    }

    public LangServeClient(Options options) {
        this.options = options;
        HttpClient.Builder builder =
                HttpClient.newBuilder()
                        .followRedirects(
                                options.isAllowRedirects()
                                        ? HttpClient.Redirect.NORMAL
                                        : HttpClient.Redirect.NEVER);
        if (options.getCookieManager() != null) {
            builder.cookieHandler(options.getCookieManager());
        }
        if (options.getExecutorService() != null) {
            builder.executor(options.getExecutorService());
        }
        httpClient = builder.build();
    }

    private CompletableFuture<String> invoke(HttpRequest request) {
        CompletableFuture<String> result = new CompletableFuture<>();
        httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(
                        response -> {
                            if (options.debug) {
                                log.info("Response {}", response);
                                log.info("Response body {}", response.body());
                            }
                            try {
                                if (response.statusCode() >= 400) {
                                    throw new RuntimeException(
                                            "Error processing, http response: " + response);
                                }
                                final Object responseBody =
                                        parseResponseBody(response.body(), false);

                                result.complete(responseBody.toString());

                            } catch (Exception e) {
                                log.error("Error processing request: {}", request, e);
                                result.completeExceptionally(e);
                            }
                        })
                .exceptionally(
                        error -> {
                            log.error("Error processing request: {}", request, error);
                            result.completeExceptionally(error);
                            return null;
                        });
        return result;
    }

    enum EventType {
        data,
        end,
        emptyLine
    }

    private CompletableFuture<String> stream(
            HttpRequest request, StreamingChunksConsumer streamingChunksConsumer) {
        StreamResponseProcessor streamResponseProcessor =
                new StreamResponseProcessor(options.minChunksPerMessage, streamingChunksConsumer);
        httpClient.sendAsync(
                request, HttpResponse.BodyHandlers.fromLineSubscriber(streamResponseProcessor));
        return streamResponseProcessor;
    }

    private static EventType parseEventType(String body) {
        if (body == null) {
            return EventType.end;
        }
        if (body.startsWith("event: end")) {
            return EventType.end;
        } else if (body.startsWith("event: data")) {
            return EventType.data;
        } else if (body.isEmpty()) {
            return EventType.emptyLine;
        } else {
            return null;
        }
    }

    private Object parseResponseBody(String body, boolean streaming) {
        if (body == null) {
            return "";
        }
        try {
            if (body.startsWith("{")) {
                Map<String, Object> map =
                        mapper.readValue(body, new TypeReference<Map<String, Object>>() {});
                if (!streaming) {
                    Object output = map.get("output");
                    if (output == null || output instanceof String) {
                        return output;
                    } else if (output instanceof Map) {
                        map = (Map<String, Object>) output;
                    }
                }
                if (options.contentField.isEmpty()) {
                    return map;
                } else {
                    return map.get(options.contentField);
                }
            } else if (body.startsWith("\"")) {
                body = mapper.readValue(body, String.class);
            }
        } catch (JsonProcessingException ex) {
            log.info("Not able to parse response to json: {}, {}", body, ex);
        }
        return body;
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

        private final StreamingChunksConsumer streamingChunksConsumer;

        private final String answerId = java.util.UUID.randomUUID().toString();

        public StreamResponseProcessor(
                int minChunksPerMessage, StreamingChunksConsumer streamingChunksConsumer) {
            this.minChunksPerMessage = minChunksPerMessage;
            this.streamingChunksConsumer = streamingChunksConsumer;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public synchronized void onNext(String body) {
            EventType eventType = parseEventType(body);
            if (eventType == null || eventType == EventType.end) {
                boolean last = false;
                String content;
                if (body.startsWith("data: ")) {
                    body = body.substring("data: ".length());
                    final Object responseBody = parseResponseBody(body, true);
                    if (responseBody == null) {
                        content = "";
                    } else {
                        content = responseBody.toString();
                    }
                } else if (eventType == EventType.end) {
                    content = "";
                    last = true;
                } else {
                    content = "";
                }

                if (!content.isEmpty()) {
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
                    String chunk = writer.toString();
                    streamingChunksConsumer.consumeChunk(
                            answerId, index.incrementAndGet(), chunk, last);
                    writer.getBuffer().setLength(0);
                    numberOfChunks.set(0);
                }
                if (last) {
                    this.complete(buildTotalAnswerMessage());
                }
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable error) {
            log.error("IO Error while calling LangServe", error);
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

    interface StreamingChunksConsumer {
        void consumeChunk(String answerId, int index, String chunk, boolean last);
    }

    private String buildBody(Map<String, Object> values) throws IOException {
        Map<String, Object> request = Map.of("input", values);
        return mapper.writeValueAsString(request);
    }

    public CompletableFuture<String> execute(
            Map<String, Object> fields,
            Map<String, String> headers,
            StreamingChunksConsumer streamingChunksConsumer) {
        try {

            final String body = buildBody(fields);

            final HttpRequest.BodyPublisher bodyPublisher =
                    HttpRequest.BodyPublishers.ofString(body);

            final HttpRequest.Builder requestBuilder =
                    HttpRequest.newBuilder()
                            .uri(new URI(options.url))
                            .version(HttpClient.Version.HTTP_1_1)
                            .method(options.method, bodyPublisher);
            requestBuilder.header("Content-Type", "application/json");
            headers.forEach((key, value) -> requestBuilder.header(key, value));
            final HttpRequest request = requestBuilder.build();
            if (options.debug) {
                log.info("Sending request {}", request);
                log.info("Body {}", body);
            }
            if (options.url.endsWith("/invoke")) {
                return invoke(request);
            } else if (options.url.endsWith("/stream")) {
                return stream(request, streamingChunksConsumer);
            } else {
                return CompletableFuture.failedFuture(
                        new UnsupportedOperationException("Unsupported url: " + options.url));
            }
        } catch (Throwable error) {
            log.error("Error processing request: {}", fields, error);
            return CompletableFuture.failedFuture(error);
        }
    }
}
