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

import ai.langstream.ai.agents.commons.JsonRecord;
import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.util.ConfigurationUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.io.IOException;
import java.io.StringWriter;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
public class LangServeInvokeAgent extends AbstractAgentCode implements AgentProcessor {

    record FieldDefinition(String name, JstlEvaluator<Object> expressionEvaluator) {}

    private final List<FieldDefinition> fields = new ArrayList<>();
    private TopicProducer topicProducer;
    private String streamToTopic;
    private int minChunksPerMessage;
    private String contentField;
    private boolean debug;
    static final ObjectMapper mapper = new ObjectMapper();
    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();

    private AgentContext agentContext;
    private ExecutorService executor;
    private HttpClient httpClient;
    private String url;
    private String method;
    private Map<String, Template> headersTemplates;
    private String outputFieldName;
    private String streamResponseCompletionField;

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> configuration) {
        url =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "url", () -> "langserve-invoke agent");

        outputFieldName =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "output-field", () -> "langserve-invoke agent");
        streamToTopic = ConfigurationUtils.getString("stream-to-topic", "", configuration);
        streamResponseCompletionField =
                ConfigurationUtils.getString(
                        "stream-response-field", outputFieldName, configuration);

        minChunksPerMessage =
                ConfigurationUtils.getInteger("min-chunks-per-message", 20, configuration);
        contentField = ConfigurationUtils.getString("content-field", "content", configuration);

        debug = ConfigurationUtils.getBoolean("debug", false, configuration);
        method = ConfigurationUtils.getString("method", "POST", configuration);
        List<Map<String, Object>> fields =
                (List<Map<String, Object>>) configuration.getOrDefault("fields", List.of());
        fields.forEach(
                r -> {
                    String name = ConfigurationUtils.getString("name", "", r);
                    String expression = ConfigurationUtils.getString("expression", "", r);
                    log.info("Sending field with name {} computed as {}", name, expression);
                    JstlEvaluator<Object> expressionEvaluator =
                            new JstlEvaluator<>("${" + expression + "}", Object.class);
                    this.fields.add(new FieldDefinition(name, expressionEvaluator));
                });
        final boolean allowRedirects =
                ConfigurationUtils.getBoolean("allow-redirects", true, configuration);
        final boolean handleCookies =
                ConfigurationUtils.getBoolean("handle-cookies", true, configuration);

        final Map<String, Object> headers =
                ConfigurationUtils.getMap("headers", new HashMap<>(), configuration);
        headersTemplates = new HashMap<>();
        for (Map.Entry<String, Object> entry : headers.entrySet()) {
            headersTemplates.put(
                    entry.getKey(), Mustache.compiler().compile(entry.getValue().toString()));
        }

        executor = Executors.newCachedThreadPool();
        CookieManager cookieManager = new CookieManager();
        cookieManager.setCookiePolicy(
                handleCookies ? CookiePolicy.ACCEPT_ALL : CookiePolicy.ACCEPT_NONE);
        httpClient =
                HttpClient.newBuilder()
                        .followRedirects(
                                allowRedirects
                                        ? HttpClient.Redirect.NORMAL
                                        : HttpClient.Redirect.NEVER)
                        .cookieHandler(cookieManager)
                        .executor(executor)
                        .build();
    }

    @Override
    public void setContext(AgentContext context) throws Exception {
        this.agentContext = context;
    }

    @Override
    public void process(List<Record> records, RecordSink recordSink) {
        for (Record record : records) {
            processRecord(record, recordSink);
        }
    }

    @Override
    public ComponentType componentType() {
        return ComponentType.PROCESSOR;
    }

    public void processRecord(Record record, RecordSink recordSink) {
        try {
            MutableRecord context = MutableRecord.recordToMutableRecord(record, true);
            final JsonRecord jsonRecord = context.toJsonRecord();

            final URI uri = URI.create(url);
            final String body = buildBody(context);

            final HttpRequest.BodyPublisher bodyPublisher =
                    HttpRequest.BodyPublishers.ofString(body);

            final HttpRequest.Builder requestBuilder =
                    HttpRequest.newBuilder()
                            .uri(uri)
                            .version(HttpClient.Version.HTTP_1_1)
                            .method(this.method, bodyPublisher);
            requestBuilder.header("Content-Type", "application/json");
            headersTemplates.forEach(
                    (key, value) -> requestBuilder.header(key, value.execute(jsonRecord)));
            final HttpRequest request = requestBuilder.build();
            if (debug) {
                log.info("Sending request {}", request);
                log.info("Body {}", body);
            }

            if (url.endsWith("/invoke")) {
                invoke(record, recordSink, request, context);
            } else if (url.endsWith("/stream")) {
                stream(record, recordSink, request, context);
            } else {
                recordSink.emitError(
                        record, new UnsupportedOperationException("Unsupported url: " + url));
            }
        } catch (Throwable error) {
            log.error("Error processing record: {}", record, error);
            recordSink.emit(new SourceRecordAndResult(record, null, error));
        }
    }

    private void invoke(
            Record record, RecordSink recordSink, HttpRequest request, MutableRecord context) {
        httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(
                        response -> {
                            if (debug) {
                                log.info("Response {}", response);
                                log.info("Response body {}", response.body());
                            }
                            try {
                                if (response.statusCode() >= 400) {
                                    throw new RuntimeException(
                                            "Error processing record: "
                                                    + record
                                                    + " with response: "
                                                    + response);
                                }
                                final Object responseBody =
                                        parseResponseBody(response.body(), false);
                                applyResultFieldToContext(context, responseBody.toString(), false);
                                Optional<Record> recordResult =
                                        MutableRecord.mutableRecordToRecord(context);
                                if (log.isDebugEnabled()) {
                                    log.debug("recordResult {}", recordResult);
                                }
                                recordSink.emit(
                                        new SourceRecordAndResult(
                                                record, List.of(recordResult.orElseThrow()), null));
                            } catch (Exception e) {
                                log.error("Error processing record: {}", record, e);
                                recordSink.emitError(record, e);
                            }
                        })
                .exceptionally(
                        error -> {
                            log.error("Error processing record: {}", record, error);
                            recordSink.emit(new SourceRecordAndResult(record, null, error));
                            return null;
                        });
    }

    enum EventType {
        data,
        end,
        emptyLine
    }

    private void stream(
            Record record, RecordSink recordSink, HttpRequest request, MutableRecord context) {
        StreamResponseProcessor streamResponseProcessor =
                new StreamResponseProcessor(
                        minChunksPerMessage,
                        new StreamingChunksConsumer() {
                            @Override
                            public void consumeChunk(
                                    String answerId, int index, String chunk, boolean last) {
                                if (topicProducer == null) {
                                    // no streaming output
                                    return;
                                }
                                MutableRecord copy = context.copy();
                                applyResultFieldToContext(copy, chunk, true);
                                copy.getProperties().put("stream-id", answerId);
                                copy.getProperties().put("stream-index", index + "");
                                copy.getProperties().put("stream-last-message", last + "");
                                Optional<Record> recordResult =
                                        MutableRecord.mutableRecordToRecord(copy);
                                if (log.isDebugEnabled()) {
                                    log.debug("recordResult {}", recordResult);
                                }
                                topicProducer
                                        .write(recordResult.orElseThrow())
                                        .exceptionally(
                                                e -> {
                                                    log.error("Error writing chunk to topic", e);
                                                    return null;
                                                });
                            }
                        });

        httpClient.sendAsync(
                request, HttpResponse.BodyHandlers.fromLineSubscriber(streamResponseProcessor));

        streamResponseProcessor.whenComplete(
                (r, e) -> {
                    if (e != null) {
                        log.error("Error processing record: {}", record, e);
                        recordSink.emitError(record, e);
                    } else {
                        MutableRecord copy = context.copy();
                        applyResultFieldToContext(
                                copy, streamResponseProcessor.buildTotalAnswerMessage(), false);
                        Optional<Record> recordResult = MutableRecord.mutableRecordToRecord(copy);
                        recordSink.emitSingleResult(record, recordResult.orElseThrow());
                    }
                });
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

    private String buildBody(MutableRecord context) throws IOException {
        Map<String, Object> values = new HashMap<>();
        for (FieldDefinition field : fields) {
            values.put(field.name, field.expressionEvaluator.evaluate(context));
        }
        Map<String, Object> request = Map.of("input", values);
        return mapper.writeValueAsString(request);
    }

    private Object parseResponseBody(String body, boolean streaming) {
        try {
            Map<String, Object> map =
                    mapper.readValue(body, new TypeReference<Map<String, Object>>() {});
            if (!streaming) {
                map = (Map<String, Object>) map.get("output");
            }
            if (contentField.isEmpty()) {
                return map;
            } else {
                return map.get(contentField);
            }
        } catch (JsonProcessingException ex) {
            log.debug("Not able to parse response to json: {}, {}", body, ex);
        }
        return body;
    }

    @Override
    public void start() throws Exception {
        if (!streamToTopic.isEmpty()) {
            log.info("Streaming answers to topic {}", streamToTopic);
            topicProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(
                                    agentContext.getGlobalAgentId(), streamToTopic, Map.of());
            topicProducer.start();
        }
    }

    @Override
    public void close() throws Exception {
        if (topicProducer != null) {
            topicProducer.close();
            topicProducer = null;
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private class StreamResponseProcessor extends CompletableFuture<Void>
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
            log.info("onSubscribe {}", subscription);
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
                    this.complete(null);
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
                log.info("Completed - but end event never received");
                this.complete(null);
            } else {
                log.info("Completed");
            }
        }

        public String buildTotalAnswerMessage() {
            return totalAnswer.toString();
        }
    }

    interface StreamingChunksConsumer {
        void consumeChunk(String answerId, int index, String chunk, boolean last);
    }

    private void applyResultFieldToContext(
            MutableRecord mutableRecord, String content, boolean streamingAnswer) {
        String fieldName = outputFieldName;

        // maybe we want a different field in the streaming answer
        // typically you want to directly stream the answer as the whole "value"
        if (streamingAnswer) {
            fieldName = streamResponseCompletionField;
        }
        mutableRecord.setResultField(
                content,
                fieldName,
                Schema.create(Schema.Type.STRING),
                avroKeySchemaCache,
                avroValueSchemaCache);
    }
}
