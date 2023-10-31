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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
public class LangServeInvokeAgent extends AbstractAgentCode implements AgentProcessor {

    record FieldDefinition(String name, JstlEvaluator<Object> expressionEvaluator) {}

    private final List<FieldDefinition> fields = new ArrayList<>();
    private TopicProducer topicProducer;
    private String destination;
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

    @SuppressWarnings("unchecked")
    @Override
    public void init(Map<String, Object> configuration) {
        this.url =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "url", () -> "langserve-invoke agent");
        destination = ConfigurationUtils.getString("stream-to-topic", "", configuration);
        contentField = ConfigurationUtils.getString("content-field", "content", configuration);
        this.outputFieldName =
                ConfigurationUtils.requiredNonEmptyField(
                        configuration, "output-field", () -> "langserve-invoke agent");
        this.debug = ConfigurationUtils.getBoolean("debug", false, configuration);
        this.method = ConfigurationUtils.getString("method", "POST", configuration);
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
                                final Object responseBody = parseResponseBody(response.body());
                                context.setResultField(
                                        responseBody,
                                        outputFieldName,
                                        null,
                                        avroKeySchemaCache,
                                        avroValueSchemaCache);
                                Optional<Record> recordResult =
                                        MutableRecord.mutableRecordToRecord(context);
                                if (log.isDebugEnabled()) {
                                    log.debug("recordResult {}", recordResult);
                                }
                                if (recordResult.isPresent()) {
                                    recordSink.emit(
                                            new SourceRecordAndResult(
                                                    record, List.of(recordResult.get()), null));
                                } else {
                                    recordSink.emitEmptyList(record);
                                }
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
        end
    }

    private void stream(
            Record record, RecordSink recordSink, HttpRequest request, MutableRecord context) {
        StringBuilder chunks = new StringBuilder();
        Flow.Subscriber<String> subscriber =
                new Flow.Subscriber<String>() {
                    Flow.Subscription subscription;

                    @Override
                    public void onSubscribe(Flow.Subscription subscription) {
                        log.info("onSubscribe {}", subscription);
                        this.subscription = subscription;
                        subscription.request(1);
                    }

                    @Override
                    public synchronized void onNext(String body) {
                        chunks.append(body).append("\n");
                        EventType eventType = parseEventType(body);
                        log.info("onNext {}", body);
                        if (eventType == null) {
                            if (body.startsWith("data: ")) {
                                body = body.substring("data: ".length());
                            }
                            final Object responseBody = parseResponseBody(body);

                            MutableRecord copy = context.copy();
                            copy.setResultField(
                                    responseBody,
                                    outputFieldName,
                                    null,
                                    avroKeySchemaCache,
                                    avroValueSchemaCache);
                            Optional<Record> recordResult =
                                    MutableRecord.mutableRecordToRecord(copy);
                            if (log.isDebugEnabled()) {
                                log.debug("recordResult {}", recordResult);
                            }
                            if (recordResult.isPresent()) {
                                topicProducer.write(recordResult.get());
                            }
                        }
                        subscription.request(1);
                    }

                    @Override
                    public void onError(Throwable error) {
                        log.error("Error processing record: {}", record, error);
                        recordSink.emit(new SourceRecordAndResult(record, null, error));
                    }

                    @Override
                    public void onComplete() {
                        log.info("Streaming answer processing completed");
                        MutableRecord copy = context.copy();
                        copy.setResultField(
                                chunks.toString(),
                                outputFieldName,
                                null,
                                avroKeySchemaCache,
                                avroValueSchemaCache);
                        Optional<Record> recordResult = MutableRecord.mutableRecordToRecord(copy);
                        if (recordResult.isPresent()) {
                            recordSink.emitSingleResult(record, recordResult.get());
                        } else {
                            recordSink.emitEmptyList(record);
                        }
                    }
                };

        httpClient.sendAsync(request, HttpResponse.BodyHandlers.fromLineSubscriber(subscriber));
    }

    private static EventType parseEventType(String body) {
        if (body == null || body.isEmpty()) {
            return EventType.end;
        }
        if (body.startsWith("event: end")) {
            return EventType.end;
        } else if (body.startsWith("event: data")) {
            return EventType.data;
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

    private Object parseResponseBody(String body) {
        try {
            Map<String, Object> map =
                    mapper.readValue(body, new TypeReference<Map<String, Object>>() {});
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
        if (!destination.isEmpty()) {
            topicProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(agentContext.getGlobalAgentId(), destination, Map.of());
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
}
