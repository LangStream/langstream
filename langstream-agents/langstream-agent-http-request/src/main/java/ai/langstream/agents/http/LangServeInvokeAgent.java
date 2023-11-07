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
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.util.ConfigurationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.io.IOException;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private ExecutorService executor;
    private LangServeClient langServeClient;
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

        langServeClient =
                new LangServeClient(
                        LangServeClient.Options.builder()
                                .allowRedirects(allowRedirects)
                                .method(method)
                                .url(url)
                                .minChunksPerMessage(minChunksPerMessage)
                                .debug(debug)
                                .executorService(executor)
                                .cookieManager(cookieManager)
                                .build());
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
            final Map<String, Object> body = buildBody(context);

            Map<String, String> headers = new HashMap<>();
            headersTemplates.forEach((key, value) -> headers.put(key, value.execute(jsonRecord)));

            CompletableFuture<String> execute =
                    langServeClient.execute(
                            body,
                            headers,
                            new LangServeClient.StreamingChunksConsumer() {
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
                                                        log.error(
                                                                "Error writing chunk to topic", e);
                                                        return null;
                                                    });
                                }
                            });
            execute.whenComplete(
                    (result, error) -> {
                        if (error != null) {
                            recordSink.emitError(record, error);
                        } else {
                            MutableRecord copy = context.copy();
                            applyResultFieldToContext(copy, result, false);
                            Record finalRecord =
                                    MutableRecord.mutableRecordToRecord(copy).orElseThrow();
                            recordSink.emitSingleResult(record, finalRecord);
                        }
                    });
        } catch (Throwable error) {
            log.error("Error processing record: {}", record, error);
            recordSink.emitError(record, error);
        }
    }

    private Map<String, Object> buildBody(MutableRecord context) throws IOException {
        Map<String, Object> values = new HashMap<>();
        for (FieldDefinition field : fields) {
            values.put(field.name, field.expressionEvaluator.evaluate(context));
        }
        return values;
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
