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
package ai.langstream.ai.agents;

import ai.langstream.ai.agents.datasource.DataSourceProviderRegistry;
import ai.langstream.ai.agents.services.ServiceProviderRegistry;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ComponentType;
import com.datastax.oss.streaming.ai.TransformContext;
import com.datastax.oss.streaming.ai.TransformStep;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.jstl.predicate.StepPredicatePair;
import com.datastax.oss.streaming.ai.model.TransformSchemaType;
import com.datastax.oss.streaming.ai.model.config.StepConfig;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.datastax.oss.streaming.ai.streaming.StreamingAnswersConsumer;
import com.datastax.oss.streaming.ai.streaming.StreamingAnswersConsumerFactory;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;

@Slf4j
public class GenAIToolKitAgent extends AbstractAgentCode implements AgentProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private StepPredicatePair step;
    private TransformStepConfig config;
    private QueryStepDataSource dataSource;
    private ServiceProvider serviceProvider;
    private AgentContext agentContext;

    private TopicProducerStreamingAnswersConsumerFactory streamingAnswersConsumerFactory;

    @Override
    public ComponentType componentType() {
        return ComponentType.PROCESSOR;
    }

    @Override
    public void setContext(AgentContext context) {
        this.agentContext = context;
    }

    @Override
    public void process(List<Record> records, RecordSink recordSink) {
        if (records == null || records.isEmpty()) {
            throw new IllegalStateException("Records cannot be null or empty");
        }
        for (Record record : records) {
            processed(1, 0);
            CompletableFuture<List<Record>> process = processRecord(record);
            process.whenComplete(
                    (resultRecords, e) -> {
                        if (e != null) {
                            log.error("Error processing record: {}", record, e);
                            recordSink.emit(new SourceRecordAndResult(record, null, e));
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Processed record {}, results {}", record, resultRecords);
                            }
                            processed(0, 1);
                            recordSink.emit(new SourceRecordAndResult(record, resultRecords, null));
                        }
                    });
        }
    }

    public CompletableFuture<List<Record>> processRecord(Record record) {
        if (log.isDebugEnabled()) {
            log.debug("Processing {}", record);
        }
        TransformContext context =
                recordToTransformContext(record, config.isAttemptJsonConversion());

        CompletableFuture<?> handle = processStep(context, step);
        return handle.thenApply(
                ___ -> {
                    try {
                        context.convertMapToStringOrBytes();
                        Optional<Record> recordResult = transformContextToRecord(context);
                        if (log.isDebugEnabled()) {
                            log.debug("Result {}", recordResult);
                        }
                        return recordResult.map(List::of).orElseGet(List::of);
                    } catch (Exception e) {
                        log.error("Error processing record: {}", record, e);
                        throw new CompletionException(e);
                    }
                });
    }

    private static CompletableFuture<?> processStep(
            TransformContext transformContext, StepPredicatePair pair) {
        TransformStep step = pair.getTransformStep();
        Predicate<TransformContext> predicate = pair.getPredicate();
        if (predicate == null || predicate.test(transformContext)) {
            return step.processAsync(transformContext);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    @SneakyThrows
    public void init(Map<String, Object> configuration) {
        configuration = new HashMap<>(configuration);

        // remove this from the config in order to avoid passing it TransformStepConfig
        Map<String, Object> datasourceConfiguration =
                (Map<String, Object>) configuration.remove("datasource");
        serviceProvider = ServiceProviderRegistry.getServiceProvider(configuration);

        configuration.remove("vertex");
        config = MAPPER.convertValue(configuration, TransformStepConfig.class);
        dataSource = DataSourceProviderRegistry.getQueryStepDataSource(datasourceConfiguration);
        dataSource.initialize(datasourceConfiguration);
        streamingAnswersConsumerFactory = new TopicProducerStreamingAnswersConsumerFactory();
        List<StepConfig> stepsConfig = config.getSteps();
        if (stepsConfig.size() != 1) {
            throw new IllegalArgumentException("Only one step is supported");
        }
        step =
                TransformFunctionUtil.buildStep(
                        config,
                        serviceProvider,
                        dataSource,
                        streamingAnswersConsumerFactory,
                        stepsConfig.get(0));
    }

    @Override
    public void start() throws Exception {
        streamingAnswersConsumerFactory.setAgentContext(agentContext);
        step.getTransformStep().start();
    }

    @Override
    public void close() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
        if (step != null) {
            step.getTransformStep().close();
        }
        if (serviceProvider != null) {
            serviceProvider.close();
        }
    }

    public static TransformContext recordToTransformContext(
            Record record, boolean attemptJsonConversion) {
        TransformContext context = new TransformContext();
        context.setKeyObject(record.key());
        context.setKeySchemaType(
                record.key() == null ? null : getSchemaType(record.key().getClass()));
        // TODO: temporary hack. We should be able to get the schema from the record
        if (record.key() instanceof GenericRecord) {
            context.setKeyNativeSchema(((GenericRecord) record.key()).getSchema());
        }
        context.setValueObject(record.value());
        context.setValueSchemaType(
                record.value() == null ? null : getSchemaType(record.value().getClass()));
        // TODO: temporary hack. We should be able to get the schema from the record
        if (record.value() instanceof GenericRecord) {
            context.setKeyNativeSchema(((GenericRecord) record.value()).getSchema());
        }
        context.setInputTopic(record.origin());
        context.setEventTime(record.timestamp());
        if (attemptJsonConversion) {
            context.setKeyObject(
                    TransformFunctionUtil.attemptJsonConversion(context.getKeyObject()));
            context.setValueObject(
                    TransformFunctionUtil.attemptJsonConversion(context.getValueObject()));
        }
        // the headers must be Strings, this is a tentative conversion
        // in the future we need a better way to handle headers
        context.setProperties(
                record.headers().stream()
                        .filter(h -> h.key() != null && h.value() != null)
                        .collect(
                                Collectors.toMap(
                                        Header::key,
                                        (h -> {
                                            if (h.value() == null) {
                                                return null;
                                            }
                                            if (h.value() instanceof byte[]) {
                                                return new String(
                                                        (byte[]) h.value(), StandardCharsets.UTF_8);
                                            } else {
                                                return h.value().toString();
                                            }
                                        }))));
        return context;
    }

    public static Optional<Record> transformContextToRecord(TransformContext context) {
        if (context.isDropCurrentRecord()) {
            return Optional.empty();
        }
        List<Header> headers = new ArrayList<>();
        context.getProperties()
                .forEach(
                        (key, value) -> {
                            SimpleRecord.SimpleHeader header =
                                    new SimpleRecord.SimpleHeader(key, value);
                            headers.add(header);
                        });
        return Optional.of(new TransformRecord(context, headers));
    }

    private record TransformRecord(TransformContext context, Collection<Header> headers)
            implements Record {
        private TransformRecord(TransformContext context, Collection<Header> headers) {
            this.context = context;
            this.headers = new ArrayList<>(headers);
        }

        @Override
        public Object key() {
            return context.getKeyObject();
        }

        @Override
        public Object value() {
            return context.getValueObject();
        }

        @Override
        public String origin() {
            return context.getInputTopic();
        }

        @Override
        public Long timestamp() {
            return context.getEventTime();
        }
    }

    private static TransformSchemaType getSchemaType(Class<?> javaType) {
        if (String.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.STRING;
        }
        if (Byte.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INT8;
        }
        if (Short.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INT16;
        }
        if (Integer.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INT32;
        }
        if (Long.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INT64;
        }
        if (Double.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.DOUBLE;
        }
        if (Float.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.FLOAT;
        }
        if (Boolean.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.BOOLEAN;
        }
        if (byte[].class.isAssignableFrom(javaType)) {
            return TransformSchemaType.BYTES;
        }
        // Must be before DATE
        if (Time.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.TIME;
        }
        // Must be before DATE
        if (Timestamp.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.TIMESTAMP;
        }
        if (Date.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.DATE;
        }
        if (Instant.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.INSTANT;
        }
        if (LocalDate.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.LOCAL_DATE;
        }
        if (LocalTime.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.LOCAL_TIME;
        }
        if (LocalDateTime.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.LOCAL_DATE_TIME;
        }
        if (GenericRecord.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.AVRO;
        }
        if (JsonNode.class.isAssignableFrom(javaType)) {
            return TransformSchemaType.JSON;
        }
        throw new IllegalArgumentException("Unsupported data type: " + javaType);
    }

    private static class TopicProducerStreamingAnswersConsumerFactory
            implements StreamingAnswersConsumerFactory {
        private AgentContext agentContext;

        public TopicProducerStreamingAnswersConsumerFactory() {}

        @Override
        public StreamingAnswersConsumer create(String topicName) {
            TopicProducer topicProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(
                                    agentContext.getGlobalAgentId(), Map.of("topic", topicName));
            topicProducer.start();
            return new TopicStreamingAnswersConsumer(topicProducer);
        }

        public void setAgentContext(AgentContext agentContext) {
            this.agentContext = agentContext;
        }
    }

    private static class TopicStreamingAnswersConsumer implements StreamingAnswersConsumer {
        private TopicProducer topicProducer;

        public TopicStreamingAnswersConsumer(TopicProducer topicProducer) {
            this.topicProducer = topicProducer;
        }

        @Override
        public void streamAnswerChunk(
                int index, String message, boolean last, TransformContext outputMessage) {
            Optional<Record> record = transformContextToRecord(outputMessage);
            if (record.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "index: {}, message: {}, last: {}: record {}",
                            index,
                            message,
                            last,
                            record);
                }
                topicProducer.write(record.get()).join();
            }
        }

        @Override
        public void close() {
            topicProducer.close();
        }
    }
}
