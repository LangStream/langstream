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

import static ai.langstream.ai.agents.commons.MutableRecord.mutableRecordToRecord;
import static ai.langstream.ai.agents.commons.MutableRecord.recordToMutableRecord;

import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.datasource.DataSourceProviderRegistry;
import ai.langstream.ai.agents.services.ServiceProviderRegistry;
import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ComponentType;
import com.datastax.oss.streaming.ai.StepPredicatePair;
import com.datastax.oss.streaming.ai.TransformStep;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.config.StepConfig;
import com.datastax.oss.streaming.ai.model.config.TransformStepConfig;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.datastax.oss.streaming.ai.streaming.StreamingAnswersConsumer;
import com.datastax.oss.streaming.ai.streaming.StreamingAnswersConsumerFactory;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GenAIToolKitAgent extends AbstractAgentCode implements AgentProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private StepPredicatePair step;
    private TransformStepConfig config;
    private QueryStepDataSource dataSource;
    private ServiceProvider serviceProvider;
    private Map<String, Object> configuration;

    private TopicProducerStreamingAnswersConsumerFactory streamingAnswersConsumerFactory;

    @Override
    public ComponentType componentType() {
        return ComponentType.PROCESSOR;
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
        MutableRecord context = recordToMutableRecord(record, config.isAttemptJsonConversion());

        CompletableFuture<?> handle = processStep(context, step);
        return handle.thenApply(
                ___ -> {
                    try {
                        context.convertMapToStringOrBytes();
                        Optional<Record> recordResult = mutableRecordToRecord(context);
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
            MutableRecord mutableRecord, StepPredicatePair pair) {
        TransformStep step = pair.getTransformStep();
        Predicate<MutableRecord> predicate = pair.getPredicate();
        if (predicate == null || predicate.test(mutableRecord)) {
            return step.processAsync(mutableRecord);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    @SneakyThrows
    public void init(Map<String, Object> configuration) {
        this.configuration = new HashMap<>(configuration);
    }

    @Override
    public void start() throws Exception {
        MetricsReporter reporter = agentContext.getMetricsReporter().withAgentName(agentId());

        // remove this from the config in order to avoid passing it TransformStepConfig
        Map<String, Object> datasourceConfiguration =
                (Map<String, Object>) configuration.remove("datasource");
        serviceProvider = ServiceProviderRegistry.getServiceProvider(configuration, reporter);

        configuration.remove("vertex");
        config = MAPPER.convertValue(configuration, TransformStepConfig.class);
        dataSource = DataSourceProviderRegistry.getQueryStepDataSource(datasourceConfiguration);
        if (dataSource != null) {
            dataSource.initialize(datasourceConfiguration);
        }
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

    private static class TopicProducerStreamingAnswersConsumerFactory
            implements StreamingAnswersConsumerFactory {
        private AgentContext agentContext;

        public TopicProducerStreamingAnswersConsumerFactory() {}

        @Override
        public StreamingAnswersConsumer create(String topicName) {
            TopicProducer topicProducer =
                    agentContext
                            .getTopicConnectionProvider()
                            .createProducer(agentContext.getGlobalAgentId(), topicName, Map.of());
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
                int index, String message, boolean last, MutableRecord outputMessage) {
            Optional<Record> record = mutableRecordToRecord(outputMessage);
            if (record.isPresent()) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "index: {}, message: {}, last: {}: record {}",
                            index,
                            message,
                            last,
                            record);
                }
                topicProducer
                        .write(record.get())
                        .exceptionally(
                                e -> {
                                    log.error("Error writing chunk to topic", e);
                                    return null;
                                });
            }
        }

        @Override
        public void close() {
            topicProducer.close();
        }
    }
}
