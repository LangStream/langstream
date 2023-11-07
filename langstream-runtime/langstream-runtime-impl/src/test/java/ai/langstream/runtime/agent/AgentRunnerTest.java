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
package ai.langstream.runtime.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class AgentRunnerTest {

    @Test
    void skip() throws Exception {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();

        AgentRunner.runMainLoop(
                source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(1);
        source.expectUncommitted(0);
    }

    private static AgentContext createMockAgentContext() {
        AgentContext context = mock(AgentContext.class);
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        return context;
    }

    @Test
    void failWithRetries() {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 3, "onFailure", "fail"));
        AgentContext context = createMockAgentContext();
        when(context.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        assertThrows(
                AgentRunner.PermanentFailureException.class,
                () ->
                        AgentRunner.runMainLoop(
                                source,
                                processor,
                                sink,
                                context,
                                errorHandler,
                                source::hasMoreRecords));
        processor.expectExecutions(3);
        source.expectUncommitted(1);
    }

    @Test
    void failNoRetries() {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "fail"));
        AgentContext context = createMockAgentContext();
        assertThrows(
                AgentRunner.PermanentFailureException.class,
                () ->
                        AgentRunner.runMainLoop(
                                source,
                                processor,
                                sink,
                                context,
                                errorHandler,
                                source::hasMoreRecords));
        processor.expectExecutions(1);
        source.expectUncommitted(1);
    }

    @Test
    void someFailedSomeGoodWithSkip() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        List.of(
                                SimpleRecord.of("key", "fail-me"),
                                SimpleRecord.of("key", "process-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();
        AgentRunner.runMainLoop(
                source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someGoodSomeFailedWithSkip() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        List.of(
                                SimpleRecord.of("key", "process-me"),
                                SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();
        AgentRunner.runMainLoop(
                source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someGoodSomeFailedWithSkipAndBatching() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        2,
                        List.of(
                                SimpleRecord.of("key", "process-me"),
                                SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();
        AgentRunner.runMainLoop(
                source, processor, sink, context, errorHandler, source::hasMoreRecords);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someFailedSomeGoodWithSkipAndBatching() throws Exception {
        SimpleSource source =
                new SimpleSource(
                        2,
                        List.of(
                                SimpleRecord.of("key", "fail-me"),
                                SimpleRecord.of("key", "process-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = createMockAgentContext();
        AgentRunner.runMainLoop(
                source, processor, sink, context, errorHandler, source::hasMoreRecords);
        // all the records are processed in one batch
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    private static class SimpleSink extends AbstractAgentCode implements AgentSink {
        @Override
        public CompletableFuture<?> write(Record record) {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class SimpleSource extends AbstractAgentCode implements AgentSource {

        final List<Record> records;
        final List<Record> uncommitted = new ArrayList<>();

        final int batchSize;

        public SimpleSource(int batchSize, List<Record> records) {
            this.batchSize = batchSize;
            this.records = new ArrayList<>(records);
        }

        public SimpleSource(List<Record> records) {
            this(1, records);
        }

        synchronized boolean hasMoreRecords() {
            return !records.isEmpty();
        }

        @Override
        public synchronized List<Record> read() {
            if (records.isEmpty()) {
                return List.of();
            }
            List<Record> result = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                Record remove = records.remove(0);
                result.add(remove);
                uncommitted.add(remove);
                if (records.isEmpty()) {
                    break;
                }
            }
            return result;
        }

        @Override
        public synchronized void commit(List<Record> records) {
            uncommitted.removeAll(records);
        }

        synchronized void expectUncommitted(int count) {
            assertEquals(count, uncommitted.size());
        }
    }

    private static class SimpleAgentProcessor extends SingleRecordAgentProcessor {

        private final Set<String> failOnContent;
        private int executionCount;

        public SimpleAgentProcessor(Set<String> failOnContent) {
            this.failOnContent = failOnContent;
        }

        @Override
        public List<Record> processRecord(Record record) {
            log.info("Processing {}", record.value());
            executionCount++;
            if (failOnContent.contains((String) record.value())) {
                throw new RuntimeException("Failed on " + record.value());
            }
            return List.of(record);
        }

        void expectExecutions(int count) {
            assertEquals(count, executionCount);
        }
    }
}
