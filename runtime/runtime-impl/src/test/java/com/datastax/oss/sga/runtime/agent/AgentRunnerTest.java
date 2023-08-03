/**
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
package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.AgentProcessor;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.code.SingleRecordAgentProcessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Slf4j
class AgentRunnerTest {

    @Test
    void skip() throws Exception {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = mock(AgentContext.class);
        AgentRunner.runMainLoop(source, processor, sink, context, errorHandler, 5);
        processor.expectExecutions(1);
        source.expectUncommitted(0);
    }

    @Test
    void failWithRetries() throws Exception {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 3, "onFailure", "fail"));
        AgentContext context = mock(AgentContext.class);
        assertThrows(AgentRunner.PermanentFailureException.class,
                () -> AgentRunner.runMainLoop(source, processor, sink, context, errorHandler, 5));
        processor.expectExecutions(3);
        source.expectUncommitted(1);
    }

    @Test
    void failNoRetries() throws Exception {
        SimpleSource source = new SimpleSource(List.of(SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "fail"));
        AgentContext context = mock(AgentContext.class);
        assertThrows(AgentRunner.PermanentFailureException.class,
                () -> AgentRunner.runMainLoop(source, processor, sink, context, errorHandler, 5));
        processor.expectExecutions(1);
        source.expectUncommitted(1);
    }

    @Test
    void someFailedSomeGoodWithSkip() throws Exception {
        SimpleSource source = new SimpleSource(List.of(
                SimpleRecord.of("key", "fail-me"),
                SimpleRecord.of("key", "process-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = mock(AgentContext.class);
        AgentRunner.runMainLoop(source, processor, sink, context, errorHandler, 5);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someGoodSomeFailedWithSkip() throws Exception {
        SimpleSource source = new SimpleSource(List.of(
                SimpleRecord.of("key", "process-me"),
                SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = mock(AgentContext.class);
        AgentRunner.runMainLoop(source, processor, sink, context, errorHandler, 5);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someGoodSomeFailedWithSkipAndBatching() throws Exception {
        SimpleSource source = new SimpleSource(2, List.of(
                SimpleRecord.of("key", "process-me"),
                SimpleRecord.of("key", "fail-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = mock(AgentContext.class);
        AgentRunner.runMainLoop(source, processor, sink, context, errorHandler, 5);
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    @Test
    void someFailedSomeGoodWithSkipAndBatching() throws Exception {
        SimpleSource source = new SimpleSource(2, List.of(
                SimpleRecord.of("key", "fail-me"),
                SimpleRecord.of("key", "process-me")));
        AgentSink sink = new SimpleSink();
        SimpleAgentProcessor processor = new SimpleAgentProcessor(Set.of("fail-me"));
        StandardErrorsHandler errorHandler =
                new StandardErrorsHandler(Map.of("retries", 0, "onFailure", "skip"));
        AgentContext context = mock(AgentContext.class);
        AgentRunner.runMainLoop(source, processor, sink, context, errorHandler, 5);
        // all the records are processed in one batch
        processor.expectExecutions(2);
        source.expectUncommitted(0);
    }

    private static class SimpleSink implements AgentSink {

        @Override
        public String agentType() {
            return "test";
        }

        CommitCallback callback;

        @Override
        public void write(List<Record> records) throws Exception {
            callback.commit(records);
        }

        @Override
        public void setCommitCallback(CommitCallback callback) {
            this.callback = callback;
        }
    }

    private static class SimpleSource implements AgentSource {

        @Override
        public String agentType() {
            return "test";
        }

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

        @Override
        public List<Record> read() throws Exception {
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
        public void commit(List<Record> records) throws Exception {
            uncommitted.removeAll(records);
        }

        void expectUncommitted(int count) {
            assertEquals(count, uncommitted.size());
        }
    }

    private static class SimpleAgentProcessor extends SingleRecordAgentProcessor {

        @Override
        public String agentType() {
            return "test";
        }

        private final Set<String> failOnContent;
        private int executionCount;

        public SimpleAgentProcessor(Set<String> failOnContent) {
            this.failOnContent = failOnContent;
        }

        @Override
        public List<Record> processRecord(Record record) throws Exception {
            log.info("Processing {}", record.value());
            executionCount++;
            if (failOnContent.contains(record.value())) {
                throw new RuntimeException("Failed on " + record.value());
            }
            return List.of(record);
        }

        void expectExecutions(int count) {
            assertEquals(count, executionCount);
        }
    }
}
