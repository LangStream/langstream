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
package ai.langstream.mockagents;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentCode;
import ai.langstream.api.runner.code.AgentCodeProvider;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.RecordSink;
import ai.langstream.api.runner.code.SingleRecordAgentProcessor;
import ai.langstream.api.runtime.ComponentType;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockProcessorAgentsCodeProvider implements AgentCodeProvider {
    @Override
    public boolean supports(String agentType) {
        return "mock-failing-processor".equals(agentType)
                || "mock-failing-sink".equals(agentType)
                || "mock-async-processor".equals(agentType);
    }

    @Override
    public AgentCode createInstance(String agentType) {
        switch (agentType) {
            case "mock-failing-processor":
                return new FailingProcessor();
            case "mock-failing-sink":
                return new FailingSink();
            case "mock-async-processor":
                return new AsyncProcessor();
            default:
                throw new IllegalStateException();
        }
    }

    private static class AsyncProcessor extends AbstractAgentCode implements AgentProcessor {

        ScheduledExecutorService executorService;
        Random random = new Random();
        AtomicInteger idGenerator = new AtomicInteger();

        @Override
        public void process(List<Record> records, RecordSink recordSink) {
            for (Record record : records) {
                int delay = random.nextInt(500);
                int id = idGenerator.incrementAndGet();
                try {
                    executorService.schedule(
                            () -> {
                                log.info("EXC{} Processing record {}", id, record.value());
                                recordSink.emit(
                                        new SourceRecordAndResult(record, List.of(record), null));
                                log.info("EXC{} Processed record {}", id, record.value());
                            },
                            delay,
                            TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException rejectedExecutionException) {
                    log.info("EXC{} Rejected processing record {}", id, record.value());
                    recordSink.emit(
                            new SourceRecordAndResult(
                                    record, List.of(record), rejectedExecutionException));
                }
            }
        }

        @Override
        public void start() throws Exception {
            executorService = Executors.newScheduledThreadPool(8);
        }

        @Override
        public void close() throws Exception {
            if (executorService != null) {
                executorService.shutdown();
                executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            }
        }

        @Override
        public ComponentType componentType() {
            return ComponentType.PROCESSOR;
        }
    }

    private static class FailingProcessor extends SingleRecordAgentProcessor {

        String failOnContent;

        @Override
        public void init(Map<String, Object> configuration) {
            failOnContent = configuration.getOrDefault("fail-on-content", "").toString();
        }

        @Override
        public List<Record> processRecord(Record record) {
            log.info("Processing record value {}, failOnContent {}", record.value(), failOnContent);
            if (Objects.equals(record.value(), failOnContent)) {
                throw new InjectedFailure("Failing on content: " + failOnContent);
            }
            if (record.value() instanceof String s) {
                if (failOnContent != null
                        && !failOnContent.isEmpty()
                        && s.contains(failOnContent)) {
                    throw new InjectedFailure("Failing on content: " + s);
                }
            }
            return List.of(record);
        }
    }

    public static class FailingSink extends AbstractAgentCode implements AgentSink {

        public static final List<Record> acceptedRecords = new CopyOnWriteArrayList<>();

        String failOnContent;
        CommitCallback callback;

        @Override
        public void init(Map<String, Object> configuration) {
            acceptedRecords.clear();
            failOnContent = configuration.getOrDefault("fail-on-content", "").toString();
        }

        @Override
        public void setCommitCallback(CommitCallback callback) {
            this.callback = callback;
        }

        @Override
        public void write(List<Record> records) {
            for (Record record : records) {
                log.info(
                        "Processing record value {}, failOnContent {}",
                        record.value(),
                        failOnContent);
                if (Objects.equals(record.value(), failOnContent)) {
                    throw new InjectedFailure("Failing on content: " + failOnContent);
                }
                if (record.value() instanceof String s) {
                    if (s.contains(failOnContent)) {
                        throw new InjectedFailure("Failing on content: " + failOnContent);
                    }
                }
                acceptedRecords.add(record);
                callback.commit(List.of(record));
            }
        }
    }

    public static class InjectedFailure extends RuntimeException {
        public InjectedFailure(String message) {
            super(message);
        }
    }
}
