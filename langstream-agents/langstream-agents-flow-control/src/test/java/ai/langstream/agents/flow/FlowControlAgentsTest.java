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
package ai.langstream.agents.flow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicProducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class FlowControlAgentsTest {

    @Test
    public void testTimerSource() throws Exception {
        try (TimerSource timerSource = new TimerSource(); ) {
            timerSource.init(
                    Map.of(
                            "period-seconds",
                            1,
                            "fields",
                            List.of(
                                    Map.of("name", "value.now", "expression", "fn:now()"),
                                    Map.of("name", "key.someid", "expression", "fn:uuid()"),
                                    Map.of(
                                            "name",
                                            "properties.someprop",
                                            "expression",
                                            "fn:random(1000)"))));
            timerSource.start();
            List<Record> all = new ArrayList<>();
            while (all.size() < 10) {
                List<Record> read = timerSource.read();
                if (read.isEmpty()) {
                    continue;
                }
                assertEquals(1, read.size());
                all.addAll(read);
                Record r = read.get(0);
                log.info("Received record {}", r);
                Map<String, Object> value = (Map<String, Object>) r.value();
                assertNotNull(value.get("now"));
                Map<String, Object> key = (Map<String, Object>) r.key();
                assertNotNull(key.get("someid"));
                Header someprop = r.getHeader("someprop");
                assertNotNull(someprop);
                assertNotNull(someprop.value());
            }
        }
    }

    @Test
    public void testTriggerEventProcessor() throws Exception {
        try (TriggerEventProcessor processor = new TriggerEventProcessor(); ) {
            processor.init(
                    Map.of(
                            "when",
                            "value.activator > 5",
                            "destination",
                            "other-topic",
                            "fields",
                            List.of(
                                    Map.of(
                                            "name",
                                            "value.computed",
                                            "expression",
                                            "fn:uppercase(value.original)"),
                                    Map.of(
                                            "name",
                                            "key.computed",
                                            "expression",
                                            "fn:lowercase(key.original)"),
                                    Map.of(
                                            "name",
                                            "properties.computed",
                                            "expression",
                                            "fn:lowercase(properties.original)"))));

            List<Record> emittedRecords = new CopyOnWriteArrayList<>();
            TopicProducer dummyProducer =
                    new TopicProducer() {
                        @Override
                        public long getTotalIn() {
                            return 0;
                        }

                        @Override
                        public CompletableFuture<?> write(Record record) {
                            emittedRecords.add(record);
                            return CompletableFuture.completedFuture(null);
                        }
                    };
            TopicConnectionProvider dummyProvider =
                    new TopicConnectionProvider() {
                        @Override
                        public TopicProducer createProducer(
                                String agentId, String topic, Map<String, Object> config) {
                            return dummyProducer;
                        }
                    };
            AgentContext context = mock(AgentContext.class);
            when(context.getTopicConnectionProvider()).thenReturn(dummyProvider);
            processor.setContext(context);
            processor.start();
            List<Record> all = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                SimpleRecord someRecord =
                        SimpleRecord.builder()
                                .key(
                                        """
                        {"original": "Hello World %s"}
                        """
                                                .formatted(i))
                                .value(
                                        """
                        {"original": "Hello Folks %s", "activator": %s}
                        """
                                                .formatted(i, i))
                                .headers(
                                        List.of(
                                                new SimpleRecord.SimpleHeader(
                                                        "original",
                                                        "Some session id %s".formatted(i))))
                                .build();

                List<Record> read = new ArrayList<>();
                processor.process(
                        List.of(someRecord),
                        (sourceRecordAndResult) ->
                                read.addAll(sourceRecordAndResult.resultRecords()));
                assertEquals(1, read.size());
                all.addAll(read);
                Record emittedToDownstream = read.get(0);
                // the processor must pass downstream the original record
                assertSame(emittedToDownstream, someRecord);
                log.info("Received record {}", emittedToDownstream);

                if (i > 5) {
                    assertEquals(1, emittedRecords.size());
                    Record r = emittedRecords.remove(0);
                    Map<String, Object> value = (Map<String, Object>) r.value();
                    assertEquals(
                            "Hello Folks %s".formatted(i).toUpperCase(), value.get("computed"));
                    Map<String, Object> key = (Map<String, Object>) r.key();
                    assertEquals("Hello World %s".formatted(i).toLowerCase(), key.get("computed"));

                    Header someprop = r.getHeader("computed");
                    assertNotNull(someprop);
                    assertEquals("Some session id %s".formatted(i).toLowerCase(), someprop.value());
                } else {
                    assertEquals(0, emittedRecords.size());
                }
            }
            assertEquals(10, all.size());
        }
    }

    @Test
    public void testTriggerEventProcessorWithTopicProducerErrors() throws Exception {
        try (TriggerEventProcessor processor = new TriggerEventProcessor(); ) {
            processor.init(
                    Map.of(
                            "destination",
                            "other-topic",
                            "fields",
                            List.of(Map.of("name", "value", "expression", "fn:uppercase(value)"))));

            List<Record> emittedRecords = new CopyOnWriteArrayList<>();
            TopicProducer dummyProducer =
                    new TopicProducer() {
                        @Override
                        public long getTotalIn() {
                            return 0;
                        }

                        @Override
                        public CompletableFuture<?> write(Record record) {
                            // here the value is UPPERCASE because the TopicProducer sees only the
                            // record
                            // to send to the side topic
                            if (record.value().toString().contains("FAIL-ME")) {
                                return CompletableFuture.failedFuture(
                                        new IOException("Simulated error"));
                            }
                            emittedRecords.add(record);
                            return CompletableFuture.completedFuture(null);
                        }
                    };
            TopicConnectionProvider dummyProvider =
                    new TopicConnectionProvider() {
                        @Override
                        public TopicProducer createProducer(
                                String agentId, String topic, Map<String, Object> config) {
                            return dummyProducer;
                        }
                    };
            AgentContext context = mock(AgentContext.class);
            when(context.getTopicConnectionProvider()).thenReturn(dummyProvider);
            processor.setContext(context);
            processor.start();

            for (int i = 0; i < 10; i++) {
                boolean fail = i % 2 == 0;
                String content = (fail ? "fail-me" : "keep-me") + " " + i;
                SimpleRecord someRecord = SimpleRecord.of(null, content);

                List<AgentProcessor.SourceRecordAndResult> resultsForRecord = new ArrayList<>();
                processor.process(List.of(someRecord), resultsForRecord::add);
                // we always have an outcome
                assertEquals(1, resultsForRecord.size());
                // the processor must pass downstream the original record
                Record emittedToDownstream = resultsForRecord.get(0).sourceRecord();
                assertSame(emittedToDownstream, someRecord);

                if (fail) {
                    assertNotNull(resultsForRecord.get(0).error());
                } else {
                    assertNull(resultsForRecord.get(0).error());
                    log.info("Received record {}", emittedToDownstream);
                    assertEquals(1, emittedRecords.size());
                    Record r = emittedRecords.remove(0);
                    assertEquals(content.toUpperCase(), r.value());
                }
            }
        }
    }

    @Test
    public void testTriggerEventProcessorStopProcessing() throws Exception {
        try (TriggerEventProcessor processor = new TriggerEventProcessor(); ) {
            processor.init(
                    Map.of(
                            "continue-processing",
                            false,
                            "destination",
                            "other-topic",
                            "fields",
                            List.of(
                                    Map.of(
                                            "name",
                                            "value.computed",
                                            "expression",
                                            "fn:uppercase(value.original)"),
                                    Map.of(
                                            "name",
                                            "key.computed",
                                            "expression",
                                            "fn:lowercase(key.original)"),
                                    Map.of(
                                            "name",
                                            "properties.computed",
                                            "expression",
                                            "fn:lowercase(properties.original)"))));

            List<Record> emittedRecords = new CopyOnWriteArrayList<>();
            TopicProducer dummyProducer =
                    new TopicProducer() {
                        @Override
                        public long getTotalIn() {
                            return 0;
                        }

                        @Override
                        public CompletableFuture<?> write(Record record) {
                            emittedRecords.add(record);
                            return CompletableFuture.completedFuture(null);
                        }
                    };
            TopicConnectionProvider dummyProvider =
                    new TopicConnectionProvider() {
                        @Override
                        public TopicProducer createProducer(
                                String agentId, String topic, Map<String, Object> config) {
                            return dummyProducer;
                        }
                    };
            AgentContext context = mock(AgentContext.class);
            when(context.getTopicConnectionProvider()).thenReturn(dummyProvider);
            processor.setContext(context);
            processor.start();

            for (int i = 0; i < 10; i++) {
                SimpleRecord someRecord =
                        SimpleRecord.builder()
                                .key(
                                        """
                        {"original": "Hello World %s"}
                        """
                                                .formatted(i))
                                .value(
                                        """
                        {"original": "Hello Folks %s", "activator": %s}
                        """
                                                .formatted(i, i))
                                .headers(
                                        List.of(
                                                new SimpleRecord.SimpleHeader(
                                                        "original",
                                                        "Some session id %s".formatted(i))))
                                .build();

                List<Record> read = new ArrayList<>();
                processor.process(
                        List.of(someRecord),
                        (sourceRecordAndResult) ->
                                read.addAll(sourceRecordAndResult.resultRecords()));
                assertEquals(0, read.size());

                assertEquals(1, emittedRecords.size());
                Record r = emittedRecords.remove(0);
                Map<String, Object> value = (Map<String, Object>) r.value();
                assertEquals("Hello Folks %s".formatted(i).toUpperCase(), value.get("computed"));
                Map<String, Object> key = (Map<String, Object>) r.key();
                assertEquals("Hello World %s".formatted(i).toLowerCase(), key.get("computed"));

                Header someprop = r.getHeader("computed");
                assertNotNull(someprop);
                assertEquals("Some session id %s".formatted(i).toLowerCase(), someprop.value());
            }
        }
    }

    @Test
    public void testLogEvent() throws Exception {
        try (LogEventProcessor processor = new LogEventProcessor(); ) {
            processor.init(
                    Map.of(
                            "message",
                            "Original is {{{value.original}}}",
                            "fields",
                            List.of(
                                    Map.of(
                                            "name",
                                            "value.computed",
                                            "expression",
                                            "fn:uppercase(value.original)"))));
            processor.start();

            for (int i = 0; i < 10; i++) {
                SimpleRecord someRecord =
                        SimpleRecord.builder()
                                .value(
                                        """
                        {"original": "Hello Folks %s", "activator": %s}
                        """
                                                .formatted(i, i))
                                .build();

                List<Record> read = new ArrayList<>();
                processor.process(
                        List.of(someRecord),
                        (sourceRecordAndResult) ->
                                read.addAll(sourceRecordAndResult.resultRecords()));
                assertEquals(1, read.size());
                Record emittedToDownstream = read.get(0);
                // the processor must pass downstream the original record
                assertSame(emittedToDownstream, someRecord);
                log.info("Received record {}", emittedToDownstream);
            }
        }
    }

    @Test
    public void testLogEventNoFieldsNoMessage() throws Exception {
        try (LogEventProcessor processor = new LogEventProcessor(); ) {
            processor.init(Map.of());
            processor.start();

            for (int i = 0; i < 10; i++) {
                SimpleRecord someRecord =
                        SimpleRecord.builder()
                                .value(
                                        """
                        {"original": "Hello Folks %s", "activator": %s}
                        """
                                                .formatted(i, i))
                                .build();

                List<Record> read = new ArrayList<>();
                processor.process(
                        List.of(someRecord),
                        (sourceRecordAndResult) ->
                                read.addAll(sourceRecordAndResult.resultRecords()));
                assertEquals(1, read.size());
                Record emittedToDownstream = read.get(0);
                // the processor must pass downstream the original record
                assertSame(emittedToDownstream, someRecord);
                log.info("Received record {}", emittedToDownstream);
            }
        }
    }
}
