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
package ai.langstream.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class FlowControlRunnerIT extends AbstractKafkaApplicationRunner {

    @Test
    public void testDispatch() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "topic1"
                                    creation-mode: create-if-not-exists
                                  - name: "topic2"
                                    creation-mode: create-if-not-exists
                                  - name: "default-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Dispatch"
                                    type: "dispatch"
                                    input: input-topic
                                    output: default-topic
                                    id: step1
                                    configuration:
                                      routes:
                                         - when: properties.language == "en"
                                           destination: topic1
                                         - when: properties.language == "fr"
                                           destination: topic2
                                         - when: properties.language == "none"
                                           action: drop
                                """);

        // query the database with re-rank
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("default-topic");
                    KafkaConsumer<String, String> consumer1 = createConsumer("topic1");
                    KafkaConsumer<String, String> consumer2 = createConsumer("topic2")) {

                sendMessage(
                        "input-topic",
                        "for-default",
                        List.of(new RecordHeader("language", "it".getBytes())),
                        producer);
                sendMessage(
                        "input-topic",
                        "for-topic1",
                        List.of(new RecordHeader("language", "en".getBytes())),
                        producer);
                sendMessage(
                        "input-topic",
                        "for-topic2",
                        List.of(new RecordHeader("language", "fr".getBytes())),
                        producer);
                executeAgentRunners(applicationRuntime);
                waitForMessages(consumer, List.of("for-default"));
                waitForMessages(consumer1, List.of("for-topic1"));
                waitForMessages(consumer2, List.of("for-topic2"));
            }
        }
    }

    /**
     * This test validates that the default destination is not mandatory. if you don't set an
     * "output" the processor behaves like a sink (no output)
     */
    @Test
    public void testDispatchNoDefaultDestination() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "input-topic-no-default"
                                    creation-mode: create-if-not-exists
                                  - name: "topic1-no-default"
                                    creation-mode: create-if-not-exists
                                  - name: "topic2-no-default"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Dispatch"
                                    type: "dispatch"
                                    input: input-topic-no-default
                                    id: step1
                                    configuration:
                                      routes:
                                         - when: properties.language == "en"
                                           destination: topic1-no-default
                                         - when: properties.language == "fr"
                                           destination: topic2-no-default
                                         - when: properties.language == "none"
                                           action: drop
                                """);

        // query the database with re-rank
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer1 = createConsumer("topic1-no-default");
                    KafkaConsumer<String, String> consumer2 = createConsumer("topic2-no-default")) {

                sendMessage(
                        "input-topic-no-default",
                        "for-default",
                        List.of(new RecordHeader("language", "it".getBytes())),
                        producer);
                sendMessage(
                        "input-topic-no-default",
                        "for-topic1",
                        List.of(new RecordHeader("language", "en".getBytes())),
                        producer);
                sendMessage(
                        "input-topic-no-default",
                        "for-topic2",
                        List.of(new RecordHeader("language", "fr".getBytes())),
                        producer);
                // executeAgentRunners validates that the source has been fully committed
                executeAgentRunners(applicationRuntime);
                waitForMessages(consumer1, List.of("for-topic1"));
                waitForMessages(consumer2, List.of("for-topic2"));
            }
        }
    }

    @Test
    public void testDispatchDefaultToAnotherAgent() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "input-topic-to-agent"
                                    creation-mode: create-if-not-exists
                                  - name: "topic1-to-agent"
                                    creation-mode: create-if-not-exists
                                  - name: "topic2-to-agent"
                                    creation-mode: create-if-not-exists
                                  - name: "default-topic-to-agent"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Dispatch"
                                    type: "dispatch"
                                    input: input-topic-to-agent
                                    id: step1
                                    configuration:
                                      routes:
                                         - when: properties.language == "en"
                                           destination: topic1-to-agent
                                           action: dispatch
                                         - when: properties.language == "fr"
                                           destination: topic2-to-agent
                                         - when: properties.language == "none"
                                           action: drop
                                  - name: "Compute"
                                    type: "compute"
                                    output: default-topic-to-agent
                                    id: step1
                                    configuration:
                                      fields:
                                         - name: "value"
                                           expression: "'modified'"
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer =
                            createConsumer("default-topic-to-agent");
                    KafkaConsumer<String, String> consumer1 = createConsumer("topic1-to-agent");
                    KafkaConsumer<String, String> consumer2 = createConsumer("topic2-to-agent")) {

                sendMessage(
                        "input-topic-to-agent",
                        "for-default",
                        List.of(new RecordHeader("language", "it".getBytes())),
                        producer);
                sendMessage(
                        "input-topic-to-agent",
                        "for-topic1",
                        List.of(new RecordHeader("language", "en".getBytes())),
                        producer);
                sendMessage(
                        "input-topic-to-agent",
                        "for-topic2",
                        List.of(new RecordHeader("language", "fr".getBytes())),
                        producer);
                executeAgentRunners(applicationRuntime);
                waitForMessages(consumer, List.of("modified"));
                waitForMessages(consumer1, List.of("for-topic1"));
                waitForMessages(consumer2, List.of("for-topic2"));
            }
        }
    }

    @Test
    public void testTimerSource() throws Exception {
        setMaxNumLoops(10);
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "timer-source-output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Timer"
                                    type: "timer-source"
                                    id: step1
                                    output: timer-source-output-topic
                                    configuration:
                                      period-seconds: 1
                                      fields:
                                         - name: "key.id"
                                           expression: "fn:uuid()"
                                         - name: "value.stringpayload"
                                           expression: "'constant-payload'"
                                         - name: "value.intpayload"
                                           expression: "42"
                                         - name: "properties.foo"
                                           expression: "'bar'"
                                """);
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaConsumer<String, String> consumer =
                    createConsumer("timer-source-output-topic"); ) {
                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        new BiConsumer<List<ConsumerRecord>, List<Object>>() {
                            @Override
                            @SneakyThrows
                            public void accept(
                                    List<ConsumerRecord> consumerRecords, List<Object> objects) {
                                assertTrue(objects.size() > 1);
                                Object o = objects.get(0);
                                log.info("Received {}", o);
                                assertEquals(
                                        "{\"intpayload\":42,\"stringpayload\":\"constant-payload\"}",
                                        o);
                                ConsumerRecord consumerRecord = consumerRecords.get(0);
                                Object key = consumerRecord.key();
                                log.info("Received key {}", key);
                                Map<String, Object> jsonKey =
                                        new ObjectMapper().readValue(key.toString(), Map.class);
                                // try to parse the UUID
                                UUID.fromString(jsonKey.get("id").toString());
                                assertEquals(
                                        "bar",
                                        new String(
                                                consumerRecord.headers().lastHeader("foo").value(),
                                                StandardCharsets.UTF_8));
                            }
                        });
            }
        }
    }

    @Test
    public void testTriggerEventProcessor() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "input-topic-splitter"
                                    creation-mode: create-if-not-exists
                                  - name: "side-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic-chunks"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Chunk some text"
                                    id: step1
                                    type: "text-splitter"
                                    input: input-topic-splitter
                                    configuration:
                                      chunk_size: 5
                                      chunk_overlap: 0
                                  - name: "Trigger event on last chunk"
                                    type: "trigger-event"
                                    output: output-topic-chunks
                                    configuration:
                                      destination: side-topic
                                      when: fn:toInt(properties.text_num_chunks) == (fn:toInt(properties.chunk_id) + 1)
                                      fields:
                                         - name: "properties.foo"
                                           expression: "'bar'"
                                """);
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic-chunks");
                    KafkaConsumer<String, String> consumerSideTopic =
                            createConsumer("side-topic"); ) {

                sendMessage("input-topic-splitter", "some very long text. end", producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(consumer, List.of("some very long", "text. end"));

                // the side topic receives only the last chunk
                waitForMessages(consumerSideTopic, List.of("text. end"));
            }
        }
    }
}
