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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.AbstractApplicationRunner;
import ai.langstream.kafka.extensions.KafkaContainerExtension;
import ai.langstream.runtime.agent.api.AgentAPIController;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
public abstract class AbstractKafkaApplicationRunner extends AbstractApplicationRunner {

    @RegisterExtension
    protected static final KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    private volatile boolean validateConsumerOffsets = true;

    public boolean isValidateConsumerOffsets() {
        return validateConsumerOffsets;
    }

    public void setValidateConsumerOffsets(boolean validateConsumerOffsets) {
        this.validateConsumerOffsets = validateConsumerOffsets;
    }

    protected String buildInstanceYaml() {
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String streamTopic = "stream-topic-" + UUID.randomUUID();
        return """
                instance:
                  globals:
                    input-topic: %s
                    output-topic: %s
                    stream-topic: %s
                  streamingCluster:
                    type: "kafka"
                    configuration:
                      admin:
                        bootstrap.servers: "%s"
                  computeCluster:
                     type: "kubernetes"
                """
                .formatted(
                        inputTopic, outputTopic, streamTopic, kafkaContainer.getBootstrapServers());
    }

    protected KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(
                Map.of(
                        "bootstrap.servers",
                        kafkaContainer.getBootstrapServers(),
                        "key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer"));
    }

    protected void sendMessage(String topic, Object content, KafkaProducer producer)
            throws Exception {
        sendMessage(topic, content, List.of(), producer);
    }

    protected void sendMessage(
            String topic, Object content, List<Header> headers, KafkaProducer producer)
            throws Exception {
        sendMessage(topic, "key", content, headers, producer);
    }

    protected void sendMessage(
            String topic, Object key, Object content, List<Header> headers, KafkaProducer producer)
            throws Exception {
        producer.send(
                        new ProducerRecord<>(
                                topic, null, System.currentTimeMillis(), key, content, headers))
                .get();
        producer.flush();
    }

    protected List<ConsumerRecord> waitForMessages(
            KafkaConsumer consumer,
            BiConsumer<List<ConsumerRecord>, List<Object>> assertionOnReceivedMessages) {
        List<ConsumerRecord> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> poll =
                                    consumer.poll(Duration.ofSeconds(2));
                            for (ConsumerRecord record : poll) {
                                log.info("Received message {}", record);
                                received.add(record.value());
                                result.add(record);
                            }
                            log.info("Result:  {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            assertionOnReceivedMessages.accept(result, received);
                        });

        return result;
    }

    protected List<ConsumerRecord> waitForMessages(KafkaConsumer consumer, List<?> expected) {
        return waitForMessages(
                consumer,
                (result, received) -> {
                    assertEquals(expected.size(), received.size());
                    for (int i = 0; i < expected.size(); i++) {
                        Object expectedValue = expected.get(i);
                        Object actualValue = received.get(i);
                        if (expectedValue instanceof Consumer fn) {
                            fn.accept(actualValue);
                        } else if (expectedValue instanceof byte[]) {
                            assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
                        } else {
                            log.info("expected: {}", expectedValue);
                            log.info("got: {}", actualValue);
                            assertEquals(expectedValue, actualValue);
                        }
                    }
                });
    }

    protected List<ConsumerRecord> waitForMessagesInAnyOrder(
            KafkaConsumer consumer, Collection<String> expected) {
        List<ConsumerRecord> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            ConsumerRecords<String, String> poll =
                                    consumer.poll(Duration.ofSeconds(2));
                            for (ConsumerRecord record : poll) {
                                log.info("Received message {}", record);
                                received.add(record.value());
                                result.add(record);
                            }
                            log.info("Result: {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            assertEquals(expected.size(), received.size());
                            for (Object expectedValue : expected) {
                                // this doesn't work for byte[]
                                assertFalse(expectedValue instanceof byte[]);
                                assertTrue(
                                        received.contains(expectedValue),
                                        "Expected value "
                                                + expectedValue
                                                + " not found in "
                                                + received);
                            }

                            for (Object receivedValue : received) {
                                // this doesn't work for byte[]
                                assertFalse(receivedValue instanceof byte[]);
                                assertTrue(
                                        expected.contains(receivedValue),
                                        "Received value "
                                                + receivedValue
                                                + " not found in "
                                                + expected);
                            }
                        });

        return result;
    }

    protected KafkaConsumer<String, String> createConsumer(String topic) {
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(
                        Map.of(
                                "bootstrap.servers",
                                kafkaContainer.getBootstrapServers(),
                                "key.deserializer",
                                "org.apache.kafka.common.serialization.StringDeserializer",
                                "value.deserializer",
                                "org.apache.kafka.common.serialization.StringDeserializer",
                                "group.id",
                                "testgroup-" + UUID.randomUUID(),
                                "auto.offset.reset",
                                "earliest"));
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    protected static AdminClient getKafkaAdmin() {
        return kafkaContainer.getAdmin();
    }

    @Override
    protected void validateAgentInfoBeforeStop(AgentAPIController agentAPIController) {
        if (!validateConsumerOffsets) {
            return;
        }
        agentAPIController
                .serveWorkerStatus()
                .forEach(
                        workerStatus -> {
                            String agentType = workerStatus.getAgentType();
                            log.info("Checking Agent type {}", agentType);
                            switch (agentType) {
                                case "topic-source":
                                    Map<String, Object> info = workerStatus.getInfo();
                                    log.info("Topic source info {}", info);
                                    Map<String, Object> consumerInfo =
                                            (Map<String, Object>) info.get("consumer");
                                    if (consumerInfo != null) {
                                        Map<String, Object> committedOffsets =
                                                (Map<String, Object>)
                                                        consumerInfo.get("committedOffsets");
                                        log.info("Committed offsets {}", committedOffsets);
                                        committedOffsets.forEach(
                                                (topic, offset) -> {
                                                    assertNotNull(offset);
                                                    assertTrue(((Number) offset).intValue() >= 0);
                                                });
                                        Map<String, Object> uncommittedOffsets =
                                                (Map<String, Object>)
                                                        consumerInfo.get("uncommittedOffsets");
                                        log.info("Uncommitted offsets {}", uncommittedOffsets);
                                        uncommittedOffsets.forEach(
                                                (topic, number) -> {
                                                    assertNotNull(number);
                                                    assertTrue(
                                                            ((Number) number).intValue() <= 0,
                                                            "for topic "
                                                                    + topic
                                                                    + " we have some uncommitted offsets: "
                                                                    + number);
                                                });
                                    }
                                default:
                                    // ignore
                            }
                        });
    }
}
