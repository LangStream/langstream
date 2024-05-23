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
package ai.langstream.pravega;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import ai.langstream.AbstractApplicationRunner;
import ai.langstream.kafka.AbstractKafkaApplicationRunner;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class PulsarRunnerDockerTest extends AbstractApplicationRunner {

    @RegisterExtension
    static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

    @Test
    public void simpleTest() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                module: "module-1"
                id: "pipeline-1"
                topics:
                  - name: "%s"
                    creation-mode: create-if-not-exists
                  - name: "%s"
                    creation-mode: create-if-not-exists
                pipeline:
                  - name: "drop-description"
                    id: "step1"
                    type: "drop-fields"
                    input: "%s"
                    output: "%s"
                    configuration:
                      fields:
                        - "description"
                """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant,
                        "app",
                        application,
                        buildInstanceYaml("public", "default"),
                        expectedAgents)) {
            try (Producer<String> producer = createProducer(inputTopic);
                    Consumer<GenericRecord> consumer = createConsumer(outputTopic)) {

                producer.newMessage()
                        .value("{\"name\": \"some name\", \"description\": \"some description\"}")
                        .property("header-key", "header-value")
                        .send();
                producer.flush();

                executeAgentRunners(applicationRuntime);

                Message<GenericRecord> record = consumer.receive(30, TimeUnit.SECONDS);
                assertEquals("{\"name\":\"some name\"}", record.getValue().getNativeObject());
                assertEquals("header-value", record.getProperties().get("header-key"));
            }
        }
    }

    @Test
    public void simpleTestDifferentTenant() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();

        PulsarAdmin admin = pulsarContainer.getAdmin();

        TenantInfo info =
                TenantInfo.builder()
                        .allowedClusters(new HashSet<>(admin.clusters().getClusters()))
                        .build();
        admin.tenants().createTenant("mytenant", info);
        admin.namespaces().createNamespace("mytenant/mynamespace");

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                module: "module-1"
                id: "pipeline-1"
                topics:
                  - name: "%s"
                    creation-mode: create-if-not-exists
                  - name: "%s"
                    creation-mode: create-if-not-exists
                pipeline:
                  - name: "drop-description"
                    id: "step1"
                    type: "drop-fields"
                    input: "%s"
                    output: "%s"
                    configuration:
                      fields:
                        - "description"
                """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant,
                        "app",
                        application,
                        buildInstanceYaml("mytenant", "mynamespace"),
                        expectedAgents)) {
            try (Producer<String> producer = createProducer("mytenant/mynamespace/" + inputTopic);
                    Consumer<GenericRecord> consumer =
                            createConsumer("mytenant/mynamespace/" + outputTopic)) {

                producer.newMessage()
                        .value("{\"name\": \"some name\", \"description\": \"some description\"}")
                        .property("header-key", "header-value")
                        .send();
                producer.flush();

                executeAgentRunners(applicationRuntime);

                Message<GenericRecord> record = consumer.receive(30, TimeUnit.SECONDS);
                assertEquals("{\"name\":\"some name\"}", record.getValue().getNativeObject());
                assertEquals("header-value", record.getProperties().get("header-key"));
            }
        }
    }

    @Test
    public void testTopicSchema() throws Exception {
        String tenant = "topic-schema";
        String[] expectedAgents = {"app-step1"};
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                module: "module-1"
                id: "pipeline-1"
                topics:
                  - name: "%s"
                    creation-mode: create-if-not-exists
                  - name: "%s"
                    creation-mode: create-if-not-exists
                    schema:
                      type: "bytes"
                pipeline:
                  - name: "drop-description"
                    id: "step1"
                    type: "drop-fields"
                    input: "%s"
                    output: "%s"
                    configuration:
                      fields:
                        - "description"
                """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant,
                        "app",
                        application,
                        buildInstanceYaml("public", "default"),
                        expectedAgents)) {
            try (Producer<String> producer = createProducer(inputTopic);
                    Consumer<GenericRecord> consumer = createConsumer(outputTopic)) {

                producer.newMessage()
                        .value("{\"name\": \"some name\", \"description\": \"some description\"}")
                        .send();
                producer.flush();

                executeAgentRunners(applicationRuntime);

                Message<GenericRecord> record = consumer.receive(30, TimeUnit.SECONDS);
                assertArrayEquals(
                        "{\"name\":\"some name\"}".getBytes(StandardCharsets.UTF_8),
                        (byte[]) record.getValue().getNativeObject());
            }
        }
    }

    @Test
    public void testKeyValueSchema() throws Exception {
        String tenant = "topic-schema";
        String[] expectedAgents = {"app-step1"};
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
        module: "module-1"
        id: "pipeline-1"
        topics:
          - name: "%s"
            creation-mode: create-if-not-exists
            schema:
              type: "string"
            keySchema:
              type: "string"
          - name: "%s"
            creation-mode: create-if-not-exists
            schema:
              type: "string"
            keySchema:
              type: "string"
        pipeline:
          - id: "step1"
            type: "identity"
            input: "%s"
            output: "%s"
        """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant,
                        "app",
                        application,
                        buildInstanceYaml("public", "default"),
                        expectedAgents)) {
            try (Producer<KeyValue<String, String>> producer =
                            createProducer(
                                    inputTopic,
                                    Schema.KeyValue(
                                            Schema.STRING,
                                            Schema.STRING,
                                            KeyValueEncodingType.SEPARATED));
                    Consumer<GenericRecord> consumer = createConsumer(outputTopic)) {

                producer.newMessage().value(new KeyValue<>("key", "value")).send();
                producer.flush();

                executeAgentRunners(applicationRuntime);

                Message<GenericRecord> record = consumer.receive(30, TimeUnit.SECONDS);
                Object value = record.getValue().getNativeObject();
                assertInstanceOf(KeyValue.class, value);
                assertEquals("key", ((KeyValue<?, ?>) value).getKey());
                assertEquals("value", ((KeyValue<?, ?>) value).getValue());
            }
        }
    }

    @Test
    public void testDeadLetter() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                        module: "module-1"
                        id: "pipeline-1"
                        topics:
                          - name: "%s"
                            creation-mode: create-if-not-exists
                          - name: "%s"
                            creation-mode: create-if-not-exists
                        pipeline:
                          - name: "some agent"
                            id: "step1"
                            type: "mock-failing-processor"
                            input: "%s"
                            output: "%s"
                            errors:
                                on-failure: dead-letter
                            configuration:
                              fail-on-content: "fail-me"
                        """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));
        setMaxNumLoops(25);
        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant,
                        "app",
                        application,
                        buildInstanceYaml("public", "default"),
                        expectedAgents)) {
            try (Producer<String> producer = createProducer(inputTopic);
                    Consumer<GenericRecord> consumer = createConsumer(outputTopic);
                    Consumer<GenericRecord> consumerDeadletter =
                            createConsumer(inputTopic + "-deadletter")) {

                List<Object> expectedMessages = new ArrayList<>();
                List<Object> expectedMessagesDeadletter = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    producer.newMessage().value("fail-me-" + i).send();
                    producer.newMessage().value("keep-me-" + i).send();
                    expectedMessages.add("keep-me-" + i);
                    expectedMessagesDeadletter.add("fail-me-" + i);
                }
                producer.flush();

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumerDeadletter, expectedMessagesDeadletter);
                waitForMessages(consumer, expectedMessages);
            }
        }
    }

    private String buildInstanceYaml(String tenant, String namespace) {
        return """
                     instance:
                       streamingCluster:
                         type: "pulsar"
                         configuration:
                           admin:
                             serviceUrl: "%s"
                           service:
                             serviceUrl: "%s"
                           default-tenant: "%s"
                           default-namespace: "%s"
                       computeCluster:
                         type: "kubernetes"
                     """
                .formatted(
                        pulsarContainer.getHttpServiceUrl(),
                        pulsarContainer.getBrokerUrl(),
                        tenant,
                        namespace);
    }

    protected Producer<String> createProducer(String topic) throws PulsarClientException {
        return createProducer(topic, Schema.STRING);
    }

    protected <T> Producer<T> createProducer(String topic, Schema<T> schema)
            throws PulsarClientException {
        return pulsarContainer.getClient().newProducer(schema).topic(topic).create();
    }

    protected Consumer<GenericRecord> createConsumer(String topic) throws PulsarClientException {
        return pulsarContainer
                .getClient()
                .newConsumer(Schema.AUTO_CONSUME())
                .topic(topic)
                .subscriptionName("test-subscription")
                .subscribe();
    }

    protected List<Message<GenericRecord>> waitForMessages(
            Consumer<GenericRecord> consumer, List<?> expected) {
        return waitForMessages(
                consumer,
                (result, received) -> {
                    assertEquals(expected.size(), received.size());
                    for (int i = 0; i < expected.size(); i++) {
                        Object expectedValue = expected.get(i);
                        Object actualValue = received.get(i);
                        if (expectedValue instanceof java.util.function.Consumer fn) {
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

    protected List<Message<GenericRecord>> waitForMessages(
            Consumer<GenericRecord> consumer,
            BiConsumer<List<Message<GenericRecord>>, List<Object>> assertionOnReceivedMessages) {
        List<Message<GenericRecord>> result = new ArrayList<>();
        List<Object> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Message<GenericRecord> message = consumer.receive(2, TimeUnit.SECONDS);
                            if (message != null) {
                                log.info("Received message {}", message);
                                received.add(message.getValue().getNativeObject());
                                result.add(message);
                            }
                            log.info("Result:  {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            assertionOnReceivedMessages.accept(result, received);
                        });

        return result;
    }
}
