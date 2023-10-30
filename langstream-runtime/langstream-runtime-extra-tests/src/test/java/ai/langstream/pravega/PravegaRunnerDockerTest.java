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

import ai.langstream.AbstractApplicationRunner;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeAndLoader;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.Topic;
import ai.langstream.kafka.AbstractKafkaApplicationRunner;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class PravegaRunnerDockerTest extends AbstractApplicationRunner {

    @RegisterExtension
    static PravegaContainerExtension pravegaContainer = new PravegaContainerExtension();

    @Test
    public void testRunAgent() throws Exception {
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
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            TopicConnectionsRuntime topicConnectionsRuntime = applicationDeployer.getTopicConnectionsRuntimeRegistry()
                    .getTopicConnectionsRuntime(applicationRuntime.applicationInstance().getInstance().streamingCluster())
                    .asTopicConnectionsRuntime();
            topicConnectionsRuntime.init(applicationRuntime.applicationInstance().getInstance().streamingCluster());
            PravegaTopic inputTopicHandle = (PravegaTopic) applicationRuntime.implementation().getTopicByName(inputTopic);
            try (TopicProducer producer = topicConnectionsRuntime.createProducer("test",
                    applicationRuntime.applicationInstance().getInstance().streamingCluster(),
                    inputTopicHandle.createProducerConfiguration())) {
                producer.start();

                producer.write(SimpleRecord.of(null,
                                "{\"name\": \"some name\", \"description\": \"some description\"}")).get();

                executeAgentRunners(applicationRuntime);

                try (EventStreamReader<String> consumer = createConsumer(outputTopic)) {
                    waitForMessages(consumer, List.of("{\"name\": \"some name\"}"));
                }
            }
        }
    }

    @Test
    @Disabled
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
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            TopicConnectionsRuntime topicConnectionsRuntime = applicationDeployer.getTopicConnectionsRuntimeRegistry()
                    .getTopicConnectionsRuntime(applicationRuntime.applicationInstance().getInstance().streamingCluster())
                    .asTopicConnectionsRuntime();
            topicConnectionsRuntime.init(applicationRuntime.applicationInstance().getInstance().streamingCluster());
            PravegaTopic inputTopicHandle = (PravegaTopic) applicationRuntime.implementation().getTopicByName(inputTopic);
            try (TopicProducer producer = topicConnectionsRuntime.createProducer("test",
                    applicationRuntime.applicationInstance().getInstance().streamingCluster(),
                    inputTopicHandle.createProducerConfiguration())) {
                producer.start();
                try (EventStreamReader<String> consumer = createConsumer(outputTopic);
                     EventStreamReader<String> consumerDeadletter =
                             createConsumer(inputTopic + "-deadletter")) {

                    List<Object> expectedMessages = new ArrayList<>();
                    List<Object> expectedMessagesDeadletter = new ArrayList<>();
                    for (int i = 0; i < 10; i++) {
                        producer.write(SimpleRecord.of(null, "fail-me-" + i)).get();
                        producer.write(SimpleRecord.of(null, "keep-me-" + i)).get();
                        expectedMessages.add("keep-me-" + i);
                        expectedMessagesDeadletter.add("fail-me-" + i);
                    }

                    executeAgentRunners(applicationRuntime);

                    waitForMessages(consumerDeadletter, expectedMessagesDeadletter);
                    waitForMessages(consumer, expectedMessages);
                }
            }
        }
    }

    private String buildInstanceYaml() {
        return """
                     instance:
                       streamingCluster:
                         type: "pravega"
                         configuration:
                           client:
                             controller-uri: "%s"
                             scope: "langstream"
                       computeCluster:
                         type: "kubernetes"
                     """
                .formatted(pravegaContainer.getControllerUri());
    }

    protected EventStreamWriter<String> createProducer(String topic) throws Exception {
        return pravegaContainer
                .getClient()
                .createEventWriter(
                        topic, new UTF8StringSerializer(), EventWriterConfig.builder().build());
    }

    protected EventStreamReader<String> createConsumer(String topic) throws Exception {
        return pravegaContainer
                .getClient()
                .createReader(
                        "test", topic, new UTF8StringSerializer(), ReaderConfig.builder().build());
    }

    protected List<String> waitForMessages(EventStreamReader<String> consumer, List<?> expected) {
        return waitForMessages(
                consumer,
                (received) -> {
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

    protected List<String> waitForMessages(
            EventStreamReader<String> consumer,
            Consumer<List<String>> assertionOnReceivedMessages) {
        List<String> received = new ArrayList<>();

        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            EventRead<String> message = consumer.readNextEvent(2000);
                            if (message != null) {
                                log.info("Received message {}", message);
                                received.add(message.getEvent());
                            }
                            log.info("Result:  {}", received);
                            received.forEach(r -> log.info("Received |{}|", r));

                            assertionOnReceivedMessages.accept(received);
                        });

        return received;
    }
}
