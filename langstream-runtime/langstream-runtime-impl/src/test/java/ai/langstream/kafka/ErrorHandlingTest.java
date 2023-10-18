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
import static org.junit.jupiter.api.Assertions.fail;

import ai.langstream.mockagents.MockProcessorAgentsCodeProvider;
import ai.langstream.runtime.agent.AgentRunner;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

@Slf4j
class ErrorHandlingTest extends AbstractKafkaApplicationRunner {

    @Test
    public void testDiscardErrors() throws Exception {
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String tenant = "tenant";
        String[] expectedAgents = {"app-step"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 10
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                errors:
                                    on-failure: fail
                                    retries: 5
                                pipeline:
                                  - name: "some agent"
                                    id: "step"
                                    type: "mock-failing-processor"
                                    input: "%s"
                                    output: "%s"
                                    errors:
                                        on-failure: skip
                                        retries: 3
                                    configuration:
                                      fail-on-content: "fail-me"
                                """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));
        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                sendMessage(inputTopic, "fail-me", producer);
                sendMessage(inputTopic, "keep-me", producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of("keep-me"));
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
        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic);
                    KafkaConsumer<String, String> consumerDeadletter =
                            createConsumer(inputTopic + "-deadletter")) {

                List<Object> expectedMessages = new ArrayList<>();
                List<Object> expectedMessagesDeadletter = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    sendMessage(inputTopic, "fail-me-" + i, producer);
                    sendMessage(inputTopic, "keep-me-" + i, producer);
                    expectedMessages.add("keep-me-" + i);
                    expectedMessagesDeadletter.add("fail-me-" + i);
                }

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumerDeadletter, expectedMessagesDeadletter);
                waitForMessages(consumer, expectedMessages);
            }
        }
    }

    @Test
    public void testFailOnErrors() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step"};
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
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 10
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                errors:
                                    on-failure: fail
                                    retries: 5
                                pipeline:
                                  - name: "some agent"
                                    id: "step"
                                    type: "mock-failing-processor"
                                    input: "%s"
                                    output: "%s"
                                    configuration:
                                      fail-on-content: "fail-me"
                                """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));
        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {

                sendMessage(inputTopic, "fail-me", producer);
                sendMessage(inputTopic, "keep-me", producer);

                try {
                    setValidateConsumerOffsets(false);
                    executeAgentRunners(applicationRuntime);
                    fail("Expected an exception");
                } catch (AgentRunner.PermanentFailureException e) {
                    // expected
                    assertEquals("Failing on content: fail-me", e.getCause().getMessage());
                } finally {
                    setValidateConsumerOffsets(true);
                }

                // the pipeline is stuck, always failing on the first message

                try {
                    setValidateConsumerOffsets(false);
                    executeAgentRunners(applicationRuntime);
                    fail("Expected an exception");
                } catch (AgentRunner.PermanentFailureException e) {
                    // expected
                    assertEquals("Failing on content: fail-me", e.getCause().getMessage());
                } finally {
                    setValidateConsumerOffsets(true);
                }
            }
        }
    }

    @Test
    public void testDiscardErrorsOnSink() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step"};
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
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 10
                                errors:
                                    on-failure: fail
                                    retries: 5
                                pipeline:
                                  - name: "some agent"
                                    id: "step"
                                    type: "mock-failing-sink"
                                    input: "%s"
                                    errors:
                                        on-failure: skip
                                        retries: 3
                                    configuration:
                                      fail-on-content: "fail-me"
                                """
                                .formatted(inputTopic, inputTopic));
        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer(); ) {

                sendMessage(inputTopic, "fail-me", producer);
                sendMessage(inputTopic, "keep-me", producer);

                executeAgentRunners(applicationRuntime);

                Awaitility.await()
                        .untilAsserted(
                                () -> {
                                    assertEquals(
                                            1,
                                            MockProcessorAgentsCodeProvider.FailingSink
                                                    .acceptedRecords
                                                    .size());
                                });
            }
        }
    }

    @Test
    public void testFailOnErrorsOnSink() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step"};
        String inputTopic = "input-topic-" + UUID.randomUUID();

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 10
                                errors:
                                    on-failure: skip
                                    retries: 5
                                pipeline:
                                  - name: "some agent"
                                    id: "step"
                                    type: "mock-failing-sink"
                                    input: "%s"
                                    errors:
                                        on-failure: fail
                                        retries: 3
                                    configuration:
                                      fail-on-content: "fail-me"
                                """
                                .formatted(inputTopic, inputTopic));
        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {

                sendMessage(inputTopic, "fail-me", producer);
                sendMessage(inputTopic, "keep-me", producer);

                try {
                    executeAgentRunners(applicationRuntime);
                    fail("Expected an exception");
                } catch (AgentRunner.PermanentFailureException e) {
                    // expected
                    assertEquals("Failing on content: fail-me", e.getCause().getMessage());
                }

                // the pipeline is stuck, always failing on the first message

                try {
                    executeAgentRunners(applicationRuntime);
                    fail("Expected an exception");
                } catch (AgentRunner.PermanentFailureException e) {
                    // expected
                    assertEquals("Failing on content: fail-me", e.getCause().getMessage());
                }
            }
        }
    }

    @Test
    public void testDeadLetterOnSink() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        String inputTopic = "input-topic-" + UUID.randomUUID();

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "some agent"
                                    id: "step1"
                                    type: "mock-failing-sink"
                                    input: "%s"
                                    errors:
                                        on-failure: dead-letter
                                        retries: 3
                                    configuration:
                                      fail-on-content: "fail-me"
                                """
                                .formatted(inputTopic, inputTopic));
        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumerDeadletter =
                            createConsumer(inputTopic + "-deadletter")) {

                List<Object> expectedMessages = new ArrayList<>();
                List<Object> expectedMessagesDeadletter = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    sendMessage(inputTopic, "fail-me-" + i, producer);
                    sendMessage(inputTopic, "keep-me-" + i, producer);
                    expectedMessages.add("keep-me-" + i);
                    expectedMessagesDeadletter.add("fail-me-" + i);
                }

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumerDeadletter, expectedMessagesDeadletter);

                Awaitility.await()
                        .untilAsserted(
                                () -> {
                                    assertEquals(
                                            10,
                                            MockProcessorAgentsCodeProvider.FailingSink
                                                    .acceptedRecords
                                                    .size());
                                });
            }
        }
    }
}
