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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.langstream.mockagents.MockProcessorAgentsCodeProvider;
import ai.langstream.runtime.agent.AgentRunner;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
class AsyncProcessingIT extends AbstractKafkaApplicationRunner {

    @Test
    public void testProcessMultiThreadOutOfOrder() throws Exception {
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
                                    partitions: 4
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    partitions: 2
                                pipeline:
                                  - name: "async-process-records"
                                    id: "step1"
                                    type: "mock-async-processor"
                                    input: "%s"
                                    output: "%s"
                                """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                Set<String> expected = new HashSet<>();
                int numMessages = 100;
                for (int i = 0; i < numMessages; i++) {
                    String content = "test message " + i;
                    expected.add(content);
                    sendMessage(inputTopic, content, producer);
                }

                executeAgentRunners(applicationRuntime);

                waitForMessagesInAnyOrder(consumer, expected);
            }
        }
    }

    @Test
    public void testCompositeMultiStepProcessMultiThreadOutOfOrder() throws Exception {
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
                                    partitions: 4
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    partitions: 2
                                pipeline:
                                  - name: "async-process-records"
                                    id: "step1"
                                    type: "mock-async-processor"
                                    input: "%s"
                                  - name: "mock-failing-processor"
                                    id: "step2"
                                    type: "mock-failing-processor"
                                  - name: "async-process-records"
                                    id: "step3"
                                    type: "mock-async-processor"
                                  - name: "mock-failing-processor"
                                    id: "step4"
                                    type: "mock-failing-processor"
                                    output: "%s"
                                """
                                .formatted(inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                Set<String> expected = new HashSet<>();
                int numMessages = 100;
                for (int i = 0; i < numMessages; i++) {
                    String content = "test message " + i;
                    expected.add(content);
                    sendMessage(inputTopic, content, producer);
                }

                executeAgentRunners(applicationRuntime);

                waitForMessagesInAnyOrder(consumer, expected);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    public void testCompositeMultiStepProcessMultiThreadOutOfOrderWithFailureAndSkip(int retries)
            throws Exception {
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
                                errors:
                                   on-failure: skip
                                   retries: %d
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    partitions: 4
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    partitions: 2
                                pipeline:
                                  - name: "mock-failing-processor"
                                    type: "mock-failing-processor"
                                    id: "step1"
                                    input: "%s"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-beginning"
                                  - name: "async-process-records"
                                    id: "step2"
                                    type: "mock-async-processor"
                                  - name: "mock-failing-processor"
                                    id: "step3"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-middle"
                                    type: "mock-failing-processor"
                                  - name: "async-process-records"
                                    id: "step4"
                                    type: "mock-async-processor"
                                  - name: "mock-failing-processor"
                                    id: "step5"
                                    type: "mock-failing-processor"
                                    output: "%s"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-end"
                                """
                                .formatted(
                                        retries, inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                Set<String> expected = new HashSet<>();
                int numMessages = 100;
                Random random = new Random();
                for (int i = 0; i < numMessages; i++) {
                    String content;
                    int num = random.nextInt(5);
                    switch (num) {
                        case 0:
                            content = "fail-this-message-in-the-beginning";
                            break;
                        case 1:
                            content = "fail-this-message-in-the-middle";
                            break;
                        case 2:
                            content = "fail-this-message-in-the-end";
                            break;
                        default:
                            content = "test message " + i;
                            expected.add(content);
                            break;
                    }
                    sendMessage(inputTopic, content, producer);
                }

                executeAgentRunners(applicationRuntime);

                waitForMessagesInAnyOrder(consumer, expected);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    public void testCompositeMultiStepProcessMultiThreadOutOfOrderWithFailureAndDeadletter(
            int retries) throws Exception {
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
                                errors:
                                   on-failure: dead-letter
                                   retries: %d
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    partitions: 4
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    partitions: 2
                                pipeline:
                                  - name: "mock-failing-processor"
                                    type: "mock-failing-processor"
                                    id: "step1"
                                    input: "%s"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-beginning"
                                  - name: "async-process-records"
                                    id: "step2"
                                    type: "mock-async-processor"
                                  - name: "mock-failing-processor"
                                    id: "step3"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-middle"
                                    type: "mock-failing-processor"
                                  - name: "async-process-records"
                                    id: "step4"
                                    type: "mock-async-processor"
                                  - name: "mock-failing-processor"
                                    id: "step5"
                                    type: "mock-failing-processor"
                                    output: "%s"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-end"
                                """
                                .formatted(
                                        retries, inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic);
                    KafkaConsumer<String, String> consumerDeadletter =
                            createConsumer(inputTopic + "-deadletter")) {

                Set<String> expected = new HashSet<>();
                Set<String> expectedOnDeadletter = new HashSet<>();
                int numMessages = 100;
                Random random = new Random();
                for (int i = 0; i < numMessages; i++) {
                    String content;
                    int num = random.nextInt(5);
                    switch (num) {
                        case 0:
                            content = "fail-this-message-in-the-beginning-" + i;
                            expectedOnDeadletter.add(content);
                            break;
                        case 1:
                            content = "fail-this-message-in-the-middle-" + i;
                            expectedOnDeadletter.add(content);
                            break;
                        case 2:
                            content = "fail-this-message-in-the-end-" + i;
                            expectedOnDeadletter.add(content);
                            break;
                        default:
                            content = "test message " + i;
                            expected.add(content);
                            break;
                    }
                    sendMessage(inputTopic, content, producer);
                }

                executeAgentRunners(applicationRuntime);

                waitForMessagesInAnyOrder(consumer, expected);
                waitForMessagesInAnyOrder(consumerDeadletter, expectedOnDeadletter);
            }
        }
    }

    static Object[][] failuresAndRetries() {
        return new Object[][] {
            new Object[] {"fail-this-message-in-the-beginning", 0},
            new Object[] {"fail-this-message-in-the-beginning", 1},
            new Object[] {"fail-this-message-in-the-beginning", 2},
            new Object[] {"fail-this-message-in-the-middle", 0},
            new Object[] {"fail-this-message-in-the-middle", 1},
            new Object[] {"fail-this-message-in-the-middle", 2},
            new Object[] {"fail-this-message-in-the-end", 0},
            new Object[] {"fail-this-message-in-the-end", 1},
            new Object[] {"fail-this-message-in-the-end", 2}
        };
    }

    @ParameterizedTest
    @MethodSource("failuresAndRetries")
    public void testCompositeMultiStepProcessMultiThreadOutOfOrderWithFail(
            String content, int retries) throws Exception {
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
                                errors:
                                   on-failure: fail
                                   retries: %d
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    partitions: 4
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    partitions: 2
                                pipeline:
                                  - name: "mock-failing-processor"
                                    type: "mock-failing-processor"
                                    id: "step1"
                                    input: "%s"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-beginning"
                                  - name: "async-process-records"
                                    id: "step2"
                                    type: "mock-async-processor"
                                  - name: "mock-failing-processor"
                                    id: "step3"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-middle"
                                    type: "mock-failing-processor"
                                  - name: "async-process-records"
                                    id: "step4"
                                    type: "mock-async-processor"
                                  - name: "mock-failing-processor"
                                    id: "step5"
                                    type: "mock-failing-processor"
                                    output: "%s"
                                    configuration:
                                       fail-on-content: "fail-this-message-in-the-end"
                                """
                                .formatted(
                                        retries, inputTopic, outputTopic, inputTopic, outputTopic));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer(); ) {
                sendMessage(inputTopic, content, producer);
                AgentRunner.PermanentFailureException permanentFailureException =
                        assertThrows(
                                AgentRunner.PermanentFailureException.class,
                                () -> executeAgentRunners(applicationRuntime));
                assertInstanceOf(
                        MockProcessorAgentsCodeProvider.InjectedFailure.class,
                        permanentFailureException.getCause());
            }
        }
    }
}
