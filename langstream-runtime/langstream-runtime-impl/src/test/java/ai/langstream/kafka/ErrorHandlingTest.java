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
package ai.langstream.kafka;


import ai.langstream.AbstractApplicationRunner;
import ai.langstream.runtime.agent.AgentRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
class ErrorHandlingTest extends AbstractApplicationRunner {


    @Test
    public void testDiscardErrors() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step"};

        Map<String, String> application = Map.of(
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 10
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                errors:
                                    on-failure: fail
                                    retries: 5
                                pipeline:
                                  - name: "some agent"
                                    id: "step"
                                    type: "mock-failing-processor"
                                    input: "input-topic"
                                    output: "output-topic"
                                    errors:
                                        on-failure: skip
                                        retries: 3
                                    configuration:
                                      fail-on-content: "fail-me"
                                """);
        try (AbstractApplicationRunner.ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "fail-me", producer);
                sendMessage("input-topic", "keep-me", producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of("keep-me"));
            }
        }
    }

    @Test
    public void testDeadLetter() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application = Map.of(
                "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "some agent"
                                    id: "step1"
                                    type: "mock-failing-processor"
                                    input: "input-topic"
                                    output: "output-topic"
                                    errors:
                                        on-failure: dead-letter
                                    configuration:
                                      fail-on-content: "fail-me"
                                """);
        try (AbstractApplicationRunner.ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("output-topic");
                 KafkaConsumer<String, String> consumerDeadletter = createConsumer("input-topic-deadletter")) {

                List<Object> expectedMessages = new ArrayList<>();
                List<Object> expectedMessagesDeadletter = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    sendMessage("input-topic", "fail-me-" + i, producer);
                    sendMessage("input-topic", "keep-me-" + i, producer);
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

        Map<String, String> application = Map.of(
                "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 10
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                errors:
                                    on-failure: fail
                                    retries: 5
                                pipeline:
                                  - name: "some agent"
                                    id: "step"
                                    type: "mock-failing-processor"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      fail-on-content: "fail-me"
                                """);
        try (AbstractApplicationRunner.ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer()) {

                sendMessage("input-topic", "fail-me", producer);
                sendMessage("input-topic", "keep-me", producer);

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
}
