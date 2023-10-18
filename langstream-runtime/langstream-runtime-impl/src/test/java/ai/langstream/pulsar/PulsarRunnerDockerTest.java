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
package ai.langstream.pulsar;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.AbstractApplicationRunner;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class PulsarRunnerDockerTest extends AbstractApplicationRunner {

    @RegisterExtension
    static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

    @Test
    public void testRunAITools() throws Exception {
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
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
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

    private String buildInstanceYaml() {
        return """
                     instance:
                       streamingCluster:
                         type: "pulsar"
                         configuration:
                           admin:
                             serviceUrl: "%s"
                           service:
                             serviceUrl: "%s"
                           defaultTenant: "public"
                           defaultNamespace: "default"
                       computeCluster:
                         type: "kubernetes"
                     """
                .formatted(pulsarContainer.getHttpServiceUrl(), pulsarContainer.getBrokerUrl());
    }

    protected Producer<String> createProducer(String topic) throws PulsarClientException {
        return pulsarContainer.getClient().newProducer(Schema.STRING).topic(topic).create();
    }

    protected Consumer<GenericRecord> createConsumer(String topic) throws PulsarClientException {
        return pulsarContainer
                .getClient()
                .newConsumer(Schema.AUTO_CONSUME())
                .topic(topic)
                .subscriptionName("test-subscription")
                .subscribe();
    }
}
