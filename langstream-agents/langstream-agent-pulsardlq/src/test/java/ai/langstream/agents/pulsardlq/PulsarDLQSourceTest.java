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
package ai.langstream.agents.pulsardlq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
public class PulsarDLQSourceTest {

    private static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();
    private static final int DEFAULT_MAX_RETRIES = 5;
    private static final long DEFAULT_RETRY_INTERVAL_MS = 2000;

    @RegisterExtension
    static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

    @BeforeAll
    static void setup() {}

    @Test
    void testReadDLQ() throws Exception {

        pulsarContainer
                .getAdmin()
                .topics()
                .createNonPartitionedTopic("public/default/test-topic-DLQ");

        AgentSource agentSource =
                buildAgentSource(
                        pulsarContainer.getBrokerUrl(),
                        "public/default",
                        "dlq-subscription",
                        "-DLQ");

        Producer producer =
                pulsarContainer
                        .getClient()
                        .newProducer()
                        .topic("persistent://public/default/test-topic-DLQ")
                        .create();

        TypedMessageBuilder<byte[]> messageBuilder =
                producer.newMessage()
                        .key("message-key") // e
                        .value("test-message-1".getBytes());

        // Adding properties to the message
        messageBuilder.property("property1", "value1");
        messageBuilder.property("property2", "value2");

        // Send the message
        messageBuilder.send();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            List<Record> read = agentSource.read();
                            assertEquals(1, read.size());
                            // Verify the message
                            assertEquals(
                                    "test-message-1",
                                    new String(
                                            (byte[]) read.get(0).value(), StandardCharsets.UTF_8));
                            // Verify the key
                            assertEquals("message-key", read.get(0).key());
                            // Verify the properties
                            assertEquals(
                                    "value1", read.get(0).getHeader("property1").valueAsString());
                            assertEquals(
                                    "value2", read.get(0).getHeader("property2").valueAsString());
                            // Verify the origin is the DLQ topic
                            assertEquals(
                                    "persistent://public/default/test-topic-DLQ",
                                    read.get(0).origin());
                            // Commiting the message means it gets acknowledged and removed from the
                            // DLQ
                            agentSource.commit(read);
                        });

        messageBuilder = producer.newMessage().value("test-message-2".getBytes());

        messageBuilder.send();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            List<Record> read = agentSource.read();
                            assertEquals(1, read.size());
                            assertEquals(
                                    "test-message-2",
                                    new String(
                                            (byte[]) read.get(0).value(), StandardCharsets.UTF_8));
                            // Verify the key
                            assertEquals(null, read.get(0).key());
                            // Verify the properties
                            assertTrue(read.get(0).headers().isEmpty(), "Headers shoudld be empty");
                            // Verify the origin is the DLQ topic
                            assertEquals(
                                    "persistent://public/default/test-topic-DLQ",
                                    read.get(0).origin());
                            // Not commiting the message means it stays in the DLQ
                        });

        // Close the agent so we can connect on its subscription
        agentSource.close();

        // Consume a message from the DLQ
        Consumer pulsarConsumer =
                pulsarContainer
                        .getClient()
                        .newConsumer()
                        .topic("test-topic-DLQ")
                        .subscriptionName("dlq-subscription")
                        .subscribe();

        Message msg = pulsarConsumer.receive();

        // This should be the second message because the first one was committed
        assertEquals("test-message-2", new String(msg.getData()));
        pulsarConsumer.close();
        producer.close();
        Thread.sleep(1000);
        deleteTopicsWithRetries(new String[] {"persistent://public/default/test-topic-DLQ"});
    }

    @Test
    void testReadMultipleDLQ() throws Exception {
        pulsarContainer
                .getAdmin()
                .topics()
                .createNonPartitionedTopic("public/default/test-topic2-deadletter");

        AgentSource agentSource =
                buildAgentSource(
                        pulsarContainer.getBrokerUrl(),
                        "public/default",
                        "dlq-subscription",
                        "-deadletter");

        Producer producer =
                pulsarContainer
                        .getClient()
                        .newProducer()
                        .topic("persistent://public/default/test-topic2-deadletter")
                        .create();

        TypedMessageBuilder<byte[]> messageBuilder =
                producer.newMessage()
                        .key("message-key") // e
                        .value("test-message-1".getBytes());

        // Adding properties to the message
        messageBuilder.property("property1", "value1");
        messageBuilder.property("property2", "value2");

        // Send the message
        messageBuilder.send();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            List<Record> read = agentSource.read();
                            assertEquals(1, read.size());
                            // Verify the message
                            assertEquals(
                                    "test-message-1",
                                    new String(
                                            (byte[]) read.get(0).value(), StandardCharsets.UTF_8));
                            // Verify the key
                            assertEquals("message-key", read.get(0).key());
                            // Verify the properties
                            assertEquals(
                                    "value1", read.get(0).getHeader("property1").valueAsString());
                            assertEquals(
                                    "value2", read.get(0).getHeader("property2").valueAsString());
                            // Verify the origin is the DLQ topic
                            assertEquals(
                                    "persistent://public/default/test-topic2-deadletter",
                                    read.get(0).origin());
                            // Commiting the message means it gets acknowledged and removed from the
                            // DLQ
                            agentSource.commit(read);
                        });
        pulsarContainer
                .getAdmin()
                .topics()
                .createNonPartitionedTopic("public/default/test-topic3-deadletter");

        producer.close();

        Thread.sleep(2000);

        producer =
                pulsarContainer
                        .getClient()
                        .newProducer()
                        .topic("persistent://public/default/test-topic3-deadletter")
                        .create();

        messageBuilder =
                producer.newMessage()
                        .key("message-key") // e
                        .value("test-message-2".getBytes());

        // Adding properties to the message
        messageBuilder.property("property1", "value1");
        messageBuilder.property("property2", "value2");

        messageBuilder.send();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            List<Record> read = agentSource.read();
                            assertEquals(1, read.size());
                            assertEquals(
                                    "test-message-2",
                                    new String(
                                            (byte[]) read.get(0).value(), StandardCharsets.UTF_8));
                            // Verify the key
                            assertEquals("message-key", read.get(0).key());
                            // Verify the properties
                            assertEquals(
                                    "value1", read.get(0).getHeader("property1").valueAsString());
                            assertEquals(
                                    "value2", read.get(0).getHeader("property2").valueAsString());
                            // Verify the origin is the DLQ topic
                            assertEquals(
                                    "persistent://public/default/test-topic3-deadletter",
                                    read.get(0).origin());
                            // Commiting the message
                            agentSource.commit(read);
                        });

        // Close the agent so we can connect on its subscription
        agentSource.close();

        // Consume a message from the DLQ
        Consumer pulsarConsumer =
                pulsarContainer
                        .getClient()
                        .newConsumer()
                        .topic("test-topic3-deadletter")
                        .subscriptionName("dlq-subscription")
                        .subscribe();

        Message msg = pulsarConsumer.receive(2, TimeUnit.SECONDS);
        // Verify there is no message
        assertEquals(null, msg);
        pulsarConsumer.close();
        producer.close();

        deleteTopicsWithRetries(
                new String[] {
                    "persistent://public/default/test-topic2-deadletter",
                    "persistent://public/default/test-topic3-deadletter"
                });
    }

    private AgentSource buildAgentSource(
            String url, String namespace, String subscription, String dlqSuffix) throws Exception {
        AgentSource agentSource =
                (AgentSource) AGENT_CODE_REGISTRY.getAgentCode("pulsardlq-source").agentCode();
        Map<String, Object> configs = new HashMap<>();
        configs.put("pulsar-url", url);
        configs.put("namespace", namespace);
        configs.put("subscription", subscription);
        configs.put("dlq-suffix", dlqSuffix);
        AgentContext agentContext = mock(AgentContext.class);
        when(agentContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agentSource.init(configs);
        agentSource.setContext(agentContext);
        agentSource.start();
        return agentSource;
    }

    public void deleteTopicsWithRetries(String[] topics) {
        deleteTopicsWithRetries(topics, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_INTERVAL_MS);
    }

    /**
     * Attempts to delete the specified topics, retrying up to a maximum number of times.
     *
     * @param topics An array of topic names to delete.
     * @param maxAttempts The maximum number of attempts to delete the topics.
     * @param sleepInterval The time in milliseconds to wait between attempts.
     */
    public void deleteTopicsWithRetries(String[] topics, int maxAttempts, long sleepInterval) {
        int attempt = 0;

        while (attempt < maxAttempts) {
            try {
                // Attempt to delete each topic
                for (String topic : topics) {
                    pulsarContainer.getAdmin().topics().delete(topic);
                }
                System.out.println("Successfully deleted all topics.");
                break; // Exit the loop if deletion is successful
            } catch (Exception e) {
                attempt++; // Increment attempt count
                if (attempt >= maxAttempts) {
                    System.err.println(
                            "Failed to delete topics after " + maxAttempts + " attempts");
                    e.printStackTrace();
                    // Throw the exception if maximum attempts are reached
                    throw new RuntimeException(
                            "Failed to delete topics after " + maxAttempts + " attempts", e);
                } else {
                    System.out.println(
                            "Attempt "
                                    + attempt
                                    + " failed, retrying in "
                                    + sleepInterval
                                    + "ms...");
                    try {
                        Thread.sleep(sleepInterval); // Sleep before retrying
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); // Restore interrupted status
                        System.err.println("Thread interrupted during sleep between retries.");
                        break;
                    }
                }
            }
        }
    }
}
