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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.kafka.extensions.KafkaContainerExtension;
import ai.langstream.kafka.runner.KafkaConsumerWrapper;
import ai.langstream.kafka.runner.KafkaTopicConnectionsRuntime;
import ai.langstream.kafka.runtime.KafkaTopic;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
class KafkaConsumerTest {

    @RegisterExtension
    static final KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    public void testKafkaConsumerCommitOffsets(int numPartitions) throws Exception {
        final AdminClient admin = kafkaContainer.getAdmin();
        String topicName = "input-topic-" + numPartitions + "parts-" + UUID.randomUUID();
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                                module: "module-1"
                                                id: "pipeline-1"
                                                topics:
                                                  - name: %s
                                                    creation-mode: create-if-not-exists
                                                    deletion-mode: delete
                                                    partitions: %d
                                                """
                                                .formatted(topicName, numPartitions)),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                new TopicConnectionsRuntimeRegistry();
        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(
                implementation.getConnectionImplementation(
                                module, Connection.fromTopic(TopicDefinition.fromName(topicName)))
                        instanceof KafkaTopic);

        deployer.setup("tenant", implementation);
        deployer.deploy("tenant", implementation, null);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains(topicName));

        Map<String, TopicDescription> stats = admin.describeTopics(Set.of(topicName)).all().get();
        assertEquals(numPartitions, stats.get(topicName).partitions().size());

        deployer.delete("tenant", implementation, null);
        deployer.cleanup("tenant", implementation);
        topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertFalse(topics.contains(topicName));

        StreamingCluster streamingCluster =
                implementation.getApplication().getInstance().streamingCluster();
        KafkaTopicConnectionsRuntime runtime = new KafkaTopicConnectionsRuntime();
        runtime.init(streamingCluster);
        String agentId = "agent-1";
        try (TopicProducer producer =
                        runtime.createProducer(
                                agentId, streamingCluster, Map.of("topic", topicName));
                KafkaConsumerWrapper consumer =
                        (KafkaConsumerWrapper)
                                runtime.createConsumer(
                                        agentId, streamingCluster, Map.of("topic", topicName))) {
            producer.start();
            consumer.start();

            // full acks
            for (int i = 0; i < 10; i++) {

                for (int j = 0; j < 2; j++) {
                    Record record1 = generateRecord("record" + i + "_" + j);
                    producer.write(record1).join();
                }

                List<Record> readFromConsumer = consumeRecords(consumer, 2);
                consumer.commit(readFromConsumer);
            }

            consumer.getUncommittedOffsets()
                    .forEach(
                            (tp, set) -> {
                                log.info("Uncommitted offsets for partition {}: {}", tp, set);
                                assertEquals(0, set.size());
                            });

            int numMessagesHere = 30;
            // partial acks, this is not an error
            for (int j = 0; j < numMessagesHere; j++) {
                Record record1 = generateRecord("record_" + j);
                producer.write(record1).get();
            }
            log.info("Producer metrics: {}", producer.getInfo());

            List<Record> readFromConsumer = consumeRecords(consumer, 6);
            List<Record> onlySome =
                    readFromConsumer.subList(readFromConsumer.size() / 2, readFromConsumer.size());
            log.info("Committing only {}", onlySome);
            List<Record> theOthers = readFromConsumer.subList(0, readFromConsumer.size() / 2);

            // partial ack is allowed
            consumer.commit(onlySome);

            consumer.getUncommittedOffsets()
                    .forEach(
                            (tp, set) -> {
                                log.info("Uncommitted offsets for partition {}: {}", tp, set);
                                assertEquals(numMessagesHere / 2, set.size());
                            });

            log.info("Committing the others {}", theOthers);

            consumer.commit(theOthers);

            consumer.getUncommittedOffsets()
                    .forEach(
                            (tp, set) -> {
                                log.info("Uncommitted offsets for partition {}: {}", tp, set);
                                assertEquals(0, set.size());
                            });
        }
    }

    @Test
    public void testKafkaConsumerCommitOffsetsMultiThread() throws Exception {
        int numPartitions = 4;
        int numThreads = 8;
        final AdminClient admin = kafkaContainer.getAdmin();
        String topicName = "input-topic-" + numPartitions + "-parts-mt-" + UUID.randomUUID();
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                                module: "module-1"
                                                id: "pipeline-1"
                                                topics:
                                                  - name: %s
                                                    creation-mode: create-if-not-exists
                                                    deletion-mode: delete
                                                    partitions: %d
                                                """
                                                .formatted(topicName, numPartitions)),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                new TopicConnectionsRuntimeRegistry();
        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(
                implementation.getConnectionImplementation(
                                module, Connection.fromTopic(TopicDefinition.fromName(topicName)))
                        instanceof KafkaTopic);

        deployer.setup("tenant", implementation);
        deployer.deploy("tenant", implementation, null);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains(topicName));

        Map<String, TopicDescription> stats = admin.describeTopics(Set.of(topicName)).all().get();
        assertEquals(numPartitions, stats.get(topicName).partitions().size());

        deployer.cleanup("tenant", implementation);
        deployer.delete("tenant", implementation, null);

        topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertFalse(topics.contains(topicName));

        StreamingCluster streamingCluster =
                implementation.getApplication().getInstance().streamingCluster();
        KafkaTopicConnectionsRuntime runtime = new KafkaTopicConnectionsRuntime();
        runtime.init(streamingCluster);
        String agentId = "agent-1";
        try (TopicProducer producer =
                        runtime.createProducer(
                                agentId, streamingCluster, Map.of("topic", topicName));
                KafkaConsumerWrapper consumer =
                        (KafkaConsumerWrapper)
                                runtime.createConsumer(
                                        agentId, streamingCluster, Map.of("topic", topicName))) {
            producer.start();
            consumer.start();

            ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
            try {

                // full acks
                for (int i = 0; i < 40; i++) {

                    for (int j = 0; j < 5; j++) {
                        Record record1 = generateRecord("record" + i + "_" + j);
                        producer.write(record1).join();
                    }

                    List<Record> readFromConsumer = consumeRecords(consumer, 2);
                    threadPool.submit(() -> consumer.commit(readFromConsumer));
                }
            } finally {
                threadPool.shutdown();
                assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
            }

            consumer.getUncommittedOffsets()
                    .forEach(
                            (tp, set) -> {
                                log.info("Uncommitted offsets for partition {}: {}", tp, set);
                                assertEquals(0, set.size());
                            });
        }
    }

    @Test
    public void testRestartConsumer() throws Exception {
        int numPartitions = 1;
        final AdminClient admin = kafkaContainer.getAdmin();
        String topicName = "input-topic-restart-" + UUID.randomUUID();
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                                module: "module-1"
                                                id: "pipeline-1"
                                                topics:
                                                  - name: %s
                                                    creation-mode: create-if-not-exists
                                                    deletion-mode: delete
                                                    partitions: %d
                                                """
                                                .formatted(topicName, numPartitions)),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                new TopicConnectionsRuntimeRegistry();
        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(
                implementation.getConnectionImplementation(
                                module, Connection.fromTopic(TopicDefinition.fromName(topicName)))
                        instanceof KafkaTopic);

        deployer.setup("tenant", implementation);
        deployer.deploy("tenant", implementation, null);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains(topicName));

        Map<String, TopicDescription> stats = admin.describeTopics(Set.of(topicName)).all().get();
        assertEquals(numPartitions, stats.get(topicName).partitions().size());

        deployer.delete("tenant", implementation, null);
        deployer.cleanup("tenant", implementation);
        topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertFalse(topics.contains(topicName));

        StreamingCluster streamingCluster =
                implementation.getApplication().getInstance().streamingCluster();
        KafkaTopicConnectionsRuntime runtime = new KafkaTopicConnectionsRuntime();
        runtime.init(streamingCluster);
        String agentId = "agent-1";
        try (TopicProducer producer =
                runtime.createProducer(agentId, streamingCluster, Map.of("topic", topicName)); ) {
            producer.start();

            int numIterations = 5;
            for (int i = 0; i < numIterations; i++) {

                List<String> expected = new ArrayList<>();

                for (int j = 0; j < 5; j++) {
                    String text = "record" + i + "_" + j;
                    expected.add(text);
                    Record record1 = generateRecord(text);
                    producer.write(record1).join();
                }

                try (KafkaConsumerWrapper consumer =
                        (KafkaConsumerWrapper)
                                runtime.createConsumer(
                                        agentId, streamingCluster, Map.of("topic", topicName))) {

                    consumer.start();
                    List<Record> readFromConsumer = consumeRecords(consumer, expected.size());
                    for (int j = 0; j < expected.size(); j++) {
                        assertEquals(expected.get(j), readFromConsumer.get(j).value());
                    }
                    consumer.commit(readFromConsumer);
                }
            }
        }
    }

    @Test
    public void testMultipleSchemas() throws Exception {
        int numPartitions = 1;
        final AdminClient admin = kafkaContainer.getAdmin();
        String topicName = "input-topic-multi-schemas-" + UUID.randomUUID();
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                                module: "module-1"
                                                id: "pipeline-1"
                                                topics:
                                                  - name: %s
                                                    creation-mode: create-if-not-exists
                                                    deletion-mode: delete
                                                    partitions: %d
                                                """
                                                .formatted(topicName, numPartitions)),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        final TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                new TopicConnectionsRuntimeRegistry();
        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(
                implementation.getConnectionImplementation(
                                module, Connection.fromTopic(TopicDefinition.fromName(topicName)))
                        instanceof KafkaTopic);

        deployer.setup("tenant", implementation);
        deployer.deploy("tenant", implementation, null);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains(topicName));

        Map<String, TopicDescription> stats = admin.describeTopics(Set.of(topicName)).all().get();
        assertEquals(numPartitions, stats.get(topicName).partitions().size());

        deployer.delete("tenant", implementation, null);
        deployer.cleanup("tenant", implementation);

        topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertFalse(topics.contains(topicName));

        StreamingCluster streamingCluster =
                implementation.getApplication().getInstance().streamingCluster();
        KafkaTopicConnectionsRuntime runtime = new KafkaTopicConnectionsRuntime();
        runtime.init(streamingCluster);
        String agentId = "agent-1";
        try (TopicProducer producer =
                runtime.createProducer(agentId, streamingCluster, Map.of("topic", topicName)); ) {
            producer.start();

            int numIterations = 5;
            for (int i = 0; i < numIterations; i++) {

                producer.write(generateRecord(1, "string")).join();
                producer.write(generateRecord("two", 2)).join();

                producer.write(
                                generateRecord(
                                        "two",
                                        2,
                                        new SimpleRecord.SimpleHeader("h1", 7),
                                        new SimpleRecord.SimpleHeader("h2", "bar")))
                        .join();

                producer.write(generateRecord(1, "string")).join();
                producer.write(generateRecord("two", 2)).join();

                producer.write(
                                generateRecord(
                                        "two",
                                        2,
                                        new SimpleRecord.SimpleHeader("h1", 7),
                                        new SimpleRecord.SimpleHeader("h2", "bar")))
                        .join();

                producer.write(generateRecord(List.of("one", "two"), Map.of("k", "v"))).join();

                try (KafkaConsumerWrapper consumer =
                        (KafkaConsumerWrapper)
                                runtime.createConsumer(
                                        agentId, streamingCluster, Map.of("topic", topicName))) {

                    consumer.start();
                    List<Record> readFromConsumer = consumeRecords(consumer, 6);
                    consumer.commit(readFromConsumer);
                }
            }
        }
    }

    @NotNull
    private static List<Record> consumeRecords(TopicConsumer consumer, int atLeast) {
        List<Record> readFromConsumer = new ArrayList<>();
        Awaitility.await()
                .untilAsserted(
                        () -> {
                            readFromConsumer.addAll(consumer.read());
                            log.info(
                                    "Read {} records:  {}",
                                    readFromConsumer.size(),
                                    readFromConsumer);
                            assertTrue(readFromConsumer.size() >= atLeast);
                        });
        log.info("Consumer metrics {}", consumer.getInfo());
        return readFromConsumer;
    }

    private static Record generateRecord(Object value) {
        return generateRecord(value, value);
    }

    private static Record generateRecord(Object key, Object value, Header... headers) {
        return SimpleRecord.builder()
                .key(key)
                .value(value)
                .origin("origin")
                .timestamp(System.currentTimeMillis())
                .headers(headers != null ? Arrays.asList(headers) : List.of())
                .build();
    }

    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "kafka"
                    configuration:
                      admin:
                        bootstrap.servers: "%s"
                  computeCluster:
                     type: "none"
                """
                .formatted(kafkaContainer.getBootstrapServers());
    }
}
