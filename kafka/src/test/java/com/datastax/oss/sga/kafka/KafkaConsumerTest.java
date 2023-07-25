package com.datastax.oss.sga.kafka;

import com.dastastax.oss.sga.kafka.runner.KafkaConsumerRecord;
import com.dastastax.oss.sga.kafka.runner.KafkaTopicConnectionsRuntime;
import com.dastastax.oss.sga.kafka.runtime.KafkaTopic;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import com.datastax.oss.sga.api.runner.code.SimpleRecord;
import com.datastax.oss.sga.api.runner.topics.TopicConsumer;
import com.datastax.oss.sga.api.runner.topics.TopicProducer;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaConsumerTest {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;


    @Test
    public void testKafkaConsumerCommitOffsets() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists                                     
                                  - name: "input-topic-2-partitions"
                                    creation-mode: create-if-not-exists
                                    partitions: 2                                     
                                """));

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                new Connection(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);

        deployer.deploy("tenant", implementation, null);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains("input-topic"));
        assertTrue(topics.contains("input-topic-2-partitions"));

        Map<String, TopicDescription> stats = admin.describeTopics(Set.of("input-topic", "input-topic-2-partitions")).all().get();
        assertEquals(1, stats.get("input-topic").partitions().size());
        assertEquals(2, stats.get("input-topic-2-partitions").partitions().size());

        deployer.delete("tenant", implementation, null);
        topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertFalse(topics.contains("input-topic"));
        assertFalse(topics.contains("input-topic-2-partitions"));


        StreamingCluster streamingCluster = implementation.getApplication().getInstance().streamingCluster();
        KafkaTopicConnectionsRuntime runtime = new KafkaTopicConnectionsRuntime();
        runtime.init(streamingCluster);
        String agentId = "agent-1";
        try (TopicProducer producer = runtime.createProducer(agentId, streamingCluster, Map.of("topic", "input-topic"));
             TopicConsumer consumer = runtime.createConsumer(agentId, streamingCluster, Map.of("topic", "input-topic"))) {
            producer.start();
            consumer.start();

            // full acks
            for (int i = 0; i < 10; i++) {

                for (int j = 0; j < 2; j++) {
                    Record record1 = generateRecord("record" + i + "_" + j);
                    producer.write(List.of(record1));
                }

                List<Record> readFromConsumer = consumeRecords(consumer, 2);
                consumer.commit(readFromConsumer);
            }

            // partial acks, this is an error
            for (int j = 0; j < 4; j++) {
                Record record1 = generateRecord("record_" + j);
                producer.write(List.of(record1));
            }

            List<Record> readFromConsumer = consumeRecords(consumer, 4);
            List<Record> onlySome = readFromConsumer.subList(readFromConsumer.size() / 2 - 1, readFromConsumer.size() - 1);

            // partial ack is not allowed
            assertThrows(IllegalStateException.class, () -> consumer.commit(onlySome));

        }

    }

    @NotNull
    private static List<Record> consumeRecords(TopicConsumer consumer, int atLeast) {
        List<Record> readFromConsumer = new ArrayList<>();
        Awaitility.await().untilAsserted(() -> {
            readFromConsumer.addAll(consumer.read());
            log.info("Read {} records:  {}", readFromConsumer.size(), readFromConsumer);
            assertTrue(readFromConsumer.size() >= atLeast);
        });
        return readFromConsumer;
    }

    private static Record generateRecord(String value) {
        return SimpleRecord.builder().key(value)
                .value(value)
                .origin("origin")
                .timestamp(System.currentTimeMillis())
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
                """.formatted(kafkaContainer.getBootstrapServers());
    }


    @BeforeAll
    public static void setup() throws Exception {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                .withLogConsumer(new Consumer<OutputFrame>() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        log.info("kafka> {}", outputFrame.getUtf8String().trim());
                    }
                });
        // start Pulsar and wait for it to be ready to accept requests
        kafkaContainer.start();
        admin =
                AdminClient.create(Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers()));
    }

    @AfterAll
    public static void teardown() {
        if (admin != null) {
            admin.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }
}