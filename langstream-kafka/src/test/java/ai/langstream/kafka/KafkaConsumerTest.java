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

import ai.langstream.kafka.extensions.KafkaContainerExtension;
import ai.langstream.kafka.runner.KafkaConsumerWrapper;
import ai.langstream.kafka.runner.KafkaTopicConnectionsRuntime;
import ai.langstream.kafka.runtime.KafkaTopic;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaConsumerTest {

    @RegisterExtension
    static final KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    @Test
    public void testKafkaConsumerCommitOffsets() throws Exception {
        final AdminClient admin = kafkaContainer.getAdmin();
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of(
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists                            
                                  - name: "input-topic-2-partitions"
                                    creation-mode: create-if-not-exists
                                    partitions: 2                                     
                                """), buildInstanceYaml(), null).getApplication();

        @Cleanup ApplicationDeployer deployer = ApplicationDeployer
                .builder()
                .registry(new ClusterRuntimeRegistry())
                .pluginsRegistry(new PluginsRegistry())
                .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(implementation.getConnectionImplementation(module,
                Connection.fromTopic(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);

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
             KafkaConsumerWrapper consumer = (KafkaConsumerWrapper) runtime.createConsumer(agentId, streamingCluster, Map.of("topic", "input-topic"))) {
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

            consumer.getUncommittedOffsets().forEach((tp, set) -> {
                log.info("Uncommitted offsets for partition {}: {}", tp, set);
                assertEquals(0, set.size());
            });

            // partial acks, this is not an error
            for (int j = 0; j < 4; j++) {
                Record record1 = generateRecord("record_" + j);
                producer.write(List.of(record1));
            }
            log.info("Producer metrics: {}", producer.getInfo());

            List<Record> readFromConsumer = consumeRecords(consumer, 4);
            List<Record> onlySome = readFromConsumer.subList(readFromConsumer.size() / 2 - 1, readFromConsumer.size() - 1);

            // partial ack is allowed
            consumer.commit(onlySome);

            consumer.getUncommittedOffsets().forEach((tp, set) -> {
                log.info("Uncommitted offsets for partition {}: {}", tp, set);
                assertEquals(2, set.size());
            });

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
        log.info("Consumer metrics {}", consumer.getInfo());
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
}
