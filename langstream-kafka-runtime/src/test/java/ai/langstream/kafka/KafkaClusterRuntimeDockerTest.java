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
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.kafka.extensions.KafkaContainerExtension;
import ai.langstream.kafka.runtime.KafkaTopic;
import java.util.Map;
import java.util.Set;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class KafkaClusterRuntimeDockerTest {

    @RegisterExtension
    static final KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    @Test
    public void testMapKafkaTopics() throws Exception {
        final AdminClient admin = kafkaContainer.getAdmin();
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "input-topic-2-partitions"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: none
                                    partitions: 2
                                  - name: "input-topic-delete"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(new TopicConnectionsRuntimeRegistry())
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        assertTrue(
                implementation.getConnectionImplementation(
                                module,
                                Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                        instanceof KafkaTopic);

        deployer.setup("tenant", implementation);
        deployer.deploy("tenant", implementation, null);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains("input-topic"));
        assertTrue(topics.contains("input-topic-2-partitions"));
        assertTrue(topics.contains("input-topic-delete"));

        Map<String, TopicDescription> stats =
                admin.describeTopics(Set.of("input-topic", "input-topic-2-partitions")).all().get();
        assertEquals(1, stats.get("input-topic").partitions().size());
        assertEquals(2, stats.get("input-topic-2-partitions").partitions().size());

        deployer.delete("tenant", implementation, null);
        topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains("input-topic"));
        assertTrue(topics.contains("input-topic-2-partitions"));
        assertTrue(topics.contains("input-topic-delete"));

        deployer.cleanup("tenant", implementation);
        topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains("input-topic"));
        assertTrue(topics.contains("input-topic-2-partitions"));
        assertFalse(topics.contains("input-topic-delete"));
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
