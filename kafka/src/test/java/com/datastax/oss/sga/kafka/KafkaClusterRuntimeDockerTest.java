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
package com.datastax.oss.sga.kafka;

import com.dastastax.oss.sga.kafka.runtime.KafkaTopic;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Connection;
import com.datastax.oss.sga.api.model.Module;
import com.datastax.oss.sga.api.model.TopicDefinition;
import com.datastax.oss.sga.api.runtime.ClusterRuntimeRegistry;
import com.datastax.oss.sga.api.runtime.ExecutionPlan;
import com.datastax.oss.sga.api.runtime.PluginsRegistry;
import com.datastax.oss.sga.impl.deploy.ApplicationDeployer;
import com.datastax.oss.sga.impl.parser.ModelBuilder;
import com.datastax.oss.sga.kafka.extensions.KafkaContainerExtension;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaClusterRuntimeDockerTest {


    @RegisterExtension
    static final KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    @Test
    public void testMapKafkaTopics() throws Exception {
        final AdminClient admin = kafkaContainer.getAdmin();
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
                Connection.from(TopicDefinition.fromName("input-topic"))) instanceof KafkaTopic);

        deployer.deploy("tenant", implementation, null);

        Set<String> topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertTrue(topics.contains("input-topic"));
        assertTrue(topics.contains("input-topic-2-partitions"));

        Map<String, TopicDescription> stats =
                admin.describeTopics(Set.of("input-topic", "input-topic-2-partitions")).all().get();
        assertEquals(1, stats.get("input-topic").partitions().size());
        assertEquals(2, stats.get("input-topic-2-partitions").partitions().size());

        deployer.delete("tenant", implementation, null);
        topics = admin.listTopics().names().get();
        log.info("Topics {}", topics);
        assertFalse(topics.contains("input-topic"));
        assertFalse(topics.contains("input-topic-2-partitions"));

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
