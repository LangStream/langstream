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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

@Slf4j
class KafkaRunnerDockerTest extends AbstractKafkaApplicationRunner {

    @Test
    public void testConnectToTopics() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                         module: "module-1"
                         id: "pipeline-1"
                         topics:
                           - name: "input-topic"
                             creation-mode: create-if-not-exists
                           - name: "output-topic"
                             creation-mode: create-if-not-exists
                         pipeline:
                           - id: "step1"
                             type: "identity"
                             input: "input-topic"
                             output: "output-topic"
                        """);

        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains("input-topic"));

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "value", producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of("value"));
            }
        }
    }

    @Test
    public void testApplyRetention() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                 module: "module-1"
                                 id: "pipeline-1"
                                 topics:
                                   - name: "input-topic-with-retention"
                                     creation-mode: create-if-not-exists
                                     config:
                                       retention.ms: 300000
                                 pipeline:
                                   - id: "step1"
                                     type: "identity"
                                     input: "input-topic-with-retention"
                                """);

        try (AbstractKafkaApplicationRunner.ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains("input-topic-with-retention"));

            Map<ConfigResource, Config> configResourceConfigMap =
                    getKafkaAdmin()
                            .describeConfigs(
                                    Set.of(
                                            new ConfigResource(
                                                    ConfigResource.Type.TOPIC,
                                                    "input-topic-with-retention")))
                            .all()
                            .get();
            AtomicBoolean found = new AtomicBoolean();
            configResourceConfigMap.forEach(
                    (configResource, config) -> {
                        log.info("Config for {}: {}", configResource, config);
                        config.entries()
                                .forEach(
                                        configEntry -> {
                                            log.info(
                                                    "Config entry {}: {}",
                                                    configEntry.name(),
                                                    configEntry.value());
                                            if (configEntry
                                                    .name()
                                                    .equals(TopicConfig.RETENTION_MS_CONFIG)) {
                                                assertEquals("300000", configEntry.value());
                                                found.set(true);
                                            }
                                        });
                    });
            assertTrue(found.get());
        }
    }
}
