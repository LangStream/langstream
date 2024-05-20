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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.DeployContext;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

@Slf4j
class PulsarClusterRuntimeDockerTest {

    @RegisterExtension
    static final PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

    @Test
    public void testMapPulsarTopics() throws Exception {
        final PulsarAdmin admin = pulsarContainer.getAdmin();
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

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .topicConnectionsRuntimeRegistry(new TopicConnectionsRuntimeRegistry())
                        .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                        .build()) {

            Module module = applicationInstance.getModule("module-1");

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof PulsarTopic);

            deployer.setup("tenant", implementation);
            deployer.deploy("tenant", implementation, null);

            RetentionPolicies retentionPolicies = admin.namespaces().getRetention("public/default");
            assertEquals(100, retentionPolicies.getRetentionSizeInMB());
            assertEquals(60, retentionPolicies.getRetentionTimeInMinutes());

            List<String> topics = admin.topics().getList("public/default");
            log.info("Topics {}", topics);
            assertTrue(topics.contains("persistent://public/default/input-topic"));
            assertTrue(
                    topics.contains(
                            "persistent://public/default/input-topic-2-partitions-partition-0"));
            assertTrue(
                    topics.contains(
                            "persistent://public/default/input-topic-2-partitions-partition-1"));
            assertTrue(topics.contains("persistent://public/default/input-topic-delete"));

            deployer.delete("tenant", implementation, null);
            topics = admin.topics().getList("public/default");
            log.info("Topics {}", topics);
            assertTrue(topics.contains("persistent://public/default/input-topic"));
            assertTrue(
                    topics.contains(
                            "persistent://public/default/input-topic-2-partitions-partition-0"));
            assertTrue(
                    topics.contains(
                            "persistent://public/default/input-topic-2-partitions-partition-1"));
            assertTrue(topics.contains("persistent://public/default/input-topic-delete"));

            deployer.cleanup("tenant", implementation);
            topics = admin.topics().getList("public/default");
            log.info("Topics {}", topics);
            assertTrue(topics.contains("persistent://public/default/input-topic"));
            assertTrue(
                    topics.contains(
                            "persistent://public/default/input-topic-2-partitions-partition-0"));
            assertTrue(
                    topics.contains(
                            "persistent://public/default/input-topic-2-partitions-partition-1"));
            assertFalse(topics.contains("persistent://public/default/input-topic-delete"));
        }
    }

    private static String buildInstanceYaml() {
        return """
                     instance:
                       streamingCluster:
                         type: "pulsar"
                         configuration:
                           admin:
                             serviceUrl: "%s"
                           service:
                             serviceUrl: "%s"
                           default-tenant: "public"
                           default-namespace: "default"
                           default-retention-policies:
                             retention-size-in-mb: 100
                             retention-time-in-minutes: 60
                       computeCluster:
                         type: "none"
                     """
                .formatted(pulsarContainer.getHttpServiceUrl(), pulsarContainer.getBrokerUrl());
    }
}
