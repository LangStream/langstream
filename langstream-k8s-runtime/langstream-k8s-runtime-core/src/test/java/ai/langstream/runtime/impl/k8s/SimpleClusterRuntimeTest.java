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
package ai.langstream.runtime.impl.k8s;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.noop.NoOpStreamingClusterRuntimeProvider;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.Map;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

@Slf4j
class SimpleClusterRuntimeTest {

    @NotNull
    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "noop"
                  computeCluster:
                    type: "none"
                """;
    }

    @Test
    public void testMapGenericPulsarFunctionsChain() throws Exception {
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
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "generic-agent"
                                    id: "function-1-id"
                                    type: "generic-agent"
                                    input: "input-topic"
                                    # the output is implicitly an intermediate topic
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                  - name: "function2"
                                    id: "function-2-id"
                                    type: "generic-agent"
                                    # the input is implicitly an intermediate topic
                                    output: "output-topic"
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        {
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("input-topic")));
        }
        {
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("output-topic")));
        }

        {
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-function-2-id-input")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("agent-function-2-id-input")));
        }

        assertEquals(3, implementation.getTopics().size());

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "function-1-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-1-id", agentImplementation.getId());
        }

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "function-2-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-2-id", agentImplementation.getId());
        }
    }

    @Test
    public void testCreateDeadletterTopicsWithIntermediateTopics1() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                errors:
                                   on-failure: dead-letter
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "generic-agent"
                                    id: "function-1-id"
                                    type: "generic-agent"
                                    input: "input-topic"
                                    # the output is implicitly an intermediate topic
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                  - name: "function2"
                                    id: "function-2-id"
                                    type: "generic-agent"
                                    # the input is implicitly an intermediate topic
                                    # a dead letter is required for this intermediate topic
                                    output: "output-topic"
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        {
            NoOpStreamingClusterRuntimeProvider.SimpleTopic simpleTopic =
                    (NoOpStreamingClusterRuntimeProvider.SimpleTopic)
                            implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")));
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("input-topic")));
            assertNotNull(simpleTopic.getDeadletterTopic());
            assertEquals("input-topic-deadletter", simpleTopic.getDeadletterTopic().topicName());
        }
        {
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("output-topic")));
        }

        {
            NoOpStreamingClusterRuntimeProvider.SimpleTopic simpleTopic =
                    (NoOpStreamingClusterRuntimeProvider.SimpleTopic)
                            implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-function-2-id-input")));
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("agent-function-2-id-input")));
            assertNotNull(simpleTopic.getDeadletterTopic());
            assertEquals(
                    "agent-function-2-id-input-deadletter",
                    simpleTopic.getDeadletterTopic().topicName());
        }

        assertEquals(5, implementation.getTopics().size());

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "function-1-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-1-id", agentImplementation.getId());
        }

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "function-2-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-2-id", agentImplementation.getId());
        }
    }

    @Test
    public void testCreateDeadletterTopicsWithIntermediateTopics2() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                errors:
                                   on-failure: dead-letter
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "generic-agent"
                                    id: "function-1-id"
                                    type: "generic-agent"
                                    input: "input-topic"
                                    # the output is implicitly an intermediate topic
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                  - name: "function2"
                                    id: "function-2-id"
                                    type: "generic-agent"
                                    # the input is implicitly an intermediate topic
                                    # no dead letter is required for this intermediate topic
                                    # as we are skipping errors
                                    errors:
                                        on-failure: skip
                                    output: "output-topic"
                                    configuration:
                                      config1: "value"
                                      config2: "value2"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        Module module = applicationInstance.getModule("module-1");

        ExecutionPlan implementation = deployer.createImplementation("app", applicationInstance);
        {
            NoOpStreamingClusterRuntimeProvider.SimpleTopic simpleTopic =
                    (NoOpStreamingClusterRuntimeProvider.SimpleTopic)
                            implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("input-topic")));
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("input-topic")));
            assertNotNull(simpleTopic.getDeadletterTopic());
            assertEquals("input-topic-deadletter", simpleTopic.getDeadletterTopic().topicName());
        }
        {
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                            instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("output-topic")));
        }

        {
            NoOpStreamingClusterRuntimeProvider.SimpleTopic simpleTopic =
                    (NoOpStreamingClusterRuntimeProvider.SimpleTopic)
                            implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName("agent-function-2-id-input")));
            assertTrue(
                    implementation.getTopics().values().stream()
                            .anyMatch(t -> t.topicName().equals("agent-function-2-id-input")));
            assertNull(simpleTopic.getDeadletterTopic());
        }

        assertEquals(4, implementation.getTopics().size());

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "function-1-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-1-id", agentImplementation.getId());
        }

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "function-2-id");
            assertNotNull(agentImplementation);
            assertEquals("generic-agent", agentImplementation.getAgentType());
            assertEquals("function-2-id", agentImplementation.getId());
        }
    }
}
