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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.common.DefaultAgentNode;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.noop.NoOpStreamingClusterRuntimeProvider;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.Map;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

@Slf4j
class KafkaConnectAgentsTest {

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
    public void testConfigureKafkaConnectSink() throws Exception {
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
                                pipeline:
                                  - name: "sink1"
                                    id: "step1"
                                    type: "sink"
                                    input: "input-topic"
                                    configuration:
                                      connector.class: FileStreamSink
                                      file: /tmp/test.sink.txt
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
        assertEquals(1, implementation.getAgents().size());
        assertTrue(
                implementation.getConnectionImplementation(
                                module,
                                Connection.fromTopic(TopicDefinition.fromName("input-topic")))
                        instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
        Map<String, Object> configuration = step.getConfiguration();
        log.info("Configuration: {}", configuration);
        assertEquals("FileStreamSink", configuration.get("connector.class"));
        assertEquals("/tmp/test.sink.txt", configuration.get("file"));
        assertEquals(ComponentType.SINK, step.getComponentType());
    }

    @Test
    public void testConfigureKafkaConnectSources() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "source1"
                                    id: "step1"
                                    type: "source"
                                    output: "output-topic"
                                    configuration:
                                      connector.class: FileStreamSource
                                      file: /tmp/test.txt
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
        assertEquals(1, implementation.getAgents().size());
        assertTrue(
                implementation.getConnectionImplementation(
                                module,
                                Connection.fromTopic(TopicDefinition.fromName("output-topic")))
                        instanceof NoOpStreamingClusterRuntimeProvider.SimpleTopic);

        AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
        assertNotNull(agentImplementation);
        DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
        Map<String, Object> configuration = step.getConfiguration();
        log.info("Configuration: {}", configuration);
        assertEquals("FileStreamSource", configuration.get("connector.class"));
        assertEquals("/tmp/test.txt", configuration.get("file"));
        assertEquals(ComponentType.SOURCE, step.getComponentType());
    }
}
