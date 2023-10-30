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

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.common.DefaultAgentNode;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.Map;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

@Slf4j
class PythonCodeAgentsTest {

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
    public void testConfigurePythonAgents() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                pipeline:
                                  - name: "source1"
                                    id: "source1"
                                    type: "python-source"
                                    configuration:
                                      className: my.python.module.MyClass
                                      config1: value1
                                      config2: value2
                                  - name: "process1"
                                    id: "process1"
                                    type: "python-processor"
                                    configuration:
                                      className: my.python.module.MyClass
                                      config1: value1
                                      config2: value2
                                      composable: false
                                  - name: "sink1"
                                    id: "sink1"
                                    type: "python-sink"
                                    configuration:
                                      className: my.python.module.MyClass
                                      config1: value1
                                      config2: value2
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
        assertEquals(3, implementation.getAgents().size());
        log.info(
                "topics {}",
                implementation.getTopics().keySet().stream()
                        .map(TopicDefinition::getName)
                        .toList());
        assertEquals(2, implementation.getTopics().size());

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "source1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertEquals("my.python.module.MyClass", configuration.get("className"));
            assertEquals("value1", configuration.get("config1"));
            assertEquals("value2", configuration.get("config2"));
            assertEquals(ComponentType.SOURCE, step.getComponentType());
        }

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "process1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertEquals("my.python.module.MyClass", configuration.get("className"));
            assertEquals("value1", configuration.get("config1"));
            assertEquals("value2", configuration.get("config2"));
            assertEquals(ComponentType.PROCESSOR, step.getComponentType());
        }

        {
            AgentNode agentImplementation = implementation.getAgentImplementation(module, "sink1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertEquals("my.python.module.MyClass", configuration.get("className"));
            assertEquals("value1", configuration.get("config1"));
            assertEquals("value2", configuration.get("config2"));
            assertEquals(ComponentType.SINK, step.getComponentType());
        }
    }

    @Test
    public void testConfigurePythonService() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                pipeline:
                                  - name: "service1"
                                    id: "service1"
                                    type: "python-service"
                                    configuration:
                                      className: my.python.module.MyClass
                                      config1: value1
                                      config2: value2
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
        log.info(
                "topics {}",
                implementation.getTopics().keySet().stream()
                        .map(TopicDefinition::getName)
                        .toList());
        assertEquals(0, implementation.getTopics().size());

        {
            AgentNode agentImplementation =
                    implementation.getAgentImplementation(module, "service1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            Map<String, Object> configuration = step.getConfiguration();
            log.info("Configuration: {}", configuration);
            assertEquals("my.python.module.MyClass", configuration.get("className"));
            assertEquals("value1", configuration.get("config1"));
            assertEquals("value2", configuration.get("config2"));
            assertEquals(ComponentType.SERVICE, step.getComponentType());
        }
    }
}
