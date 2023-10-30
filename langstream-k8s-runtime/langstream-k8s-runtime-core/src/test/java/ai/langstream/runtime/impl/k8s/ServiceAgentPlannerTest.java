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
import ai.langstream.api.runtime.AgentNode;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ComponentType;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.impl.common.DefaultAgentNode;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

@Slf4j
class ServiceAgentPlannerTest {

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
    public void testGenericService() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "pipeline.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                pipeline:
                                  - name: "service"
                                    id: "step1"
                                    type: "generic-service"
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        try (ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build()) {

            Module module = applicationInstance.getModule("module-1");

            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);
            assertEquals(1, implementation.getAgents().size());
            assertEquals(0, implementation.getTopics().size());

            AgentNode agentImplementation = implementation.getAgentImplementation(module, "step1");
            assertNotNull(agentImplementation);
            DefaultAgentNode step = (DefaultAgentNode) agentImplementation;
            assertEquals(ComponentType.SERVICE, agentImplementation.getComponentType());
        }
    }
}
