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
package ai.langstream.model.parser;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.impl.parser.ModelBuilder;
import org.junit.jupiter.api.Test;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ResourcesSpecsTest {

    @Test
    public void testConfigureResourceSpecs() throws Exception {
        Application applicationInstance = ModelBuilder
                .buildApplicationInstance(Map.of(
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"
                                resources:
                                   parallelism: 7
                                   size: 7             
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "step1"                                    
                                    type: "noop"
                                    input: "input-topic"                                    
                                  - name: "step2"
                                    type: "noop"
                                    resources:
                                       parallelism: 2
                                  - name: "step3"
                                    type: "noop"
                                    resources:
                                       size: 3                                      
                                  - name: "step3"
                                    type: "noop"
                                    resources:
                                       size: 3
                                       parallelism: 5
                                """,
                        "module2.yaml", """
                                module: "module-2"
                                id: "pipeline-2"             
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "step1"                                    
                                    type: "noop"
                                    input: "input-topic"                                    
                                  - name: "step2"
                                    type: "noop"
                                    resources:
                                       parallelism: 2
                                  - name: "step3"
                                    type: "noop"
                                    resources:
                                       size: 3                                      
                                  - name: "step3"
                                    type: "noop"
                                    resources:
                                       size: 3
                                       parallelism: 5
                                """), buildInstanceYaml(), null).getApplication();

        {
            Module module = applicationInstance.getModule("module-1");
            Pipeline pipeline = module.getPipelines().get("pipeline-1");

            AgentConfiguration agent1 = pipeline.getAgents().get(0);
            assertNotNull(agent1.getResources());
            assertEquals(7, agent1.getResources().parallelism());
            assertEquals(7, agent1.getResources().size());

            AgentConfiguration agent2 = pipeline.getAgents().get(1);
            assertNotNull(agent2.getResources());
            assertEquals(2, agent2.getResources().parallelism());
            assertEquals(7, agent2.getResources().size());

            AgentConfiguration agent3 = pipeline.getAgents().get(2);
            assertNotNull(agent3.getResources());
            assertEquals(7, agent3.getResources().parallelism());
            assertEquals(3, agent3.getResources().size());
        }

        {
            Module module = applicationInstance.getModule("module-2");
            Pipeline pipeline = module.getPipelines().get("pipeline-2");

            AgentConfiguration agent1 = pipeline.getAgents().get(0);
            assertNotNull(agent1.getResources());
            assertEquals(1, agent1.getResources().parallelism());
            assertEquals(1, agent1.getResources().size());

            AgentConfiguration agent2 = pipeline.getAgents().get(1);
            assertNotNull(agent2.getResources());
            assertEquals(2, agent2.getResources().parallelism());
            assertEquals(1, agent2.getResources().size());

            AgentConfiguration agent3 = pipeline.getAgents().get(2);
            assertNotNull(agent3.getResources());
            assertEquals(1, agent3.getResources().parallelism());
            assertEquals(3, agent3.getResources().size());
        }

    }

    private static String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "noop"
                  computeCluster:
                    type: "none"
                """;
    }
}
