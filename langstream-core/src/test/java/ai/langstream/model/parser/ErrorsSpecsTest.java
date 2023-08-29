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
package ai.langstream.model.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.langstream.api.model.AgentConfiguration;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.Pipeline;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ErrorsSpecsTest {

    @Test
    public void testConfigureErrors() throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        """
                                module: "module-1"
                                id: "pipeline-1"
                                errors:
                                   retries: 7
                                   on-failure: skip
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "step1"
                                    type: "noop"
                                    input: "input-topic"
                                  - name: "step2"
                                    type: "noop"
                                    errors:
                                       on-failure: fail
                                  - name: "step3"
                                    type: "noop"
                                    errors:
                                       retries: 3
                                  - name: "step4"
                                    type: "noop"
                                    errors:
                                       retries: 5
                                       on-failure: fail
                                """,
                                        "module2.yaml",
                                        """
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
                                    errors:
                                       on-failure: skip
                                  - name: "step3"
                                    type: "noop"
                                    errors:
                                       retries: 3
                                  - name: "step3"
                                    type: "noop"
                                    errors:
                                       retries: 5
                                       on-failure: skip
                                """),
                                buildInstanceYaml(),
                                null)
                        .getApplication();

        {
            // use pipeline defaults
            Module module = applicationInstance.getModule("module-1");
            Pipeline pipeline = module.getPipelines().get("pipeline-1");

            AgentConfiguration agent1 = pipeline.getAgents().get(0);
            assertNotNull(agent1.getErrors());
            assertEquals(7, agent1.getErrors().getRetries());
            assertEquals("skip", agent1.getErrors().getOnFailure());

            AgentConfiguration agent2 = pipeline.getAgents().get(1);
            assertNotNull(agent2.getErrors());
            assertEquals(7, agent2.getErrors().getRetries());
            assertEquals("fail", agent2.getErrors().getOnFailure());

            AgentConfiguration agent3 = pipeline.getAgents().get(2);
            assertNotNull(agent3.getErrors());
            assertEquals(3, agent3.getErrors().getRetries());
            assertEquals("skip", agent3.getErrors().getOnFailure());

            AgentConfiguration agent4 = pipeline.getAgents().get(3);
            assertNotNull(agent4.getErrors());
            assertEquals(5, agent4.getErrors().getRetries());
            assertEquals("fail", agent4.getErrors().getOnFailure());
        }

        {
            // use system defaults
            Module module = applicationInstance.getModule("module-2");
            Pipeline pipeline = module.getPipelines().get("pipeline-2");

            AgentConfiguration agent1 = pipeline.getAgents().get(0);
            assertNotNull(agent1.getErrors());
            assertEquals(0, agent1.getErrors().getRetries());
            assertEquals("fail", agent1.getErrors().getOnFailure());

            AgentConfiguration agent2 = pipeline.getAgents().get(1);
            assertNotNull(agent2.getErrors());
            assertEquals(0, agent2.getErrors().getRetries());
            assertEquals("skip", agent2.getErrors().getOnFailure());

            AgentConfiguration agent3 = pipeline.getAgents().get(2);
            assertNotNull(agent3.getErrors());
            assertEquals(3, agent3.getErrors().getRetries());
            assertEquals("fail", agent3.getErrors().getOnFailure());

            AgentConfiguration agent4 = pipeline.getAgents().get(3);
            assertNotNull(agent4.getErrors());
            assertEquals(5, agent4.getErrors().getRetries());
            assertEquals("skip", agent4.getErrors().getOnFailure());
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
