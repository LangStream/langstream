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
package ai.langstream.runtime.impl.k8s.agents;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.doc.AgentConfigurationModel;
import ai.langstream.api.model.Application;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import ai.langstream.impl.parser.ModelBuilder;
import java.util.Map;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KubernetesGenAIToolKitFunctionAgentProviderTest {

    @Test
    @SneakyThrows
    public void testValidationDropFields() {
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                      a-field: "val"
                """,
                "Found error on an agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'a-field' is unknown");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration: {}
                """,
                "Found error on an agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'fields' is required");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                        fields: {}
                """,
                "Found error on an agent configuration (agent: 'drop-my-field', type: 'drop-fields'). Property 'fields' has a wrong data type. Expected type: java.util.ArrayList");
        validate(
                """
                topics: []
                pipeline:
                  - name: "drop-my-field"
                    type: "drop-fields"
                    configuration:
                      fields:
                      - "a"
                      part: value
                """,
                null);
    }

    private void validate(String pipeline, String expectErrMessage) throws Exception {
        Application applicationInstance =
                ModelBuilder.buildApplicationInstance(
                                Map.of("module.yaml", pipeline),
                                """
                                        instance:
                                          streamingCluster:
                                            type: "noop"
                                          computeCluster:
                                            type: "none"
                                        """,
                                null)
                        .getApplication();

        @Cleanup
        ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .registry(new ClusterRuntimeRegistry())
                        .pluginsRegistry(new PluginsRegistry())
                        .build();

        try {
            ExecutionPlan implementation =
                    deployer.createImplementation("app", applicationInstance);
            if (expectErrMessage != null) {
                fail("Expected error message: " + expectErrMessage);
            }
        } catch (IllegalArgumentException e) {
            if (expectErrMessage != null && e.getMessage().contains(expectErrMessage)) {
                return;
            }
            fail("Expected error message: " + expectErrMessage + " but got: " + e.getMessage());
        }
    }

    @Test
    @SneakyThrows
    public void testStepsDoc() {
        final Map<String, AgentConfigurationModel> model =
                new PluginsRegistry()
                        .lookupAgentImplementation(
                                "drop-fields",
                                new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """
                        {
                          "ai-chat-completions" : { },
                          "ai-text-completions" : { },
                          "cast" : { },
                          "compute" : { },
                          "compute-ai-embeddings" : { },
                          "drop" : {
                            "properties" : {
                              "composable" : {
                                "description" : "Whether this step can be composed with other steps.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "true"
                              },
                              "when" : {
                                "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "drop-fields" : {
                            "name" : "Drop fields from the input record",
                            "properties" : {
                              "composable" : {
                                "description" : "Whether this step can be composed with other steps.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "true"
                              },
                              "fields" : {
                                "description" : "Fields to drop from the input record.",
                                "required" : true,
                                "type" : "array",
                                "items" : {
                                  "description" : "Fields to drop from the input record.",
                                  "required" : true,
                                  "type" : "string"
                                }
                              },
                              "part" : {
                                "description" : "Part to drop. (value or key)",
                                "required" : false,
                                "type" : "string"
                              },
                              "when" : {
                                "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "flatten" : { },
                          "merge-key-value" : {
                            "properties" : {
                              "composable" : {
                                "description" : "Whether this step can be composed with other steps.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "true"
                              },
                              "when" : {
                                "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "query" : { },
                          "unwrap-key-value" : { }
                        }""",
                SerializationUtil.prettyPrintJson(model));
    }
}
