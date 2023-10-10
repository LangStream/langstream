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
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.deployer.k8s.util.SerializationUtil;
import ai.langstream.impl.noop.NoOpComputeClusterRuntimeProvider;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class QueryVectorDBAgentProviderTest {

    @Test
    @SneakyThrows
    public void testValidationQueryDb() {
        validate(
                """
                pipeline:
                  - name: "db"
                    type: "query-vector-db"
                    configuration:
                      output-field: result
                      query: "select xxx"
                      datasource: "cassandra"
                      unknown-field: "..."
                """,
                "Found error on agent configuration (agent: 'db', type: 'query-vector-db'). Property 'unknown-field' is unknown");

        validate(
                """
                pipeline:
                  - name: "db"
                    type: "query-vector-db"
                    configuration:
                      output-field: result
                      query: "select xxx"
                      datasource: "not exists"
                """,
                "Resource 'not exists' not found");

        validate(
                """
                pipeline:
                  - name: "db"
                    type: "query-vector-db"
                    configuration:
                      output-field: result
                      query: "select xxx"
                      datasource: "cassandra"
                """,
                null);
    }

    @Test
    @SneakyThrows
    public void testWriteDb() {
        validate(
                """
                pipeline:
                  - name: "db"
                    type: "vector-db-sink"
                    configuration:
                      datasource: "not exists"
                      unknown-field: "..."
                """,
                "Resource 'not exists' not found");

        validate(
                """
                pipeline:
                  - name: "db"
                    type: "vector-db-sink"
                    configuration:
                      unknown-field: "..."
                """,
                "Found error on agent configuration (agent: 'db', type: 'vector-db-sink'). Property 'datasource' is required");

        validate(
                """
                pipeline:
                  - name: "db"
                    type: "vector-db-sink"
                    configuration:
                      datasource: "cassandra"
                      unknown-field: "..."
                """,
                null);
    }

    private void validate(String pipeline, String expectErrMessage) throws Exception {
        AgentValidationTestUtil.validate(pipeline, expectErrMessage);
    }

    @Test
    @SneakyThrows
    public void testDocumentation() {
        final Map<String, AgentConfigurationModel> model =
                new PluginsRegistry()
                        .lookupAgentImplementation(
                                "vector-db-sink",
                                new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """
                        {
                          "query-vector-db" : {
                            "name" : "Query a vector database",
                            "description" : "Query a vector database using Vector Search capabilities.",
                            "properties" : {
                              "composable" : {
                                "description" : "Whether this step can be composed with other steps.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "true"
                              },
                              "datasource" : {
                                "description" : "Reference to a datasource id configured in the application.",
                                "required" : true,
                                "type" : "string"
                              },
                              "fields" : {
                                "description" : "Fields of the record to use as input parameters for the query.",
                                "required" : false,
                                "type" : "array",
                                "items" : {
                                  "description" : "Fields of the record to use as input parameters for the query.",
                                  "required" : false,
                                  "type" : "string"
                                }
                              },
                              "loop-over" : {
                                "description" : "Loop over a list of items taken from the record. For instance value.documents.\\nIt must refer to a list of maps. In this case the output-field parameter\\nbut be like \\"record.fieldname\\" in order to replace or set a field in each record\\nwith the results of the query. In the query parameters you can refer to the\\nrecord fields using \\"record.field\\".",
                                "required" : false,
                                "type" : "string"
                              },
                              "only-first" : {
                                "description" : "If true, only the first result of the query is stored in the output field.",
                                "required" : false,
                                "type" : "boolean",
                                "defaultValue" : "false"
                              },
                              "output-field" : {
                                "description" : "The name of the field to use to store the query result.",
                                "required" : true,
                                "type" : "string"
                              },
                              "query" : {
                                "description" : "The query to use to extract the data.",
                                "required" : true,
                                "type" : "string"
                              },
                              "when" : {
                                "description" : "Execute the step only when the condition is met.\\nYou can use the expression language to reference the message.\\nExample: when: \\"value.first == 'f1' && value.last.toUpperCase() == 'L1'\\"",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
                          "vector-db-sink" : {
                            "name" : "Vector database sink",
                            "description" : "Store vectors in a vector database.\\nConfiguration properties depends on the vector database implementation, specified by the \\"datasource\\" property.",
                            "properties" : {
                              "datasource" : {
                                "description" : "The defined datasource ID to use to store the vectors.",
                                "required" : true,
                                "type" : "string"
                              }
                            }
                          }
                        }""",
                SerializationUtil.prettyPrintJson(model));
    }
}
