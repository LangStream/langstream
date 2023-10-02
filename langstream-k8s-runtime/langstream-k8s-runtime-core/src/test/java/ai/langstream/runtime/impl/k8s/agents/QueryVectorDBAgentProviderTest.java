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
                "Found error on an agent configuration (agent: 'db', type: 'query-vector-db'). Property 'unknown-field' is unknown");

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
                "Found error on an agent configuration (agent: 'db', type: 'vector-db-sink'). Property 'datasource' is required");

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
                                "s3-source",
                                new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """
                {
                  "s3-source" : {
                    "name" : "S3 Source",
                    "description" : "Reads data from S3 bucket",
                    "properties" : {
                      "access-key" : {
                        "description" : "Access key for the S3 server.",
                        "required" : false,
                        "type" : "string",
                        "defaultValue" : "minioadmin"
                      },
                      "bucketName" : {
                        "description" : "The name of the bucket to read from.",
                        "required" : false,
                        "type" : "string",
                        "defaultValue" : "langstream-source"
                      },
                      "endpoint" : {
                        "description" : "The endpoint of the S3 server.",
                        "required" : false,
                        "type" : "string",
                        "defaultValue" : "http://minio-endpoint.-not-set:9090"
                      },
                      "file-extensions" : {
                        "description" : "Comma separated list of file extensions to filter by.",
                        "required" : false,
                        "type" : "string",
                        "defaultValue" : "pdf,docx,html,htm,md,txt"
                      },
                      "idle-time" : {
                        "description" : "Region for the S3 server.",
                        "required" : false,
                        "type" : "integer",
                        "defaultValue" : "5"
                      },
                      "region" : {
                        "description" : "Region for the S3 server.",
                        "required" : false,
                        "type" : "string"
                      },
                      "secret-key" : {
                        "description" : "Secret key for the S3 server.",
                        "required" : false,
                        "type" : "string",
                        "defaultValue" : "minioadmin"
                      }
                    }
                  }
                }""",
                SerializationUtil.prettyPrintJson(model));
    }
}
