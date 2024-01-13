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

class S3ProcessorAgentProviderTest {
    @Test
    @SneakyThrows
    public void testValidation() {
        validate(
                """
                topics: []
                pipeline:
                  - name: "s3-processor"
                    type: "s3-processor"
                    configuration:
                      a-field: "val"
                      objectName: "simple.pdf"
                """,
                "Found error on agent configuration (agent: 's3-processor', type: 's3-processor'). Property 'a-field' is unknown");
        validate(
                """
                topics: []
                pipeline:
                  - name: "s3-processor"
                    type: "s3-processor"
                    configuration:
                      objectName: "simple.pdf"
                """,
                null);
        validate(
                """
                topics: []
                pipeline:
                  - name: "s3-processor"
                    type: "s3-processor"
                    configuration:
                      bucketName: "my-bucket"
                      access-key: KK
                      secret-key: SS
                      endpoint: "http://localhost:9000"
                      region: "us-east-1"
                      objectName: "simple.pdf"
                """,
                null);
        validate(
                """
                topics: []
                pipeline:
                  - name: "s3-processor"
                    type: "s3-processor"
                    configuration:
                      bucketName: 12
                      objectName: "simple.pdf"
                """,
                null);
        validate(
                """
                        topics: []
                        pipeline:
                          - name: "s3-processor"
                            type: "s3-processor"
                            configuration:
                              bucketName: {object: true}
                              objectName: "simple.pdf"
                        """,
                "Found error on agent configuration (agent: 's3-processor', type: 's3-processor'). Property 'bucketName' has a wrong data type. Expected type: java.lang.String");
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
                                "s3-processor",
                                new NoOpComputeClusterRuntimeProvider.NoOpClusterRuntime())
                        .generateSupportedTypesDocumentation();

        Assertions.assertEquals(
                """
                        {
                          "s3-processor" : {
                            "name" : "S3 Processor",
                            "description" : "Processes a file from an S3 bucket",
                            "properties" : {
                              "access-key" : {
                                "description" : "Access key for the S3 server.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "minioadmin"
                              },
                              "bucketName" : {
                                "description" : "The name of the bucket that contains the file.",
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
                              "objectName" : {
                                "description" : "The object name to read from the S3 server.",
                                "required" : true,
                                "type" : "string"
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
