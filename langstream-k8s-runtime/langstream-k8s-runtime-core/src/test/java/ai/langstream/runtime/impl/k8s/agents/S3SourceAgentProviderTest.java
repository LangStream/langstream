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

class S3SourceAgentProviderTest {
    @Test
    @SneakyThrows
    public void testValidation() {
        validate(
                """
                topics: []
                pipeline:
                  - name: "s3-source"
                    type: "s3-source"
                    configuration:
                      a-field: "val"
                """,
                "Found error on agent configuration (agent: 's3-source', type: 's3-source'). Property 'a-field' is unknown");
        validate(
                """
                topics: []
                pipeline:
                  - name: "s3-source"
                    type: "s3-source"
                    configuration: {}
                """,
                null);
        validate(
                """
                topics: []
                pipeline:
                  - name: "s3-source"
                    type: "s3-source"
                    configuration:
                      bucketName: "my-bucket"
                      access-key: KK
                      secret-key: SS
                      endpoint: "http://localhost:9000"
                      idle-time: 0
                      region: "us-east-1"
                      file-extensions: "csv"
                """,
                null);
        validate(
                """
                topics: []
                pipeline:
                  - name: "s3-source"
                    type: "s3-source"
                    configuration:
                      bucketName: 12
                """,
                null);
        validate(
                """
                        topics: []
                        pipeline:
                          - name: "s3-source"
                            type: "s3-source"
                            configuration:
                              bucketName: {object: true}
                        """,
                "Found error on agent configuration (agent: 's3-source', type: 's3-source'). Property 'bucketName' has a wrong data type. Expected type: java.lang.String");
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
                          "azure-blob-storage-source" : {
                            "name" : "Azure Blob Storage Source",
                            "description" : "Reads data from Azure blobs. There are three supported ways to authenticate:\\n- SAS token\\n- Storage account name and key\\n- Storage account connection string",
                            "properties" : {
                              "container" : {
                                "description" : "The name of the Azure econtainer to read from.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "langstream-azure-source"
                              },
                              "endpoint" : {
                                "description" : "Endpoint to connect to. Usually it's https://<storage-account>.blob.core.windows.net.",
                                "required" : true,
                                "type" : "string"
                              },
                              "file-extensions" : {
                                "description" : "Comma separated list of file extensions to filter by.",
                                "required" : false,
                                "type" : "string",
                                "defaultValue" : "pdf,docx,html,htm,md,txt"
                              },
                              "idle-time" : {
                                "description" : "Time in seconds to sleep after polling for new files.",
                                "required" : false,
                                "type" : "integer",
                                "defaultValue" : "5"
                              },
                              "sas-token" : {
                                "description" : "Azure SAS token. If not provided, storage account name and key must be provided.",
                                "required" : false,
                                "type" : "string"
                              },
                              "storage-account-connection-string" : {
                                "description" : "Azure storage account connection string. If not provided, SAS token must be provided.",
                                "required" : false,
                                "type" : "string"
                              },
                              "storage-account-key" : {
                                "description" : "Azure storage account key. If not provided, SAS token must be provided.",
                                "required" : false,
                                "type" : "string"
                              },
                              "storage-account-name" : {
                                "description" : "Azure storage account name. If not provided, SAS token must be provided.",
                                "required" : false,
                                "type" : "string"
                              }
                            }
                          },
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
                                "description" : "Time in seconds to sleep after polling for new files.",
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
