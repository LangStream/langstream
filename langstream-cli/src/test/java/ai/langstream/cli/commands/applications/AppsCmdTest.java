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
package ai.langstream.cli.commands.applications;

import static com.github.tomakehurst.wiremock.client.WireMock.aMultipart;
import static com.github.tomakehurst.wiremock.client.WireMock.binaryEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.client.WireMock;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AppsCmdTest extends CommandTestBase {

    @Test
    public void testDeploy() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final Path zipFile =
                AbstractDeployApplicationCmd.buildZip(langstream.toFile(), System.out::println);

        wireMock.register(
                WireMock.post("/api/applications/%s/my-app".formatted(TENANT))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testDeployWithDependencies() throws Exception {

        final String fileContent = "dep-content";
        final String fileContentSha =
                "e1ebfd0f4e4a624eeeffc52c82b048739ea615dca9387630ae7767cb9957aa4ce2cf7afbd032ac8d5fcb73f42316655ea390e37399f14155ed794a6f53c066ec";
        wireMock.register(
                WireMock.get("/local/get-dependency.jar").willReturn(WireMock.ok(fileContent)));

        Path langstream = Files.createTempDirectory("langstream");
        Files.createDirectories(Path.of(langstream.toFile().getAbsolutePath(), "java", "lib"));
        final String configurationYamlContent =
                """
                configuration:
                  dependencies:
                    - name: "PostGRES JDBC Driver"
                      url: "%s"
                      sha512sum: "%s"
                      type: "java-library"
                """
                        .formatted(wireMockBaseUrl + "/local/get-dependency.jar", fileContentSha);
        Files.writeString(
                Path.of(langstream.toFile().getAbsolutePath(), "configuration.yaml"),
                configurationYamlContent);
        Files.writeString(
                Path.of(langstream.toFile().getAbsolutePath(), "java", "lib", "get-dependency.jar"),
                fileContent);
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final Path zipFile =
                AbstractDeployApplicationCmd.buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.post("/api/applications/%s/my-app".formatted(TENANT))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance);
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(0, result.exitCode());
    }

    @Test
    public void testUpdateAll() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final Path zipFile =
                AbstractDeployApplicationCmd.buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.patch(urlEqualTo("/api/applications/%s/my-app".formatted(TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "update",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateInstance() throws Exception {
        final String instance = createTempFile("instance: {}");

        wireMock.register(
                WireMock.patch(urlEqualTo("/api/applications/%s/my-app".formatted(TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result = executeCommand("apps", "update", "my-app", "-i", instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateAppAndInstance() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);
        final String instance = createTempFile("instance: {}");

        final Path zipFile =
                AbstractDeployApplicationCmd.buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.patch(urlEqualTo("/api/applications/%s/my-app".formatted(TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "update",
                        "my-app",
                        "-i",
                        instance,
                        "-app",
                        langstream.toFile().getAbsolutePath());
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateApp() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String app = createTempFile("module: module-1", langstream);

        final Path zipFile =
                AbstractDeployApplicationCmd.buildZip(langstream.toFile(), System.out::println);
        wireMock.register(
                WireMock.patch(urlEqualTo("/api/applications/%s/my-app".formatted(TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps", "update", "my-app", "-app", langstream.toFile().getAbsolutePath());
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testUpdateSecrets() throws Exception {
        final String secrets = createTempFile("secrets: []");
        wireMock.register(
                WireMock.patch(urlEqualTo("/api/applications/%s/my-app".formatted(TENANT)))
                        .withMultipartRequestBody(
                                aMultipart("secrets").withBody(equalTo("secrets: []")))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result = executeCommand("apps", "update", "my-app", "-s", secrets);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }

    @Test
    public void testGet() throws Exception {
        final String jsonValue =
                """
                {
                  "application-id" : "test",
                  "application" : {
                    "resources" : {
                      "OpenAI Azure configuration" : {
                        "id" : null,
                        "name" : "OpenAI Azure configuration",
                        "type" : "open-ai-configuration",
                        "configuration" : {
                          "access-key" : "{{ secrets.open-ai.access-key }}",
                          "provider" : "azure",
                          "url" : "{{ secrets.open-ai.url }}"
                        }
                      }
                    },
                    "modules" : [ {
                      "id" : "default",
                      "pipelines" : [ {
                        "id" : "extract-text",
                        "module" : "default",
                        "name" : "Extract and manipulate text",
                        "resources" : {
                          "parallelism" : 1,
                          "size" : 1
                        },
                        "errors" : {
                          "retries" : 0,
                          "on-failure" : "fail"
                        },
                        "agents" : [ {
                          "id" : "extract-text-s3-source-1",
                          "name" : "Read from S3",
                          "type" : "s3-source",
                          "input" : null,
                          "output" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-text-extractor-2",
                            "enableDeadletterQueue" : false
                          },
                          "configuration" : {
                            "access-key" : "{{{secrets.s3-credentials.access-key}}}",
                            "bucketName" : "{{{secrets.s3-credentials.bucket-name}}}",
                            "endpoint" : "{{{secrets.s3-credentials.endpoint}}}",
                            "idle-time" : 5,
                            "region" : "{{{secrets.s3-credentials.region}}}",
                            "secret-key" : "{{{secrets.s3-credentials.secret}}}"
                          },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        }, {
                          "id" : "extract-text-text-extractor-2",
                          "name" : "Extract text",
                          "type" : "text-extractor",
                          "input" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-s3-source-1",
                            "enableDeadletterQueue" : false
                          },
                          "output" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-text-normaliser-3",
                            "enableDeadletterQueue" : false
                          },
                          "configuration" : { },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        }, {
                          "id" : "extract-text-text-normaliser-3",
                          "name" : "Normalise text",
                          "type" : "text-normaliser",
                          "input" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-text-extractor-2",
                            "enableDeadletterQueue" : false
                          },
                          "output" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-language-detector-4",
                            "enableDeadletterQueue" : false
                          },
                          "configuration" : {
                            "make-lowercase" : true,
                            "trim-spaces" : true
                          },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        }, {
                          "id" : "extract-text-language-detector-4",
                          "name" : "Detect language",
                          "type" : "language-detector",
                          "input" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-text-normaliser-3",
                            "enableDeadletterQueue" : false
                          },
                          "output" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-text-splitter-5",
                            "enableDeadletterQueue" : false
                          },
                          "configuration" : {
                            "allowedLanguages" : [ "en" ],
                            "property" : "language"
                          },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        }, {
                          "id" : "extract-text-text-splitter-5",
                          "name" : "Split into chunks",
                          "type" : "text-splitter",
                          "input" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-language-detector-4",
                            "enableDeadletterQueue" : false
                          },
                          "output" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-document-to-json-6",
                            "enableDeadletterQueue" : false
                          },
                          "configuration" : {
                            "chunk_overlap" : 100,
                            "chunk_size" : 400,
                            "keep_separator" : false,
                            "length_function" : "cl100k_base",
                            "separators" : [ "\\n\\n", "\\n", " ", "" ],
                            "splitter_type" : "RecursiveCharacterTextSplitter"
                          },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        }, {
                          "id" : "extract-text-document-to-json-6",
                          "name" : "Convert to structured data",
                          "type" : "document-to-json",
                          "input" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-text-splitter-5",
                            "enableDeadletterQueue" : false
                          },
                          "output" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-compute-7",
                            "enableDeadletterQueue" : false
                          },
                          "configuration" : {
                            "copy-properties" : true,
                            "text-field" : "text"
                          },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        }, {
                          "id" : "extract-text-compute-7",
                          "name" : "prepare-structure",
                          "type" : "compute",
                          "input" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-document-to-json-6",
                            "enableDeadletterQueue" : false
                          },
                          "output" : {
                            "connectionType" : "AGENT",
                            "definition" : "step1",
                            "enableDeadletterQueue" : false
                          },
                          "configuration" : {
                            "fields" : [ {
                              "expression" : "properties.name",
                              "name" : "value.filename",
                              "type" : "STRING"
                            }, {
                              "expression" : "properties.chunk_id",
                              "name" : "value.chunk_id",
                              "type" : "STRING"
                            }, {
                              "expression" : "properties.language",
                              "name" : "value.language",
                              "type" : "STRING"
                            }, {
                              "expression" : "properties.chunk_num_tokens",
                              "name" : "value.chunk_num_tokens",
                              "type" : "STRING"
                            } ]
                          },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        }, {
                          "id" : "step1",
                          "name" : "compute-embeddings",
                          "type" : "compute-ai-embeddings",
                          "input" : {
                            "connectionType" : "AGENT",
                            "definition" : "extract-text-compute-7",
                            "enableDeadletterQueue" : false
                          },
                          "output" : {
                            "connectionType" : "TOPIC",
                            "definition" : "chunks-topic",
                            "enableDeadletterQueue" : false
                          },
                          "configuration" : {
                            "embeddings-field" : "value.embeddings_vector",
                            "model" : "text-embedding-ada-002",
                            "text" : "{{% value.text }}"
                          },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        } ]
                      }, {
                        "id" : "write-to-astra",
                        "module" : "default",
                        "name" : "Write to AstraDB",
                        "resources" : {
                          "parallelism" : 1,
                          "size" : 1
                        },
                        "errors" : {
                          "retries" : 0,
                          "on-failure" : "fail"
                        },
                        "agents" : [ {
                          "id" : "write-to-astra-sink-1",
                          "name" : "Write to AstraDB",
                          "type" : "sink",
                          "input" : {
                            "connectionType" : "TOPIC",
                            "definition" : "chunks-topic",
                            "enableDeadletterQueue" : false
                          },
                          "output" : null,
                          "configuration" : {
                            "auth.password" : "{{{ secrets.cassandra.password }}}",
                            "auth.username" : "{{{ secrets.cassandra.username }}}",
                            "cloud.secureConnectBundle" : "{{{ secrets.cassandra.secure-connect-bundle }}}",
                            "connector.class" : "com.datastax.oss.kafka.sink.CassandraSinkConnector",
                            "key.converter" : "org.apache.kafka.connect.storage.StringConverter",
                            "name" : "cassandra-sink",
                            "topic.chunks-topic.documents.documents.mapping" : "filename=value.filename, chunk_id=value.chunk_id, language=value.language, text=value.text, embeddings_vector=value.embeddings_vector, num_tokens=value.chunk_num_tokens",
                            "value.converter" : "org.apache.kafka.connect.storage.StringConverter"
                          },
                          "resources" : {
                            "parallelism" : 1,
                            "size" : 1
                          },
                          "errors" : {
                            "retries" : 0,
                            "on-failure" : "fail"
                          }
                        } ]
                      } ],
                      "topics" : [ {
                        "name" : "chunks-topic",
                        "config" : null,
                        "options" : null,
                        "keySchema" : null,
                        "valueSchema" : null,
                        "partitions" : 0,
                        "implicit" : false,
                        "creation-mode" : "create-if-not-exists"
                      } ]
                    } ],
                    "gateways" : {
                      "gateways" : [ {
                        "id" : "consume-chunks",
                        "type" : "consume",
                        "topic" : "chunks-topic",
                        "authentication" : null,
                        "parameters" : null,
                        "produceOptions" : null,
                        "consumeOptions" : null
                      } ]
                    },
                    "instance" : {
                      "streamingCluster" : {
                        "type" : "kafka",
                        "configuration" : {
                          "admin" : {
                            "bootstrap.servers" : "my-cluster-kafka-bootstrap.kafka:9092"
                          }
                        }
                      },
                      "computeCluster" : {
                        "type" : "kubernetes",
                        "configuration" : { }
                      },
                      "globals" : null
                    }
                  },
                  "status" : {
                    "status" : {
                      "status" : "DEPLOYED",
                      "reason" : null
                    },
                    "executors" : [ {
                      "id" : "write-to-astra-sink-1",
                      "status" : {
                        "status" : "DEPLOYED",
                        "reason" : null
                      },
                      "replicas" : [ {
                        "id" : "test-write-to-astra-sink-1-0",
                        "status" : "RUNNING",
                        "reason" : null,
                        "agents" : [ {
                          "agent-id" : "topic-source",
                          "agent-type" : "topic-source",
                          "component-type" : "SOURCE",
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194882003,
                            "last-processed-at" : 1692200828990
                          },
                          "info" : {
                            "consumer" : {
                              "kafkaConsumerMetrics" : {
                                "app-info[client-id=consumer-langstream-agent-write-to-astra-sink-1-1]" : {
                                  "commit-id" : "c97b88d5db4de28d",
                                  "start-time-ms" : 1692194882718,
                                  "version" : "3.5.0"
                                },
                                "consumer-coordinator-metrics[client-id=consumer-langstream-agent-write-to-astra-sink-1-1]" : {
                                  "assigned-partitions" : 1.0,
                                  "commit-latency-avg" : "NaN",
                                  "commit-latency-max" : "NaN",
                                  "commit-rate" : 0.0,
                                  "commit-total" : 0.0,
                                  "failed-rebalance-rate-per-hour" : 0.0,
                                  "failed-rebalance-total" : 1.0,
                                  "heartbeat-rate" : 0.34097108565193673,
                                  "heartbeat-response-time-max" : 5.0,
                                  "heartbeat-total" : 1976.0,
                                  "join-rate" : 0.0,
                                  "join-time-avg" : "NaN",
                                  "join-time-max" : "NaN",
                                  "join-total" : 1.0,
                                  "last-heartbeat-seconds-ago" : 1.0,
                                  "last-rebalance-seconds-ago" : 5935.0,
                                  "partition-assigned-latency-avg" : "NaN",
                                  "partition-assigned-latency-max" : "NaN",
                                  "partition-lost-latency-avg" : "NaN",
                                  "partition-lost-latency-max" : "NaN",
                                  "partition-revoked-latency-avg" : "NaN",
                                  "partition-revoked-latency-max" : "NaN",
                                  "rebalance-latency-avg" : "NaN",
                                  "rebalance-latency-max" : "NaN",
                                  "rebalance-latency-total" : 3066.0,
                                  "rebalance-rate-per-hour" : 0.0,
                                  "rebalance-total" : 1.0,
                                  "sync-rate" : 0.0,
                                  "sync-time-avg" : "NaN",
                                  "sync-time-max" : "NaN",
                                  "sync-total" : 1.0
                                },
                                "consumer-fetch-manager-metrics[client-id=consumer-langstream-agent-write-to-astra-sink-1-1,topic=chunks-topic,partition=0]" : {
                                  "preferred-read-replica" : -1,
                                  "records-lag" : 0.0,
                                  "records-lag-avg" : "NaN",
                                  "records-lag-max" : "NaN",
                                  "records-lead" : 0.0,
                                  "records-lead-avg" : "NaN",
                                  "records-lead-min" : "NaN"
                                },
                                "consumer-fetch-manager-metrics[client-id=consumer-langstream-agent-write-to-astra-sink-1-1,topic=chunks-topic]" : {
                                  "bytes-consumed-rate" : 0.0,
                                  "bytes-consumed-total" : 0.0,
                                  "fetch-size-avg" : "NaN",
                                  "fetch-size-max" : "NaN",
                                  "records-consumed-rate" : 0.0,
                                  "records-consumed-total" : 0.0,
                                  "records-per-request-avg" : "NaN"
                                },
                                "consumer-fetch-manager-metrics[client-id=consumer-langstream-agent-write-to-astra-sink-1-1]" : {
                                  "bytes-consumed-rate" : 0.0,
                                  "bytes-consumed-total" : 0.0,
                                  "fetch-latency-avg" : 503.57798165137615,
                                  "fetch-latency-max" : 533.0,
                                  "fetch-rate" : 1.9975809111901184,
                                  "fetch-size-avg" : "NaN",
                                  "fetch-size-max" : "NaN",
                                  "fetch-throttle-time-avg" : 0.0,
                                  "fetch-throttle-time-max" : 0.0,
                                  "fetch-total" : 11740.0,
                                  "records-consumed-rate" : 0.0,
                                  "records-consumed-total" : 0.0,
                                  "records-lag-max" : "NaN",
                                  "records-lead-min" : "NaN",
                                  "records-per-request-avg" : "NaN"
                                },
                                "consumer-metrics[client-id=consumer-langstream-agent-write-to-astra-sink-1-1]" : {
                                  "commit-sync-time-ns-total" : 0.0,
                                  "committed-time-ns-total" : 0.0,
                                  "connection-close-rate" : 0.0,
                                  "connection-close-total" : 1.0,
                                  "connection-count" : 2.0,
                                  "connection-creation-rate" : 0.0,
                                  "connection-creation-total" : 3.0,
                                  "failed-authentication-rate" : 0.0,
                                  "failed-authentication-total" : 0.0,
                                  "failed-reauthentication-rate" : 0.0,
                                  "failed-reauthentication-total" : 0.0,
                                  "incoming-byte-rate" : 47.42197964773386,
                                  "incoming-byte-total" : 283708.0,
                                  "io-ratio" : 8.506736056114945E-4,
                                  "io-time-ns-avg" : 79427.15492957746,
                                  "io-time-ns-total" : 6.782935772E9,
                                  "io-wait-ratio" : 0.9947785247954143,
                                  "io-wait-time-ns-avg" : 9.288219064084508E7,
                                  "io-wait-time-ns-total" : 5.904977084387E12,
                                  "io-waittime-total" : 5.904977084387E12,
                                  "iotime-total" : 6.782935772E9,
                                  "last-poll-seconds-ago" : 0.0,
                                  "network-io-rate" : 4.641421859964907,
                                  "network-io-total" : 27491.0,
                                  "outgoing-byte-rate" : 221.88260598856627,
                                  "outgoing-byte-total" : 1311837.0,
                                  "poll-idle-ratio-avg" : 0.49997226986885257,
                                  "reauthentication-latency-avg" : "NaN",
                                  "reauthentication-latency-max" : "NaN",
                                  "request-rate" : 2.3207109299824533,
                                  "request-size-avg" : 95.60975609756098,
                                  "request-size-max" : 175.0,
                                  "request-total" : 13746.0,
                                  "response-rate" : 2.3391111377676483,
                                  "response-total" : 13745.0,
                                  "select-rate" : 10.710110495154053,
                                  "select-total" : 58528.0,
                                  "successful-authentication-no-reauth-total" : 0.0,
                                  "successful-authentication-rate" : 0.0,
                                  "successful-authentication-total" : 0.0,
                                  "successful-reauthentication-rate" : 0.0,
                                  "successful-reauthentication-total" : 0.0,
                                  "time-between-poll-avg" : 1001.8695652173913,
                                  "time-between-poll-max" : 1019.0
                                },
                                "consumer-node-metrics[client-id=consumer-langstream-agent-write-to-astra-sink-1-1,node-id=node--1]" : {
                                  "incoming-byte-rate" : 0.0,
                                  "incoming-byte-total" : 730.0,
                                  "outgoing-byte-rate" : 0.0,
                                  "outgoing-byte-total" : 265.0,
                                  "request-latency-avg" : "NaN",
                                  "request-latency-max" : "NaN",
                                  "request-rate" : 0.0,
                                  "request-size-avg" : "NaN",
                                  "request-size-max" : "NaN",
                                  "request-total" : 3.0,
                                  "response-rate" : 0.0,
                                  "response-total" : 3.0
                                },
                                "consumer-node-metrics[client-id=consumer-langstream-agent-write-to-astra-sink-1-1,node-id=node-0]" : {
                                  "incoming-byte-rate" : 41.95150559903231,
                                  "incoming-byte-total" : 250347.0,
                                  "outgoing-byte-rate" : 163.80343587694765,
                                  "outgoing-byte-total" : 964720.0,
                                  "request-latency-avg" : "NaN",
                                  "request-latency-max" : "NaN",
                                  "request-rate" : 1.9976028765481422,
                                  "request-size-avg" : 82.0,
                                  "request-size-max" : 82.0,
                                  "request-total" : 11762.0,
                                  "response-rate" : 1.9976175203885276,
                                  "response-total" : 11761.0
                                },
                                "consumer-node-metrics[client-id=consumer-langstream-agent-write-to-astra-sink-1-1,node-id=node-2147483647]" : {
                                  "incoming-byte-rate" : 5.440870539286286,
                                  "incoming-byte-total" : 32631.0,
                                  "outgoing-byte-rate" : 59.432840889794534,
                                  "outgoing-byte-total" : 346852.0,
                                  "request-latency-avg" : "NaN",
                                  "request-latency-max" : "NaN",
                                  "request-rate" : 0.33961623365596877,
                                  "request-size-avg" : 175.0,
                                  "request-size-max" : 175.0,
                                  "request-total" : 1981.0,
                                  "response-rate" : 0.3400544087053929,
                                  "response-total" : 1981.0
                                },
                                "kafka-metrics-count[client-id=consumer-langstream-agent-write-to-astra-sink-1-1]" : {
                                  "count" : 144.0
                                }
                              }
                            },
                            "topic" : "chunks-topic"
                          }
                        }, {
                          "agent-id" : "write-to-astra-sink-1",
                          "agent-type" : "sink",
                          "component-type" : "SINK",
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194881424,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "connector.class" : "com.datastax.oss.kafka.sink.CassandraSinkConnector"
                          }
                        } ]
                      } ]
                    }, {
                      "id" : "extract-text-s3-source-1",
                      "status" : {
                        "status" : "DEPLOYED",
                        "reason" : null
                      },
                      "replicas" : [ {
                        "id" : "test-extract-text-s3-source-1-0",
                        "status" : "RUNNING",
                        "reason" : null,
                        "agents" : [ {
                          "agent-id" : "extract-text-s3-source-1",
                          "agent-type" : "s3-source",
                          "component-type" : null,
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194873570,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "bucketName" : "enrico-dev"
                          }
                        }, {
                          "agent-id" : "extract-text-text-extractor-2",
                          "agent-type" : "text-extractor",
                          "component-type" : null,
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194873570,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "errors" : 0
                          }
                        }, {
                          "agent-id" : "extract-text-text-normaliser-3",
                          "agent-type" : "text-normaliser",
                          "component-type" : null,
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194873570,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "errors" : 0
                          }
                        }, {
                          "agent-id" : "extract-text-language-detector-4",
                          "agent-type" : "language-detector",
                          "component-type" : null,
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194873570,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "errors" : 0
                          }
                        }, {
                          "agent-id" : "extract-text-text-splitter-5",
                          "agent-type" : "text-splitter",
                          "component-type" : null,
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194873570,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "errors" : 0
                          }
                        }, {
                          "agent-id" : "extract-text-document-to-json-6",
                          "agent-type" : "document-to-json",
                          "component-type" : null,
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194873570,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "errors" : 0
                          }
                        }, {
                          "agent-id" : "extract-text-compute-7",
                          "agent-type" : "compute",
                          "component-type" : null,
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194873570,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "errors" : 0
                          }
                        }, {
                          "agent-id" : "step1",
                          "agent-type" : "compute-ai-embeddings",
                          "component-type" : null,
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194873570,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "errors" : 0
                          }
                        }, {
                          "agent-id" : "topic-sink",
                          "agent-type" : "topic-sink",
                          "component-type" : "SINK",
                          "metrics" : {
                            "total-in" : 0,
                            "total-out" : 0,
                            "started-at" : 1692194879717,
                            "last-processed-at" : 0
                          },
                          "info" : {
                            "producer" : {
                              "kafkaProducerMetrics" : {
                                "app-info[client-id=producer-1]" : {
                                  "commit-id" : "c97b88d5db4de28d",
                                  "start-time-ms" : 1692194885294,
                                  "version" : "3.5.0"
                                },
                                "kafka-metrics-count[client-id=producer-1]" : {
                                  "count" : 103.0
                                },
                                "producer-metrics[client-id=producer-1]" : {
                                  "batch-size-avg" : "NaN",
                                  "batch-size-max" : "NaN",
                                  "batch-split-rate" : 0.0,
                                  "batch-split-total" : 0.0,
                                  "buffer-available-bytes" : 3.3554432E7,
                                  "buffer-exhausted-rate" : 0.0,
                                  "buffer-exhausted-total" : 0.0,
                                  "buffer-total-bytes" : 3.3554432E7,
                                  "bufferpool-wait-ratio" : 0.0,
                                  "bufferpool-wait-time-ns-total" : 0.0,
                                  "bufferpool-wait-time-total" : 0.0,
                                  "compression-rate-avg" : "NaN",
                                  "connection-close-rate" : 0.0,
                                  "connection-close-total" : 1.0,
                                  "connection-count" : 1.0,
                                  "connection-creation-rate" : 0.0,
                                  "connection-creation-total" : 2.0,
                                  "failed-authentication-rate" : 0.0,
                                  "failed-authentication-total" : 0.0,
                                  "failed-reauthentication-rate" : 0.0,
                                  "failed-reauthentication-total" : 0.0,
                                  "flush-time-ns-total" : 0.0,
                                  "incoming-byte-rate" : 2.8013404544978533,
                                  "incoming-byte-total" : 3165.0,
                                  "io-ratio" : 1.24278129657228E-4,
                                  "io-time-ns-avg" : 1000687.5,
                                  "io-time-ns-total" : 1.28880366E8,
                                  "io-wait-ratio" : 1.8557742784935887,
                                  "io-wait-time-ns-avg" : 1.4943158434E10,
                                  "io-wait-time-ns-total" : 5.940378844624E12,
                                  "io-waittime-total" : 5.940378844624E12,
                                  "iotime-total" : 1.28880366E8,
                                  "metadata-age" : 2.432,
                                  "metadata-wait-time-ns-total" : 0.0,
                                  "network-io-rate" : 0.052361503822389785,
                                  "network-io-total" : 48.0,
                                  "outgoing-byte-rate" : 0.7592418054246518,
                                  "outgoing-byte-total" : 750.0,
                                  "produce-throttle-time-avg" : 0.0,
                                  "produce-throttle-time-max" : 0.0,
                                  "reauthentication-latency-avg" : "NaN",
                                  "reauthentication-latency-max" : "NaN",
                                  "record-error-rate" : 0.0,
                                  "record-error-total" : 0.0,
                                  "record-queue-time-avg" : "NaN",
                                  "record-queue-time-max" : "NaN",
                                  "record-retry-rate" : 0.0,
                                  "record-retry-total" : 0.0,
                                  "record-send-rate" : 0.0,
                                  "record-send-total" : 0.0,
                                  "record-size-avg" : "NaN",
                                  "record-size-max" : "NaN",
                                  "records-per-request-avg" : "NaN",
                                  "request-latency-avg" : "NaN",
                                  "request-latency-max" : "NaN",
                                  "request-rate" : 0.026180751911194892,
                                  "request-size-avg" : 29.0,
                                  "request-size-max" : 29.0,
                                  "request-total" : 24.0,
                                  "requests-in-flight" : 0.0,
                                  "response-rate" : 0.026180751911194892,
                                  "response-total" : 24.0,
                                  "select-rate" : 0.12418889130367287,
                                  "select-total" : 248.0,
                                  "successful-authentication-no-reauth-total" : 0.0,
                                  "successful-authentication-rate" : 0.0,
                                  "successful-authentication-total" : 0.0,
                                  "successful-reauthentication-rate" : 0.0,
                                  "successful-reauthentication-total" : 0.0,
                                  "txn-abort-time-ns-total" : 0.0,
                                  "txn-begin-time-ns-total" : 0.0,
                                  "txn-commit-time-ns-total" : 0.0,
                                  "txn-init-time-ns-total" : 0.0,
                                  "txn-send-offsets-time-ns-total" : 0.0,
                                  "waiting-threads" : 0.0
                                },
                                "producer-node-metrics[client-id=producer-1,node-id=node--1]" : {
                                  "incoming-byte-rate" : 0.0,
                                  "incoming-byte-total" : 579.0,
                                  "outgoing-byte-rate" : 0.0,
                                  "outgoing-byte-total" : 120.0,
                                  "request-latency-avg" : "NaN",
                                  "request-latency-max" : "NaN",
                                  "request-rate" : 0.0,
                                  "request-size-avg" : "NaN",
                                  "request-size-max" : "NaN",
                                  "request-total" : 3.0,
                                  "response-rate" : 0.0,
                                  "response-total" : 3.0
                                },
                                "producer-node-metrics[client-id=producer-1,node-id=node-0]" : {
                                  "incoming-byte-rate" : 2.8013404544978533,
                                  "incoming-byte-total" : 2586.0,
                                  "outgoing-byte-rate" : 0.7592418054246518,
                                  "outgoing-byte-total" : 630.0,
                                  "request-latency-avg" : "NaN",
                                  "request-latency-max" : "NaN",
                                  "request-rate" : 0.026180751911194892,
                                  "request-size-avg" : 29.0,
                                  "request-size-max" : 29.0,
                                  "request-total" : 21.0,
                                  "response-rate" : 0.026180751911194892,
                                  "response-total" : 21.0
                                }
                              }
                            },
                            "topic" : "chunks-topic"
                          }
                        } ]
                      } ]
                    } ]
                  }
                }
                """;
        wireMock.register(
                WireMock.get("/api/applications/%s/my-app?stats=false".formatted(TENANT))
                        .willReturn(WireMock.ok(jsonValue)));

        CommandResult result = executeCommand("apps", "get", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(
                """
                        ID          STREAMING   COMPUTE     STATUS      EXECUTORS   REPLICAS \s
                        test        kafka       kubernetes  DEPLOYED    2/2         2/2""",
                result.out());
        ObjectMapper jsonPrinter =
                new ObjectMapper()
                        .enable(SerializationFeature.INDENT_OUTPUT)
                        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        result = executeCommand("apps", "get", "my-app", "-o", "json");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());

        final String expectedJson =
                jsonPrinter.writeValueAsString(jsonPrinter.readValue(jsonValue, JsonNode.class));
        Assertions.assertEquals(expectedJson, result.out());

        final ObjectMapper yamlPrinter =
                new ObjectMapper(new YAMLFactory())
                        .enable(SerializationFeature.INDENT_OUTPUT)
                        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        final String expectedYaml =
                yamlPrinter.writeValueAsString(jsonPrinter.readValue(jsonValue, JsonNode.class));

        result = executeCommand("apps", "get", "my-app", "-o", "yaml");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(expectedYaml.strip(), result.out());
    }

    @Test
    public void testDelete() {
        wireMock.register(
                WireMock.delete("/api/applications/%s/my-app".formatted(TENANT))
                        .willReturn(WireMock.ok()));

        CommandResult result = executeCommand("apps", "delete", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Application my-app deleted", result.out());
    }

    @Test
    public void testList() {
        wireMock.register(
                WireMock.get("/api/applications/%s".formatted(TENANT))
                        .willReturn(WireMock.ok("[]")));

        CommandResult result = executeCommand("apps", "list");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(
                "ID         STREAMING  COMPUTE    STATUS     EXECUTORS  REPLICAS", result.out());
        result = executeCommand("apps", "list", "-o", "json");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("[ ]", result.out());
        result = executeCommand("apps", "list", "-o", "yaml");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("--- []", result.out());
    }

    @Test
    public void testLogs() {
        wireMock.register(
                WireMock.get("/api/applications/%s/my-app/logs".formatted(TENANT))
                        .willReturn(WireMock.ok()));

        CommandResult result = executeCommand("apps", "logs", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("", result.out());
    }

    @Test
    public void testDownload() {
        wireMock.register(
                WireMock.get("/api/applications/%s/my-app/code".formatted(TENANT))
                        .willReturn(WireMock.ok()));

        final File expectedFile = new File("%s-my-app.zip".formatted(TENANT));
        try {
            CommandResult result = executeCommand("apps", "download", "my-app");
            Assertions.assertEquals(0, result.exitCode());
            Assertions.assertEquals("", result.err());
            Assertions.assertEquals(
                    "Downloaded application code to " + expectedFile.getAbsolutePath(),
                    result.out());
        } finally {
            try {
                Files.deleteIfExists(expectedFile.toPath());
            } catch (IOException e) {
            }
        }
    }

    @Test
    public void testDownloadToFile() {
        wireMock.register(
                WireMock.get("/api/applications/%s/my-app/code".formatted(TENANT))
                        .willReturn(WireMock.ok()));

        CommandResult result =
                executeCommand("apps", "download", "my-app", "-o", "/tmp/download.zip");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Downloaded application code to /tmp/download.zip", result.out());
    }

    @Test
    public void testDeployWithFilePlaceholders() throws Exception {
        Path langstream = Files.createTempDirectory("langstream");
        final String instance = createTempFile("instance: {}");
        final String jsonFileRelative =
                Paths.get(createTempFile("{\"client-id\":\"xxx\"}")).getFileName().toString();
        assertFalse(jsonFileRelative.contains("/"));

        final String secrets =
                createTempFile(
                        """
                                             secrets:
                                                  - name: vertex-ai
                                                    id: vertex-ai
                                                    data:
                                                      url: https://us-central1-aiplatform.googleapis.com
                                                      token: xxx
                                                      serviceAccountJson: "<file:%s>"
                                                      region: us-central1
                                                      project: myproject
                                             """
                                .formatted(jsonFileRelative));

        final Path zipFile =
                AbstractDeployApplicationCmd.buildZip(langstream.toFile(), System.out::println);

        wireMock.register(
                WireMock.post("/api/applications/%s/my-app".formatted(TENANT))
                        .withMultipartRequestBody(
                                aMultipart("app")
                                        .withBody(binaryEqualTo(Files.readAllBytes(zipFile))))
                        .withMultipartRequestBody(
                                aMultipart("instance").withBody(equalTo("instance: {}")))
                        .withMultipartRequestBody(
                                aMultipart("secrets")
                                        .withBody(
                                                equalTo(
                                                        """
                                       ---
                                       secrets:
                                       - name: "vertex-ai"
                                         id: "vertex-ai"
                                         data:
                                           url: "https://us-central1-aiplatform.googleapis.com"
                                           token: "xxx"
                                           serviceAccountJson: "{\\"client-id\\":\\"xxx\\"}"
                                           region: "us-central1"
                                           project: "myproject"
                                        """)))
                        .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result =
                executeCommand(
                        "apps",
                        "deploy",
                        "my-app",
                        "-s",
                        secrets,
                        "-app",
                        langstream.toAbsolutePath().toString(),
                        "-i",
                        instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
    }
}
