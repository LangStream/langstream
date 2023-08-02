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
package com.datastax.oss.sga.cli.commands.applications;

import static com.github.tomakehurst.wiremock.client.WireMock.aMultipart;
import com.datastax.oss.sga.cli.commands.BaseCmd;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.BinaryEqualToPattern;
import com.github.tomakehurst.wiremock.matching.MatchResult;
import com.github.tomakehurst.wiremock.matching.MultipartValuePatternBuilder;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class AppsCmdTest extends CommandTestBase {

    class InterceptDeploySupport {
        private final Path extract = Paths.get(tempDir.toFile().getAbsolutePath(), "extract");
        private final Path toExtract = Paths.get(tempDir.toFile().getAbsolutePath(), "toExtract");


        MultipartValuePatternBuilder multipartValuePatternBuilder() {
            return aMultipart()
                    .withName("file")
                    .withBody(new BinaryEqualToPattern("") {
                        @Override
                        @SneakyThrows
                        public MatchResult match(byte[] value) {
                            Files.write(extract, value);
                            new net.lingala.zip4j.ZipFile(extract.toFile()).extractAll(
                                    toExtract.toFile().getAbsolutePath());
                            return MatchResult.exactMatch();
                        }
                    });
        }

        ;

        @SneakyThrows
        public int countFiles() {
            return Files.list(toExtract).collect(Collectors.toList()).size();
        }

        @SneakyThrows
        public String getFileContent(String localZipName) {
            return Files.readString(Paths.get(toExtract.toFile().getAbsolutePath(), localZipName));
        }
    }

    @Test
    public void testDeploy() throws Exception {
        Path sga = Files.createTempDirectory("sga");
        final String app = createTempFile("module: module-1", sga);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final InterceptDeploySupport support = new InterceptDeploySupport();
        wireMock.register(WireMock.post("/api/applications/%s/my-app"
                        .formatted(TENANT))
                .withMultipartRequestBody(support.multipartValuePatternBuilder())
                .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result = executeCommand("apps", "deploy", "my-app", "-s", secrets, "-app", sga.toAbsolutePath().toString(), "-i", instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());

        Assertions.assertEquals(3, support.countFiles());
        Assertions.assertEquals("module: module-1",
                support.getFileContent(new File(app).getName()));
        Assertions.assertEquals("instance: {}",
                support.getFileContent("instance.yaml"));
        Assertions.assertEquals("secrets: []",
                support.getFileContent("secrets.yaml"));

    }

    @Test
    public void testUpdate() throws Exception {
        Path sga = Files.createTempDirectory("sga");
        final String app = createTempFile("module: module-1", sga);
        final String instance = createTempFile("instance: {}");
        final String secrets = createTempFile("secrets: []");

        final InterceptDeploySupport support = new InterceptDeploySupport();
        wireMock.register(WireMock.put("/api/applications/%s/my-app"
                        .formatted(TENANT))
                .withMultipartRequestBody(support.multipartValuePatternBuilder())
                .willReturn(WireMock.ok("{ \"name\": \"my-app\" }")));

        CommandResult result = executeCommand("apps", "update", "my-app", "-s", secrets, "-app", sga.toAbsolutePath().toString(), "-i", instance);
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());

        Assertions.assertEquals(3, support.countFiles());
        Assertions.assertEquals("module: module-1",
                support.getFileContent(new File(app).getName()));
        Assertions.assertEquals("instance: {}",
                support.getFileContent("instance.yaml"));
        Assertions.assertEquals("secrets: []",
                support.getFileContent("secrets.yaml"));

    }

    @Test
    public void testGet() throws Exception {
        final String jsonValue = """
                {
                  "applicationId": "test",
                  "instance": {
                    "resources": {
                      "OpenAI Azure configuration": {
                        "id": null,
                        "name": "OpenAI Azure configuration",
                        "type": "open-ai-configuration",
                        "configuration": {
                          "access-key": "{{ secrets.open-ai.access-key }}",
                          "provider": "azure",
                          "url": "{{ secrets.open-ai.url }}"
                        }
                      }
                    },
                    "modules": {
                      "module-1": {
                        "id": "module-1",
                        "pipelines": {
                          "pipeline-1": {
                            "id": "pipeline-1",
                            "module": "module-1",
                            "name": null,
                            "resources": {
                              "parallelism": 1,
                              "size": 1
                            },
                            "agents": [
                              {
                                "connectableType": "agent",
                                "id": "step1",
                                "name": "compute-embeddings",
                                "type": "compute-ai-embeddings",
                                "input": {
                                  "endpoint": {
                                    "connectableType": "topic",
                                    "name": "input-topic",
                                    "keySchema": null,
                                    "valueSchema": {
                                      "type": "avro",
                                      "schema": "{\\"type\\":\\"record\\",\\"namespace\\":\\"examples\\",\\"name\\":\\"Product\\",\\"fields\\":[{\\"name\\":\\"id\\",\\"type\\":\\"string\\"},{\\"name\\":\\"name\\",\\"type\\":\\"string\\"},{\\"name\\":\\"description\\",\\"type\\":\\"string\\"}]}}",
                                      "name": "Schema"
                                    },
                                    "partitions": 0,
                                    "creation-mode": "create-if-not-exists"
                                  }
                                },
                                "output": {
                                  "endpoint": {
                                    "connectableType": "topic",
                                    "name": "output-topic",
                                    "keySchema": null,
                                    "valueSchema": null,
                                    "partitions": 0,
                                    "creation-mode": "create-if-not-exists"
                                  }
                                },
                                "configuration": {
                                  "embeddings-field": "value.embeddings",
                                  "model": "text-embedding-ada-003",
                                  "text": "{{% value.name }} {{% value.description }}"
                                },
                                "resources": {
                                  "parallelism": 1,
                                  "size": 1
                                }
                              }
                            ]
                          }
                        },
                        "topics": {
                          "input-topic": {
                            "connectableType": "topic",
                            "name": "input-topic",
                            "keySchema": null,
                            "valueSchema": {
                              "type": "avro",
                              "schema": "{\\"type\\":\\"record\\",\\"namespace\\":\\"examples\\",\\"name\\":\\"Product\\",\\"fields\\":[{\\"name\\":\\"id\\",\\"type\\":\\"string\\"},{\\"name\\":\\"name\\",\\"type\\":\\"string\\"},{\\"name\\":\\"description\\",\\"type\\":\\"string\\"}]}}",
                              "name": "Schema"
                            },
                            "partitions": 0,
                            "creation-mode": "create-if-not-exists"
                          },
                          "output-topic": {
                            "connectableType": "topic",
                            "name": "output-topic",
                            "keySchema": null,
                            "valueSchema": null,
                            "partitions": 0,
                            "creation-mode": "create-if-not-exists"
                          }
                        }
                      }
                    },
                    "dependencies": [],
                    "instance": {
                      "streamingCluster": {
                        "type": "kafka",
                        "configuration": {
                          "admin": {
                            "bootstrap.servers": "my-cluster-kafka-bootstrap.kafka:9092"
                          }
                        }
                      },
                      "computeCluster": {
                        "type": "kubernetes",
                        "configuration": {}
                      },
                      "globals": null
                    },
                    "secrets": null
                  },
                  "status": {
                    "status": {
                      "status": "ERROR_DEPLOYING",
                      "reason": "pInfoParser -- Kafka version: 3.5.0\\n18:38:42.792 [main] INFO  o.a.kafka.common.utils.AppInfoParser -- Kafka commitId: c97b88d5db4de28d\\n18:38:42.792 [main] INFO  o.a.kafka.common.utils.AppInfoParser -- Kafka startTimeMs: 1690223922791\\n18:38:43.176 [kafka-admin-client-thread | adminclient-1] INFO  o.a.kafka.common.utils.AppInfoParser -- App info kafka.admin.client for adminclient-1 unregistered\\n18:38:43.186 [kafka-admin-client-thread | adminclient-1] INFO  o.a.kafka.common.metrics.Metrics -- Metrics scheduler closed\\n18:38:43.186 [kafka-admin-client-thread | adminclient-1] INFO  o.a.kafka.common.metrics.Metrics -- Closing reporter org.apache.kafka.common.metrics.JmxReporter\\n18:38:43.187 [kafka-admin-client-thread | adminclient-1] INFO  o.a.kafka.common.metrics.Metrics -- Metrics reporters closed\\n18:38:43.190 [main] ERROR c.d.o.s.r.deployer.RuntimeDeployer -- Unexpected error\\njava.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TopicExistsException: Topic 'output-topic' already exists.\\n\\tat java.base/java.util.concurrent.CompletableFuture.reportGet(CompletableFuture.java:396)\\n\\tat java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2073)\\n\\tat org.apache.kafka.common.internals.KafkaFutureImpl.get(KafkaFutureImpl.java:165)\\n\\tat com.dastastax.oss.sga.kafka.runtime.KafkaStreamingClusterRuntime.deployTopic(KafkaStreamingClusterRuntime.java:63)\\n\\tat com.dastastax.oss.sga.kafka.runtime.KafkaStreamingClusterRuntime.deploy(KafkaStreamingClusterRuntime.java:53)\\n\\tat com.datastax.oss.sga.runtime.impl.k8s.KubernetesClusterRuntime.deploy(KubernetesClusterRuntime.java:57)\\n\\tat com.datastax.oss.sga.impl.deploy.ApplicationDeployer.deploy(ApplicationDeployer.java:41)\\n\\tat com.datastax.oss.sga.runtime.deployer.RuntimeDeployer.deploy(RuntimeDeployer.java:100)\\n\\tat com.datastax.oss.sga.runtime.deployer.RuntimeDeployer.main(RuntimeDeployer.java:69)\\n\\tat com.datastax.oss.sga.runtime.Main.main(Main.java:26)\\nCaused by: org.apache.kafka.common.errors.TopicExistsException: Topic 'output-topic' already exists.\\n"
                    },
                    "agents": {
                      "step1": {
                        "status": {
                          "status": "DEPLOYING",
                          "reason": null
                        },
                        "workers": null
                      }
                    }
                  }
                }
                """;
        wireMock.register(WireMock.get("/api/applications/%s/my-app"
                .formatted(TENANT)).willReturn(WireMock.ok(jsonValue)));

        CommandResult result = executeCommand("apps", "get", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("""
                ID               STREAMING        COMPUTE          STATUS           AGENTS           RUNNERS       \s
                test             kafka            kubernetes       ERROR_DEPLOYING  0/1""", result.out());
        ObjectMapper jsonPrinter = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        result = executeCommand("apps", "get", "my-app", "-o", "json");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());

        final String expectedJson = jsonPrinter.writeValueAsString(jsonPrinter.readValue(jsonValue, JsonNode.class));
        Assertions.assertEquals(expectedJson, result.out());


        final ObjectMapper yamlPrinter = new ObjectMapper(new YAMLFactory())
                .enable(SerializationFeature.INDENT_OUTPUT)
                .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);
        final String expectedYaml = yamlPrinter.writeValueAsString(jsonPrinter.readValue(jsonValue, JsonNode.class));

        result = executeCommand("apps", "get", "my-app", "-o", "yaml");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals(expectedYaml.strip(), result.out());

    }

    @Test
    public void testDelete() throws Exception {
        wireMock.register(WireMock.delete("/api/applications/%s/my-app"
                .formatted(TENANT)).willReturn(WireMock.ok()));

        CommandResult result = executeCommand("apps", "delete", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("Application my-app deleted", result.out());

    }

    @Test
    public void testList() throws Exception {
        wireMock.register(WireMock.get("/api/applications/%s"
                .formatted(TENANT)).willReturn(WireMock.ok("[]")));

        CommandResult result = executeCommand("apps", "list");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("ID               STREAMING        COMPUTE          STATUS           AGENTS           RUNNERS", result.out());
        result = executeCommand("apps", "list",  "-o", "json");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("[ ]", result.out());
        result = executeCommand("apps", "list",  "-o", "yaml");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("--- []", result.out());

    }

    @Test
    public void testLogs() throws Exception {
        wireMock.register(WireMock.get("/api/applications/%s/my-app/logs"
                .formatted(TENANT)).willReturn(WireMock.ok()));

        CommandResult result = executeCommand("apps", "logs", "my-app");
        Assertions.assertEquals(0, result.exitCode());
        Assertions.assertEquals("", result.err());
        Assertions.assertEquals("", result.out());

    }
}
