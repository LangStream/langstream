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
package ai.langstream.webservice.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.impl.codestorage.NoopCodeStorageProvider;
import ai.langstream.impl.k8s.tests.KubeK3sServer;
import ai.langstream.webservice.WebAppTestConfig;
import java.nio.file.Path;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpMethod;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@Slf4j
@Import(WebAppTestConfig.class)
@DirtiesContext
class ApplicationResourceTest {

    @Autowired MockMvc mockMvc;

    @RegisterExtension static final KubeK3sServer k3s = new KubeK3sServer(true);

    protected Path tempDir;

    @BeforeEach
    public void beforeEach(@TempDir Path tempDir) {
        this.tempDir = tempDir;
    }

    @Test
    void testAppCrud() throws Exception {
        mockMvc.perform(put("/api/tenants/my-tenant")).andExpect(status().isOk());

        final String appContent =
                """
                id: app1
                name: test
                topics: []
                pipeline: []
                """;
        final String instanceContent =
                """
                instance:
                  streamingCluster:
                    type: pulsar
                  computeCluster:
                    type: none
                """;
        final String secretsContent =
                """
                secrets:
                - name: secret1
                  id: secret1
                  data:
                    key1: value1
                    key2: value2
                """;
        AppTestHelper.deployApp(
                mockMvc, "my-tenant", "test", appContent, instanceContent, secretsContent);

        AppTestHelper.updateApp(mockMvc, "my-tenant", "test", appContent, instanceContent, null);

        AppTestHelper.updateApp(mockMvc, "my-tenant", "test", appContent, null, null);

        AppTestHelper.updateApp(mockMvc, "my-tenant", "test", null, instanceContent, null);
        mockMvc.perform(get("/api/applications/my-tenant/test"))
                .andExpect(status().isOk())
                .andExpect(
                        result ->
                                assertEquals(
                                        """
                        {
                          "application-id" : "test",
                          "application" : {
                            "resources" : { },
                            "modules" : [ {
                              "id" : "default",
                              "pipelines" : [ {
                                "id" : "app1",
                                "module" : "default",
                                "name" : "test",
                                "resources" : {
                                  "parallelism" : 1,
                                  "size" : 1
                                },
                                "errors" : {
                                  "retries" : 0,
                                  "on-failure" : "fail"
                                },
                                "agents" : [ ]
                              } ],
                              "topics" : [ ]
                            } ],
                            "instance" : {
                              "streamingCluster" : {
                                "type" : "pulsar",
                                "configuration" : { }
                              },
                              "computeCluster" : {
                                "type" : "none",
                                "configuration" : { }
                              },
                              "globals" : null
                            }
                          },
                          "status" : {
                            "status" : {
                              "status" : "CREATED",
                              "reason" : null
                            },
                            "executors" : [ ]
                          }
                        }""",
                                        result.getResponse().getContentAsString()));

        mockMvc.perform(get("/api/applications/my-tenant"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].application-id").value("test"));

        mockMvc.perform(delete("/api/applications/my-tenant/test")).andExpect(status().isOk());

        // the delete is actually a crd update
        mockMvc.perform(get("/api/applications/my-tenant/test")).andExpect(status().isOk());
    }

    @Test
    void testDownloadCode() throws Exception {
        mockMvc.perform(put("/api/tenants/my-tenant2")).andExpect(status().isOk());
        AppTestHelper.deployApp(
                mockMvc,
                "my-tenant2",
                "test",
                """
                        id: app1
                        name: test
                        topics: []
                        pipeline: []
                        """,
                """
                        instance:
                          streamingCluster:
                            type: pulsar
                          computeCluster:
                            type: none
                        """,
                null);

        mockMvc.perform(multipart(HttpMethod.GET, "/api/applications/my-tenant2/test/code"))
                .andExpect(status().isOk())
                .andExpect(
                        result -> {
                            assertEquals(
                                    "attachment; filename=\"my-tenant2-test.zip\"",
                                    result.getResponse().getHeader("Content-Disposition"));
                            assertEquals(
                                    "application/zip",
                                    result.getResponse().getHeader("Content-Type"));
                            assertEquals(
                                    "content-of-the-code-archive-my-tenant2-my-tenant2-test",
                                    result.getResponse().getContentAsString());
                        });

        final String archiveId = NoopCodeStorageProvider.computeCodeArchiveId("my-tenant2", "test");

        mockMvc.perform(
                        multipart(
                                HttpMethod.GET,
                                "/api/applications/my-tenant2/test/code/" + archiveId))
                .andExpect(status().isOk())
                .andExpect(
                        result -> {
                            assertEquals(
                                    "attachment; filename=\"my-tenant2-test.zip\"",
                                    result.getResponse().getHeader("Content-Disposition"));
                            assertEquals(
                                    "application/zip",
                                    result.getResponse().getHeader("Content-Type"));
                            assertEquals(
                                    "content-of-the-code-archive-my-tenant2-my-tenant2-test",
                                    result.getResponse().getContentAsString());
                        });

        mockMvc.perform(get("/api/applications/my-tenant2/test/code/" + archiveId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.digests.java").doesNotExist())
                .andExpect(jsonPath("$.digests.python").doesNotExist());
    }

    @Test
    void testTenantInDeletion() throws Exception {
        mockMvc.perform(put("/api/tenants/my-tenant3")).andExpect(status().isOk());
        AppTestHelper.deployApp(
                mockMvc,
                "my-tenant3",
                "test",
                """
                        id: app1
                        name: test
                        topics: []
                        pipeline: []
                        """,
                """
                        instance:
                          streamingCluster:
                            type: pulsar
                          computeCluster:
                            type: none
                        """,
                null);
        k3s.getClient().namespaces().withName("langstream-my-tenant3").delete();
        AppTestHelper.deployApp(
                        mockMvc,
                        "my-tenant3",
                        "test1",
                        """
                        id: app1
                        name: test
                        topics: []
                        pipeline: []
                        """,
                        """
                        instance:
                          streamingCluster:
                            type: pulsar
                          computeCluster:
                            type: none
                        """,
                        null,
                        false)
                .andExpect(status().isInternalServerError());

        AppTestHelper.updateApp(
                        mockMvc,
                        true,
                        "my-tenant3",
                        "test1",
                        """
                                id: app1
                                name: test
                                topics: []
                                pipeline: []
                                """,
                        """
                                instance:
                                  streamingCluster:
                                    type: pulsar
                                  computeCluster:
                                    type: none
                                """,
                        null,
                        false)
                .andExpect(status().isInternalServerError());

        mockMvc.perform(get("/api/applications/my-tenant3"))
                .andExpect(status().isInternalServerError());

        mockMvc.perform(delete("/api/applications/my-tenant3/test"))
                .andExpect(status().isInternalServerError());

        mockMvc.perform(get("/api/applications/my-tenant3/test"))
                .andExpect(status().isInternalServerError());
    }

    @Test
    void testDeployDryRun() throws Exception {
        mockMvc.perform(put("/api/tenants/my-tenant4")).andExpect(status().isOk());
        AppTestHelper.updateApp(
                        mockMvc,
                        false,
                        "my-tenant4",
                        "test",
                        """
                        id: app1
                        name: test
                        topics:
                          - name: "history-topic"
                        pipeline:
                            - name: "ai-chat-completions"
                              type: "ai-chat-completions"
                              output: "history-topic"
                              configuration:
                                model: "${secrets.s1.key-s}"
                        """,
                        """
                        instance:
                          streamingCluster:
                            type: pulsar
                          computeCluster:
                            type: none
                        """,
                        """
                        secrets:
                          - id: s1
                            data:
                              key-s: value-s

                        """,
                        true,
                        Map.of("dry-run", "true"))
                .andExpect(
                        content()
                                .string(
                                        """
                                {
                                  "resources" : { },
                                  "modules" : [ {
                                    "id" : "default",
                                    "pipelines" : [ {
                                      "id" : "app1",
                                      "module" : "default",
                                      "name" : "test",
                                      "resources" : {
                                        "parallelism" : 1,
                                        "size" : 1
                                      },
                                      "errors" : {
                                        "retries" : 0,
                                        "on-failure" : "fail"
                                      },
                                      "agents" : [ {
                                        "id" : "app1-ai-chat-completions-1",
                                        "name" : "ai-chat-completions",
                                        "type" : "ai-chat-completions",
                                        "input" : null,
                                        "output" : {
                                          "connectionType" : "TOPIC",
                                          "definition" : "history-topic",
                                          "enableDeadletterQueue" : false
                                        },
                                        "configuration" : {
                                          "model" : "value-s"
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
                                      "name" : "history-topic",
                                      "config" : null,
                                      "options" : null,
                                      "keySchema" : null,
                                      "valueSchema" : null,
                                      "partitions" : 0,
                                      "implicit" : false,
                                      "creation-mode" : "none",
                                      "deletion-mode" : "none"
                                    } ]
                                  } ],
                                  "instance" : {
                                    "streamingCluster" : {
                                      "type" : "pulsar",
                                      "configuration" : { }
                                    },
                                    "computeCluster" : {
                                      "type" : "none",
                                      "configuration" : { }
                                    },
                                    "globals" : { }
                                  }
                                }"""));
    }
}
