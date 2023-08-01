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
package com.datastax.oss.sga.webservice.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import com.datastax.oss.sga.cli.commands.applications.AbstractDeployApplicationCmd;
import com.datastax.oss.sga.impl.k8s.tests.KubeK3sServer;
import com.datastax.oss.sga.webservice.WebAppTestConfig;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

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
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultMatcher;

@SpringBootTest
@AutoConfigureMockMvc
@Slf4j
@Import(WebAppTestConfig.class)
@DirtiesContext
class ApplicationResourceTest {

    @Autowired
    MockMvc mockMvc;

    @RegisterExtension
    static final KubeK3sServer k3s = new KubeK3sServer(true);

    protected Path tempDir;

    @BeforeEach
    public void beforeEach(@TempDir Path tempDir) throws Exception {
        this.tempDir = tempDir;
    }

    protected File createTempFile(String content) {
        try {
            Path tempFile = Files.createTempFile(tempDir, "sga-cli-test", ".yaml");
            Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8));
            return tempFile.toFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    private MockMultipartFile getMultipartFile(String application, String instance, String secrets) throws Exception {

        final Path zip = AbstractDeployApplicationCmd.buildZip(
                application == null ? null : createTempFile(application),
                instance == null ? null : createTempFile(instance),
                secrets == null ? null : createTempFile(secrets), s -> log.info(s));
        MockMultipartFile firstFile = new MockMultipartFile(
                "file", "content.zip", MediaType.APPLICATION_OCTET_STREAM_VALUE,
                Files.newInputStream(zip));
        return firstFile;

    }


    @Test
    void testAppCrud() throws Exception {
        mockMvc.perform(put("/api/tenants/my-tenant"))
                        .andExpect(status().isOk());
        mockMvc
                .perform(
                        multipart(HttpMethod.POST, "/api/applications/my-tenant/test")
                                .file(getMultipartFile("""
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
                                        """
                                            secrets:
                                            - name: secret1
                                              id: secret1
                                              data:
                                                key1: value1
                                                key2: value2
                                            """))
                )
                .andExpect(status().isOk());

        mockMvc
                .perform(
                        multipart(HttpMethod.PUT, "/api/applications/my-tenant/test")
                                .file(getMultipartFile("""
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
                                        null))
                )
                .andExpect(status().isOk());

        mockMvc
                .perform(
                        multipart(HttpMethod.PUT, "/api/applications/my-tenant/test")
                                .file(getMultipartFile("""
                                                id: app1
                                                name: test
                                                topics: []
                                                pipeline: []
                                                """,
                                        null,
                                        null))
                )
                .andExpect(status().isOk());

        mockMvc
                .perform(
                        multipart(HttpMethod.PUT, "/api/applications/my-tenant/test")
                                .file(getMultipartFile(null,
                                        """
                                                        instance:
                                                          streamingCluster:
                                                            type: pulsar
                                                          computeCluster:
                                                            type: none
                                                        """,
                                        null))
                )
                .andExpect(status().isOk());


        mockMvc
                .perform(
                        get("/api/applications/my-tenant/test")
                )
                .andExpect(status().isOk())
                .andExpect(result -> assertEquals("""
                        {
                          "applicationId" : "test",
                          "instance" : {
                            "resources" : { },
                            "modules" : {
                              "default" : {
                                "id" : "default",
                                "pipelines" : {
                                  "app1" : {
                                    "id" : "app1",
                                    "module" : "default",
                                    "name" : "test",
                                    "resources" : {
                                      "parallelism" : 1,
                                      "size" : 1
                                    },
                                    "errors" : {
                                      "retries" : 0,
                                      "on-failure" : "fail",
                                      "dead-letter-topic" : null
                                    },
                                    "agents" : [ ]
                                  }
                                },
                                "topics" : { }
                              }
                            },
                            "dependencies" : [ ],
                            "gateways" : null,
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
                            },
                            "secrets" : null
                          },
                          "status" : {
                            "status" : {
                              "status" : "CREATED",
                              "reason" : null
                            },
                            "agents" : { }
                          }
                        }""", result.getResponse().getContentAsString()));

        mockMvc
                .perform(
                        get("/api/applications/my-tenant")
                )
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.[0].applicationId").value("test"));

        mockMvc
                .perform(
                        delete("/api/applications/my-tenant/test")
                )
                .andExpect(status().isOk());

        mockMvc
                .perform(
                        get("/api/applications/my-tenant/test")
                )
                .andExpect(status().isNotFound());
    }

}
