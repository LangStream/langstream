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

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.admin.client.AdminClient;
import ai.langstream.admin.client.AdminClientConfiguration;
import ai.langstream.admin.client.HttpRequestFailedException;
import ai.langstream.admin.client.http.HttpClientFacade;
import ai.langstream.cli.CLILogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WireMockTest
@Slf4j
class AbstractDeployApplicationCmdTest {

    protected WireMock wireMock;
    protected String wireMockBaseUrl;

    @BeforeEach
    public void beforeEach(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        wireMock = wmRuntimeInfo.getWireMock();
        wireMockBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
    }

    @Test
    void testRemoteFile() throws Exception {
        wireMock.register(
                WireMock.get("/my-remote-dir/my-remote-file").willReturn(WireMock.ok("content!")));

        final HttpClientFacade client = new HttpClientFacade();
        final File file =
                AbstractDeployApplicationCmd.downloadHttpsFile(
                        wireMockBaseUrl + "/my-remote-dir/my-remote-file",
                        client,
                        new CLILogger.SystemCliLogger(),
                        null,
                        false);
        assertEquals("content!", Files.readString(file.toPath()));

        try {
            AbstractDeployApplicationCmd.downloadHttpsFile(
                    wireMockBaseUrl + "/unknown",
                    client,
                    new CLILogger.SystemCliLogger(),
                    null,
                    false);
            fail();
        } catch (HttpRequestFailedException ex) {
            assertEquals(404, ex.getResponse().statusCode());
        }
    }

    @Test
    void testDependencies() throws Exception {

        wireMock.register(WireMock.get("/the-dep.jar").willReturn(WireMock.ok("content!")));
        final String sha =
                "bed0f8673c13f8431d5e4f5e4a0e496b2eddc4a18e03cff19256964a6132521a485afa59ace702b76c2832274d1c3914e3fec0221fee9d698eaedc7f5810a284";

        Object content =
                Map.of(
                        "configuration",
                        Map.of(
                                "dependencies",
                                List.of(
                                        Map.of(
                                                "name",
                                                "My dep",
                                                "url",
                                                wireMockBaseUrl + "/the-dep.jar",
                                                "sha512sum",
                                                "-",
                                                "type",
                                                "java-library"))));
        testConfig(content, false, "File at " + wireMockBaseUrl + "/the-dep.jar, seems corrupted");

        content =
                Map.of(
                        "configuration",
                        Map.of(
                                "dependencies",
                                List.of(
                                        Map.of(
                                                "name",
                                                "My dep",
                                                "url",
                                                wireMockBaseUrl + "/the-dep.jar",
                                                "sha512sum",
                                                "",
                                                "type",
                                                "java-library"))));
        testConfig(content, false, "File at " + wireMockBaseUrl + "/the-dep.jar, seems corrupted");

        content =
                Map.of(
                        "configuration",
                        Map.of(
                                "dependencies",
                                List.of(
                                        Map.of(
                                                "name",
                                                "My dep",
                                                "url",
                                                wireMockBaseUrl + "/the-dep.jar"))));
        testConfig(content, false, "dependency type must be set");

        content =
                Map.of(
                        "configuration",
                        Map.of(
                                "dependencies",
                                List.of(
                                        Map.of(
                                                "name",
                                                "My dep",
                                                "url",
                                                wireMockBaseUrl + "/the-dep.jar",
                                                "sha512sum",
                                                sha,
                                                "type",
                                                "java-library"),
                                        Map.of(
                                                "name",
                                                "My dep2",
                                                "url",
                                                wireMockBaseUrl + "/the-dep.jar",
                                                "sha512sum",
                                                sha,
                                                "type",
                                                "java-library"))));
        testConfig(content, true, null);
    }

    private void testConfig(Object content, boolean expectOk, String errMessage) throws Exception {
        final Path appDir = Files.createTempDirectory("langstream-cli-test");
        final Path configurationFile = appDir.resolve("configuration.yaml");
        new ObjectMapper(new YAMLFactory()).writeValue(configurationFile.toFile(), content);

        final AdminClient adminClient =
                new AdminClient(
                        AdminClientConfiguration.builder()
                                .webServiceUrl(wireMockBaseUrl)
                                .tenant("t")
                                .build());
        try {
            AbstractDeployApplicationCmd.downloadDependencies(
                    appDir, adminClient, System.out::println);
        } catch (Throwable t) {
            t.printStackTrace();
            if (expectOk) {
                fail(errMessage, t);
            }
            assertFalse(Files.exists(appDir.resolve("the-dep.jar")));
            assertEquals(errMessage, t.getMessage());
            return;
        }
        if (expectOk) {
            assertTrue(Files.exists(appDir.resolve(Path.of("java", "lib", "the-dep.jar"))));
        } else {
            fail();
        }
    }
}
