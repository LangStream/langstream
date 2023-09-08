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

import ai.langstream.admin.client.AdminClientLogger;
import ai.langstream.admin.client.HttpRequestFailedException;
import ai.langstream.admin.client.http.HttpClientFacade;
import ai.langstream.admin.client.http.HttpClientProperties;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.File;
import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@WireMockTest
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

        final HttpClientFacade client =
                new HttpClientFacade(
                        new AdminClientLogger() {
                            @Override
                            public void log(Object message) {
                                System.out.println(message);
                            }

                            @Override
                            public void error(Object message) {
                                System.err.println(message);
                            }

                            @Override
                            public boolean isDebugEnabled() {
                                return false;
                            }

                            @Override
                            public void debug(Object message) {}
                        },
                        new HttpClientProperties());
        final File file =
                AbstractDeployApplicationCmd.downloadHttpsFile(
                        wireMockBaseUrl + "/my-remote-dir/my-remote-file",
                        client,
                        log -> System.out.println(log));
        assertEquals("content!", Files.readString(file.toPath()));

        try {
            AbstractDeployApplicationCmd.downloadHttpsFile(
                    wireMockBaseUrl + "/unknown", client, log -> System.out.println(log));
            fail();
        } catch (HttpRequestFailedException ex) {
            assertEquals(404, ex.getResponse().statusCode());
        }
    }
}
