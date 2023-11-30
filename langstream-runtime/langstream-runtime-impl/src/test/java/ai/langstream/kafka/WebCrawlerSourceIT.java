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
package ai.langstream.kafka;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okForContentType;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class WebCrawlerSourceIT extends AbstractKafkaApplicationRunner {

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    @Test
    public void test(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

        stubFor(
                get("/index.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                <a href="secondPage.html">link</a>
                            """)));
        stubFor(
                get("/secondPage.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  <a href="thirdPage.html">link</a>
                                  <a href="index.html">link to home</a>
                              """)));
        stubFor(
                get("/thirdPage.html")
                        .willReturn(
                                okForContentType(
                                        "text/html",
                                        """
                                  Hello!
                              """)));

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "${globals.output-topic}"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - type: "webcrawler-source"
                                    id: "step1"
                                    output: "${globals.output-topic}"
                                    configuration:\s
                                        seed-urls: ["%s/index.html"]
                                        allow-non-html-contents: true
                                        allowed-domains: ["%s"]
                                        state-storage: disk
                                """
                                .formatted(
                                        wireMockRuntimeInfo.getHttpBaseUrl(),
                                        wireMockRuntimeInfo.getHttpBaseUrl()));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {

            try (KafkaConsumer<String, String> consumer =
                    createConsumer(applicationRuntime.getGlobal("output-topic")); ) {

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                """
                        <html>
                         <head></head>
                         <body>
                          <a href="secondPage.html">link</a>
                         </body>
                        </html>""",
                                """
                                <html>
                                 <head></head>
                                 <body>
                                  <a href="thirdPage.html">link</a> <a href="index.html">link to home</a>
                                 </body>
                                </html>""",
                                """
                                <html>
                                 <head></head>
                                 <body>
                                  Hello!
                                 </body>
                                </html>"""));

                final Path statusFile =
                        getBasePersistenceDirectory()
                                .resolve("step1")
                                .resolve("%s-%s.webcrawler.status.json".formatted(appId, "step1"));
                assertTrue(Files.exists(statusFile));
            }
        }
    }
}
