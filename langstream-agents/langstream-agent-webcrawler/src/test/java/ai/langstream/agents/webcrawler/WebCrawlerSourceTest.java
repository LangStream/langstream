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
package ai.langstream.agents.webcrawler;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okForContentType;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicAdmin;
import ai.langstream.api.runner.topics.TopicConnectionProvider;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.minio.MinioClient;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Slf4j
@WireMockTest
public class WebCrawlerSourceTest {

    private static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();
    private static final DockerImageName localstackImage = DockerImageName.parse("localstack/localstack:2.2.0");

    @Container
    private static final LocalStackContainer localstack = new LocalStackContainer(localstackImage)
        .withServices(S3);

    private static MinioClient minioClient;

    @BeforeAll
    static void setup() {
        minioClient = MinioClient.builder()
            .endpoint(localstack.getEndpointOverride(S3).toString())
            .build();
    }

    @Test
    @Disabled("This test is disabled because it connects to a real live website")
    void testReadLangStreamDocs() throws Exception {
        String bucket = "langstream-test-" + UUID.randomUUID();
        String url = "https://docs.langstream.ai/";
        String allowed = "https://docs.langstream.ai/";

        // perform multiple iterations, in order to see the recovery mechanism in action
        for (int i = 0; i < 3; i++) {
            log.info("ITERATION #{}", i);
            WebCrawlerSource agentSource = buildAgentSource(bucket, allowed, url);
            List<Record> read = agentSource.read();
            Set<String> urls = new HashSet<>();
            int count = 0;
            while (!read.isEmpty() && count < 30) {
                log.info("read: {}", read);
                for (Record r : read) {
                    String docUrl = r.key().toString();
                    assertTrue(urls.add(docUrl), "Read twice the same url: " + docUrl);
                }
                count += read.size();
                agentSource.commit(read);
                read = agentSource.read();
            }
            agentSource.close();
        }
    }


    @Test
    @Disabled("This test is disabled because it connects to a real live website")
    void testReadLangStreamGithubRepo() throws Exception {
        try {
            String bucket = "langstream-test-" + UUID.randomUUID();
            String url = "https://github.com/LangStream/langstream";
            String allowed = "https://github.com/LangStream/langstream";

            // perform multiple iterations, in order to see the recovery mechanism in action

            int count = 1000;
            WebCrawlerSource agentSource = buildAgentSource(bucket, allowed, url);
            List<Record> read = agentSource.read();
            Set<String> urls = new HashSet<>();
            while (count -- > 0) {
                log.info("read: {}", read);
                for (Record r : read) {
                    String docUrl = r.key().toString();
                    log.info("content: {}", new String((byte[]) r.value()));
                    assertTrue(urls.add(docUrl), "Read twice the same url: " + docUrl);
                }
                agentSource.commit(read);
                read = agentSource.read();
            }
            agentSource.close();
        } catch (Throwable error) {
            log.error("Bad error", error);
            throw error;
        }
    }


    @Test
    void testBasic(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {


        stubFor(get("/index.html")
                .willReturn(okForContentType("text/html",
                      """
                                <a href="secondPage.html">link</a>
                            """)));
        stubFor(get("/secondPage.html")
                .willReturn(okForContentType("text/html",
                        """
                                  <a href="thirdPage.html">link</a>
                                  <a href="index.html">link to home</a>
                              """)));
        stubFor(get("/thirdPage.html")
                .willReturn(okForContentType("text/html",
                        """
                                  Hello!
                              """)));

        String bucket = "langstream-test-" + UUID.randomUUID();
        String url = wmRuntimeInfo.getHttpBaseUrl() + "/index.html";
        String allowed = wmRuntimeInfo.getHttpBaseUrl();


        WebCrawlerSource agentSource = buildAgentSource(bucket, allowed, url);
        List<Record> read = agentSource.read();
        Set<String> urls = new HashSet<>();
        Map<String, String> pages = new HashMap<>();
        while (!read.isEmpty()) {
            log.info("read: {}", read);
            for (Record r : read) {
                String docUrl = r.key().toString();
                String pageName = docUrl.substring(docUrl.lastIndexOf('/') + 1);
                pages.put(pageName, new String((byte[]) r.value()));
                assertTrue(urls.add(docUrl), "Read twice the same url: " + docUrl);
            }
            agentSource.commit(read);
            read = agentSource.read();
        }
        agentSource.close();
        assertEquals(3, pages.size());
        // please note that JSoup normalised the HTML
        assertEquals("""
                        <html>
                         <head></head>
                         <body>
                          <a href="secondPage.html">link</a>
                         </body>
                        </html>""", pages.get("index.html"));
        assertEquals(
                """
                        <html>
                         <head></head>
                         <body>
                          <a href="thirdPage.html">link</a> <a href="index.html">link to home</a>
                         </body>
                        </html>""", pages.get("secondPage.html"));
        assertEquals("""
                <html>
                 <head></head>
                 <body>
                  Hello!
                 </body>
                </html>""", pages.get("thirdPage.html"));

    }

    private WebCrawlerSource buildAgentSource(String bucket, String domain, String seedUrl) throws Exception {
        AgentSource agentSource = (AgentSource) AGENT_CODE_REGISTRY.getAgentCode("webcrawler-source").agentCode();
        Map<String, Object> configs = new HashMap<>();
        String endpoint = localstack.getEndpointOverride(S3).toString();
        configs.put("endpoint", endpoint);
        configs.put("bucketName", bucket);
        configs.put("seed-urls", seedUrl);
        configs.put("allowed-domains", domain);
        configs.put("max-unflushed-pages", 5);
        configs.put("min-time-between-requests", 500);
        agentSource.init(configs);
        agentSource.setContext(new AgentContext() {
            @Override
            public TopicConsumer getTopicConsumer() {
                return null;
            }

            @Override
            public TopicProducer getTopicProducer() {
                return null;
            }

            @Override
            public String getGlobalAgentId() {
                return "test-global-agent-id";
            }

            @Override
            public TopicAdmin getTopicAdmin() {
                return null;
            }

            @Override
            public TopicConnectionProvider getTopicConnectionProvider() {
                return null;
            }
        });
        agentSource.start();
        return (WebCrawlerSource) agentSource;
    }
}
