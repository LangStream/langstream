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
package ai.langstream.apigateway.http;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.impl.parser.ModelBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.main.allow-bean-definition-overriding=true",
        })
@WireMockTest
@Slf4j
@AutoConfigureObservability
@AutoConfigureMockMvc
class ServiceAgentGatewayResourceTest {

    public static final Path agentsDirectory;
    protected static final HttpClient CLIENT = HttpClient.newHttpClient();

    static {
        agentsDirectory = Path.of(System.getProperty("user.dir"), "target", "agents");
        log.info("Agents directory is {}", agentsDirectory);
    }

    static List<String> topics;
    static Gateways testGateways;

    @TestConfiguration
    public static class WebSocketTestConfig {

        @Bean
        @Primary
        public ApplicationStore store() {
            String instanceYaml =
                    """
                    instance:
                      streamingCluster:
                        type: "noop"
                      computeCluster:
                         type: "none"
                    """;
            return getMockedStore(instanceYaml);
        }
    }

    protected static ApplicationStore getMockedStore(String instanceYaml) {
        ApplicationStore mock = Mockito.mock(ApplicationStore.class);
        doAnswer(
                        invocationOnMock -> {
                            final StoredApplication storedApplication = new StoredApplication();
                            final Application application = buildApp(instanceYaml);
                            storedApplication.setInstance(application);
                            return storedApplication;
                        })
                .when(mock)
                .get(anyString(), anyString(), anyBoolean());
        doAnswer(
                        invocationOnMock ->
                                ApplicationSpecs.builder()
                                        .application(buildApp(instanceYaml))
                                        .build())
                .when(mock)
                .getSpecs(anyString(), anyString());

        doAnswer(invocationOnMock -> "http://localhost:" + wireMockPort)
                .when(mock)
                .getExecutorServiceURI(anyString(), anyString(), anyString());

        return mock;
    }

    @NotNull
    private static Application buildApp(String instanceYaml) throws Exception {
        final Map<String, Object> module = Map.of("module", "mod1", "id", "p");

        final Application application =
                ModelBuilder.buildApplicationInstance(
                                Map.of(
                                        "module.yaml",
                                        new ObjectMapper(new YAMLFactory())
                                                .writeValueAsString(module)),
                                instanceYaml,
                                null)
                        .getApplication();
        application.setGateways(testGateways);
        return application;
    }

    @LocalServerPort int port;

    @Autowired MockMvc mockMvc;
    @Autowired ApplicationStore store;

    static WireMock wireMock;
    static String wireMockBaseUrl;
    static int wireMockPort;

    @BeforeAll
    public static void beforeAll(WireMockRuntimeInfo wmRuntimeInfo) {
        wireMock = wmRuntimeInfo.getWireMock();
        wireMockBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
        wireMockPort = wmRuntimeInfo.getHttpPort();
    }

    @BeforeEach
    public void beforeEach(WireMockRuntimeInfo wmRuntimeInfo) {
        testGateways = null;
        topics = null;
        Awaitility.setDefaultTimeout(30, TimeUnit.SECONDS);
    }

    @AfterAll
    public static void afterAll() {
        Awaitility.reset();
    }

    @Test
    void testServiceAgent() throws Exception {
        wireMock.register(
                WireMock.post("/agent-endpoint/custom-path")
                        .willReturn(WireMock.ok("agent response")));
        wireMock.register(
                WireMock.post("/agent-endpoint/custom-path-json?q=v")
                        .withHeader("X-Custom-Header", equalTo("XXX"))
                        .withHeader("Content-Type", equalTo("application/json"))
                        .willReturn(WireMock.ok("agent response")));

        wireMock.register(
                WireMock.post("/")
                        .withRequestBody(equalTo("hello"))
                        .willReturn(WireMock.ok("agent response ROOT")));
        wireMock.register(WireMock.get("/").willReturn(WireMock.ok("agent response ROOT")));

        wireMock.register(WireMock.put("/").willReturn(WireMock.ok("agent response ROOT")));
        wireMock.register(WireMock.delete("/").willReturn(WireMock.ok("agent response ROOT")));

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("svc")
                                        .type(Gateway.GatewayType.service)
                                        .serviceOptions(
                                                new Gateway.ServiceOptions(
                                                        "my-agent", null, null, List.of()))
                                        .build()));

        String url =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc".formatted(port);
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "text/plain")
                        .POST(HttpRequest.BodyPublishers.ofString("hello"))
                        .build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("agent response ROOT", response.body());

        for (String method : List.of("GET", "PUT", "DELETE")) {
            request =
                    HttpRequest.newBuilder(URI.create(url))
                            .header("Content-Type", "text/plain")
                            .method(method, HttpRequest.BodyPublishers.ofString("hello"))
                            .build();
            response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertEquals("agent response ROOT", response.body());
        }

        url =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc/not-found"
                        .formatted(port);
        request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "text/plain")
                        .POST(HttpRequest.BodyPublishers.ofString("hello"))
                        .build();
        response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        log.info("Response {}", response.body());
        assertEquals(404, response.statusCode());

        url =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc/agent-endpoint/custom-path"
                        .formatted(port);
        request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "text/plain")
                        .POST(HttpRequest.BodyPublishers.ofString("hello"))
                        .build();
        response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("agent response", response.body());

        url =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc/agent-endpoint/custom-path-json?q=v"
                        .formatted(port);
        request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .header("X-Custom-Header", "XXX")
                        .POST(HttpRequest.BodyPublishers.ofString("hello"))
                        .build();
        response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("agent response", response.body());
    }
}
