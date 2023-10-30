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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.api.ConsumePushMessage;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.apigateway.runner.TopicConnectionsRuntimeProviderBean;
import ai.langstream.impl.deploy.ApplicationDeployer;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.main.allow-bean-definition-overriding=true",
        })
@WireMockTest
@Slf4j
abstract class GatewayResourceTest {

    public static final Path agentsDirectory;
    protected static final HttpClient CLIENT = HttpClient.newHttpClient();

    static {
        agentsDirectory = Path.of(System.getProperty("user.dir"), "target", "agents");
        log.info("Agents directory is {}", agentsDirectory);
    }

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    static List<String> topics;
    static Gateways testGateways;

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

        return mock;
    }

    protected static GatewayTestAuthenticationProperties getGatewayTestAuthenticationProperties() {
        final GatewayTestAuthenticationProperties props = new GatewayTestAuthenticationProperties();
        props.setType("http");
        props.setConfiguration(
                Map.of(
                        "base-url",
                        wireMockBaseUrl,
                        "path-template",
                        "/auth/{tenant}",
                        "headers",
                        Map.of("h1", "v1")));
        return props;
    }

    @Autowired private TopicConnectionsRuntimeProviderBean topicConnectionsRuntimeProvider;

    @NotNull
    private static Application buildApp(String instanceYaml) throws Exception {
        final Map<String, Object> module =
                Map.of(
                        "module",
                        "mod1",
                        "id",
                        "p",
                        "topics",
                        topics.stream()
                                .map(
                                        t ->
                                                Map.of(
                                                        "name",
                                                        t,
                                                        "creation-mode",
                                                        "create-if-not-exists"))
                                .collect(Collectors.toList()));

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

    @Autowired ApplicationStore store;

    static WireMock wireMock;
    static String wireMockBaseUrl;
    static AtomicInteger topicCounter = new AtomicInteger();

    private static String genTopic() {
        return "topic" + topicCounter.incrementAndGet();
    }

    @BeforeAll
    public static void beforeAll(WireMockRuntimeInfo wmRuntimeInfo) {
        wireMock = wmRuntimeInfo.getWireMock();
        wireMockBaseUrl = wmRuntimeInfo.getHttpBaseUrl();
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

    @SneakyThrows
    void produceAndExpectOk(String url, String content) {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(content))
                        .build();
        final HttpResponse<String> response =
                CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("""
                {"status":"OK","reason":null}""", response.body());
    }

    @SneakyThrows
    String produceAndGetBody(String url, String content) {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(content))
                        .build();
        final HttpResponse<String> response =
                CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    @SneakyThrows
    void produceAndExpectBadRequest(String url, String content, String errorMessage) {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(content))
                        .build();
        final HttpResponse<String> response =
                CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode());
        log.info("Response body: {}", response.body());
        final Map map = new ObjectMapper().readValue(response.body(), Map.class);
        String detail = (String) map.get("detail");
        assertTrue(detail.contains(errorMessage));
    }

    @SneakyThrows
    void produceAndExpectUnauthorized(String url, String content) {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(content))
                        .build();
        final HttpResponse<String> response =
                CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(401, response.statusCode());
        log.info("Response body: {}", response.body());
    }

    @Test
    void testSimpleProduce() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .build(),
                                Gateway.builder()
                                        .id("consume")
                                        .type(Gateway.GatewayType.consume)
                                        .topic(topic)
                                        .build()));

        final String url =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);

        produceAndExpectOk(url, "{\"key\": \"my-key\", \"value\": \"my-value\"}");
        produceAndExpectOk(url, "{\"key\": \"my-key\"}");
        produceAndExpectOk(url, "{\"key\": \"my-key\", \"headers\": {\"h1\": \"v1\"}}");
    }

    @Test
    void testParametersRequired() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("gw")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .parameters(List.of("session-id"))
                                        .build()));

        final String baseUrl =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/gw".formatted(port);

        final String content = "{\"value\": \"my-value\"}";
        produceAndExpectBadRequest(baseUrl, content, "missing required parameter session-id");
        produceAndExpectBadRequest(
                baseUrl + "?param:otherparam=1", content, "missing required parameter session-id");
        produceAndExpectBadRequest(
                baseUrl + "?param:session-id=", content, "missing required parameter session-id");
        produceAndExpectBadRequest(
                baseUrl + "?param:session-id=ok&param:another-non-declared=y",
                content,
                "unknown parameters: [another-non-declared]");
        produceAndExpectOk(baseUrl + "?param:session-id=1", content);
        produceAndExpectOk(baseUrl + "?param:session-id=string-value", content);
    }

    @Test
    void testAuthentication() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth", Map.of(), true))
                                        .produceOptions(
                                                new Gateway.ProduceOptions(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromAuthentication(
                                                                                "header1",
                                                                                "login"))))
                                        .build(),
                                Gateway.builder()
                                        .id("consume")
                                        .type(Gateway.GatewayType.consume)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth", Map.of(), true))
                                        .consumeOptions(
                                                new Gateway.ConsumeOptions(
                                                        new Gateway.ConsumeOptionsFilters(
                                                                List.of(
                                                                        Gateway.KeyValueComparison
                                                                                .valueFromAuthentication(
                                                                                        "header1",
                                                                                        "login")))))
                                        .build()));

        final String baseUrl =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);

        produceAndExpectUnauthorized(baseUrl, "{\"value\": \"my-value\"}");
        produceAndExpectUnauthorized(baseUrl + "?credentials=", "{\"value\": \"my-value\"}");
        produceAndExpectUnauthorized(baseUrl + "?credentials=error", "{\"value\": \"my-value\"}");
        produceAndExpectOk(
                baseUrl + "?credentials=test-user-password", "{\"value\": \"my-value\"}");
    }

    @Test
    void testTestCredentials() throws Exception {
        wireMock.register(
                WireMock.get("/auth/tenant1")
                        .withHeader("Authorization", WireMock.equalTo("Bearer test-user-password"))
                        .withHeader("h1", WireMock.equalTo("v1"))
                        .willReturn(WireMock.ok("")));
        final String topic = genTopic();
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("produce")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth", Map.of(), true))
                                        .produceOptions(
                                                new Gateway.ProduceOptions(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromAuthentication(
                                                                                "header1",
                                                                                "login"))))
                                        .build(),
                                Gateway.builder()
                                        .id("produce-no-test")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .authentication(
                                                new Gateway.Authentication(
                                                        "test-auth", Map.of(), false))
                                        .build()));

        final String baseUrl =
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce"
                        .formatted(port);

        produceAndExpectUnauthorized(
                baseUrl + "?test-credentials=test", "{\"value\": \"my-value\"}");
        produceAndExpectOk(
                baseUrl + "?test-credentials=test-user-password", "{\"value\": \"my-value\"}");
        produceAndExpectUnauthorized(
                "http://localhost:%d/api/gateways/produce/tenant1/application1/produce-no-test?test-credentials=test-user-password"
                        .formatted(port),
                "{\"value\": \"my-value\"}");
    }


    @Test
    void testService() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("svc")
                                        .type(Gateway.GatewayType.service)
                                        .serviceOptions(
                                                new Gateway.ServiceOptions(
                                                        topic, topic, List.of())
                                        )
                                        .build()));

        final String url =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc"
                        .formatted(port);


        assertMessageContent(new MsgRecord("my-key", "my-value", Map.of()), produceAndGetBody(url, "{\"key\": \"my-key\", \"value\": \"my-value\"}"));
        assertMessageContent(new MsgRecord("my-key2", "my-value", Map.of()), produceAndGetBody(url, "{\"key\": \"my-key2\", \"value\": \"my-value\"}"));
        assertMessageContent(new MsgRecord("my-key2", "my-value", Map.of("header1", "value1")), produceAndGetBody(url, "{\"key\": \"my-key2\", \"value\": \"my-value\", \"headers\": {\"header1\":\"value1\"}}"));

    }

    private record MsgRecord(Object key, Object value, Map<String, String> headers) {}

    @SneakyThrows
    private void assertMessageContent(MsgRecord expected, String actual) {
        ConsumePushMessage consume =
                MAPPER.readValue(actual, ConsumePushMessage.class);
        final MsgRecord actualMsgRecord = new MsgRecord(
                consume.record().key(),
                consume.record().value(),
                consume.record().headers());

        assertEquals(expected, actualMsgRecord);
    }




    protected abstract StreamingCluster getStreamingCluster();

    private void prepareTopicsForTest(String... topic) throws Exception {
        topics = List.of(topic);
        TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                topicConnectionsRuntimeProvider.getTopicConnectionsRuntimeRegistry();
        final ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .pluginsRegistry(new PluginsRegistry())
                        .registry(new ClusterRuntimeRegistry())
                        .topicConnectionsRuntimeRegistry(topicConnectionsRuntimeRegistry)
                        .build();
        final StreamingCluster streamingCluster = getStreamingCluster();
        topicConnectionsRuntimeRegistry
                .getTopicConnectionsRuntime(streamingCluster)
                .asTopicConnectionsRuntime()
                .deploy(
                        deployer.createImplementation(
                                "app", store.get("t", "app", false).getInstance()));
    }
}
