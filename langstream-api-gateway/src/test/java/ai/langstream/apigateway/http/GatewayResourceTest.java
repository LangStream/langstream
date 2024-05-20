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
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import ai.langstream.api.model.*;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.topics.TopicConnectionsRuntime;
import ai.langstream.api.runner.topics.TopicConnectionsRuntimeRegistry;
import ai.langstream.api.runner.topics.TopicConsumer;
import ai.langstream.api.runner.topics.TopicProducer;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.DeployContext;
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
import io.micrometer.core.instrument.Metrics;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;
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
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
abstract class GatewayResourceTest {

    public static final Path agentsDirectory;
    protected static final HttpClient CLIENT = HttpClient.newHttpClient();

    static {
        agentsDirectory = Path.of(System.getProperty("user.dir"), "target", "agents");
        log.info("Agents directory is {}", agentsDirectory);
    }

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    static List<String> topics;
    static List<CompletableFuture<Void>> futures = new ArrayList<>();
    static Gateways testGateways;

    protected static ApplicationStore getMockedStore(String instanceYaml) {
        wireMock.register(
                WireMock.get("/agent-endpoint/custom-path")
                        .willReturn(WireMock.ok("agent response")));

        wireMock.register(
                WireMock.get("/agent-endpoint").willReturn(WireMock.ok("agent response ROOT")));
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

    @Autowired MockMvc mockMvc;
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

    @AfterEach
    public void afterEach() {
        Metrics.globalRegistry.clear();
        for (CompletableFuture<Void> future : futures) {
            future.cancel(true);
        }
        futures.clear();
    }

    @SneakyThrows
    void produceJsonAndExpectOk(String url, String content) {
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
    String produceTextAndGetBody(String url, String content) {
        final HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .POST(HttpRequest.BodyPublishers.ofString(content))
                        .build();
        final HttpResponse<String> response =
                CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        return response.body();
    }

    @SneakyThrows
    String produceJsonAndGetBody(String url, String content) {
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
    void produceJsonAndExpectBadRequest(String url, String content, String errorMessage) {
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
    void produceJsonAndExpectUnauthorized(String url, String content) {
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
        produceJsonAndExpectOk(url, "{\"key\": \"my-key\", \"value\": \"my-value\"}");
        produceJsonAndExpectOk(url, "{\"key\": \"my-key\"}");
        produceJsonAndExpectOk(url, "{\"key\": \"my-key\", \"headers\": {\"h1\": \"v1\"}}");
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(url))
                        .POST(HttpRequest.BodyPublishers.ofString("my-string"))
                        .build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertEquals("""
                {"status":"OK","reason":null}""", response.body());

        request =
                HttpRequest.newBuilder(URI.create(url))
                        .header("Content-Type", "text/plain")
                        .POST(HttpRequest.BodyPublishers.ofString("my-string"))
                        .build();
        response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        log.info("Response body: {}", response.body());
        assertEquals(200, response.statusCode());
        assertEquals("""
                {"status":"OK","reason":null}""", response.body());
    }

    @Test
    void testSimpleProduceCacheProducer() throws Exception {
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
                                        .id("produce1")
                                        .type(Gateway.GatewayType.produce)
                                        .topic(topic)
                                        .build(),
                                Gateway.builder()
                                        .id("produce2")
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

        produceJsonAndExpectOk(url + "1", "{\"key\": \"my-key\", \"value\": \"my-value\"}");
        produceJsonAndExpectOk(url + "2", "{\"key\": \"my-key\"}");
        produceJsonAndExpectOk(url + "2", "{\"key\": \"my-key\"}");
        produceJsonAndExpectOk(url, "{\"key\": \"my-key\", \"headers\": {\"h1\": \"v1\"}}");

        final String metrics =
                mockMvc.perform(get("/management/prometheus"))
                        .andExpect(status().isOk())
                        .andReturn()
                        .getResponse()
                        .getContentAsString();

        final List<String> cacheMetrics =
                metrics.lines()
                        .filter(l -> l.contains("topic_producer_cache"))
                        .collect(Collectors.toList());
        System.out.println(cacheMetrics);
        assertEquals(5, cacheMetrics.size());

        for (String cacheMetric : cacheMetrics) {
            if (cacheMetric.contains("cache_puts_total")) {
                assertTrue(cacheMetric.contains("3.0"));
            } else if (cacheMetric.contains("hit")) {
                assertTrue(cacheMetric.contains("1.0"));
            } else if (cacheMetric.contains("miss")) {
                assertTrue(cacheMetric.contains("3.0"));
            } else if (cacheMetric.contains("cache_size")) {
                assertTrue(cacheMetric.contains("2.0"));
            } else if (cacheMetric.contains("cache_evictions_total")) {
                assertTrue(cacheMetric.contains("1.0"));
            } else {
                throw new RuntimeException(cacheMetric);
            }
        }
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
        produceJsonAndExpectBadRequest(baseUrl, content, "missing required parameter session-id");
        produceJsonAndExpectBadRequest(
                baseUrl + "?param:otherparam=1", content, "missing required parameter session-id");
        produceJsonAndExpectBadRequest(
                baseUrl + "?param:session-id=", content, "missing required parameter session-id");
        produceJsonAndExpectBadRequest(
                baseUrl + "?param:session-id=ok&param:another-non-declared=y",
                content,
                "unknown parameters: [another-non-declared]");
        produceJsonAndExpectOk(baseUrl + "?param:session-id=1", content);
        produceJsonAndExpectOk(baseUrl + "?param:session-id=string-value", content);
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

        produceJsonAndExpectUnauthorized(baseUrl, "{\"value\": \"my-value\"}");
        produceJsonAndExpectUnauthorized(baseUrl + "?credentials=", "{\"value\": \"my-value\"}");
        produceJsonAndExpectUnauthorized(
                baseUrl + "?credentials=error", "{\"value\": \"my-value\"}");
        produceJsonAndExpectOk(
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

        produceJsonAndExpectUnauthorized(
                baseUrl + "?test-credentials=test", "{\"value\": \"my-value\"}");
        produceJsonAndExpectOk(
                baseUrl + "?test-credentials=test-user-password", "{\"value\": \"my-value\"}");
        produceJsonAndExpectUnauthorized(
                ("http://localhost:%d/api/gateways/produce/tenant1/application1/produce-no-test?test-credentials=test"
                                + "-user-password")
                        .formatted(port),
                "{\"value\": \"my-value\"}");
    }

    @Test
    void testService() throws Exception {
        final String inputTopic = genTopic();
        final String outputTopic = genTopic();
        prepareTopicsForTest(inputTopic, outputTopic);

        startTopicExchange(inputTopic, outputTopic);

        testGateways =
                new Gateways(
                        List.of(
                                Gateway.builder()
                                        .id("svc")
                                        .type(Gateway.GatewayType.service)
                                        .serviceOptions(
                                                new Gateway.ServiceOptions(
                                                        null, inputTopic, outputTopic, List.of()))
                                        .build()));

        final String url =
                "http://localhost:%d/api/gateways/service/tenant1/application1/svc".formatted(port);

        assertMessageContent(
                new MsgRecord("my-key", "my-value", Map.of()),
                produceJsonAndGetBody(url, "{\"key\": \"my-key\", \"value\": \"my-value\"}"));
        assertMessageContent(
                new MsgRecord("my-key2", "my-value", Map.of()),
                produceJsonAndGetBody(url, "{\"key\": \"my-key2\", \"value\": \"my-value\"}"));

        assertMessageContent(
                new MsgRecord(null, "my-text", Map.of()), produceTextAndGetBody(url, "my-text"));
        assertMessageContent(
                new MsgRecord("my-key2", "my-value", Map.of("header1", "value1")),
                produceJsonAndGetBody(
                        url,
                        "{\"key\": \"my-key2\", \"value\": \"my-value\", \"headers\": {\"header1\":\"value1\"}}"));

        final int numParallel = 10;

        List<CompletableFuture<Void>> futures1 = new ArrayList<>();
        for (int i = 0; i < numParallel; i++) {
            CompletableFuture<Void> future =
                    CompletableFuture.runAsync(
                            () -> {
                                for (int j = 0; j < 10; j++) {
                                    assertMessageContent(
                                            new MsgRecord("my-key", "my-value", Map.of()),
                                            produceJsonAndGetBody(
                                                    url,
                                                    "{\"key\": \"my-key\", \"value\": \"my-value\"}"));
                                }
                            });
            futures1.add(future);
        }
        CompletableFuture.allOf(futures1.toArray(new CompletableFuture[] {}))
                .get(3, TimeUnit.MINUTES);
    }

    private void startTopicExchange(String logicalFromTopic, String logicalToTopic)
            throws Exception {
        final CompletableFuture<Void> future =
                CompletableFuture.runAsync(
                        () -> {
                            TopicConnectionsRuntimeRegistry topicConnectionsRuntimeRegistry =
                                    topicConnectionsRuntimeProvider
                                            .getTopicConnectionsRuntimeRegistry();
                            final StreamingCluster streamingCluster = getStreamingCluster();
                            final TopicConnectionsRuntime runtime =
                                    topicConnectionsRuntimeRegistry
                                            .getTopicConnectionsRuntime(streamingCluster)
                                            .asTopicConnectionsRuntime();
                            runtime.init(streamingCluster);
                            final String fromTopic = resolveTopicName(logicalFromTopic);
                            final String toTopic = resolveTopicName(logicalToTopic);
                            try (final TopicConsumer consumer =
                                    runtime.createConsumer(
                                            null,
                                            streamingCluster,
                                            Map.of(
                                                    "topic",
                                                    fromTopic,
                                                    "subscriptionName",
                                                    "s")); ) {
                                consumer.start();

                                try (final TopicProducer producer =
                                        runtime.createProducer(
                                                null,
                                                streamingCluster,
                                                Map.of("topic", toTopic)); ) {

                                    producer.start();
                                    while (true) {
                                        final List<Record> records = consumer.read();
                                        if (records.isEmpty()) {
                                            continue;
                                        }
                                        log.info(
                                                "read {} records from {}: {}",
                                                records.size(),
                                                fromTopic,
                                                records);
                                        for (Record record : records) {
                                            producer.write(record).get();
                                        }
                                        consumer.commit(records);
                                        log.info(
                                                "written {} records to {}: {}",
                                                records.size(),
                                                toTopic,
                                                records);
                                    }
                                }
                            } catch (Throwable e) {
                                e.printStackTrace();
                                throw new RuntimeException(e);
                            }
                        });
        futures.add(future);
    }

    private record MsgRecord(Object key, Object value, Map<String, String> headers) {}

    @SneakyThrows
    private void assertMessageContent(MsgRecord expected, String actual) {
        ConsumePushMessage consume = MAPPER.readValue(actual, ConsumePushMessage.class);
        final Map<String, String> headers = consume.record().headers();
        assertNotNull(headers.remove("langstream-service-request-id"));
        final MsgRecord actualMsgRecord =
                new MsgRecord(consume.record().key(), consume.record().value(), headers);

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
                        .deployContext(DeployContext.NO_DEPLOY_CONTEXT)
                        .build();
        final StreamingCluster streamingCluster = getStreamingCluster();
        topicConnectionsRuntimeRegistry
                .getTopicConnectionsRuntime(streamingCluster)
                .asTopicConnectionsRuntime()
                .deploy(
                        deployer.createImplementation(
                                "app", store.get("t", "app", false).getInstance()));
    }

    protected String resolveTopicName(String topic) {
        return topic;
    }
}
