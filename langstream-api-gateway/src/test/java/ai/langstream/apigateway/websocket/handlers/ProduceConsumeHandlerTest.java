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
package ai.langstream.apigateway.websocket.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;

import ai.langstream.api.events.EventRecord;
import ai.langstream.api.events.EventSources;
import ai.langstream.api.events.GatewayEventData;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.ApplicationSpecs;
import ai.langstream.api.model.Gateway;
import ai.langstream.api.model.Gateways;
import ai.langstream.api.model.StoredApplication;
import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.runtime.ClusterRuntimeRegistry;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.apigateway.websocket.api.ConsumePushMessage;
import ai.langstream.apigateway.websocket.api.ProduceRequest;
import ai.langstream.apigateway.websocket.api.ProduceResponse;
import ai.langstream.impl.deploy.ApplicationDeployer;
import ai.langstream.impl.parser.ModelBuilder;
import ai.langstream.kafka.extensions.KafkaContainerExtension;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import jakarta.websocket.CloseReason;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.Session;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "spring.main.allow-bean-definition-overriding=true",
        })
@WireMockTest
class ProduceConsumeHandlerTest {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    @RegisterExtension
    static KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    static List<String> topics;
    static Gateways testGateways;

    @TestConfiguration
    public static class WebSocketTestConfig {

        @Bean
        @Primary
        public ApplicationStore store() {
            final ApplicationStore mock = Mockito.mock(ApplicationStore.class);
            doAnswer(
                            invocationOnMock -> {
                                final StoredApplication storedApplication = new StoredApplication();
                                final Application application = buildApp();
                                storedApplication.setInstance(application);
                                return storedApplication;
                            })
                    .when(mock)
                    .get(anyString(), anyString(), anyBoolean());
            doAnswer(invocationOnMock -> ApplicationSpecs.builder().application(buildApp()).build())
                    .when(mock)
                    .getSpecs(anyString(), anyString());

            return mock;
        }

        @Bean
        @Primary
        public GatewayTestAuthenticationProperties gatewayTestAuthenticationProperties() {
            final GatewayTestAuthenticationProperties props =
                    new GatewayTestAuthenticationProperties();
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
    }

    @NotNull
    private static Application buildApp() throws Exception {
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
                                """
                                instance:
                                  streamingCluster:
                                    type: "kafka"
                                    configuration:
                                      admin:
                                        bootstrap.servers: "%s"
                                  computeCluster:
                                     type: "none"
                                """
                                        .formatted(kafkaContainer.getBootstrapServers()),
                                null)
                        .getApplication();
        application.setGateways(testGateways);
        return application;
    }

    @LocalServerPort int port;

    @Autowired ApplicationStore store;

    static WireMock wireMock;
    static String wireMockBaseUrl;

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

    @Test
    void testSimpleProduceConsume() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "produce",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        List.of(),
                                        null,
                                        null),
                                new Gateway(
                                        "consume",
                                        Gateway.GatewayType.consume,
                                        topic,
                                        List.of(),
                                        null,
                                        null)));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<String> messages = new ArrayList<>();
        try (final TestWebSocketClient ignored =
                new TestWebSocketClient(
                                new TestWebSocketClient.Handler() {
                                    @Override
                                    public void onMessage(String msg) {
                                        messages.add(msg);
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onClose(CloseReason closeReason) {
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onError(Throwable throwable) {
                                        countDownLatch.countDown();
                                    }
                                })
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/consume/tenant1/application1/consume"
                                                .formatted(port)))) {
            try (final TestWebSocketClient producer =
                    new TestWebSocketClient(TestWebSocketClient.NOOP)
                            .connect(
                                    URI.create(
                                            "ws://localhost:%d/v1/produce/tenant1/application1/produce"
                                                    .formatted(port)))) {
                final ProduceRequest produceRequest =
                        new ProduceRequest(null, "this is a message", null);
                produce(produceRequest, producer);
            }
            countDownLatch.await();
            assertMessagesContent(
                    List.of(new MsgRecord(null, "this is a message", Map.of())), messages);
        }
    }

    private void prepareTopicsForTest(String... topic) {
        topics = List.of(topic);
        final ApplicationDeployer deployer =
                ApplicationDeployer.builder()
                        .pluginsRegistry(new PluginsRegistry())
                        .registry(new ClusterRuntimeRegistry())
                        .build();
        final StreamingCluster streamingCluster =
                new StreamingCluster(
                        "kafka",
                        Map.of(
                                "admin",
                                Map.of(
                                        "bootstrap.servers",
                                        kafkaContainer.getBootstrapServers(),
                                        "default.api.timeout.ms",
                                        5000)));
        new ClusterRuntimeRegistry()
                .getStreamingClusterRuntime(streamingCluster)
                .deploy(
                        deployer.createImplementation(
                                "app", store.get("t", "app", false).getInstance()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"consume", "produce"})
    void testParametersRequired(String type) {
        final String topic = genTopic();
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "gw",
                                        Gateway.GatewayType.valueOf(type),
                                        topic,
                                        List.of("session-id"),
                                        null,
                                        null)));
        connectAndExpectClose(
                URI.create("ws://localhost:%d/v1/%s/tenant1/application1/gw".formatted(port, type)),
                new CloseReason(
                        CloseReason.CloseCodes.VIOLATED_POLICY,
                        "missing required parameter session-id"));
        connectAndExpectClose(
                URI.create(
                        "ws://localhost:%d/v1/%s/tenant1/application1/gw?param:otherparam=1"
                                .formatted(port, type)),
                new CloseReason(
                        CloseReason.CloseCodes.VIOLATED_POLICY,
                        "missing required parameter session-id"));
        connectAndExpectClose(
                URI.create(
                        "ws://localhost:%d/v1/%s/tenant1/application1/gw?param:session-id="
                                .formatted(port, type)),
                new CloseReason(
                        CloseReason.CloseCodes.VIOLATED_POLICY,
                        "missing required parameter session-id"));

        connectAndExpectClose(
                URI.create(
                        ("ws://localhost:%d/v1/%s/tenant1/application1/gw?param:session-id=ok&param:another-non"
                                        + "-declared=y")
                                .formatted(port, type)),
                new CloseReason(
                        CloseReason.CloseCodes.VIOLATED_POLICY,
                        "unknown parameters: [another-non-declared]"));

        connectAndExpectRunning(
                URI.create(
                        "ws://localhost:%d/v1/%s/tenant1/application1/gw?param:session-id=1"
                                .formatted(port, type)));
        connectAndExpectRunning(
                URI.create(
                        "ws://localhost:%d/v1/%s/tenant1/application1/gw?param:session-id=string-value"
                                .formatted(port, type)));
    }

    @Test
    void testFilterOutMessagesByFixedValue() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "produce",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        List.of("session-id"),
                                        new Gateway.ProduceOptions(
                                                List.of(
                                                        Gateway.KeyValueComparison.value(
                                                                "header1", "langstream"))),
                                        null),
                                new Gateway(
                                        "produce-non-langstream",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        List.of("session-id"),
                                        null,
                                        null),
                                new Gateway(
                                        "consume",
                                        Gateway.GatewayType.consume,
                                        topic,
                                        List.of("session-id"),
                                        null,
                                        new Gateway.ConsumeOptions(
                                                new Gateway.ConsumeOptionsFilters(
                                                        List.of(
                                                                Gateway.KeyValueComparison.value(
                                                                        "header1",
                                                                        "langstream")))))));

        List<String> user1Messages = new ArrayList<>();
        List<String> user2Messages = new ArrayList<>();

        @Cleanup
        final ClientSession client1 =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:session-id=user1"
                                        .formatted(port)),
                        user1Messages);
        @Cleanup
        final ClientSession client2 =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:session-id=user2"
                                        .formatted(port)),
                        user2Messages);

        try (final TestWebSocketClient producer =
                new TestWebSocketClient(TestWebSocketClient.NOOP)
                        .connect(
                                URI.create(
                                        ("ws://localhost:%d/v1/produce/tenant1/application1/produce-non-langstream?param:session-id"
                                                        + "=user1")
                                                .formatted(port)))) {
            final ProduceRequest produceRequest =
                    new ProduceRequest(null, "this is a message non from langstream", null);
            produce(produceRequest, producer);
        }

        try (final TestWebSocketClient producer =
                new TestWebSocketClient(TestWebSocketClient.NOOP)
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:session-id=user1"
                                                .formatted(port)))) {
            final ProduceRequest produceRequest =
                    new ProduceRequest(null, "this is a message for everyone", null);
            produce(produceRequest, producer);
        }

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(
                                                        null,
                                                        "this is a message for everyone",
                                                        Map.of("header1", "langstream"))),
                                        user1Messages));
        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(
                                                        null,
                                                        "this is a message for everyone",
                                                        Map.of("header1", "langstream"))),
                                        user2Messages));
    }

    @Test
    void testAuthentication() {
        final String topic = genTopic();
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "produce",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        new Gateway.Authentication("test-auth", Map.of(), true),
                                        List.of(),
                                        new Gateway.ProduceOptions(
                                                List.of(
                                                        Gateway.KeyValueComparison
                                                                .valueFromAuthentication(
                                                                        "header1", "login"))),
                                        null),
                                new Gateway(
                                        "consume",
                                        Gateway.GatewayType.consume,
                                        topic,
                                        new Gateway.Authentication("test-auth", Map.of(), true),
                                        List.of(),
                                        null,
                                        new Gateway.ConsumeOptions(
                                                new Gateway.ConsumeOptionsFilters(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromAuthentication(
                                                                                "header1",
                                                                                "login")))))));

        connectAndExpectClose(
                URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/produce"
                                .formatted(port)),
                new CloseReason(
                        CloseReason.CloseCodes.VIOLATED_POLICY,
                        "missing required parameter session-id"));
        connectAndExpectClose(
                URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?credentials="
                                .formatted(port)),
                new CloseReason(
                        CloseReason.CloseCodes.VIOLATED_POLICY,
                        "missing required parameter session-id"));
        connectAndExpectClose(
                URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?credentials=error"
                                .formatted(port)),
                new CloseReason(
                        CloseReason.CloseCodes.VIOLATED_POLICY,
                        "missing required parameter session-id"));
        connectAndExpectRunning(
                URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?credentials=test-user-password"
                                .formatted(port)));

        List<String> user1Messages = new ArrayList<>();
        List<String> user2Messages = new ArrayList<>();

        @Cleanup
        final ClientSession client1 =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?credentials=test-user-password&option:position=earliest"
                                        .formatted(port)),
                        user1Messages);
        @Cleanup
        final ClientSession client2 =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?credentials=test-user-password-2&option:position=earliest"
                                        .formatted(port)),
                        user2Messages);

        connectAndProduce(
                URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?credentials=test-user-password"
                                .formatted(port)),
                new ProduceRequest(null, "hello user", null));

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(
                                                        null,
                                                        "hello user",
                                                        Map.of("header1", "test-user-password"))),
                                        user1Messages));
        assertEquals(List.of(), user2Messages);
    }

    @Test
    void testTestCredentials() {
        wireMock.register(
                WireMock.get("/auth/tenant1")
                        .withHeader("Authorization", WireMock.equalTo("Bearer test-user-password"))
                        .withHeader("h1", WireMock.equalTo("v1"))
                        .willReturn(WireMock.ok("")));
        final String topic = genTopic();
        prepareTopicsForTest(topic);

        List<String> user1Messages = new ArrayList<>();

        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "produce",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        new Gateway.Authentication("test-auth", Map.of(), true),
                                        List.of(),
                                        new Gateway.ProduceOptions(
                                                List.of(
                                                        Gateway.KeyValueComparison
                                                                .valueFromAuthentication(
                                                                        "header1", "login"))),
                                        null),
                                new Gateway(
                                        "consume",
                                        Gateway.GatewayType.consume,
                                        topic,
                                        new Gateway.Authentication("test-auth", Map.of(), true),
                                        List.of(),
                                        null,
                                        new Gateway.ConsumeOptions(
                                                new Gateway.ConsumeOptionsFilters(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromAuthentication(
                                                                                "header1",
                                                                                "login"))))),
                                new Gateway(
                                        "consume-no-test",
                                        Gateway.GatewayType.consume,
                                        topic,
                                        new Gateway.Authentication("test-auth", Map.of(), false),
                                        List.of(),
                                        null,
                                        null)));

        @Cleanup
        final ClientSession client1 =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?test-credentials=test-user-password"
                                        .formatted(port)),
                        user1Messages);

        connectAndProduce(
                URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?test-credentials=test-user-password"
                                .formatted(port)),
                new ProduceRequest(null, "hello user", null));

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(
                                                        null,
                                                        "hello user",
                                                        Map.of(
                                                                "header1",
                                                                "9d75ff199d33e051209b59702de27d1e470eafb58ac6d8865788bf23b48e6818"))),
                                        user1Messages));

        connectAndExpectClose(
                URI.create(
                        "ws://localhost:%d/v1/consume/tenant1/application1/consume-no-admin?test-credentials=test-user-password"
                                .formatted(port)),
                new CloseReason(
                        CloseReason.CloseCodes.VIOLATED_POLICY,
                        "Gateway consume-no-test of tenant tenant1 does not allow test mode."));

        connectAndExpectClose(
                URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?test-credentials=test-user-password-but-wrong"
                                .formatted(port)),
                new CloseReason(CloseReason.CloseCodes.VIOLATED_POLICY, "Invalid credentials"));
    }

    private record MsgRecord(Object key, Object value, Map<String, String> headers) {}

    private void assertMessagesContent(List<MsgRecord> expected, List<String> actual) {
        assertEquals(
                expected,
                actual.stream()
                        .map(
                                string -> {
                                    try {
                                        ConsumePushMessage consume =
                                                MAPPER.readValue(string, ConsumePushMessage.class);
                                        return new MsgRecord(
                                                consume.record().key(),
                                                consume.record().value(),
                                                consume.record().headers());
                                    } catch (JsonProcessingException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                        .collect(Collectors.toList()));
    }

    private void produce(ProduceRequest produceRequest, TestWebSocketClient producer)
            throws JsonProcessingException {
        final String json = MAPPER.writeValueAsString(produceRequest);
        producer.send(json);
    }

    @Test
    void testFilterOutMessagesByParamValue() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "produce",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        List.of("session-id"),
                                        new Gateway.ProduceOptions(
                                                List.of(
                                                        Gateway.KeyValueComparison
                                                                .valueFromParameters(
                                                                        "header1", "session-id"))),
                                        null),
                                new Gateway(
                                        "consume",
                                        Gateway.GatewayType.consume,
                                        topic,
                                        List.of("session-id"),
                                        null,
                                        new Gateway.ConsumeOptions(
                                                new Gateway.ConsumeOptionsFilters(
                                                        List.of(
                                                                Gateway.KeyValueComparison
                                                                        .valueFromParameters(
                                                                                "header1",
                                                                                "session-id")))))));

        List<String> user1Messages = new ArrayList<>();
        List<String> user2Messages = new ArrayList<>();

        @Cleanup
        final ClientSession client1 =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:session-id=user1&option:position=earliest"
                                        .formatted(port)),
                        user1Messages);
        @Cleanup
        final ClientSession client2 =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:session-id=user2"
                                        .formatted(port)),
                        user2Messages);

        try (final TestWebSocketClient producer =
                new TestWebSocketClient(TestWebSocketClient.NOOP)
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:session-id=user1"
                                                .formatted(port)))) {
            final ProduceRequest produceRequest =
                    new ProduceRequest(null, "this is a message for user1", null);
            produce(produceRequest, producer);
        }

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(
                                                        null,
                                                        "this is a message for user1",
                                                        Map.of("header1", "user1"))),
                                        user1Messages));

        assertEquals(List.of(), user2Messages);

        try (final TestWebSocketClient producer =
                new TestWebSocketClient(TestWebSocketClient.NOOP)
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:session-id=user1"
                                                .formatted(port)))) {
            final ProduceRequest produceRequest =
                    new ProduceRequest(null, "this is a message for user1, again", null);
            produce(produceRequest, producer);
        }
        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(
                                                        null,
                                                        "this is a message for user1",
                                                        Map.of("header1", "user1")),
                                                new MsgRecord(
                                                        null,
                                                        "this is a message for user1, again",
                                                        Map.of("header1", "user1"))),
                                        user1Messages));
        assertEquals(List.of(), user2Messages);

        try (final TestWebSocketClient producer =
                new TestWebSocketClient(TestWebSocketClient.NOOP)
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:session-id=user2"
                                                .formatted(port)))) {
            final ProduceRequest produceRequest =
                    new ProduceRequest(null, "this is a message for user2", null);
            produce(produceRequest, producer);
        }

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(
                                                        null,
                                                        "this is a message for user2",
                                                        Map.of("header1", "user2"))),
                                        user2Messages));

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(
                                                        null,
                                                        "this is a message for user1",
                                                        Map.of("header1", "user1")),
                                                new MsgRecord(
                                                        null,
                                                        "this is a message for user1, again",
                                                        Map.of("header1", "user1"))),
                                        user1Messages));
    }

    @Test
    void testProduce() {
        final String topic = genTopic();
        prepareTopicsForTest(topic);

        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "gw",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        List.of("session-id"),
                                        new Gateway.ProduceOptions(
                                                List.of(
                                                        Gateway.KeyValueComparison
                                                                .valueFromParameters(
                                                                        "header1", "session-id"))),
                                        null)));
        ProduceResponse response =
                connectAndProduce(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/gw?param:session-id=s"
                                        .formatted(port)),
                        new ProduceRequest(
                                null, "hello", Map.of("header0", "value0", "header2", "value2")));

        assertEquals(ProduceResponse.Status.OK, response.status());

        response =
                connectAndProduce(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/gw?param:session-id=s"
                                        .formatted(port)),
                        new ProduceRequest(null, "hello", Map.of("header1", "value1")));

        assertEquals(ProduceResponse.Status.BAD_REQUEST, response.status());
        assertEquals("Header header1 is configured as parameter-level header.", response.reason());

        response =
                connectAndProduce(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/gw?param:session-id=s"
                                        .formatted(port)),
                        "{}");

        assertEquals(ProduceResponse.Status.BAD_REQUEST, response.status());
        assertEquals("Either key or value must be set.", response.reason());

        response =
                connectAndProduce(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/gw?param:session-id=s"
                                        .formatted(port)),
                        "invalid-json");

        assertEquals(ProduceResponse.Status.BAD_REQUEST, response.status());
        assertEquals(
                "Unrecognized token 'invalid': was expecting (JSON String, Number, Array, Object or token "
                        + "'null', 'true' or 'false')\n"
                        + " at [Source: (String)\"invalid-json\"; line: 1, column: 8]",
                response.reason());
    }

    @Test
    void testStartFromOffsets() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "produce",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        List.of(),
                                        null,
                                        null),
                                new Gateway(
                                        "consume",
                                        Gateway.GatewayType.consume,
                                        topic,
                                        List.of(),
                                        null,
                                        null)));

        List<String> messages = new ArrayList<>();

        @Cleanup
        final ClientSession client1 =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume"
                                        .formatted(port)),
                        messages);

        try (final TestWebSocketClient producer =
                new TestWebSocketClient(TestWebSocketClient.NOOP)
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/produce/tenant1/application1/produce"
                                                .formatted(port)))) {
            final ProduceRequest produceRequest = new ProduceRequest(null, "msg1", null);
            produce(produceRequest, producer);
        }

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(new MsgRecord(null, "msg1", Map.of())), messages));
        final String msg1Offset =
                MAPPER.readValue(messages.get(0), ConsumePushMessage.class).offset();

        try (final TestWebSocketClient producer =
                new TestWebSocketClient(TestWebSocketClient.NOOP)
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/produce/tenant1/application1/produce"
                                                .formatted(port)))) {
            final ProduceRequest produceRequest = new ProduceRequest(null, "msg2", null);
            produce(produceRequest, producer);
        }

        List<String> messagesFromOffset = new ArrayList<>();
        @Cleanup
        final ClientSession client1Offset =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?option:position=%s"
                                        .formatted(port, msg1Offset)),
                        messagesFromOffset);

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(new MsgRecord(null, "msg2", Map.of())),
                                        messagesFromOffset));

        List<String> messagesFromEarliest = new ArrayList<>();
        @Cleanup
        final ClientSession clientFromEarliest =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?option:position=earliest"
                                        .formatted(port)),
                        messagesFromEarliest);

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(null, "msg1", Map.of()),
                                                new MsgRecord(null, "msg2", Map.of())),
                                        messagesFromEarliest));

        List<String> messagesFromLatest = new ArrayList<>();
        @Cleanup
        final ClientSession clientFromLatest =
                connectAndCollectMessages(
                        URI.create(
                                "ws://localhost:%d/v1/consume/tenant1/application1/consume?option:position=latest"
                                        .formatted(port)),
                        messagesFromLatest);

        try (final TestWebSocketClient producer =
                new TestWebSocketClient(TestWebSocketClient.NOOP)
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/produce/tenant1/application1/produce"
                                                .formatted(port)))) {
            final ProduceRequest produceRequest = new ProduceRequest(null, "msg3", null);
            produce(produceRequest, producer);
        }

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(null, "msg1", Map.of()),
                                                new MsgRecord(null, "msg2", Map.of()),
                                                new MsgRecord(null, "msg3", Map.of())),
                                        messages));

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(null, "msg2", Map.of()),
                                                new MsgRecord(null, "msg3", Map.of())),
                                        messagesFromOffset));

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(
                                                new MsgRecord(null, "msg1", Map.of()),
                                                new MsgRecord(null, "msg2", Map.of()),
                                                new MsgRecord(null, "msg3", Map.of())),
                                        messagesFromEarliest));

        Awaitility.await()
                .untilAsserted(
                        () ->
                                assertMessagesContent(
                                        List.of(new MsgRecord(null, "msg3", Map.of())),
                                        messagesFromLatest));
    }

    @Test
    void testConcurrentConsume() throws Exception {
        final String topic = genTopic();
        prepareTopicsForTest(topic);
        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "produce",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        List.of(),
                                        null,
                                        null),
                                new Gateway(
                                        "consume",
                                        Gateway.GatewayType.consume,
                                        topic,
                                        List.of(),
                                        null,
                                        null)));

        List<List<String>> allMessages = new ArrayList<>();

        List<ClientSession> clients = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            final ClientSession client1 =
                    connectAndCollectMessages(
                            URI.create(
                                    "ws://localhost:%d/v1/consume/tenant1/application1/consume?option:position=earliest"
                                            .formatted(port)),
                            new ArrayList<>());
            clients.add(client1);
        }
        try {

            try (final TestWebSocketClient producer =
                    new TestWebSocketClient(TestWebSocketClient.NOOP)
                            .connect(
                                    URI.create(
                                            "ws://localhost:%d/v1/produce/tenant1/application1/produce"
                                                    .formatted(port)))) {
                final ProduceRequest produceRequest = new ProduceRequest(null, "msg1", null);
                produce(produceRequest, producer);
            }

            for (List<String> messages : allMessages) {
                Awaitility.await()
                        .untilAsserted(
                                () ->
                                        assertMessagesContent(
                                                List.of(new MsgRecord(null, "msg1", Map.of())),
                                                messages));
            }

            try (final TestWebSocketClient producer =
                    new TestWebSocketClient(TestWebSocketClient.NOOP)
                            .connect(
                                    URI.create(
                                            "ws://localhost:%d/v1/produce/tenant1/application1/produce"
                                                    .formatted(port)))) {
                final ProduceRequest produceRequest = new ProduceRequest(null, "msg2", null);
                produce(produceRequest, producer);
            }

            for (List<String> messages : allMessages) {
                Awaitility.await()
                        .untilAsserted(
                                () ->
                                        assertMessagesContent(
                                                List.of(
                                                        new MsgRecord(null, "msg1", Map.of()),
                                                        new MsgRecord(null, "msg2", Map.of())),
                                                messages));
            }
        } finally {
            for (ClientSession client : clients) {
                client.close();
            }
        }
    }

    static AtomicInteger topicCounter = new AtomicInteger();

    private static String genTopic() {
        return "topic" + topicCounter.incrementAndGet();
    }

    @SneakyThrows
    private void connectAndExpectClose(URI connectTo, CloseReason expectedCloseReason) {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        AtomicReference<CloseReason> closeReason = new AtomicReference<>();
        try (final TestWebSocketClient client =
                new TestWebSocketClient(
                                new TestWebSocketClient.Handler() {
                                    @Override
                                    public void onMessage(String msg) {
                                        fail("should not receive a message");
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onClose(CloseReason cr) {
                                        closeReason.set(cr);
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onError(Throwable throwable) {
                                        throw new RuntimeException(throwable);
                                    }
                                })
                        .connect(connectTo)) {
            Thread.sleep(5000);
            countDownLatch.await();
            assertEquals(
                    expectedCloseReason.getReasonPhrase(), closeReason.get().getReasonPhrase());
            assertEquals(expectedCloseReason.getCloseCode(), closeReason.get().getCloseCode());
        } catch (DeploymentException e) {
            // ok
        }
    }

    @SneakyThrows
    private void connectAndExpectRunning(URI connectTo) {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        AtomicReference<CloseReason> closeReason = new AtomicReference<>();
        try (final TestWebSocketClient client =
                new TestWebSocketClient(
                                new TestWebSocketClient.Handler() {
                                    @Override
                                    public void onOpen(Session session) {
                                        TestWebSocketClient.Handler.super.onOpen(session);
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onMessage(String msg) {}

                                    @Override
                                    public void onClose(CloseReason cr) {
                                        closeReason.set(cr);
                                        countDownLatch.countDown();
                                    }
                                })
                        .connect(connectTo)) {
            client.send("message");
            countDownLatch.await();
            assertNull(closeReason.get());
        }
    }

    @SneakyThrows
    private ProduceResponse connectAndProduce(URI connectTo, ProduceRequest produceRequest) {
        return connectAndProduce(connectTo, new ObjectMapper().writeValueAsString(produceRequest));
    }

    @SneakyThrows
    private ProduceResponse connectAndProduce(URI connectTo, String json) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicReference<ProduceResponse> response = new AtomicReference<>();

        AtomicReference<CloseReason> closeReason = new AtomicReference<>();
        try (final TestWebSocketClient client =
                new TestWebSocketClient(
                                new TestWebSocketClient.Handler() {
                                    @Override
                                    public void onOpen(Session session) {
                                        TestWebSocketClient.Handler.super.onOpen(session);
                                    }

                                    @Override
                                    @SneakyThrows
                                    public void onMessage(String msg) {
                                        response.set(
                                                new ObjectMapper()
                                                        .readValue(msg, ProduceResponse.class));
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onClose(CloseReason cr) {
                                        closeReason.set(cr);
                                        countDownLatch.countDown();
                                    }
                                })
                        .connect(connectTo)) {
            client.send(json);
            countDownLatch.await();
            assertNull(closeReason.get());
            return response.get();
        }
    }

    interface ClientSession {
        CloseReason getCloseReason();

        void assertOk();

        void close();
    }

    @SneakyThrows
    private ClientSession connectAndCollectMessages(URI connectTo, List<String> collect) {
        AtomicReference<CloseReason> closeReason = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        final TestWebSocketClient client =
                new TestWebSocketClient(
                                new TestWebSocketClient.Handler() {
                                    @Override
                                    public void onOpen(Session session) {
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onMessage(String msg) {
                                        collect.add(msg);
                                    }

                                    @Override
                                    public void onClose(CloseReason cr) {
                                        closeReason.set(cr);
                                    }
                                })
                        .connect(connectTo);
        countDownLatch.await();
        return new ClientSession() {
            @Override
            public CloseReason getCloseReason() {
                return closeReason.get();
            }

            @Override
            public void assertOk() {
                assertNull(closeReason.get());
            }

            @Override
            @SneakyThrows
            public void close() {
                this.assertOk();
                client.close();
            }
        };
    }

    @Test
    void testSendEvents() throws Exception {
        final String topic = genTopic();
        final String eventsTopic = genTopic();
        prepareTopicsForTest(topic, eventsTopic);
        testGateways =
                new Gateways(
                        List.of(
                                new Gateway(
                                        "produce",
                                        Gateway.GatewayType.produce,
                                        topic,
                                        null,
                                        List.of("p"),
                                        null,
                                        null,
                                        eventsTopic),
                                new Gateway(
                                        "consume",
                                        Gateway.GatewayType.consume,
                                        eventsTopic,
                                        null,
                                        List.of("p"),
                                        null,
                                        null,
                                        eventsTopic)));

        CountDownLatch consumerReady = new CountDownLatch(1);
        CountDownLatch countDownLatch = new CountDownLatch(5);
        List<String> messages = new ArrayList<>();
        try (final TestWebSocketClient consumerClient =
                new TestWebSocketClient(
                                new TestWebSocketClient.Handler() {
                                    @Override
                                    public void onMessage(String msg) {
                                        messages.add(msg);
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onOpen(Session session) {
                                        consumerReady.countDown();
                                    }

                                    @Override
                                    public void onClose(CloseReason closeReason) {
                                        countDownLatch.countDown();
                                    }

                                    @Override
                                    public void onError(Throwable throwable) {
                                        countDownLatch.countDown();
                                    }
                                })
                        .connect(
                                URI.create(
                                        "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:p=consumer"
                                                .formatted(port)))) {
            consumerReady.await();

            try (final TestWebSocketClient producer =
                    new TestWebSocketClient(TestWebSocketClient.NOOP)
                            .connect(
                                    URI.create(
                                            "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:p=producer"
                                                    .formatted(port)))) {
                final ProduceRequest produceRequest =
                        new ProduceRequest(null, "this is a message", null);
                produce(produceRequest, producer);
            }
            new TestWebSocketClient(new TestWebSocketClient.Handler() {})
                    .connect(
                            URI.create(
                                    "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:p=consumer1"
                                            .formatted(port)))
                    .close();

            countDownLatch.await();
            System.out.println("got messages: " + messages);

            ConsumePushMessage msg = MAPPER.readValue(messages.get(0), ConsumePushMessage.class);
            EventRecord event = MAPPER.readValue(msg.record().value() + "", EventRecord.class);
            assertEquals(EventRecord.Categories.Gateway, event.getCategory());
            assertEquals(EventRecord.Types.ClientConnected + "", event.getType());
            EventSources.GatewaySource source =
                    MAPPER.convertValue(event.getSource(), EventSources.GatewaySource.class);
            assertEquals("tenant1", source.getTenant());
            assertEquals("application1", source.getApplicationId());
            assertEquals("consume", source.getGateway().id());
            GatewayEventData data = MAPPER.convertValue(event.getData(), GatewayEventData.class);
            assertEquals("consumer", data.getUserParameters().get("p"));
            assertEquals(0, data.getOptions().size());
            assertNotNull(data.getHttpRequestHeaders().get("host"));
            assertTrue(event.getTimestamp() > 0);

            msg = MAPPER.readValue(messages.get(1), ConsumePushMessage.class);
            event = MAPPER.readValue(msg.record().value() + "", EventRecord.class);
            assertEquals(EventRecord.Categories.Gateway, event.getCategory());
            assertEquals(EventRecord.Types.ClientConnected + "", event.getType());
            source = MAPPER.convertValue(event.getSource(), EventSources.GatewaySource.class);
            assertEquals("tenant1", source.getTenant());
            assertEquals("application1", source.getApplicationId());
            assertEquals("produce", source.getGateway().id());
            data = MAPPER.convertValue(event.getData(), GatewayEventData.class);
            assertEquals("producer", data.getUserParameters().get("p"));
            assertNotNull(data.getHttpRequestHeaders().get("host"));
            assertEquals(0, data.getOptions().size());
            assertTrue(event.getTimestamp() > 0);

            msg = MAPPER.readValue(messages.get(2), ConsumePushMessage.class);
            event = MAPPER.readValue(msg.record().value() + "", EventRecord.class);
            assertEquals(EventRecord.Categories.Gateway, event.getCategory());
            assertEquals(EventRecord.Types.ClientDisconnected + "", event.getType());
            source = MAPPER.convertValue(event.getSource(), EventSources.GatewaySource.class);
            assertEquals("tenant1", source.getTenant());
            assertEquals("application1", source.getApplicationId());
            assertEquals("produce", source.getGateway().id());
            data = MAPPER.convertValue(event.getData(), GatewayEventData.class);
            assertEquals("producer", data.getUserParameters().get("p"));
            assertNotNull(data.getHttpRequestHeaders().get("host"));
            assertEquals(0, data.getOptions().size());
            assertTrue(event.getTimestamp() > 0);

            msg = MAPPER.readValue(messages.get(3), ConsumePushMessage.class);
            event = MAPPER.readValue(msg.record().value() + "", EventRecord.class);
            assertEquals(EventRecord.Categories.Gateway, event.getCategory());
            assertEquals(EventRecord.Types.ClientConnected + "", event.getType());
            source = MAPPER.convertValue(event.getSource(), EventSources.GatewaySource.class);
            assertEquals("tenant1", source.getTenant());
            assertEquals("application1", source.getApplicationId());
            assertEquals("consume", source.getGateway().id());
            data = MAPPER.convertValue(event.getData(), GatewayEventData.class);
            assertEquals("consumer1", data.getUserParameters().get("p"));
            assertNotNull(data.getHttpRequestHeaders().get("host"));
            assertEquals(0, data.getOptions().size());
            assertTrue(event.getTimestamp() > 0);

            msg = MAPPER.readValue(messages.get(4), ConsumePushMessage.class);
            event = MAPPER.readValue(msg.record().value() + "", EventRecord.class);
            assertEquals(EventRecord.Categories.Gateway, event.getCategory());
            assertEquals(EventRecord.Types.ClientDisconnected + "", event.getType());
            source = MAPPER.convertValue(event.getSource(), EventSources.GatewaySource.class);
            assertEquals("tenant1", source.getTenant());
            assertEquals("application1", source.getApplicationId());
            assertEquals("consume", source.getGateway().id());
            data = MAPPER.convertValue(event.getData(), GatewayEventData.class);
            assertEquals("consumer1", data.getUserParameters().get("p"));
            assertNotNull(data.getHttpRequestHeaders().get("host"));
            assertEquals(0, data.getOptions().size());
            assertTrue(event.getTimestamp() > 0);
        }
    }
}
