package com.datastax.oss.sga.apigateway.websocket.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import com.datastax.oss.sga.api.model.Application;
import com.datastax.oss.sga.api.model.Gateway;
import com.datastax.oss.sga.api.model.Gateways;
import com.datastax.oss.sga.api.model.Instance;
import com.datastax.oss.sga.api.model.StoredApplication;
import com.datastax.oss.sga.api.model.StreamingCluster;
import com.datastax.oss.sga.api.storage.ApplicationStore;
import com.datastax.oss.sga.apigateway.websocket.api.ProduceRequest;
import com.datastax.oss.sga.apigateway.websocket.api.ProduceResponse;
import com.datastax.oss.sga.kafka.extensions.KafkaContainerExtension;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.websocket.CloseReason;
import jakarta.websocket.Session;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.web.socket.CloseStatus;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.main.allow-bean-definition-overriding=true"})
class ProduceConsumeHandlerTest {


    protected static final ObjectMapper MAPPER = new ObjectMapper();
    @RegisterExtension
    static KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    static Gateways testGateways;

    @TestConfiguration
    public static class WebSocketTestConfig {

        @Bean
        @Primary
        public ApplicationStore store() {
            final ApplicationStore mock = Mockito.mock(ApplicationStore.class);
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                    final StoredApplication storedApplication = new StoredApplication();
                    final Application application = new Application();
                    application.setGateways(testGateways);
                    application.setInstance(new Instance(
                            new StreamingCluster("kafka",
                                    Map.of("admin", Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers()))),
                            null,
                            null
                    ));
                    storedApplication.setInstance(application);
                    return storedApplication;
                }
            }).when(mock).get(anyString(), anyString());
            return mock;

        }
    }

    @LocalServerPort
    int port;

    @Autowired
    ApplicationStore store;

    @BeforeEach
    public void beforeEach() {
        testGateways = null;
    }

    @Test
    void testSimpleProduceConsume() throws Exception {
        final String topic = genTopic();
        testGateways = new Gateways(List.of(
                new Gateway("produce", Gateway.GatewayType.produce, topic, List.of(), null, null),
                new Gateway("consume", Gateway.GatewayType.consume, topic, List.of(), null, null)
        ));

        CountDownLatch countDownLatch = new CountDownLatch(1);
        try (final TestWebSocketClient consumer = new TestWebSocketClient(new TestWebSocketClient.Handler() {
            @Override
            public void onMessage(String msg) {
                try {
                    assertEquals("{\"record\":{\"key\":null,\"value\":\"this is a message\",\"headers\":{}}}", msg);
                } finally {
                    countDownLatch.countDown();
                }
            }

            @Override
            public void onClose(CloseReason closeReason) {
            }

            @Override
            public void onError(Throwable throwable) {
                fail(throwable);
            }
        }).connect(URI.create("ws://localhost:%d/v1/consume/tenant1/application1/consume".formatted(port)));) {
            try (final TestWebSocketClient producer = new TestWebSocketClient(TestWebSocketClient.NOOP)
                    .connect(
                            URI.create("ws://localhost:%d/v1/produce/tenant1/application1/produce".formatted(port)));) {
                final ProduceRequest produceRequest = new ProduceRequest(null, "this is a message", null);
                produce(produceRequest, producer);
            }
            countDownLatch.await();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"consume", "produce"})
    void testParametersRequired(String type) throws Exception {
        final String topic = genTopic();

        testGateways = new Gateways(List.of(
                new Gateway("gw", Gateway.GatewayType.valueOf(type), topic, List.of("session-id"), null, null)
        ));
        connectAndExpectClose(URI.create("ws://localhost:%d/v1/%s/tenant1/application1/gw".formatted(port, type)),
                new CloseReason(CloseReason.CloseCodes.VIOLATED_POLICY, "missing required parameter session-id"));
        connectAndExpectClose(
                URI.create("ws://localhost:%d/v1/%s/tenant1/application1/gw?param:otherparam=1".formatted(port, type)),
                new CloseReason(CloseReason.CloseCodes.VIOLATED_POLICY, "missing required parameter session-id"));
        connectAndExpectClose(
                URI.create("ws://localhost:%d/v1/%s/tenant1/application1/gw?param:session-id=".formatted(port, type)),
                new CloseReason(CloseReason.CloseCodes.VIOLATED_POLICY, "missing required parameter session-id"));

        connectAndExpectClose(
                URI.create("ws://localhost:%d/v1/%s/tenant1/application1/gw?param:session-id=ok&param:another-non-declared=y".formatted(port, type)),
                new CloseReason(CloseReason.CloseCodes.VIOLATED_POLICY, "unknown parameters: [another-non-declared]"));

        connectAndExpectRunning(
                URI.create("ws://localhost:%d/v1/%s/tenant1/application1/gw?param:session-id=1".formatted(port, type)));
        connectAndExpectRunning(URI.create(
                "ws://localhost:%d/v1/%s/tenant1/application1/gw?param:session-id=string-value".formatted(port, type)));

    }


    @Test
    void testFilterOutMessagesByFixedValue() throws Exception {
        final String topic = genTopic();
        testGateways = new Gateways(List.of(
                new Gateway("produce", Gateway.GatewayType.produce, topic, List.of("session-id"), new Gateway.ProduceOptions(
                        List.of(new Gateway.KeyValueComparison("header1", "sga", null))
                ), null),
                new Gateway("produce-non-sga", Gateway.GatewayType.produce, topic, List.of("session-id"), null, null),
                new Gateway("consume", Gateway.GatewayType.consume, topic, List.of("session-id"), null, new Gateway.ConsumeOptions(
                        new Gateway.ConsumeOptionsFilters(
                                List.of(new Gateway.KeyValueComparison("header1", "sga", null))
                        )
                ))
        ));


        List<String> user1Messages = new ArrayList<>();
        List<String> user2Messages = new ArrayList<>();

        final ClientSession client1 = connectAndCollectMessages(URI.create(
                        "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:session-id=user1".formatted(port)),
                user1Messages);
        final ClientSession client2 = connectAndCollectMessages(URI.create(
                        "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:session-id=user2".formatted(port)),
                user2Messages);

        try (final TestWebSocketClient producer = new TestWebSocketClient(TestWebSocketClient.NOOP)
                .connect(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/produce-non-sga?param:session-id=user1".formatted(
                                        port)));) {
            final ProduceRequest produceRequest = new ProduceRequest(null, "this is a message non from sga", null);
            produce(produceRequest, producer);
        }

        try (final TestWebSocketClient producer = new TestWebSocketClient(TestWebSocketClient.NOOP)
                .connect(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:session-id=user1".formatted(
                                        port)));) {
            final ProduceRequest produceRequest = new ProduceRequest(null, "this is a message for everyone", null);
            produce(produceRequest, producer);
        }

        Awaitility.await()
                .untilAsserted(() -> assertEquals(List.of("{\"record\":{\"key\":null,\"value\":\"this is a message for everyone\",\"headers\":{\"header1\":\"sga\"}}}"), user1Messages));
        Awaitility.await()
                .untilAsserted(() -> assertEquals(List.of("{\"record\":{\"key\":null,\"value\":\"this is a message for everyone\",\"headers\":{\"header1\":\"sga\"}}}"), user2Messages));
    }

    private void produce(ProduceRequest produceRequest, TestWebSocketClient producer) throws JsonProcessingException {
        final String json = MAPPER.writeValueAsString(produceRequest);
        producer.send(json);
    }

    @Test
    void testFilterOutMessagesByParamValue() throws Exception {
        final String topic = genTopic();
        testGateways = new Gateways(List.of(
                new Gateway("produce", Gateway.GatewayType.produce, topic, List.of("session-id"), new Gateway.ProduceOptions(
                        List.of(new Gateway.KeyValueComparison("header1", null, "session-id"))
                ), null),
                new Gateway("consume", Gateway.GatewayType.consume, topic, List.of("session-id"), null, new Gateway.ConsumeOptions(
                        new Gateway.ConsumeOptionsFilters(
                                List.of(new Gateway.KeyValueComparison("header1", null, "session-id"))
                        )
                ))
        ));


        List<String> user1Messages = new ArrayList<>();
        List<String> user2Messages = new ArrayList<>();

        final ClientSession client1 = connectAndCollectMessages(URI.create(
                        "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:session-id=user1".formatted(port)),
                user1Messages);
        final ClientSession client2 = connectAndCollectMessages(URI.create(
                        "ws://localhost:%d/v1/consume/tenant1/application1/consume?param:session-id=user2".formatted(port)),
                user2Messages);

        try (final TestWebSocketClient producer = new TestWebSocketClient(TestWebSocketClient.NOOP)
                .connect(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:session-id=user1".formatted(
                                        port)));) {
            final ProduceRequest produceRequest = new ProduceRequest(null, "this is a message for user1", null);
            produce(produceRequest, producer);
        }
        Awaitility.await()
                .untilAsserted(() -> assertEquals(List.of("{\"record\":{\"key\":null,\"value\":\"this is a message "
                        + "for user1\",\"headers\":{\"header1\":\"user1\"}}}"), user1Messages));
        assertEquals(List.of(), user2Messages);

        try (final TestWebSocketClient producer = new TestWebSocketClient(TestWebSocketClient.NOOP)
                .connect(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:session-id=user1".formatted(
                                        port)));) {
            final ProduceRequest produceRequest = new ProduceRequest(null, "this is a message for user1, again", null);
            produce(produceRequest, producer);
        }
        Awaitility.await()
                .untilAsserted(() -> assertEquals(List.of("{\"record\":{\"key\":null,\"value\":\"this is a message "
                        + "for user1\",\"headers\":{\"header1\":\"user1\"}}}", "{\"record\":{\"key\":null,"
                        + "\"value\":\"this is a message for user1, again\",\"headers\":{\"header1\":\"user1\"}}}"), user1Messages));
        assertEquals(List.of(), user2Messages);

        try (final TestWebSocketClient producer = new TestWebSocketClient(TestWebSocketClient.NOOP)
                .connect(
                        URI.create(
                                "ws://localhost:%d/v1/produce/tenant1/application1/produce?param:session-id=user2".formatted(
                                        port)));) {
            final ProduceRequest produceRequest = new ProduceRequest(null, "this is a message for user2", null);
            produce(produceRequest, producer);
        }

        Awaitility.await()
                .untilAsserted(() -> assertEquals(List.of("{\"record\":{\"key\":null,\"value\":\"this is a message "
                                + "for user2\",\"headers\":{\"header1\":\"user2\"}}}"), user2Messages));
        Awaitility.await()
                .untilAsserted(() -> assertEquals(List.of("{\"record\":{\"key\":null,\"value\":\"this is a message "
                        + "for user1\",\"headers\":{\"header1\":\"user1\"}}}", "{\"record\":{\"key\":null,"
                        + "\"value\":\"this is a message for user1, again\",\"headers\":{\"header1\":\"user1\"}}}"), user1Messages));


    }

    @Test
    void testProduce() throws Exception {
        testGateways = new Gateways(List.of(
                new Gateway("gw", Gateway.GatewayType.produce, genTopic(), List.of("session-id"), new Gateway.ProduceOptions(
                        List.of(new Gateway.KeyValueComparison("header1", null, "session-id"))
                ), null)
        ));
        ProduceResponse response = connectAndProduce(URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/gw?param:session-id=s".formatted(port)),
                new ProduceRequest(null, "hello", Map.of("header0", "value0", "header2", "value2")));

        assertEquals(ProduceResponse.Status.OK, response.status());


        response = connectAndProduce(URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/gw?param:session-id=s".formatted(port)),
                new ProduceRequest(null, "hello", Map.of("header1", "value1")));

        assertEquals(ProduceResponse.Status.BAD_REQUEST, response.status());
        assertEquals("Header header1 is configured as parameter-level header.", response.reason());

        response = connectAndProduce(URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/gw?param:session-id=s".formatted(port)),
                "{}");

        assertEquals(ProduceResponse.Status.BAD_REQUEST, response.status());
        assertEquals("Either key or value must be set.", response.reason());

        response = connectAndProduce(URI.create(
                        "ws://localhost:%d/v1/produce/tenant1/application1/gw?param:session-id=s".formatted(port)),
                "invalid-json");

        assertEquals(ProduceResponse.Status.BAD_REQUEST, response.status());
        assertEquals("Unrecognized token 'invalid': was expecting (JSON String, Number, Array, Object or token "
                + "'null', 'true' or 'false')\n"
                + " at [Source: (String)\"invalid-json\"; line: 1, column: 8]", response.reason());

    }

    static AtomicInteger topicCounter = new AtomicInteger();
    private static String genTopic() {
        return "topic" + topicCounter.incrementAndGet();
    }

    @SneakyThrows
    private void connectAndExpectClose(URI connectTo, CloseReason expectedCloseReason) {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        AtomicReference<CloseReason> closeReason = new AtomicReference<>();
        try (final TestWebSocketClient client = new TestWebSocketClient(new TestWebSocketClient.Handler() {
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
        }).connect(connectTo);) {
            Thread.sleep(5000);
            countDownLatch.await();
            assertEquals(expectedCloseReason.getReasonPhrase(), closeReason.get().getReasonPhrase());
            assertEquals(expectedCloseReason.getCloseCode(), closeReason.get().getCloseCode());
        }
    }

    @SneakyThrows
    private void connectAndExpectRunning(URI connectTo) {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        AtomicReference<CloseReason> closeReason = new AtomicReference<>();
        try (final TestWebSocketClient client = new TestWebSocketClient(new TestWebSocketClient.Handler() {
            @Override
            public void onOpen(Session session) {
                TestWebSocketClient.Handler.super.onOpen(session);
                countDownLatch.countDown();
            }

            @Override
            public void onMessage(String msg) {
            }

            @Override
            public void onClose(CloseReason cr) {
                closeReason.set(cr);
                countDownLatch.countDown();
            }

        }).connect(connectTo);) {
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
        try (final TestWebSocketClient client = new TestWebSocketClient(new TestWebSocketClient.Handler() {
            @Override
            public void onOpen(Session session) {
                TestWebSocketClient.Handler.super.onOpen(session);
            }

            @Override
            @SneakyThrows
            public void onMessage(String msg) {
                response.set(new ObjectMapper().readValue(msg, ProduceResponse.class));
                countDownLatch.countDown();
            }

            @Override
            public void onClose(CloseReason cr) {
                closeReason.set(cr);
                countDownLatch.countDown();
            }

        }).connect(connectTo);) {
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
        final TestWebSocketClient client = new TestWebSocketClient(new TestWebSocketClient.Handler() {

            @Override
            public void onMessage(String msg) {
                collect.add(msg);
            }

            @Override
            public void onClose(CloseReason cr) {
                closeReason.set(cr);
            }

        }).connect(connectTo);
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
                client.close();
            }
        };
    }


}