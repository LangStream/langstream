package com.datastax.oss.sga.apigateway.websocket.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.datastax.oss.sga.kafka.extensions.KafkaContainerExtension;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.main.allow-bean-definition-overriding=true"})
class ProduceConsumeHandlerTest {


    @RegisterExtension
    static KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    @TestConfiguration
    public static class WebSocketTestConfig {

        @Bean
        @Primary
        public ApplicationStore applicationStoreFactory() {
            final ApplicationStore mock = Mockito.mock(ApplicationStore.class);
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                    final StoredApplication storedApplication = new StoredApplication();
                    final Application application = new Application();
                    application.setGateways(new Gateways(List.of(
                            new Gateway("produce", "produce", "my-topic", List.of(), null, null),
                            new Gateway("consume", "consume", "my-topic", List.of(), null, null)
                    )));
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
    ApplicationStore applicationStoreFactory;

    @Test
    void testSimpleProduceConsume() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(1);
        try (final WebSocketClient consumer = new WebSocketClient(message -> {
            assertEquals("this is a message", message);
            countDownLatch.countDown();
        }).connect(URI.create("ws://localhost:%d/v1/consume/tenant1/application1/consume".formatted(port)));) {
            try (final WebSocketClient producer = new WebSocketClient(message -> {
            }).connect(URI.create("ws://localhost:%d/v1/produce/tenant1/application1/produce".formatted(port)));) {
                final ProduceRequest produceRequest = new ProduceRequest(null, "this is a message", null);
                final String json = new ObjectMapper().writeValueAsString(produceRequest);
                producer.send(json);
            }
            countDownLatch.await();
        }
    }
}