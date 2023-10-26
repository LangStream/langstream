package ai.langstream.apigateway.http;

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.apigateway.websocket.handlers.PulsarContainerExtension;
import ai.langstream.kafka.extensions.KafkaContainerExtension;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;


class PulsarGatewayResourceTest extends GatewayResourceTest {

    @RegisterExtension
    static PulsarContainerExtension pulsarContainer = new PulsarContainerExtension();

    @Override
    protected StreamingCluster getStreamingCluster() {
        return new StreamingCluster(
                "pulsar",
                Map.of(
                        "admin",
                        Map.of("serviceUrl", pulsarContainer.getHttpServiceUrl()),
                        "service",
                        Map.of("serviceUrl", pulsarContainer.getBrokerUrl()),
                        "default-tenant",
                        "public",
                        "default-namespace",
                        "default"));
    }

    @TestConfiguration
    public static class WebSocketTestConfig {

        @Bean
        @Primary
        public ApplicationStore store() {
            String instanceYaml =
                    """
                     instance:
                       streamingCluster:
                         type: "pulsar"
                         configuration:
                           admin:
                             serviceUrl: "%s"
                           service:
                             serviceUrl: "%s"
                           default-tenant: "public"
                           default-namespace: "default"
                       computeCluster:
                         type: "none"
                     """
                            .formatted(
                                    pulsarContainer.getHttpServiceUrl(),
                                    pulsarContainer.getBrokerUrl());
            return getMockedStore(instanceYaml);
        }

        @Bean
        @Primary
        public GatewayTestAuthenticationProperties gatewayTestAuthenticationProperties() {
            return getGatewayTestAuthenticationProperties();
        }
    }
}