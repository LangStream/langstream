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

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import java.util.Map;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

public class PulsarProduceConsumeHandlerTest extends ProduceConsumeHandlerTest {

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
