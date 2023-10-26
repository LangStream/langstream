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

import ai.langstream.api.model.StreamingCluster;
import ai.langstream.api.storage.ApplicationStore;
import ai.langstream.apigateway.config.GatewayTestAuthenticationProperties;
import ai.langstream.kafka.extensions.KafkaContainerExtension;
import java.util.Map;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

class KafkaGatewayResourceTest extends GatewayResourceTest {

    @RegisterExtension
    static KafkaContainerExtension kafkaContainer = new KafkaContainerExtension();

    @Override
    protected StreamingCluster getStreamingCluster() {
        return new StreamingCluster(
                "kafka",
                Map.of(
                        "admin",
                        Map.of(
                                "bootstrap.servers",
                                kafkaContainer.getBootstrapServers(),
                                "default.api.timeout.ms",
                                5000)));
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
                        type: "kafka"
                        configuration:
                          admin:
                            bootstrap.servers: "%s"
                      computeCluster:
                         type: "none"
                    """
                            .formatted(kafkaContainer.getBootstrapServers());
            return getMockedStore(instanceYaml);
        }

        @Bean
        @Primary
        public GatewayTestAuthenticationProperties gatewayTestAuthenticationProperties() {
            return getGatewayTestAuthenticationProperties();
        }
    }
}
