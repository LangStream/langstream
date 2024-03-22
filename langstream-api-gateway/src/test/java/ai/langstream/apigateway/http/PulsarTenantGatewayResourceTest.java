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
import ai.langstream.apigateway.websocket.handlers.PulsarContainerExtension;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

class PulsarTenantGatewayResourceTest extends GatewayResourceTest {

    @RegisterExtension
    static PulsarContainerExtension pulsarContainer =
            new PulsarContainerExtension()
                    .withEnv(
                            Map.of(
                                    "PULSAR_PREFIX_forceDeleteTenantAllowed",
                                    "true",
                                    "PULSAR_PREFIX_forceDeleteNamespaceAllowed",
                                    "true"))
                    .withOnContainerReady(
                            new Consumer<PulsarContainerExtension>() {
                                @Override
                                @SneakyThrows
                                public void accept(
                                        PulsarContainerExtension pulsarContainerExtension) {
                                    try (PulsarAdmin admin =
                                            PulsarAdmin.builder()
                                                    .serviceHttpUrl(
                                                            pulsarContainerExtension
                                                                    .getHttpServiceUrl())
                                                    .build(); ) {

                                        TenantInfo info =
                                                TenantInfo.builder()
                                                        .allowedClusters(
                                                                new HashSet<>(
                                                                        admin.clusters()
                                                                                .getClusters()))
                                                        .build();
                                        admin.tenants().createTenant("mytenant", info);
                                        admin.namespaces().createNamespace("mytenant/mynamespace");
                                        admin.tenants().deleteTenant("public", true);
                                    }
                                }
                            });

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
                        "mytenant",
                        "default-namespace",
                        "mynamespace"));
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
                                  default-tenant: "mytenant"
                                  default-namespace: "mynamespace"
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

    @Override
    protected String resolveTopicName(String topic) {
        return "mytenant/mynamespace/" + topic;
    }
}
