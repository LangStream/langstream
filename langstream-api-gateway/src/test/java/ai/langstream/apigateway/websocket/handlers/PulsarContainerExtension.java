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

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class PulsarContainerExtension implements BeforeAllCallback, AfterAllCallback {
    private PulsarContainer pulsarContainer;

    private Network network;

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        if (pulsarContainer != null) {
            pulsarContainer.close();
        }
        if (network != null) {
            network.close();
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        network = Network.newNetwork();
        pulsarContainer =
                new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.1.0"))
                        .withNetwork(network)
                        .withLogConsumer(
                                outputFrame ->
                                        log.debug(
                                                "pulsar> {}", outputFrame.getUtf8String().trim()));
        // start Pulsar and wait for it to be ready to accept requests
        pulsarContainer.start();
    }

    public String getBrokerUrl() {
        return pulsarContainer.getPulsarBrokerUrl();
    }

    public String getHttpServiceUrl() {
        return pulsarContainer.getHttpServiceUrl();
    }

    public PulsarContainer getPulsarContainer() {
        return pulsarContainer;
    }
}
