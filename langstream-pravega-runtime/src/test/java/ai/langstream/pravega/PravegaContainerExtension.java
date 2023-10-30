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
package ai.langstream.pravega;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class PravegaContainerExtension implements BeforeAllCallback, AfterAllCallback {
    private PravegaContainer pravegaContainer;

    private Network network;

    private StreamManager admin;
    private EventStreamClientFactory client;

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (client != null) {
            client.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (pravegaContainer != null) {
            pravegaContainer.close();
        }
        if (network != null) {
            network.close();
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        network = Network.newNetwork();
        pravegaContainer =
                new PravegaContainer()
                        .withNetwork(network)
                        .withLogConsumer(
                                outputFrame ->
                                        log.info(
                                                "pravega> {}", outputFrame.getUtf8String().trim()));
        // start Pulsar and wait for it to be ready to accept requests
        pravegaContainer.start();
        /*
        admin =
                PulsarAdmin.builder()
                        .serviceHttpUrl("http://localhost:" + pravegaContainer.getMappedPort(8080))
                        .build();

        client = PulsarClient.builder().serviceUrl(pravegaContainer.getPulsarBrokerUrl()).build();

        try {
            admin.namespaces().createNamespace("public/default");
        } catch (PulsarAdminException.ConflictException exists) {
            // ignore
        } */
    }

    public String getControllerUri() {
        return pravegaContainer.getControllerUri();
    }

    public PravegaContainer getPravegaContainer() {
        return pravegaContainer;
    }

    public static class PravegaContainer extends GenericContainer<PravegaContainer> {

        private static final DockerImageName DEFAULT_IMAGE_NAME =
                DockerImageName.parse("pravega/pravega");
        private static final String DEFAULT_TAG = "0.12.0";
        private static final int CONTROLLER_PORT = 9090;
        private static final int SEGMENT_STORE_PORT = 12345;

        public PravegaContainer() {
            this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
        }

        public PravegaContainer(final DockerImageName dockerImageName) {
            super(dockerImageName);

            dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
            addFixedExposedPort(CONTROLLER_PORT, CONTROLLER_PORT);
            addFixedExposedPort(SEGMENT_STORE_PORT, SEGMENT_STORE_PORT);
            withStartupTimeout(Duration.ofSeconds(90));
            withEnv("HOST_IP", getHost());
            withCommand("standalone");
            waitingFor(Wait.forLogMessage(".* Pravega Sandbox is running locally now.*", 1));
        }

        public String getControllerUri() {
            return String.format("tcp://%s:%d", getHost(), CONTROLLER_PORT);
        }
    }
}
