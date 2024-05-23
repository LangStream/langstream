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
package ai.langstream.pulsar;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
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

    private PulsarAdmin admin;
    private PulsarClient client;

    @Override
    public void afterAll(ExtensionContext extensionContext) throws PulsarClientException {
        if (client != null) {
            client.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (pulsarContainer != null) {
            pulsarContainer.close();
        }
        if (network != null) {
            network.close();
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext)
            throws PulsarClientException, PulsarAdminException {
        network = Network.newNetwork();
        pulsarContainer =
                new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.2.1"))
                        .withNetwork(network)
                        .withLogConsumer(
                                outputFrame ->
                                        log.debug(
                                                "pulsar> {}", outputFrame.getUtf8String().trim()));
        // start Pulsar and wait for it to be ready to accept requests
        pulsarContainer.start();

        admin =
                PulsarAdmin.builder()
                        .serviceHttpUrl("http://localhost:" + pulsarContainer.getMappedPort(8080))
                        .build();

        client = PulsarClient.builder().serviceUrl(pulsarContainer.getPulsarBrokerUrl()).build();

        try {
            admin.namespaces().createNamespace("public/default");
        } catch (PulsarAdminException.ConflictException exists) {
            // ignore
        }
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

    public PulsarAdmin getAdmin() {
        return admin;
    }

    public PulsarClient getClient() {
        return client;
    }
}
