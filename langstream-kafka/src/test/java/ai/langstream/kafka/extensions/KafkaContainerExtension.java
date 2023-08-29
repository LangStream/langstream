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
package ai.langstream.kafka.extensions;

import java.util.Map;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class KafkaContainerExtension implements BeforeAllCallback, AfterAllCallback {
    private KafkaContainer kafkaContainer;

    private Network network;
    private AdminClient admin;

    public Network getNetwork() {
        return network;
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
        if (network != null) {
            network.close();
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        network = Network.newNetwork();
        kafkaContainer =
                new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                        .withNetwork(network)
                        .withLogConsumer(
                                new Consumer<OutputFrame>() {
                                    @Override
                                    public void accept(OutputFrame outputFrame) {
                                        log.debug("kafka> {}", outputFrame.getUtf8String().trim());
                                    }
                                });
        // start Pulsar and wait for it to be ready to accept requests
        kafkaContainer.start();
        admin = AdminClient.create(Map.of("bootstrap.servers", getBootstrapServers()));
    }

    public String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }

    public KafkaContainer getKafkaContainer() {
        return kafkaContainer;
    }

    public AdminClient getAdmin() {
        return admin;
    }
}
