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

import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class KafkaRegistryContainerExtension implements BeforeAllCallback, AfterAllCallback {
    private KafkaContainerExtension kafka;

    private GenericContainer registryContainer;

    public KafkaRegistryContainerExtension(KafkaContainerExtension kafka) {
        this.kafka = kafka;
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (registryContainer != null) {
            registryContainer.close();
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        registryContainer =
                new GenericContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.4.0"))
                        .withLogConsumer(
                                new Consumer<OutputFrame>() {
                                    @Override
                                    public void accept(OutputFrame outputFrame) {
                                        log.debug(
                                                "schemaregistry> {}",
                                                outputFrame.getUtf8String().trim());
                                    }
                                })
                        .withNetwork(kafka.getNetwork())
                        .withEnv(
                                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                                "PLAINTEXT://"
                                        + kafka.getKafkaContainer().getNetworkAliases().get(0)
                                        + ":9092")
                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                        .withExposedPorts(8081);
        registryContainer.start();
    }

    public String getSchemaRegistryUrl() {
        return "http://"
                + registryContainer.getHost()
                + ":"
                + registryContainer.getMappedPort(8081);
    }
}
