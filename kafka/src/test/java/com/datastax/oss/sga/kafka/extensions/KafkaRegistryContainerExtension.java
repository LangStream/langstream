package com.datastax.oss.sga.kafka.extensions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.function.Consumer;

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
                        .withLogConsumer(new Consumer<OutputFrame>() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        log.info("schemaregistry> {}", outputFrame.getUtf8String().trim());
                    }
                })
                .withNetwork(kafka.getNetwork())
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + kafka.getKafkaContainer().getNetworkAliases().get(0) + ":9092")
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                .withExposedPorts(8081);
        registryContainer.start();

    }

    public String getSchemaRegistryUrl() {
        return "http://" + registryContainer.getHost() + ":" + registryContainer.getMappedPort(8081);
    }
}