package com.datastax.oss.sga.kafka.extensions;

import java.util.Map;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class KafkaContainerExtension implements BeforeAllCallback, AfterAllCallback {
    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (kafkaContainer != null) {
            kafkaContainer.close();
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"))
                .withLogConsumer(new Consumer<OutputFrame>() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        log.info("kafka> {}", outputFrame.getUtf8String().trim());
                    }
                });
        // start Pulsar and wait for it to be ready to accept requests
        kafkaContainer.start();
        admin =
                AdminClient.create(Map.of("bootstrap.servers", getBootstrapServers()));

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
