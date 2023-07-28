package com.datastax.oss.sga.kafka;

import com.datastax.oss.sga.common.AbstractApplicationRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import java.util.List;
import java.util.Map;

@Slf4j
class KafkaRunnerDockerTest extends AbstractApplicationRunner {

    private static KafkaContainer kafkaContainer;
    private static AdminClient admin;

    @Test
    public void testConnectToTopics() throws Exception {

        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        Map<String, String> application = Map.of("instance.yaml",
                        buildInstanceYaml(),
                        "module.yaml", """
                                module: "module-1"
                                id: "pipeline-1"                                
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                   - type: identity
                                     id: "step1"
                                     input: "input-topic"
                                     output: "output-topic"
                                """);

        try (ApplicationRuntime runtime = deployApplication(tenant, "app", application, expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "value", producer);
                executeAgentRunners(runtime);
                waitForMessages(consumer, List.of("value"));
            }
        }
    }

}