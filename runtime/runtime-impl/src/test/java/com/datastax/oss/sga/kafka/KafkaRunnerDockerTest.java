package com.datastax.oss.sga.kafka;

import com.datastax.oss.sga.common.AbstractApplicationRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class KafkaRunnerDockerTest extends AbstractApplicationRunner {

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
                           - id: "step1"
                             type: "identity"
                             input: "input-topic"
                             output: "output-topic"    
                        """);

        try (AbstractApplicationRunner.ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {
            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains("input-topic"));

            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic","value", producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of("value"));

            }
        }
    }
}