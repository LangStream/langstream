package com.datastax.oss.sga.kafka;


import com.datastax.oss.sga.common.AbstractApplicationRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

@Slf4j
class ErrorHandlingTest extends AbstractApplicationRunner {


    @Test
    public void testDiscardErrors() throws Exception {
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
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 10
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                errors:
                                    on-failure: fail
                                    retries: 5
                                pipeline:
                                  - name: "some agent"
                                    id: "step1"
                                    type: "mock-failing-processor"
                                    input: "input-topic"
                                    output: "output-topic"
                                    errors:
                                        on-failure: skip
                                        retries: 3                                          
                                    configuration:
                                      fail-on-content: "fail-me"
                                """);
        try (AbstractApplicationRunner.ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "fail-me", producer);
                sendMessage("input-topic", "keep-me", producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of("keep-me"));
            }
        }
    }


    @Test
    public void testDeadLetter() throws Exception {
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
                                errors:
                                    on-failure: fail
                                    retries: 5
                                pipeline:
                                  - name: "some agent"
                                    id: "step1"
                                    type: "mock-failing-processor"
                                    input: "input-topic"
                                    output: "output-topic"
                                    errors:
                                        on-failure: dead-letter
                                        retries: 3                                          
                                    configuration:
                                      fail-on-content: "fail-me"
                                """);
        try (AbstractApplicationRunner.ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                 KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "fail-me", producer);
                sendMessage("input-topic", "keep-me", producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of("keep-me"));
            }
        }
    }
}