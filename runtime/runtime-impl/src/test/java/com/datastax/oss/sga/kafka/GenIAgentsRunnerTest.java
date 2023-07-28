package com.datastax.oss.sga.kafka;
import com.datastax.oss.sga.common.AbstractApplicationRunner;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class GenIAgentsRunnerTest extends AbstractApplicationRunner  {



    @Test
    public void testRunAITools() throws Exception {
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
                                  - name: "drop-description"
                                    id: "step1"
                                    type: "drop-fields"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      fields:
                                        - "description"
                                """);

        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {


        try (KafkaProducer<String, String> producer = createProducer();
                     KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {


            sendMessage("input-topic","{\"name\": \"some name\", \"description\": \"some description\"}",
                    List.of(new RecordHeader("header-key", "header-value".getBytes(StandardCharsets.UTF_8))),
                    producer);

            executeAgentRunners(applicationRuntime);

            List<ConsumerRecord> records = waitForMessages(consumer, List.of("{\"name\":\"some name\"}"));

            ConsumerRecord<String, String> record = records.get(0);
            assertEquals("{\"name\":\"some name\"}", record.value());
            assertEquals("header-value", new String(record.headers().lastHeader("header-key").value(), StandardCharsets.UTF_8));
        }
        }

    }

}