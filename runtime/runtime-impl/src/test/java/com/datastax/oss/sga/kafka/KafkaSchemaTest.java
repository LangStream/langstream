package com.datastax.oss.sga.kafka;

import com.datastax.oss.sga.common.AbstractApplicationRunner;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

@Slf4j
class KafkaSchemaTest extends AbstractApplicationRunner {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static final class Pojo  {

        private String name;
    }

    @Test
    public void testUseSchemaWithKafka() throws Exception {
        String schemaDefinition = """
                        {
                          "type" : "record",
                          "name" : "Pojo",
                          "namespace" : "mynamespace",
                          "fields" : [ {
                            "name" : "name",
                            "type" : "string"
                          } ]
                        }
                """;
        Schema schema = new Schema.Parser().parse(schemaDefinition);
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "foo");
        log.info("Schema: {}", schemaDefinition);
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
                            schema:
                              type: avro
                              schema: '%s'             
                          - name: "output-topic"
                            creation-mode: create-if-not-exists
                            schema:
                              type: avro
                        pipeline:
                          - name: "identity"
                            id: "step1"
                            type: "identity"
                            input: "input-topic"
                            output: "output-topic"
                        """.formatted(schemaDefinition));
        try (ApplicationRuntime applicationRuntime = deployApplication(tenant, "app", application, expectedAgents)) {
            try (KafkaProducer<String, String> producer = createAvroProducer();
                 KafkaConsumer<String, String> consumer = createAvroConsumer("output-topic")) {

                sendMessage("input-topic", record, producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of(record));
            }
        }
    }

}