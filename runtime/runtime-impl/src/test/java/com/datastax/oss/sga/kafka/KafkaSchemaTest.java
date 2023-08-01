package com.datastax.oss.sga.kafka;

import com.datastax.oss.sga.common.AbstractApplicationRunner;
import com.datastax.oss.sga.kafka.extensions.KafkaRegistryContainerExtension;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
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
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Map;

@Slf4j
class KafkaSchemaTest extends AbstractApplicationRunner {


    @RegisterExtension
    static KafkaRegistryContainerExtension registry = new KafkaRegistryContainerExtension(kafkaContainer);

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

    @Override
    protected String buildInstanceYaml() {
        return """
                instance:
                  streamingCluster:
                    type: "kafka"
                    configuration:
                      admin:
                        bootstrap.servers: "%s"
                        schema.registry.url: "%s"
                  computeCluster:
                     type: "kubernetes"
                """.formatted(kafkaContainer.getBootstrapServers(),
                registry.getSchemaRegistryUrl());
    }


    protected KafkaProducer createAvroProducer() {
        return new KafkaProducer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", KafkaAvroSerializer.class.getName(),
                        "schema.registry.url", registry.getSchemaRegistryUrl())
        );
    }


    protected KafkaConsumer createAvroConsumer(String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "value.deserializer", KafkaAvroDeserializer.class.getName(),
                        "group.id", "testgroup",
                        "auto.offset.reset", "earliest",
                        "schema.registry.url", registry.getSchemaRegistryUrl())
        );
        consumer.subscribe(List.of(topic));
        return consumer;
    }

}