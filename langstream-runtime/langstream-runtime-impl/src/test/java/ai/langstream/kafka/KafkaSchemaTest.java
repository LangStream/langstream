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
package ai.langstream.kafka;

import ai.langstream.kafka.extensions.KafkaRegistryContainerExtension;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.List;
import java.util.Map;
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

@Slf4j
class KafkaSchemaTest extends AbstractKafkaApplicationRunner {

    @RegisterExtension
    static KafkaRegistryContainerExtension registry =
            new KafkaRegistryContainerExtension(kafkaContainer);

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static final class Pojo {

        private String name;
    }

    @Test
    public void testUseSchemaWithKafka() throws Exception {
        String schemaDefinition =
                """
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

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
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
                                      schema: |
                                           {
                                              "type" : "record",
                                              "name" : "Pojo",
                                              "namespace" : "mynamespace",
                                              "fields" : [ {
                                                "name" : "name",
                                                "type" : "string"
                                              } ]
                                            }
                                pipeline:
                                  - name: "identity"
                                    id: "step1"
                                    type: "identity"
                                    input: "input-topic"
                                    output: "output-topic"
                                """
                                .formatted(schemaDefinition));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
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
                """
                .formatted(kafkaContainer.getBootstrapServers(), registry.getSchemaRegistryUrl());
    }

    protected KafkaProducer<String, String> createAvroProducer() {
        return new KafkaProducer<>(
                Map.of(
                        "bootstrap.servers",
                        kafkaContainer.getBootstrapServers(),
                        "key.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer",
                        KafkaAvroSerializer.class.getName(),
                        "schema.registry.url",
                        registry.getSchemaRegistryUrl()));
    }

    protected KafkaConsumer<String, String> createAvroConsumer(String topic) {
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(
                        Map.of(
                                "bootstrap.servers",
                                kafkaContainer.getBootstrapServers(),
                                "key.deserializer",
                                "org.apache.kafka.common.serialization.StringDeserializer",
                                "value.deserializer",
                                KafkaAvroDeserializer.class.getName(),
                                "group.id",
                                "testgroup",
                                "auto.offset.reset",
                                "earliest",
                                "schema.registry.url",
                                registry.getSchemaRegistryUrl()));
        consumer.subscribe(List.of(topic));
        return consumer;
    }
}
