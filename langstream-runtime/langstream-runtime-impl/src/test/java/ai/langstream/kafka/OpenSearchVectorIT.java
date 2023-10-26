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

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.opensearch.testcontainers.OpensearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class OpenSearchVectorIT extends AbstractKafkaApplicationRunner {
    @Container
    static OpensearchContainer OPENSEARCH =
            new OpensearchContainer(DockerImageName.parse("opensearchproject/opensearch:2"))
                    .withEnv("discovery.type", "single-node");

    @Test
    public void test() throws Exception {
        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                        configuration:
                          resources:
                            - type: "vector-database"
                              name: "OSDatasource"
                              configuration:
                                service: "opensearch"
                                https: false
                                port: %d
                                host: "localhost"
                                username: "admin"
                                password: "admin"
                                index-name: "my-index-1"
                        """
                                .formatted(OPENSEARCH.getMappedPort(9200)),
                        "pipeline-write.yaml",
                        """
                                topics:
                                  - name: "insert-topic"
                                    creation-mode: create-if-not-exists
                                assets:
                                  - name: "os-index"
                                    asset-type: "opensearch-index"
                                    creation-mode: create-if-not-exists
                                    config:
                                       datasource: "OSDatasource"
                                       settings: |
                                           {
                                                "index": {
                                                      "knn": true,
                                                      "knn.algo_param.ef_search": 100
                                                }
                                            }
                                       mappings: |
                                           {
                                                "properties": {
                                                      "content": {
                                                            "type": "text"
                                                      },
                                                      "embeddings": {
                                                            "type": "knn_vector",
                                                            "dimension": 3
                                                      }
                                                }
                                            }
                                pipeline:
                                  - id: write
                                    name: "Write"
                                    type: "vector-db-sink"
                                    input: "insert-topic"
                                    configuration:
                                      datasource: "OSDatasource"
                                      bulk-parameters:
                                        refresh: "true"
                                      id: "key"
                                      fields:
                                        - name: "content"
                                          expression: "value.content"
                                        - name: "embeddings"
                                          expression: "value.embeddings"
                                """,
                        "pipeline-read.yaml",
                        """
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "result-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - id: read
                                    name: "read"
                                    type: "query-vector-db"
                                    input: "input-topic"
                                    output: "result-topic"
                                    configuration:
                                      datasource: "OSDatasource"
                                      query: |
                                        {
                                          "query": {
                                            "knn": {
                                              "embeddings": {
                                                "vector": ?,
                                                "k": 1
                                              }
                                            }
                                          }
                                        }
                                      fields:
                                        - "value.embeddings"
                                      output-field: "value.query-result"
                                """);

        String[] expectedAgents = new String[] {"app-write", "app-read"};
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        "tenant", "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("result-topic")) {

                for (int i = 0; i < 10; i++) {
                    sendMessage(
                            "insert-topic",
                            "key" + i,
                            "{\"content\": \"hello" + i + "\", \"embeddings\":[999,999," + i + "]}",
                            List.of(),
                            producer);
                }
                sendMessage("input-topic", "{\"embeddings\":[999,999,5]}", producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of(
                                "{\"embeddings\":[999,999,5],\"query-result\":[{\"score\":1.0,"
                                        + "\"document\":{\"embeddings\":[999,999,5],\"content\":\"hello5\"},"
                                        + "\"index\":\"my-index-1\",\"id\":\"key5\"}]}"));
            }
        }
    }
}
