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
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class ElasticSearchVectorIT extends AbstractKafkaApplicationRunner {
    @Container
    static ElasticsearchContainer ELASTICSEARCH =
            new ElasticsearchContainer(
                            DockerImageName.parse(
                                    "docker.elastic.co/elasticsearch/elasticsearch:8.14.0"))
                    .withEnv("discovery.type", "single-node")
                    .withEnv("xpack.security.enabled", "false")
                    .withEnv("xpack.security.http.ssl.enabled", "false")
                    .withLogConsumer(
                            frame -> log.info("elasticsearch > {}", frame.getUtf8String()));

    @Test
    @Disabled
    public void testElasticCloud() throws Exception {
        test(true, "xx.es.us-east-1.aws.elastic.cloud", 443, "==");
    }

    @Test
    public void testElastic8() throws Exception {
        test(false, "localhost", ELASTICSEARCH.getMappedPort(9200), null);
    }

    public void test(boolean https, String host, int port, String apiKey) throws Exception {
        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                                configuration:
                                  resources:
                                    - type: "vector-database"
                                      name: "ESDatasource"
                                      configuration:
                                        service: "elasticsearch"
                                        https: %s
                                        host: "%s"
                                        port: "%d"
                                        api-key: %s
                                """
                                .formatted(https, host, port, apiKey),
                        "pipeline-write.yaml",
                        """
                                topics:
                                  - name: "insert-topic"
                                    creation-mode: create-if-not-exists
                                assets:
                                  - name: "es-index"
                                    asset-type: "elasticsearch-index"
                                    creation-mode: create-if-not-exists
                                    config:
                                       datasource: "ESDatasource"
                                       index: my-index-000
                                       settings: |
                                           {
                                                "index": {}
                                            }
                                       mappings: |
                                           {
                                                "properties": {
                                                      "content": {
                                                            "type": "text"
                                                      },
                                                      "embeddings": {
                                                            "type": "dense_vector",
                                                            "dims": 3,
                                                            "similarity": "cosine"
                                                      }
                                                }
                                            }
                                pipeline:
                                  - id: write
                                    name: "Write"
                                    type: "vector-db-sink"
                                    input: "insert-topic"
                                    configuration:
                                      datasource: "ESDatasource"
                                      index: "{{{ properties.index_name }}}"
                                      id: "key"
                                      bulk-parameters:
                                        refresh: "wait_for"
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
                                      datasource: "ESDatasource"
                                      query: |
                                        {
                                            "index": [?],
                                            "knn": {
                                              "field": "embeddings",
                                              "query_vector": ?,
                                              "k": 1,
                                              "num_candidates": 10
                                            }
                                        }
                                      fields:
                                        - "properties.index_name"
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
                            List.of(new RecordHeader("index_name", "my-index-000".getBytes())),
                            producer);
                }
                executeAgentRunners(applicationRuntime);
                sendMessage(
                        "input-topic",
                        "{\"embeddings\":[999,999,5]}",
                        List.of(new RecordHeader("index_name", "my-index-000".getBytes())),
                        producer);
                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                "{\"embeddings\":[999,999,5],\"query-result\":[{\"score\":0.9999989,"
                                        + "\"document\":{\"embeddings\":[999,999,2],\"content\":\"hello2\"},"
                                        + "\"index\":\"my-index-000\",\"id\":\"key2\"}]}"));
            }
        }
    }
}
