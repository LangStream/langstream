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

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.ai.agents.commons.jstl.JstlFunctions;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
class SolrAssetQueryWriteIT extends AbstractKafkaApplicationRunner {

    @Container
    private GenericContainer solrCloudContainer =
            new GenericContainer("solr:9.3.0")
                    .withExposedPorts(8983)
                    .withCommand("-c"); // enable SolarCloud mode

    @Test
    public void testSolr() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        String collectionName = "documents";

        String configuration =
                """
                        configuration:
                          resources:
                            - type: "vector-database"
                              name: "SolrDatasource"
                              configuration:
                                service: "solr"
                                host: "localhost"
                                port: %s
                                collection-name: %s
                        """
                        .formatted(solrCloudContainer.getMappedPort(8983), collectionName);

        Map<String, String> applicationWriter =
                Map.of(
                        "configuration.yaml",
                        configuration,
                        "pipeline.yaml",
                        """
                                assets:
                                  - name: "documents-table"
                                    asset-type: "solr-collection"
                                    creation-mode: create-if-not-exists
                                    config:
                                       datasource: "SolrDatasource"
                                       create-statements:
                                          - api: "/api/collections"
                                            method: "POST"
                                            body: |
                                                       {
                                                         "name": "documents",
                                                         "numShards": 1,
                                                         "replicationFactor": 1
                                                        }
                                          - "api": "/schema"
                                            "body": |
                                                      {
                                                       "add-field-type" : {
                                                             "name": "knn_vector",
                                                             "class": "solr.DenseVectorField",
                                                             "vectorDimension": "5",
                                                             "similarityFunction": "cosine"
                                                        }
                                                       }

                                          - "api": "/schema"
                                            "body": |
                                                     {
                                                       "add-field":{
                                                         "name":"embeddings",
                                                         "type":"knn_vector",
                                                         "stored":true,
                                                         "indexed":true
                                                         }
                                                     }
                                          - "api": "/schema"
                                            "body": |
                                                      {
                                                         "add-field":{
                                                             "name":"name",
                                                             "type":"string",
                                                             "stored":true,
                                                             "indexed":false,
                                                             "multiValued": false
                                                         }
                                                      }
                                          - "api": "/schema"
                                            "body": |
                                                        {
                                                            "add-field":{
                                                                 "name":"description",
                                                                 "type":"string",
                                                                 "stored":true,
                                                                 "indexed":false,
                                                                 "multiValued": false
                                                             }
                                                        }

                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - id: step1
                                    name: "Write a record to Solr"
                                    type: "vector-db-sink"
                                    input: "input-topic"
                                    configuration:
                                      datasource: "SolrDatasource"
                                      fields:
                                        - name: id
                                          expression: "fn:toString(value.documentId)"
                                        - name: embeddings
                                          expression: "fn:toListOfFloat(value.embeddings)"
                                        - name: name
                                          expression: "value.name"
                                        - name: description
                                          expression: "value.description"
                                """
                                .formatted(collectionName));

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", applicationWriter, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer(); ) {

                sendMessage(
                        "input-topic",
                        "{\"documentId\":1, \"embeddings\":[0.1,0.2,0.3,0.4,0.5],\"name\":\"A name\",\"description\":\"A description\"}",
                        producer);

                executeAgentRunners(applicationRuntime);

                applicationDeployer.cleanup(tenant, applicationRuntime.implementation());
            }
        }

        Map<String, String> applicationReader =
                Map.of(
                        "configuration.yaml",
                        configuration,
                        "pipeline.yaml",
                        """
                            topics:
                              - name: "questions-topic"
                                creation-mode: create-if-not-exists
                              - name: "answers-topic"
                                creation-mode: create-if-not-exists
                            pipeline:
                              - id: step1
                                name: "Read a record from Solr"
                                type: "query-vector-db"
                                input: "questions-topic"
                                output: "answers-topic"
                                configuration:
                                  datasource: "SolrDatasource"
                                  query: |
                                        {
                                          "q": "{!knn f=embeddings topK=10}?"
                                        }
                                  only-first: true
                                  output-field: "value.queryresult"
                                  fields:
                                     - "value.embeddings"

                            """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", applicationReader, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("answers-topic")) {

                sendMessage("questions-topic", "{\"embeddings\":[0.1,0.2,0.3,0.4,0.5]}", producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                new Consumer<String>() {
                                    @Override
                                    @SneakyThrows
                                    public void accept(String m) {
                                        Map<String, Object> map =
                                                new ObjectMapper().readValue(m, Map.class);
                                        Map<String, Object> queryresult =
                                                (Map<String, Object>) map.get("queryresult");
                                        assertEquals("1", queryresult.get("id"));
                                        assertEquals(
                                                List.of(0.1f, 0.2f, 0.3f, 0.4f, 0.5f),
                                                JstlFunctions.toListOfFloat(
                                                        queryresult.get("embeddings")));
                                        assertEquals("A name", queryresult.get("name"));
                                    }
                                }));

                applicationDeployer.cleanup(tenant, applicationRuntime.implementation());
            }
        }
    }
}
