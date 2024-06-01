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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Slf4j
@Testcontainers
@Disabled
class CouchbaseAssetQueryWriteIT extends AbstractKafkaApplicationRunner {

    @Container
    private GenericContainer couchbaseContainer =
            new GenericContainer("couchbase:latest")
                    .withExposedPorts(8091)
                    .withEnv("COUCHBASE_SERVER_MEMORY_QUOTA", "300")
                    .withEnv("COUCHBASE_CLUSTER_RAMSIZE", "300")
                    .withEnv("COUCHBASE_CLUSTER_INDEX_RAMSIZE", "300")
                    .withEnv("COUCHBASE_CLUSTER_EVENTING_RAMSIZE", "300")
                    .withEnv("COUCHBASE_CLUSTER_FTS_RAMSIZE", "300")
                    .withEnv("COUCHBASE_CLUSTER_USERNAME", "Administrator")
                    .withEnv("COUCHBASE_CLUSTER_PASSWORD", "password")
                    .withEnv("COUCHBASE_BUCKET", "default")
                    .withEnv("COUCHBASE_BUCKET_TYPE", "couchbase")
                    .withEnv("COUCHBASE_BUCKET_RAMSIZE", "100")
                    .withEnv("COUCHBASE_RBAC_USERNAME", "${COUCHBASE_CLUSTER_USERNAME}")
                    .withEnv("COUCHBASE_RBAC_PASSWORD", "${COUCHBASE_CLUSTER_PASSWORD}")
                    .withEnv("COUCHBASE_RBAC_NAME", "Administrator")
                    .withEnv("COUCHBASE_RBAC_ROLES", "admin");

    @Test
    public void testCouchbase() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        String bucketName = "vectorize";

        String configuration =
                """
                        configuration:
                          resources:
                            - type: "vector-database"
                              name: "CouchbaseDatasource"
                              configuration:
                                service: "couchbase"
                                connection-string: "${secrets.couchbase.connection-string}"
                                bucket-name: "${secrets.couchbase.bucket-name}"
                                username: "${secrets.couchbase.username}"
                                password: "${secrets.couchbase.password}"

                          """
                        .formatted(couchbaseContainer.getMappedPort(8091), bucketName);

        Map<String, String> applicationWriter =
                Map.of(
                        "configuration.yaml",
                        configuration,
                        "pipeline.yaml",
                        """
                                assets:
                                  - name: "documents-table"
                                    asset-type: "couchbase-bucket"
                                    creation-mode: create-if-not-exists
                                    config:
                                       datasource: "CouchbaseDatasource"
                                       create-statements:
                                          - api: "/api/buckets"
                                            method: "POST"
                                            body: |
                                                       {
                                                         "name": "documents",
                                                         "bucketType": "couchbase",
                                                         "ramQuotaMB": 100
                                                        }
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - id: step1
                                    name: "Write a record to Couchbase"
                                    type: "vector-db-sink"
                                    input: "input-topic"
                                    configuration:
                                      datasource: "CouchbaseDatasource"
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
                                .formatted(bucketName));

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
                                name: "Read a record from Couchbase"
                                type: "query-vector-db"
                                input: "questions-topic"
                                output: "answers-topic"
                                configuration:
                                  datasource: "CouchbaseDatasource"
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
