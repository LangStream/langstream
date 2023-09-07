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

import ai.langstream.AbstractApplicationRunner;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class CassandraAssetQueryWriteIT extends AbstractApplicationRunner {

    @Container
    private CassandraContainer cassandra =
            new CassandraContainer(new DockerImageName("cassandra", "latest"));

    @Test
    public void testCassandra() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                        configuration:
                          resources:
                            - type: "vector-database"
                              name: "CassandraDatasource"
                              configuration:
                                service: "cassandra"
                                contact-points: "%s"
                                loadBalancing-localDc: "%s"
                                port: %d
                        """
                                .formatted(
                                        cassandra.getContactPoint().getHostString(),
                                        cassandra.getLocalDatacenter(),
                                        cassandra.getContactPoint().getPort()),
                        "pipeline.yaml",
                        """
                                assets:
                                  - name: "documents-table"
                                    asset-type: "cassandra-table"
                                    creation-mode: create-if-not-exists
                                    config:
                                       table-name: "documents"
                                       keyspace: "vsearch"
                                       datasource: "CassandraDatasource"
                                       statements:
                                          - "CREATE TABLE IF NOT EXISTS vsearch.documents (id text PRIMARY KEY, name text, description text);"
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - id: step1
                                    name: "Execute Query"
                                    type: "query-vector-db"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                      datasource: "CassandraDatasource"
                                      query: "SELECT id FROM vsearch.documents WHERE vsearch.documents = ?"
                                      fields:
                                        - "value.documentId"
                                      output-field: "value.query-result"
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "{\"documentId\":1}", producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of(
                                "{\"documentId\":1,\"query-result\":[{\"id\":\"C\"},{\"id\":\"B\"},{\"id\":\"D\"}]}"));
            }
        }
    }
}
