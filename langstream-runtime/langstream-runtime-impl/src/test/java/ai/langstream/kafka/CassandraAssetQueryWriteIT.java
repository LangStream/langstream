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

import ai.langstream.AbstractApplicationRunner;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.streaming.ai.datasource.CassandraDataSource;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
        String[] expectedAgents = {"app-step1", "app-step2"};

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
                                  - name: "vsearch-keyspace"
                                    asset-type: "cassandra-keyspace"
                                    creation-mode: create-if-not-exists
                                    config:
                                       keyspace: "vsearch"
                                       datasource: "CassandraDatasource"
                                       create-statements:
                                          - "CREATE KEYSPACE vsearch WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor' : 1};"
                                  - name: "documents-table"
                                    asset-type: "cassandra-table"
                                    creation-mode: create-if-not-exists
                                    config:
                                       table-name: "documents"
                                       keyspace: "vsearch"
                                       datasource: "CassandraDatasource"
                                       create-statements:
                                          - "CREATE TABLE IF NOT EXISTS vsearch.documents (id int PRIMARY KEY, name text, description text);"
                                          - "INSERT INTO vsearch.documents (id, name, description) VALUES (1, 'A', 'A description');"
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
                                    configuration:
                                      datasource: "CassandraDatasource"
                                      query: "SELECT * FROM vsearch.documents WHERE id=?;"
                                      only-first: true
                                      output-field: "value.queryresult"
                                      fields:
                                        - "value.documentId"
                                  - name: "Generate a new record, with a new id"
                                    type: "compute"
                                    output: "output-topic"
                                    configuration:
                                      fields:
                                        - expression : "value.documentId + 1"
                                          name : "value.documentId"
                                        - expression : "value.queryresult.name"
                                          name : "value.name"
                                        - expression : "value.queryresult.description"
                                          name : "value.description"
                                  - id: step2
                                    name: "Write a new record to Cassandra"
                                    type: "vector-db-sink"
                                    input: "output-topic"
                                    configuration:
                                      datasource: "CassandraDatasource"
                                      table-name: "documents"
                                      keyspace: "vsearch"
                                      mapping: "id=value.documentId,name=value.name,description=value.description"
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
                                "{\"documentId\":2,\"queryresult\":{\"name\":\"A\",\"description\":\"A description\",\"id\":\"1\"},\"name\":\"A\",\"description\":\"A description\"}"));

                try (CassandraDataSource cassandraDataSource = new CassandraDataSource()) {
                    cassandraDataSource.initialize(
                            Map.of(
                                    "service", "cassandra",
                                    "contact-points", cassandra.getContactPoint().getHostString(),
                                    "loadBalancing-localDc", cassandra.getLocalDatacenter(),
                                    "port", cassandra.getContactPoint().getPort()));
                    ResultSet execute =
                            cassandraDataSource
                                    .getSession()
                                    .execute("SELECT * FROM vsearch.documents");
                    List<Row> all = execute.all();
                    Set<Integer> documentIds =
                            all.stream().map(row -> row.getInt("id")).collect(Collectors.toSet());
                    all.forEach(row -> log.info("row id {}", row.get("id", Integer.class)));
                    assertEquals(2, all.size());
                    assertEquals(Set.of(1, 2), documentIds);
                }
            }
        }
    }
}
