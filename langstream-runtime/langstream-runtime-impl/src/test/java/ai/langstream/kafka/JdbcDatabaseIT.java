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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class JdbcDatabaseIT extends AbstractKafkaApplicationRunner {

    static final ObjectMapper MAPPER = new ObjectMapper();

    @Container
    static GenericContainer database =
            new GenericContainer(DockerImageName.parse("herddb/herddb:0.28.0"))
                    .withExposedPorts(7000);

    @BeforeAll
    public static void startDatabase() {
        database.start();
    }

    @AfterAll
    public static void stopDatabase() {
        database.stop();
    }

    @Test
    public void testSimpleQueries() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};
        String jdbcUrl = "jdbc:herddb:server:localhost:" + database.getMappedPort(7000);

        Map<String, String> applicationWriter =
                Map.of(
                        "configuration.yaml",
                        """
                        configuration:
                          resources:
                            - type: "datasource"
                              name: "JdbcDatasource"
                              configuration:
                                service: "jdbc"
                                driverClass: "herddb.jdbc.Driver"
                                url: "%s"
                                user: "sa"
                                password: "hdb"
                                """
                                .formatted(jdbcUrl),
                        "module.yaml",
                        """
                                assets:
                                  - name: "documents-table"
                                    asset-type: "jdbc-table"
                                    creation-mode: create-if-not-exists
                                    config:
                                      table-name: "documents"
                                      datasource: "JdbcDatasource"
                                      create-statements:
                                        - |
                                          CREATE TABLE documents (
                                          pkfield integer auto_increment primary key,
                                          text string)
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Write"
                                    type: "query"
                                    input: input-topic
                                    id: step1
                                    configuration:
                                      mode: "execute"
                                      datasource: "JdbcDatasource"
                                      output-field: "value.command_results"
                                      generated-keys:
                                        - "pkfield"
                                      query: |
                                            INSERT INTO DOCUMENTS (text) values(?)
                                      fields:
                                        - "value.text"
                                  - name: "Read"
                                    type: "query"
                                    output: output-topic
                                    configuration:
                                      mode: "query"
                                      datasource: "JdbcDatasource"
                                      output-field: "value.query_results"
                                      query: |
                                            SELECT * FROM DOCUMENTS where pkfield = ?
                                      fields:
                                        - "fn:toInt(value.command_results.generatedKeys.key)"

                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", applicationWriter, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {
                List<Consumer<String>> expectedMessages = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    int expectedPk = i + 1;
                    String message =
                            """
                                    {"text":"doc%s.pdf"}"""
                                    .formatted(i);
                    sendMessage("input-topic", message, producer);

                    final int index = i;
                    expectedMessages.add(
                            new Consumer<String>() {
                                @Override
                                @SneakyThrows
                                public void accept(String s) {
                                    Map<String, Object> parsed = MAPPER.readValue(s, Map.class);
                                    log.info("message: {}", message);
                                    log.info("Parsed message: {}", parsed);
                                    try {
                                        assertEquals(
                                                "doc%s.pdf".formatted(index), parsed.get("text"));
                                        Map<String, Object> commandResults =
                                                (Map<String, Object>) parsed.get("command_results");
                                        assertEquals(1, commandResults.get("count"));
                                        assertEquals(
                                                Map.of("key", expectedPk),
                                                commandResults.get("generatedKeys"));
                                        List<Map<String, Object>> query_results =
                                                (List<Map<String, Object>>)
                                                        parsed.get("query_results");
                                        assertEquals(1, query_results.size());
                                        Map<String, Object> firstResult = query_results.get(0);
                                        assertEquals(expectedPk, firstResult.get("pkfield"));
                                        assertEquals(
                                                "doc%s.pdf".formatted(index),
                                                firstResult.get("text"));
                                    } catch (AssertionError err) {
                                        log.error("Error on message: {}", message, err);
                                        throw err;
                                    }
                                    log.info("Looks good");
                                }
                            });
                }
                executeAgentRunners(applicationRuntime);
                waitForMessages(consumer, expectedMessages);
            }
        }
    }
}
