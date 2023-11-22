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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
@Disabled
class AstraVectorDBAssetQueryWriteIT extends AbstractKafkaApplicationRunner {

    static final String SECRETS_PATH = "";

    @Test
    public void testAstra() throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        String secrets = Files.readString(Paths.get(SECRETS_PATH));

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                                configuration:
                                  resources:
                                    - type: "vector-database"
                                      name: "AstraDBDatasource"
                                      configuration:
                                        service: "astra-vector-db"
                                        token: "${ secrets.astra.token }"
                                        endpoint: "${ secrets.astra.endpoint }"
                                """,
                        "pipeline.yaml",
                        """
                                assets:
                                  - name: "documents-collection"
                                    asset-type: "astra-collection"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                    config:
                                       collection-name: "documents"
                                       datasource: "AstraDBDatasource"
                                       vector-dimension: 3
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - id: step1
                                    name: "Write a document using a query"
                                    type: "query-vector-db"
                                    input: "input-topic"
                                    configuration:
                                      mode: execute
                                      datasource: "AstraDBDatasource"
                                      query: |
                                           {
                                              "action": "insertOne",
                                              "collection-name": "documents",
                                              "document": {
                                                "id": "the-id",
                                                "name": "A name",
                                                "text": "A text",
                                                "vector": [1,2,3]
                                              }
                                           }
                                      output-field: "value.insertresult"
                                      fields:
                                        - "value.documentId"
                                  - name: "Read the document using a query"
                                    type: "query-vector-db"
                                    configuration:
                                      datasource: "AstraDBDatasource"
                                      query: |
                                           {
                                              "collection-name": "documents",
                                              "filter": {
                                                "id": ?
                                              },
                                              "vector": [1,2,3]
                                           }
                                      only-first: true
                                      output-field: "value.queryresult"
                                      fields:
                                        - "value.insertresult.id"
                                  - id: step2
                                    name: "Write a new record to Astra"
                                    type: "vector-db-sink"
                                    configuration:
                                      datasource: "AstraDBDatasource"
                                      collection-name: "documents"
                                      fields:
                                         - name: "id"
                                           expression: "fn:toString('new-id')"
                                         - name: "vector"
                                           expression: "value.queryresult.vector"
                                         - name: "text"
                                           expression: "value.queryresult.text"
                                         - name: "name"
                                           expression: "value.queryresult.name"
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplicationWithSecrets(
                        tenant, "app", application, buildInstanceYaml(), secrets, expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer(); ) {

                sendMessage("input-topic", "{\"documentId\":1}", producer);

                executeAgentRunners(applicationRuntime);
            }

            applicationDeployer.cleanup(tenant, applicationRuntime.implementation());
        }
    }
}
