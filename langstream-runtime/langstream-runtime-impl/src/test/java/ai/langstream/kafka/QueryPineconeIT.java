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

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class QueryPineconeIT extends AbstractKafkaApplicationRunner {

    @Test
    public void testQueryPinecone(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {
        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        // this is a mock version, Pinecode uses GRPC, but we are mocking the query endpoint
        // and in the Pinecone client we are using a dummy HTTP client

        String endpoint = vmRuntimeInfo.getHttpBaseUrl() + "/query";
        stubFor(
                post("/query")
                        .willReturn(
                                okJson(
                                        """
             [
                   {
                    "id": "C"
                   },
                   {
                    "id": "B"
                   },
                   {
                    "id": "D"
                   }
               ]
        """)));

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                        configuration:
                          resources:
                            - type: "vector-database"
                              name: "PineconeDatasource"
                              configuration:
                                service: "pinecone"
                                api-key: "xxx"
                                environment: "the-env"
                                index-name: "the-index"
                                project-name: "the-project"
                                server-side-timeout-sec: 10
                                endpoint: "%s"
                        """
                                .formatted(endpoint),
                        "pipeline.yaml",
                        """
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
                                      datasource: "PineconeDatasource"
                                      query: |
                                        {
                                              "vector": ?,
                                              "topK": 5,
                                              "filter":
                                                {"$and": [{"genre": "comedy"}, {"year":2019}]}
                                         }
                                      fields:
                                        - "value.embeddings"
                                      output-field: "value.query-result"
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage("input-topic", "{\"embeddings\":[0.1,0.2,0.3]}", producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of(
                                "{\"embeddings\":[0.1,0.2,0.3],\"query-result\":[{\"id\":\"C\"},{\"id\":\"B\"},{\"id\":\"D\"}]}"));
            }
        }
    }
}
