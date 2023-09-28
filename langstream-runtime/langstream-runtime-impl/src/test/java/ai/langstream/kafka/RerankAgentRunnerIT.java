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

@Slf4j
class RerankAgentRunnerIT extends AbstractApplicationRunner {

    @Test
    public void testSimpleRerank() throws Exception {
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
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "Re-rank query results"
                                    id: step1
                                    type: "re-rank"
                                    input: "input-topic"
                                    output: "output-topic"
                                    configuration:
                                        field: "value.query_results"
                                        output-field: "value.reranked_results"
                                        query-text: "value.query"
                                        query-embeddings: "value.query_embeddings"
                                        text-field: "record.text"
                                        embeddings-field: "record.embeddings"
                                        algorithm: "MMR"
                                        lambda: 0.5
                                        b: 2
                                        k1: 1.2
                                """);

        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, "app", application, buildInstanceYaml(), expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic")) {

                sendMessage(
                        "input-topic",
                        """
                                                        {
                                                            "query_results": [
                                                                {
                                                                    "text": "one",
                                                                    "embeddings": [1,2,3,4,5]
                                                                }, {
                                                                    "text": "two",
                                                                    "embeddings": [1,2,3,4,6]
                                                                }, {
                                                                    "text": "three",
                                                                    "embeddings": [1,2,3,4,7]
                                                                }
                                                            ],
                                                            "query": "Tell me something",
                                                            "query_embeddings": [1,2,3,4,5]
                                                        }
                                                        """,
                        producer);

                executeAgentRunners(applicationRuntime);
                waitForMessages(
                        consumer,
                        List.of(
                                """
                                    {"query_embeddings":[1,2,3,4,5],"query_results":[{"embeddings":[1,2,3,4,5],"text":"one"},{"embeddings":[1,2,3,4,6],"text":"two"},{"embeddings":[1,2,3,4,7],"text":"three"}],"query":"Tell me something","reranked_results":[{"embeddings":[1,2,3,4,5],"text":"one"},{"embeddings":[1,2,3,4,7],"text":"three"},{"embeddings":[1,2,3,4,6],"text":"two"}]}"""));
            }
        }
    }
}
