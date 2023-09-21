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

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.AbstractApplicationRunner;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
@WireMockTest
class ComputeEmbeddingsIT extends AbstractApplicationRunner {

    @AllArgsConstructor
    private static class EmbeddingsConfig {
        String model;
        String providerConfiguration;
        Runnable stubMakers;

        @Override
        public String toString() {
            return "EmbeddingsConfig{" + "model='" + model + '\'' + '}';
        }
    }

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    private static Stream<Arguments> providers() {
        return Stream.of(
                Arguments.of(
                        new EmbeddingsConfig(
                                "textembedding-gecko",
                                """
                             configuration:
                                 resources:
                                    - type: "vertex-configuration"
                                      name: "Vertex configuration"
                                      configuration:
                                        url: "%s"
                                        region: "us-east1"
                                        project: "the-project"
                                        token: "some-token"
                        """
                                        .formatted(wireMockRuntimeInfo.getHttpBaseUrl()),
                                () ->
                                        stubFor(
                                                post("/v1/projects/the-project/locations/us-east1/publishers/google/models/textembedding-gecko:predict")
                                                        .willReturn(
                                                                okJson(
                                                                        """
                                           {
                                              "predictions": [
                                                {
                                                  "embeddings": {
                                                    "statistics": {
                                                      "truncated": false,
                                                      "token_count": 6
                                                    },
                                                    "values": [ 1.0, 5.4, 8.7]
                                                  }
                                                }
                                              ]
                                            }
            """))))),
                Arguments.of(
                        new EmbeddingsConfig(
                                "text-embedding-ada-002",
                                """
                                 configuration:
                                      resources:
                                        - type: "open-ai-configuration"
                                          name: "OpenAI Azure configuration"
                                          configuration:
                                            url: "%s"
                                            access-key: "xxx"
                                            provider: "azure"
                                """
                                        .formatted(wireMockRuntimeInfo.getHttpBaseUrl()),
                                () ->
                                        stubFor(
                                                post("/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-08-01-preview")
                                                        .willReturn(
                                                                okJson(
                                                                        """
                                           {
                                               "data": [
                                                 {
                                                   "embedding": [1.0, 5.4, 8.7],
                                                   "index": 0,
                                                   "object": "embedding"
                                                 }
                                               ],
                                               "model": "text-embedding-ada-002",
                                               "object": "list",
                                               "usage": {
                                                 "prompt_tokens": 5,
                                                 "total_tokens": 5
                                               }
                                             }
            """))))),
                Arguments.of(
                        new EmbeddingsConfig(
                                "some-model",
                                """
                             configuration:
                                 resources:
                                    - type: "hugging-face-configuration"
                                      name: "Hugging Face API configuration"
                                      configuration:
                                        api-url: "%s"
                                        model-check-url: "%s"
                                        access-key: "some-token"
                                        provider: "api"
                        """
                                        .formatted(
                                                wireMockRuntimeInfo.getHttpBaseUrl()
                                                        + "/embeddings/",
                                                wireMockRuntimeInfo.getHttpBaseUrl()
                                                        + "/modelcheck/"),
                                () -> {
                                    stubFor(
                                            get("/modelcheck/some-model")
                                                    .willReturn(
                                                            okJson(
                                                                    "{\"modelId\": \"some-model\",\"tags\": [\"sentence-transformers\"]}")));
                                    stubFor(
                                            post("/embeddings/some-model")
                                                    .willReturn(okJson("[[1.0, 5.4, 8.7]]")));
                                })));
    }

    @ParameterizedTest
    @MethodSource("providers")
    public void testComputeEmbeddings(EmbeddingsConfig config) throws Exception {
        wireMockRuntimeInfo
                .getWireMock()
                .allStubMappings()
                .getMappings()
                .forEach(
                        stubMapping -> {
                            log.info("Removing stub {}", stubMapping);
                            wireMockRuntimeInfo.getWireMock().removeStubMapping(stubMapping);
                        });
        config.stubMakers.run();
        // wait for WireMock to be ready
        Thread.sleep(1000);

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        config.providerConfiguration,
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "%s"
                                    output: "%s"
                                    configuration:
                                      model: "%s"
                                      embeddings-field: "value.embeddings"
                                      text: "something to embed"
                                      flush-interval: 0
                                """
                                .formatted(
                                        inputTopic,
                                        outputTopic,
                                        inputTopic,
                                        outputTopic,
                                        config.model));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {
            ExecutionPlan implementation = applicationRuntime.implementation();
            Application applicationInstance = applicationRuntime.applicationInstance();

            Module module = applicationInstance.getModule("module-1");
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName(inputTopic)))
                            instanceof Topic);

            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains(inputTopic));

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                // produce one message to the input-topic
                sendMessage(
                        inputTopic,
                        "{\"name\": \"some name\", \"description\": \"some description\"}",
                        producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                "{\"name\":\"some name\",\"description\":\"some description\",\"embeddings\":[1.0,5.4,8.7]}"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testComputeBatchEmbeddings(boolean sameKey) throws Exception {
        wireMockRuntimeInfo
                .getWireMock()
                .allStubMappings()
                .getMappings()
                .forEach(
                        stubMapping -> {
                            log.info("Removing stub {}", stubMapping);
                            wireMockRuntimeInfo.getWireMock().removeStubMapping(stubMapping);
                        });
        String embeddingFirst = "[1.0,5.4,8.7]";
        String embeddingSecond = "[2.0,5.4,8.7]";
        String embeddingThird = "[3.0,5.4,8.7]";
        stubFor(
                post("/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-08-01-preview")
                        .willReturn(
                                okJson(
                                        """
                                               {
                                                   "data": [
                                                     {
                                                       "embedding": %s,
                                                       "index": 0,
                                                       "object": "embedding"
                                                     },
                                                     {
                                                       "embedding": %s,
                                                       "index": 0,
                                                       "object": "embedding"
                                                     },
                                                     {
                                                       "embedding": %s,
                                                       "index": 0,
                                                       "object": "embedding"
                                                     }
                                                   ],
                                                   "model": "text-embedding-ada-002",
                                                   "object": "list",
                                                   "usage": {
                                                     "prompt_tokens": 5,
                                                     "total_tokens": 5
                                                   }
                                                 }
                                            """
                                                .formatted(
                                                        embeddingFirst,
                                                        embeddingSecond,
                                                        embeddingThird))));
        // wait for WireMock to be ready
        Thread.sleep(1000);

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};
        String model = "text-embedding-ada-002";

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                               configuration:
                                 resources:
                                   - type: "open-ai-configuration"
                                     name: "OpenAI Azure configuration"
                                     configuration:
                                       url: "%s"
                                       access-key: "%s"
                                       provider: "azure"
                               """
                                .formatted(wireMockRuntimeInfo.getHttpBaseUrl(), "sdòflkjsòlfkj"),
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                    options:
                                      # we want to read more than one record at a time
                                      consumer.max.poll.records: 100
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "compute-embeddings"
                                    id: "step1"
                                    type: "compute-ai-embeddings"
                                    input: "%s"
                                    output: "%s"
                                    configuration:
                                      model: "%s"
                                      embeddings-field: "value.embeddings"
                                      text: "something to embed"
                                      batch-size: 3
                                      flush-interval: 10000
                                """
                                .formatted(
                                        inputTopic, outputTopic, inputTopic, outputTopic, model));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {
            ExecutionPlan implementation = applicationRuntime.implementation();
            Application applicationInstance = applicationRuntime.applicationInstance();

            Module module = applicationInstance.getModule("module-1");
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(TopicDefinition.fromName(inputTopic)))
                            instanceof Topic);

            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains(inputTopic));

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic)) {

                // produce 10 messages to the input-topic
                List<String> expected = new ArrayList<>();
                for (int i = 0; i < 9; i++) {
                    String name = "name_" + i;
                    String key = sameKey ? "key" : "key_" + (i % 3);
                    sendMessage(
                            inputTopic,
                            key,
                            "{\"name\": \" " + name + "\", \"description\": \"some description\"}",
                            List.of(),
                            producer);

                    String embeddings;
                    if (sameKey) {
                        if (i % 3 == 0) {
                            embeddings = embeddingFirst;
                        } else if (i % 3 == 1) {
                            embeddings = embeddingSecond;
                        } else {
                            embeddings = embeddingThird;
                        }
                    } else {
                        // this may look weird, but given the key distribution, we build 3 batches
                        // that contain 3 messages each
                        // the first 3 messages become the head of each batch, the next 3 messages
                        // are the second element of each batch, and so on
                        embeddings =
                                switch (i) {
                                    case 0, 1, 2 -> embeddingFirst;
                                    case 3, 4, 5 -> embeddingSecond;
                                    case 6, 7, 8 -> embeddingThird;
                                    default -> throw new IllegalStateException();
                                };
                    }
                    String expectedContent =
                            "{\"name\":\" "
                                    + name
                                    + "\",\"description\":\"some description\",\"embeddings\":"
                                    + embeddings
                                    + "}";
                    expected.add(expectedContent);
                }

                executeAgentRunners(applicationRuntime);

                if (sameKey) {
                    // all the messages have the same key, so they must be processed in order
                    waitForMessages(consumer, expected);
                } else {
                    waitForMessagesInAnyOrder(consumer, expected);
                }
            }
        }
    }
}
