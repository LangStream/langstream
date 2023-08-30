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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.AbstractApplicationRunner;
import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.kafka.runtime.KafkaTopic;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class ChatCompletionsIT extends AbstractApplicationRunner {

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    @Test
    public void testChatCompletionWithStreaming() throws Exception {

        String model = "gpt-35-turbo";

        stubFor(
                post("/openai/deployments/gpt-35-turbo/chat/completions?api-version=2023-03-15-preview")
                        .willReturn(
                                okJson(
                                        """
                      data: {"id":"chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u","object":"chat.completion.chunk","created":1693397792,"model":"gpt-35-turbo","choices":[{"index":0,"finish_reason":null,"delta":{"role":"assistant"}}],"usage":null}

                      data: {"id":"chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u","object":"chat.completion.chunk","created":1693397792,"model":"gpt-35-turbo","choices":[{"index":0,"finish_reason":null,"delta":{"content":"A"}}],"usage":null}

                      data: {"id":"chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u","object":"chat.completion.chunk","created":1693397792,"model":"gpt-35-turbo","choices":[{"index":0,"finish_reason":null,"delta":{"content":" car"}}],"usage":null}

                      data: {"id":"chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u","object":"chat.completion.chunk","created":1693397792,"model":"gpt-35-turbo","choices":[{"index":0,"finish_reason":null,"delta":{"content":" is"}}],"usage":null}

                      data: {"id":"chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u","object":"chat.completion.chunk","created":1693397792,"model":"gpt-35-turbo","choices":[{"index":0,"finish_reason":null,"delta":{"content":" a"}}],"usage":null}

                      data: {"id":"chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u","object":"chat.completion.chunk","created":1693397792,"model":"gpt-35-turbo","choices":[{"index":0,"finish_reason":null,"delta":{"content":" vehicle"}}],"usage":null}

                      data: {"id":"chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u","object":"chat.completion.chunk","created":1693397792,"model":"gpt-35-turbo","choices":[{"index":0,"finish_reason":"stop","delta":{}}],"usage":null}

                      data: [DONE]
                      """)));

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);
        String inputTopic = "input-topic-" + UUID.randomUUID();
        String outputTopic = "output-topic-" + UUID.randomUUID();
        String streamTopic = "stream-topic-" + UUID.randomUUID();
        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};

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
                                        access-key: "xxx"
                                        provider: "azure"
                                """
                                .formatted(wireMockRuntimeInfo.getHttpBaseUrl()),
                        "module.yaml",
                        """
                                module: "module-1"
                                id: "pipeline-1"
                                topics:
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                  - name: "%s"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "chat-completions"
                                    id: "step1"
                                    type: "ai-chat-completions"
                                    input: "%s"
                                    output: "%s"
                                    configuration:
                                      model: "%s"
                                      stream-to-topic: "%s"
                                      completion-field: "value"
                                      stream-to-topic: "%s"
                                      min-chunks-per-message: 3
                                      stream: true
                                      messages:
                                        - role: user
                                          content: "%s"
                                """
                                .formatted(
                                        inputTopic,
                                        outputTopic,
                                        streamTopic,
                                        inputTopic,
                                        outputTopic,
                                        model,
                                        streamTopic,
                                        streamTopic,
                                        "What can you tell me about {{% value}} ?"));
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
                            instanceof KafkaTopic);

            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains(inputTopic));
            assertTrue(topics.contains(outputTopic));
            assertTrue(topics.contains(streamTopic));

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic);
                    KafkaConsumer<String, String> streamConsumer = createConsumer(streamTopic)) {

                // produce one message to the input-topic
                sendMessage(inputTopic, "the car", producer);

                executeAgentRunners(applicationRuntime);

                waitForMessages(consumer, List.of("A car is a vehicle"));

                waitForMessages(streamConsumer, List.of("A", " car is", " a vehicle"));
            }
        }
    }
}
