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

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
@WireMockTest
class ChatCompletionsIT extends AbstractKafkaApplicationRunner {

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testChatCompletionWithStreaming(boolean legacy) throws Exception {

        String model = "gpt-35-turbo";

        stubFor(
                post("/openai/deployments/gpt-35-turbo/chat/completions?api-version=2023-08-01-preview")
                        .withRequestBody(
                                equalTo(
                                        "{\"messages\":[{\"role\":\"user\",\"content\":\"What can you tell me about the car ?\"}],\"stream\":true}"))
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

        String tenant = "tenant";

        String prompt =
                legacy
                        ? "What can you tell me about {{% value.question }} ?"
                        : "What can you tell me about {{{ value.question }}} ?";

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
                                  - name: "${globals.input-topic}"
                                    creation-mode: create-if-not-exists
                                  - name: "${globals.output-topic}"
                                    creation-mode: create-if-not-exists
                                  - name: "${globals.stream-topic}"
                                    creation-mode: create-if-not-exists
                                pipeline:
                                  - name: "convert-to-json"
                                    id: "step1"
                                    type: "document-to-json"
                                    input: "${globals.input-topic}"
                                    configuration:
                                      text-field: "question"
                                  - name: "chat-completions"
                                    type: "ai-chat-completions"
                                    output: "${globals.output-topic}"
                                    configuration:
                                      model: "%s"
                                      stream-to-topic: "${globals.stream-topic}"
                                      stream-response-completion-field: "value"
                                      completion-field: "value.answer"
                                      log-field: "value.prompt"
                                      min-chunks-per-message: 3
                                      stream: true
                                      messages:
                                        - role: user
                                          content: "%s"
                                """
                                .formatted(model, prompt));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {
            ExecutionPlan implementation = applicationRuntime.implementation();
            Application applicationInstance = applicationRuntime.applicationInstance();

            Module module = applicationInstance.getModule("module-1");
            assertTrue(
                    implementation.getConnectionImplementation(
                                    module,
                                    Connection.fromTopic(
                                            TopicDefinition.fromName(
                                                    applicationRuntime.getGlobal("input-topic"))))
                            instanceof Topic);

            Set<String> topics = getKafkaAdmin().listTopics().names().get();
            log.info("Topics {}", topics);
            assertTrue(topics.contains(applicationRuntime.getGlobal("input-topic")));
            assertTrue(topics.contains(applicationRuntime.getGlobal("output-topic")));
            final String streamToTopic = applicationRuntime.getGlobal("stream-topic");
            assertTrue(topics.contains(streamToTopic));

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer =
                            createConsumer(applicationRuntime.getGlobal("output-topic"));
                    KafkaConsumer<String, String> streamConsumer = createConsumer(streamToTopic)) {

                // produce one message to the input-topic
                // simulate a session-id header
                sendMessage(
                        applicationRuntime.getGlobal("input-topic"),
                        "the car",
                        List.of(
                                new RecordHeader(
                                        "session-id",
                                        "2139847128764192".getBytes(StandardCharsets.UTF_8))),
                        producer);

                executeAgentRunners(applicationRuntime);

                String expected =
                        """
                                                {"question":"the car","session-id":"2139847128764192","answer":"A car is a vehicle","prompt":"{\\"options\\":{\\"type\\":\\"ai-chat-completions\\",\\"when\\":null,\\"model\\":\\"gpt-35-turbo\\",\\"messages\\":[{\\"role\\":\\"user\\",\\"content\\":\\"%s\\"}],\\"stream-to-topic\\":\\"%s\\",\\"stream-response-completion-field\\":\\"value\\",\\"min-chunks-per-message\\":3,\\"completion-field\\":\\"value.answer\\",\\"stream\\":true,\\"log-field\\":\\"value.prompt\\",\\"max-tokens\\":null,\\"temperature\\":null,\\"top-p\\":null,\\"logit-bias\\":null,\\"user\\":null,\\"stop\\":null,\\"presence-penalty\\":null,\\"frequency-penalty\\":null,\\"options\\":null},\\"messages\\":[{\\"role\\":\\"user\\",\\"content\\":\\"What can you tell me about the car ?\\"}],\\"model\\":\\"gpt-35-turbo\\"}"}"""
                                .formatted(prompt, streamToTopic);
                List<ConsumerRecord> mainOutputRecords =
                        waitForMessages(consumer, List.of(expected));
                ConsumerRecord record = mainOutputRecords.get(0);

                assertNull(record.headers().lastHeader("stream-id"));
                assertNull(record.headers().lastHeader("stream-last-message"));
                assertNull(record.headers().lastHeader("stream-index"));
                // verify that session id is copied
                assertEquals(
                        "2139847128764192",
                        new String(record.headers().lastHeader("session-id").value()));

                List<ConsumerRecord> streamingAnswers =
                        waitForMessages(streamConsumer, List.of("A", " car is", " a vehicle"));
                record = streamingAnswers.get(0);
                assertEquals(
                        "chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u",
                        new String(record.headers().lastHeader("stream-id").value()));
                assertEquals(
                        "false",
                        new String(record.headers().lastHeader("stream-last-message").value()));
                assertEquals("1", new String(record.headers().lastHeader("stream-index").value()));
                // verify that session id is copied
                assertEquals(
                        "2139847128764192",
                        new String(record.headers().lastHeader("session-id").value()));

                record = streamingAnswers.get(1);
                assertEquals(
                        "chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u",
                        new String(record.headers().lastHeader("stream-id").value()));
                assertEquals(
                        "false",
                        new String(record.headers().lastHeader("stream-last-message").value()));
                assertEquals("2", new String(record.headers().lastHeader("stream-index").value()));
                assertEquals(
                        "2139847128764192",
                        new String(record.headers().lastHeader("session-id").value()));

                record = streamingAnswers.get(2);
                assertEquals(
                        "chatcmpl-7tEPYbaK1YcjxwbmkuDqv22vE5w7u",
                        new String(record.headers().lastHeader("stream-id").value()));
                assertEquals(
                        "true",
                        new String(record.headers().lastHeader("stream-last-message").value()));
                assertEquals("3", new String(record.headers().lastHeader("stream-index").value()));
                assertEquals(
                        "2139847128764192",
                        new String(record.headers().lastHeader("session-id").value()));
            }
        }
    }
}
