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
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class LangServeInvokeAgentRunnerIT extends AbstractKafkaApplicationRunner {

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    @Test
    void testStreamOuput() throws Exception {

        String response =
                """
                event: data
                data: {"content": "", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "Why", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " don", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "'t", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " cats", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " play", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " poker", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " in", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " the", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " wild", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "?\\n\\n", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "Too", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " many", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": " che", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "et", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "ah", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "s", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "!", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: data
                data: {"content": "", "additional_kwargs": {}, "type": "AIMessageChunk", "example": false}

                event: end""";

        stubFor(
                post("/chain/stream")
                        .withRequestBody(
                                equalTo("""
                        {"input":{"topic":"cats"}}"""))
                        .willReturn(ok(response)));

        Map<String, String> application =
                Map.of(
                        "module.yaml",
                        """
                                topics:
                                  - name: "input-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                  - name: "output-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                  - name: "streaming-answers-topic"
                                    creation-mode: create-if-not-exists
                                    deletion-mode: delete
                                pipeline:
                                  - type: "langserve-invoke"
                                    input: input-topic
                                    output: output-topic
                                    id: step1
                                    configuration:
                                        output-field: value.answer
                                        stream-to-topic: streaming-answers-topic
                                        stream-response-field: value
                                        min-chunks-per-message: 10
                                        debug: false
                                        method: POST
                                        allow-redirects: true
                                        handle-cookies: false
                                        url: %s/chain/stream
                                        headers:
                                           Authorisation: "Bearer {{secrets.langserve.token}}"
                                        fields:
                                           - name: topic
                                             expression: "value.topic"
                                """
                                .formatted(wireMockRuntimeInfo.getHttpBaseUrl()));

        String tenant = "tenant";
        String[] expectedAgents = {"app-step1"};

        // write some data
        try (ApplicationRuntime applicationRuntime =
                deployApplicationWithSecrets(
                        tenant,
                        "app",
                        application,
                        buildInstanceYaml(),
                        """
                                secrets:
                                  - id: langserve
                                    data:
                                      token: "my-token"
                                """,
                        expectedAgents)) {
            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer("output-topic");
                    KafkaConsumer<String, String> consumerStreaming =
                            createConsumer("streaming-answers-topic")) {
                sendMessage("input-topic", "{\"topic\":\"cats\"}", producer);
                executeAgentRunners(applicationRuntime);

                waitForMessages(
                        consumer,
                        List.of(
                                "{\"answer\":\"Why don't cats play poker in the wild?\\n\\nToo many cheetahs!\",\"topic\":\"cats\"}"));

                List<ConsumerRecord> streamingAnswers =
                        waitForMessages(
                                consumerStreaming,
                                List.of(
                                        "Why",
                                        " don't",
                                        " cats play poker in",
                                        " the wild?\n\nToo many cheetah",
                                        "s!"));
                streamingAnswers.forEach(
                        a -> {
                            log.info("Record: {}={}", a.key(), a.value());
                            a.headers()
                                    .forEach(
                                            h -> {
                                                log.info(
                                                        "header: {}={}",
                                                        h.key(),
                                                        new String(h.value()));
                                            });
                        });
            }
        }
    }
}
