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

import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.MultiValuePattern;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class BedrockCompletionsIT extends AbstractKafkaApplicationRunner {
    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    @Test
    public void testCompletions() throws Exception {

        String model = "ai21.j2-mid-v1";
        String prompt = "What can you tell me about {{{ value.question }}} ?";

        final String amzDate = new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis());
        stubFor(
                post("/model/%s/invoke".formatted(model))
                        .withHeader(
                                "Authorization",
                                MultiValuePattern.of(
                                        containing(
                                                "AWS4-HMAC-SHA256 Credential=xx/%s/us-east-1/bedrock/aws4_request"
                                                        .formatted(amzDate))))
                        .withRequestBody(
                                equalToJson(
                                        """
                                                {
                                                "prompt": "What can you tell me about the car ?",
                                                "maxTokens": 5,
                                                "temperature": 0.2
                                                }
                                                """))
                        .willReturn(
                                okJson(
                                        """
                                                {"id":1234,"prompt":{"text":"\\n\\nWhat is a car ","tokens":[{"generatedToken":{"token":"<|newline|>","logprob":-3.389676094055176,"raw_logprob":-3.389676094055176},"topTokens":null,"textRange":{"start":0,"end":1}},{"generatedToken":{"token":"<|newline|>","logprob":-0.09020456671714783,"raw_logprob":-0.09020456671714783},"topTokens":null,"textRange":{"start":1,"end":2}},{"generatedToken":{"token":"▁What▁is","logprob":-8.349833488464355,"raw_logprob":-8.349833488464355},"topTokens":null,"textRange":{"start":2,"end":9}},{"generatedToken":{"token":"▁a▁car","logprob":-9.213733673095703,"raw_logprob":-9.213733673095703},"topTokens":null,"textRange":{"start":9,"end":15}},{"generatedToken":{"token":"▁","logprob":-6.901504039764404,"raw_logprob":-6.901504039764404},"topTokens":null,"textRange":{"start":15,"end":16}}]},"completions":[{"data":{"text":"\\nA car is a vehicle","tokens":[{"generatedToken":{"token":"<|newline|>","logprob":-0.28188949823379517,"raw_logprob":-0.28188949823379517},"topTokens":null,"textRange":{"start":0,"end":1}},{"generatedToken":{"token":"▁A","logprob":-0.20620585978031158,"raw_logprob":-0.20620585978031158},"topTokens":null,"textRange":{"start":1,"end":2}},{"generatedToken":{"token":"▁car","logprob":-0.007475498132407665,"raw_logprob":-0.007475498132407665},"topTokens":null,"textRange":{"start":2,"end":6}},{"generatedToken":{"token":"▁is▁a","logprob":-0.03556148707866669,"raw_logprob":-0.03556148707866669},"topTokens":null,"textRange":{"start":6,"end":11}},{"generatedToken":{"token":"▁vehicle","logprob":-1.5543583631515503,"raw_logprob":-1.5543583631515503},"topTokens":null,"textRange":{"start":11,"end":19}}]},"finishReason":{"reason":"length","length":5}}]}""")));

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

        String tenant = "tenant";

        String[] expectedAgents = new String[] {appId + "-step1"};

        Map<String, String> application =
                Map.of(
                        "configuration.yaml",
                        """
                                configuration:
                                  resources:
                                    - type: "bedrock-configuration"
                                      name: "bedrock configuration"
                                      configuration:
                                        endpoint-override: "%s"
                                        access-key: "xx"
                                        secret-key: "yy"
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
                                      completion-field: "value.answer"
                                      log-field: "value.prompt"
                                      min-chunks-per-message: 3
                                      messages:
                                        - content: "%s"
                                      options:
                                          request-parameters:
                                            maxTokens: 5
                                            temperature: 0.2
                                          response-completions-expression: "completions[0].data.text"
                                """
                                .formatted(model, prompt));
        try (ApplicationRuntime applicationRuntime =
                deployApplication(
                        tenant, appId, application, buildInstanceYaml(), expectedAgents)) {

            final String outputTopic = applicationRuntime.getGlobal("output-topic");

            try (KafkaProducer<String, String> producer = createProducer();
                    KafkaConsumer<String, String> consumer = createConsumer(outputTopic); ) {

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
                                {"question":"the car","session-id":"2139847128764192","answer":"A car is a vehicle","prompt":"{\\"options\\":{\\"type\\":\\"ai-chat-completions\\",\\"when\\":null,\\"model\\":\\"ai21.j2-mid-v1\\",\\"messages\\":[{\\"role\\":null,\\"content\\":\\"What can you tell me about {{{ value.question }}} ?\\"}],\\"stream-to-topic\\":null,\\"stream-response-completion-field\\":null,\\"min-chunks-per-message\\":3,\\"completion-field\\":\\"value.answer\\",\\"stream\\":true,\\"log-field\\":\\"value.prompt\\",\\"max-tokens\\":null,\\"temperature\\":null,\\"top-p\\":null,\\"logit-bias\\":null,\\"user\\":null,\\"stop\\":null,\\"presence-penalty\\":null,\\"frequency-penalty\\":null,\\"options\\":{\\"request-parameters\\":{\\"maxTokens\\":5,\\"temperature\\":0.2},\\"response-completions-expression\\":\\"completions[0].data.text\\"}},\\"messages\\":[{\\"role\\":null,\\"content\\":\\"What can you tell me about the car ?\\"}],\\"model\\":\\"ai21.j2-mid-v1\\"}"}"""
                                .formatted(prompt, outputTopic);
                waitForMessages(consumer, List.of(expected));
            }
        }
    }
}
