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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.model.Application;
import ai.langstream.api.model.Connection;
import ai.langstream.api.model.Module;
import ai.langstream.api.model.TopicDefinition;
import ai.langstream.api.runtime.ExecutionPlan;
import ai.langstream.api.runtime.Topic;
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
class TextCompletionsIT extends AbstractKafkaApplicationRunner {

    static WireMockRuntimeInfo wireMockRuntimeInfo;

    @BeforeAll
    static void onBeforeAll(WireMockRuntimeInfo info) {
        wireMockRuntimeInfo = info;
    }

    @Test
    public void testTextCompletionsWithLogProbs(WireMockRuntimeInfo vmRuntimeInfo)
            throws Exception {

        String model = "gpt-3.5-turbo-instruct";

        stubFor(
                post("/openai/deployments/gpt-3.5-turbo-instruct/completions?api-version=2023-08-01-preview")
                        .withRequestBody(
                                equalTo(
                                        "{\"prompt\":[\"What can you tell me about the car ?\"],\"logprobs\":5,\"stream\":true}"))
                        .willReturn(
                                okJson(
                                        """

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":"\\n\\n","index":0,"logprobs":{"tokens":["\\n\\n"],"token_logprobs":[-0.16865084],"top_logprobs":[{"\\n\\n":-0.16865084,"\\n":-1.9655257," \\n\\n":-5.4967756," \\n":-6.278025,"It":-6.465525}],"text_offset":[36]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":"I","index":0,"logprobs":{"tokens":["I"],"token_logprobs":[-0.50947005],"top_logprobs":[{"I":-0.50947005,"Without":-1.69697,"Unfortunately":-2.556345,"As":-3.2907197,"The":-3.2907197}],"text_offset":[38]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" am","index":0,"logprobs":{"tokens":[" am"],"token_logprobs":[-0.81064594],"top_logprobs":[{" am":-0.81064594,"'m":-1.2325209," cannot":-2.3106458," do":-2.701271," apologize":-3.2637708}],"text_offset":[39]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" an","index":0,"logprobs":{"tokens":[" an"],"token_logprobs":[-0.0639758],"top_logprobs":[{" an":-0.0639758," sorry":-3.5014758," a":-3.8608508," not":-5.001476," unable":-5.907726}],"text_offset":[42]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" AI","index":0,"logprobs":{"tokens":[" AI"],"token_logprobs":[-0.007819127],"top_logprobs":[{" AI":-0.007819127," artificial":-4.929694," Artificial":-8.476568," A":-9.304694," digital":-10.007819}],"text_offset":[45]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" language","index":0,"logprobs":{"tokens":[" language"],"token_logprobs":[-4.2176867],"top_logprobs":[{" language":-4.2176867," and":-0.03018677,",":-4.8114367," program":-5.983311," so":-6.6083117}],"text_offset":[48]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" model","index":0,"logprobs":{"tokens":[" model"],"token_logprobs":[-0.00009771052],"top_logprobs":[{" model":-0.00009771052," processing":-10.531347," AI":-10.578222," program":-11.875096," generation":-12.046971}],"text_offset":[57]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" and","index":0,"logprobs":{"tokens":[" and"],"token_logprobs":[-0.38906613],"top_logprobs":[{" and":-0.38906613,",":-1.2171912," so":-3.982816," trained":-5.9359407," created":-6.3421907}],"text_offset":[63]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" I","index":0,"logprobs":{"tokens":[" I"],"token_logprobs":[-1.1028589],"top_logprobs":[{" I":-1.1028589," do":-0.52473384," cannot":-3.1497335," don":-4.040359," therefore":-5.6653585}],"text_offset":[67]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" do","index":0,"logprobs":{"tokens":[" do"],"token_logprobs":[-0.18535662],"top_logprobs":[{" do":-0.18535662," don":-2.2478566," cannot":-3.1541064," am":-4.2791066," can":-5.482231}],"text_offset":[69]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" not","index":0,"logprobs":{"tokens":[" not"],"token_logprobs":[-0.00009115311],"top_logprobs":[{" not":-0.00009115311," no":-10.093841," n":-11.328216," have":-11.359466," ":-11.421966}],"text_offset":[72]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" have","index":0,"logprobs":{"tokens":[" have"],"token_logprobs":[-0.0122308275],"top_logprobs":[{" have":-0.0122308275," possess":-4.496606," know":-7.418481," physically":-8.746605," personally":-8.949731}],"text_offset":[76]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" personal","index":0,"logprobs":{"tokens":[" personal"],"token_logprobs":[-0.9290634],"top_logprobs":[{" personal":-0.9290634," access":-0.96031344," the":-2.1790633," any":-3.2571883," information":-3.929063}],"text_offset":[81]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" experiences","index":0,"logprobs":{"tokens":[" experiences"],"token_logprobs":[-0.2772571],"top_logprobs":[{" experiences":-0.2772571," experience":-2.121007," knowledge":-2.152257," or":-6.074132," information":-6.449132}],"text_offset":[90]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" or","index":0,"logprobs":{"tokens":[" or"],"token_logprobs":[-0.06607247],"top_logprobs":[{" or":-0.06607247,",":-3.3004472," with":-4.144197,".":-5.4566975," like":-5.862947}],"text_offset":[102]},"finish_reason":null}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":" the","index":0,"logprobs":{"tokens":[" the"],"token_logprobs":[-2.1178281],"top_logprobs":[{" the":-2.1178281," knowledge":-0.74282813," information":-2.680328," senses":-2.9459531," opinions":-2.9615781}],"text_offset":[105]},"finish_reason":"length"}],"model":"gpt-3.5-turbo-instruct"}

                                  data: {"id":"cmpl-85xN9HjW7xxICcseHdvb5k5fXL04G","object":"text_completion","created":1696430559,"choices":[{"text":"","index":0,"logprobs":{"tokens":[],"token_logprobs":[],"top_logprobs":[],"text_offset":[]},"finish_reason":"length"}],"model":"gpt-3.5-turbo-instruct"}

                                  data: [DONE]
                                  """)));

        final String appId = "app-" + UUID.randomUUID().toString().substring(0, 4);

        String tenant = "tenant";

        String prompt = "What can you tell me about {{{ value.question }}} ?";

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
                                        access-key: "xxx"
                                        provider: "openai"
                                        url: "%s"
                                """
                                .formatted(vmRuntimeInfo.getHttpBaseUrl()),
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
                                  - name: "text-completions"
                                    type: "ai-text-completions"
                                    output: "${globals.output-topic}"
                                    configuration:
                                      model: "%s"
                                      completion-field: "value.answer"
                                      log-field: "value.prompt"
                                      logprobs: 5
                                      logprobs-field: "value.logprobs"
                                      min-chunks-per-message: 3
                                      stream: true
                                      prompt:
                                        - "%s"
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
                            createConsumer(applicationRuntime.getGlobal("output-topic")); ) {

                // produce one message to the input-topic
                // simulate a session-id header
                sendMessage(applicationRuntime.getGlobal("input-topic"), "the car", producer);

                executeAgentRunners(applicationRuntime);

                String expected =
                        "{\"question\":\"the car\",\"answer\":\"I am an AI language model and I do not have personal experiences or the\",\"prompt\":\"{\\\"options\\\":{\\\"type\\\":\\\"ai-text-completions\\\",\\\"when\\\":null,\\\"model\\\":\\\"gpt-3.5-turbo-instruct\\\",\\\"prompt\\\":[\\\"What can you tell me about {{{ value.question }}} ?\\\"],\\\"stream-to-topic\\\":null,\\\"stream-response-completion-field\\\":null,\\\"min-chunks-per-message\\\":3,\\\"completion-field\\\":\\\"value.answer\\\",\\\"stream\\\":true,\\\"log-field\\\":\\\"value.prompt\\\",\\\"logprobs-field\\\":\\\"value.logprobs\\\",\\\"logprobs\\\":5.0,\\\"max-tokens\\\":null,\\\"temperature\\\":null,\\\"top-p\\\":null,\\\"logit-bias\\\":null,\\\"user\\\":null,\\\"stop\\\":null,\\\"presence-penalty\\\":null,\\\"frequency-penalty\\\":null,\\\"options\\\":null},\\\"messages\\\":[\\\"What can you tell me about the car ?\\\"],\\\"model\\\":\\\"gpt-3.5-turbo-instruct\\\"}\",\"logprobs\":{\"tokens\":[\"I\",\" am\",\" an\",\" AI\",\" language\",\" model\",\" and\",\" I\",\" do\",\" not\",\" have\",\" personal\",\" experiences\",\" or\",\" the\"],\"logprobs\":[-0.50947005,-0.81064594,-0.0639758,-0.007819127,-4.2176867,-9.771052E-5,-0.38906613,-1.1028589,-0.18535662,-9.115311E-5,-0.0122308275,-0.9290634,-0.2772571,-0.06607247,-2.1178281]}}";
                waitForMessages(consumer, List.of(expected));
            }
        }
    }
}
