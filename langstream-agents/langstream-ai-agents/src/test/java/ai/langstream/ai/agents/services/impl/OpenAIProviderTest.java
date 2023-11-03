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
package ai.langstream.ai.agents.services.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.ai.agents.services.ServiceProviderProvider;
import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.Chunk;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class OpenAIProviderTest {

    @Test
    void testStreamingChatCompletion(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {
        resetWiremockStubs(vmRuntimeInfo);
        stubFor(
                post("/openai/deployments/gpt-35-turbo/chat/completions?api-version=2023-08-01-preview")
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

        ServiceProviderProvider provider = new OpenAIServiceProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "openai",
                                Map.of(
                                        "provider",
                                        "azure",
                                        "access-key",
                                        "xxxxxxx",
                                        "url",
                                        vmRuntimeInfo.getHttpBaseUrl())),
                        MetricsReporter.DISABLED);

        List<String> chunks = new CopyOnWriteArrayList<>();
        CompletionsService service = implementation.getCompletionsService(Map.of());
        ChatCompletions chatCompletions =
                service.getChatCompletions(
                                List.of(new ChatMessage("user").setContent("What is a car?")),
                                new CompletionsService.StreamingChunksConsumer() {
                                    @Override
                                    public void consumeChunk(
                                            String answerId, int index, Chunk chunk, boolean last) {
                                        ChatChoice chatChoice = (ChatChoice) chunk;
                                        chunks.add(chunk.content());
                                        log.info(
                                                "chunk: (last={}) {} {}",
                                                last,
                                                chatChoice.getMessage().getRole(),
                                                chatChoice.getMessage().getContent());
                                    }
                                },
                                Map.of(
                                        "model",
                                        "gpt-35-turbo",
                                        "stream",
                                        true,
                                        "min-chunks-per-message",
                                        3))
                        .get();

        Awaitility.await()
                .untilAsserted(
                        () -> {
                            log.info("result: {}", chatCompletions);
                            log.info("chunks: {}", chunks);
                            assertEquals(
                                    "A car is a vehicle",
                                    chatCompletions.getChoices().get(0).getMessage().getContent());
                            assertEquals(3, chunks.size());
                            assertEquals("A", chunks.get(0));
                            assertEquals(" car is", chunks.get(1));
                            assertEquals(" a vehicle", chunks.get(2));
                        });
    }

    @Test
    void testStreamingTextCompletion(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {
        resetWiremockStubs(vmRuntimeInfo);
        stubFor(
                post("/openai/deployments/gpt-35-turbo-instruct/completions?api-version=2023-08-01-preview")
                        .withRequestBody(
                                equalTo(
                                        "{\"prompt\":[\"Translate from English to Italian: \\\"I love cars\\\" with quotes\"],\"stream\":true}"))
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

        ServiceProviderProvider provider = new OpenAIServiceProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "openai",
                                Map.of(
                                        "provider",
                                        "azure",
                                        "access-key",
                                        "xxxxxxx",
                                        "url",
                                        vmRuntimeInfo.getHttpBaseUrl())),
                        MetricsReporter.DISABLED);

        List<String> chunks = new CopyOnWriteArrayList<>();
        CompletionsService service = implementation.getCompletionsService(Map.of());
        String completions =
                service.getTextCompletions(
                                List.of(
                                        "Translate from English to Italian: \"I love cars\" with quotes"),
                                new CompletionsService.StreamingChunksConsumer() {
                                    @Override
                                    public void consumeChunk(
                                            String answerId, int index, Chunk chunk, boolean last) {
                                        chunks.add(chunk.content());
                                        log.info("chunk: (last={}) {}", last, chunk.content());
                                    }
                                },
                                Map.of(
                                        "model",
                                        "gpt-35-turbo-instruct",
                                        "stream",
                                        true,
                                        "min-chunks-per-message",
                                        3))
                        .get()
                        .text();
        Awaitility.await()
                .untilAsserted(
                        () -> {
                            log.info("result: {}", completions);
                            log.info("chunks: {}", chunks);
                            assertEquals(
                                    "I am an AI language model and I do not have personal experiences or the",
                                    completions);
                            assertEquals(7, chunks.size());
                            assertEquals("I", chunks.get(0));
                            assertEquals(" am an", chunks.get(1));
                            assertEquals(" AI language model", chunks.get(2));
                            assertEquals(" and I do", chunks.get(3));
                            assertEquals(" not have personal", chunks.get(4));
                            assertEquals(" experiences or the", chunks.get(5));
                            assertEquals("", chunks.get(6));
                        });
    }

    private static void resetWiremockStubs(WireMockRuntimeInfo wireMockRuntimeInfo)
            throws Exception {
        wireMockRuntimeInfo
                .getWireMock()
                .allStubMappings()
                .getMappings()
                .forEach(
                        stubMapping -> {
                            log.info("Removing stub {}", stubMapping);
                            wireMockRuntimeInfo.getWireMock().removeStubMapping(stubMapping);
                        });
        Thread.sleep(1000);
    }
}
