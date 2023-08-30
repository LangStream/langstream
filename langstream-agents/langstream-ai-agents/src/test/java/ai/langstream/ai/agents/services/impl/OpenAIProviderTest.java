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

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.ai.agents.services.ServiceProviderProvider;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class OpenAIProviderTest {

    @Test
    void testStreamingCompletion(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {

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
                                        vmRuntimeInfo.getHttpBaseUrl())));

        List<String> chunks = new CopyOnWriteArrayList<>();
        CompletionsService service = implementation.getCompletionsService(Map.of());
        ChatCompletions chatCompletions =
                service.getChatCompletions(
                                List.of(new ChatMessage("user").setContent("What is a car?")),
                                new CompletionsService.StreamingChunksConsumer() {
                                    @Override
                                    public void consumeChunk(
                                            String answerId,
                                            int index,
                                            ChatChoice chunk,
                                            boolean last) {
                                        chunks.add(chunk.getMessage().getContent());
                                        log.info(
                                                "chunk: (last={}) {} {}",
                                                last,
                                                chunk.getMessage().getRole(),
                                                chunk.getMessage().getContent());
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
        log.info("result: {}", chatCompletions);
        assertEquals(
                "A car is a vehicle",
                chatCompletions.getChoices().get(0).getMessage().getContent());
        assertEquals(3, chunks.size());
        assertEquals("A", chunks.get(0));
        assertEquals(" car is", chunks.get(1));
        assertEquals(" a vehicle", chunks.get(2));
    }
}
