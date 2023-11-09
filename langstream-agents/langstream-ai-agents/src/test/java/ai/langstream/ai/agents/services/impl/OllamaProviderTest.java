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

import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.Chunk;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
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
class OllamaProviderTest {

    @Test
    void testCompletions(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {

        stubFor(
                post("/api/generate")
                        .willReturn(
                                ok(
                                        """
                        {"model":"llama2","created_at":"2023-11-09T13:48:51.788062Z","response":"one","done":false}
                        {"model":"llama2","created_at":"2023-11-09T13:48:51.788062Z","response":" two","done":false}
                        {"model":"llama2","created_at":"2023-11-09T13:48:51.788062Z","response":" three","done":true}
                      """)));
        OllamaProvider provider = new OllamaProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of("ollama", Map.of("url", wmRuntimeInfo.getHttpBaseUrl())),
                        MetricsReporter.DISABLED);

        List<String> chunks = new CopyOnWriteArrayList<>();
        CompletionsService service =
                implementation.getCompletionsService(Map.of("model", "llama2"));
        String result =
                service.getChatCompletions(
                                List.of(
                                        new ChatMessage("user")
                                                .setContent("Tell me three numberss")),
                                new CompletionsService.StreamingChunksConsumer() {
                                    @Override
                                    public void consumeChunk(
                                            String answerId, int index, Chunk chunk, boolean last) {
                                        log.info(
                                                "answerId: {}, index: {}, chunk: {}, last: {}",
                                                answerId,
                                                index,
                                                chunk,
                                                last);
                                        chunks.add(chunk.content());
                                    }
                                },
                                Map.of())
                        .get()
                        .getChoices()
                        .get(0)
                        .content();
        log.info("result: {}", result);
        assertEquals("one two three", result);
        assertEquals(List.of("one", " two three"), chunks);
    }

    @Test
    void testEmbeddings(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(
                post("/api/embeddings")
                        .willReturn(
                                ok(
                                        """
                        {"embedding":[-0.9004754424095154,1.2847540378570557,1.1102418899536133,-0.18884147703647614]}

                      """)));

        OllamaProvider provider = new OllamaProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of("ollama", Map.of("url", wmRuntimeInfo.getHttpBaseUrl())),
                        MetricsReporter.DISABLED);

        EmbeddingsService service = implementation.getEmbeddingsService(Map.of("model", "llama2"));

        List<List<Double>> result = service.computeEmbeddings(List.of("test")).get();
        log.info("result: {}", result);
        assertEquals(
                List.of(
                        List.of(
                                -0.9004754424095154,
                                1.2847540378570557,
                                1.1102418899536133,
                                -0.18884147703647614)),
                result);
    }
}
