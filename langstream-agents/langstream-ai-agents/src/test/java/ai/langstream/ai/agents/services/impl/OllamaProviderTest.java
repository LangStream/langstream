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

import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.Chunk;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class OllamaProviderTest {

    @Test
    void testCompletions(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {

        OllamaProvider provider = new OllamaProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of("ollama", Map.of("url", "http://localhost:11434/api/generate")),
                        MetricsReporter.DISABLED);

        CompletionsService service =
                implementation.getCompletionsService(Map.of("model", "llama2"));
        String result =
                service.getChatCompletions(
                                List.of(
                                        new ChatMessage("user")
                                                .setContent(
                                                        "Here is a story about llamas eating grass")),
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
                                    }
                                },
                                Map.of())
                        .get()
                        .getChoices()
                        .get(0)
                        .content();
        log.info("result: {}", result);
    }
}
