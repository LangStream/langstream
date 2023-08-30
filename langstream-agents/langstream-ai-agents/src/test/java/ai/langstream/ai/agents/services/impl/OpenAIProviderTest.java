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

import ai.langstream.ai.agents.services.ServiceProviderProvider;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import java.util.List;
import java.util.Map;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
@WireMockTest
class OpenAIProviderTest {

    @Test
    void testStreamingCompletion(WireMockRuntimeInfo vmRuntimeInfo) throws Exception {
        ServiceProviderProvider provider = new OpenAIServiceProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "openai",
                                Map.of("provider", "azure", "access-key", "783fe7bc013149f2a197ce3a4ef54531", "url", "https://datastax-openai-dev.openai.azure.com")));

        CompletionsService service = implementation.getCompletionsService(Map.of());
        ChatCompletions chatCompletions =
                service.getChatCompletions(
                                List.of(new ChatMessage("user").setContent("What is a car?")),
                                new CompletionsService.StreamingChunksConsumer() {
                                    @Override
                                    public void consumeChunk(int index, ChatChoice chunk, boolean last) {
                                        log.info(
                                                "chunk: (last={}) {} {}",
                                                last,
                                                chunk.getMessage().getRole(),
                                                chunk.getMessage().getContent());
                                    }
                                },
                                Map.of("model", "gpt-35-turbo", "stream", true))
                        .get();
        log.info("result: {}", chatCompletions);
    }
}
