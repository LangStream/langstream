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

import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
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
class HuggingAIProviderTest {

    @Test
    void testCallCompletionAPI(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(
                post("/models/bert-base-uncased")
                        .willReturn(
                                okJson(
                                        """
                      [
                        {
                          "token_str": "foo",
                          "sequence": "answer 1",
                          "score": 10
                        },{
                          "token_str": "foo2",
                          "sequence": "answer 2",
                          "score": 5
                        }
                      ]
                """)));
        HuggingFaceProvider provider = new HuggingFaceProvider();
        ServiceProvider implementation =
                provider.createImplementation(
                        Map.of(
                                "huggingface",
                                Map.of(
                                        "inference-url",
                                        wmRuntimeInfo.getHttpBaseUrl(),
                                        "access-key",
                                        "xxxx")),
                        MetricsReporter.DISABLED);

        CompletionsService service = implementation.getCompletionsService(Map.of());
        String message = "The cop arrested the bad [MASK].";
        ChatCompletions chatCompletions =
                service.getChatCompletions(
                                List.of(new ChatMessage("user").setContent(message)),
                                null,
                                Map.of("model", "bert-base-uncased"))
                        .get();
        log.info("result: {}", chatCompletions);
        assertEquals(2, chatCompletions.getChoices().size());
        assertEquals("answer 1", chatCompletions.getChoices().get(0).getMessage().getContent());
        assertEquals("answer 2", chatCompletions.getChoices().get(1).getMessage().getContent());
    }
}
