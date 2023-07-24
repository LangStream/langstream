package com.datastax.oss.sga.ai.agents.services.impl;

import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@WireMockTest
class HuggingAIProviderTest {

    @Test
    void testCallCompletionAPI(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(post("/models/bert-base-uncased")
                .willReturn(okJson("""
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
        ServiceProvider implementation = provider.createImplementation(
                Map.of("huggingface",
                Map.of("inference-url", wmRuntimeInfo.getHttpBaseUrl(),
                "access-key", "xxxx")));

        CompletionsService service = implementation.getCompletionsService(Map.of());
        String message = "The cop arrested the bad [MASK].";
        ChatCompletions chatCompletions = service.getChatCompletions(List.of(new ChatMessage("user").setContent(message)), Map.of("model", "bert-base-uncased"));
        log.info("result: {}", chatCompletions);
        assertEquals(2, chatCompletions.getChoices().size());
        assertEquals("answer 1", chatCompletions.getChoices().get(0).getMessage().getContent());
        assertEquals("answer 2", chatCompletions.getChoices().get(1).getMessage().getContent());
    }

}