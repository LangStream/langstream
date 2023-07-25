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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@WireMockTest
class VertexAIProviderTest {

    @Test
    void testCallEmbeddings(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(post("/v1/projects/the-project/locations/us-central1/publishers/google/models/textembedding-gecko:predict")
                .willReturn(okJson(""" 
                   {
                      "predictions": [
                        {
                          "embeddings": {
                            "statistics": {
                              "truncated": false,
                              "token_count": 6
                            },
                            "values": [ 1.0, 5.4, 8.7]
                          }
                        }
                      ]
                    }
                """)));

        VertexAIProvider provider = new VertexAIProvider();
        ServiceProvider implementation = provider.createImplementation(
                Map.of("vertex",
                Map.of("url", wmRuntimeInfo.getHttpBaseUrl(),
                "project", "the-project",
                "region", "us-central1",
                "token", "xxxx")));

        EmbeddingsService embeddingsService = implementation.getEmbeddingsService(Map.of("model", "textembedding-gecko"));
        List<List<Double>> result = embeddingsService.computeEmbeddings(List.of("hello world"));
        log.info("result: {}", result);
        assertEquals(1, result.size());
        assertEquals(List.of(1.d, 5.4d, 8.7d), result.get(0));
    }

    @Test
    void testCallCompletion(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubFor(post("/v1/projects/the-project/locations/us-central1/publishers/google/models/chat-bison:predict")
                .willReturn(okJson(""" 
                   {
                     "predictions": [
                       {
                         "safetyAttributes": [
                           {
                             "blocked": false,
                             "scores": [],
                             "categories": []
                           }
                         ],
                         "citationMetadata": [
                           {
                             "citations": []
                           }
                         ],
                         "candidates": [
                           {
                             "author": "1",
                             "content": "A car is a wheeled, self-propelled motor vehicle used for transportation."
                           }
                         ]
                       }
                     ],
                     "metadata": {
                       "tokenMetadata": {
                         "inputTokenCount": {
                           "totalTokens": 5,
                           "totalBillableCharacters": 11
                         },
                         "outputTokenCount": {
                           "totalBillableCharacters": 63,
                           "totalTokens": 15
                         }
                       }
                     }
                   }
                """)));


        VertexAIProvider provider = new VertexAIProvider();
        ServiceProvider implementation = provider.createImplementation(
                Map.of("vertex",
                        Map.of("url", wmRuntimeInfo.getHttpBaseUrl(),
                                "project", "the-project",
                                "region", "us-central1",
                                "token", "xxxx")));
        CompletionsService service = implementation.getCompletionsService(Map.of("model", "chat-bison",
                "max-tokens", 3, "temperature", 0.5, "top-p", 1.0, "top-k", 3.0));
        ChatCompletions chatCompletions = service.getChatCompletions(
                List.of(new ChatMessage("user")
                        .setContent("What is a car?")), Map.of("max_tokens", 3));
        log.info("result: {}", chatCompletions);
        assertEquals("A car is a wheeled, self-propelled motor vehicle used for transportation.", chatCompletions.getChoices().get(0).getMessage().getContent());
    }


    @Test
    @Disabled
    void testCallEmbeddingsUsingServiceAccount() throws Exception {
        VertexAIProvider provider = new VertexAIProvider();
        ServiceProvider implementation = provider.createImplementation(
                Map.of("vertex",
                        Map.of("url", "https://us-central1-aiplatform.googleapis.com",
                                "project", "datastax-astrastreaming-dev",
                                "region", "us-central1",
                                "serviceAccountJson", "PUT-HERE-YOUR-JSON-KEY")));

        EmbeddingsService embeddingsService = implementation.getEmbeddingsService(Map.of("model", "textembedding-gecko"));
        List<List<Double>> result = embeddingsService.computeEmbeddings(List.of("hello world"));
        log.info("result: {}", result);
        assertEquals(1, result.size());
    }

}