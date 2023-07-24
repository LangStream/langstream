package com.datastax.oss.sga.ai.agents.services.impl;

import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import lombok.extern.slf4j.Slf4j;
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

        WireMock wireMock = wmRuntimeInfo.getWireMock();

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

}