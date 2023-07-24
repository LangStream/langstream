package com.datastax.oss.sga.ai.agents.services.impl;

import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class VertexAIProviderTest {

    @Test
    @Disabled
    void testCallEmbeddings() throws Exception {
        VertexAIProvider provider = new VertexAIProvider();
        ServiceProvider implementation = provider.createImplementation(
                Map.of("vertex",
                Map.of("url", "https://us-central1-aiplatform.googleapis.com",
                "project", "datastax-astrastreaming-dev",
                "region", "us-central1",
                "token", "xxxx")));

        EmbeddingsService embeddingsService = implementation.getEmbeddingsService(Map.of("model", "textembedding-gecko"));
        List<List<Double>> result = embeddingsService.computeEmbeddings(List.of("hello world"));
        log.info("result: {}", result);
    }

}