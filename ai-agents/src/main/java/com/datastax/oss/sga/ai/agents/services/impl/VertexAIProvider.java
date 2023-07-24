package com.datastax.oss.sga.ai.agents.services.impl;

import com.datastax.oss.sga.ai.agents.services.ServiceProviderProvider;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class VertexAIProvider implements ServiceProviderProvider {

    private static final String URL_TEMPLATE = "%s/v1/projects/%s/locations/%s/publishers/google/models/%s:predict";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("vertex");
    }

    @Override
    public ServiceProvider createImplementation(Map<String, Object> agentConfiguration) {

        Map<String, Object> config = (Map<String, Object>) agentConfiguration.get("vertex");
        String token = (String) config.get("token");
        String url = (String) config.get("url");
        String project = (String) config.get("project");
        String region = (String) config.get("region");


        return new VertexAIServiceProvider(url, project, region, token);
    }

    private static class VertexAIServiceProvider implements ServiceProvider {

        final HttpClient httpClient;
        private final String url;
        private final String project;
        private final String region;


        private final String token;

        public VertexAIServiceProvider(String url, String project, String region, String token) {
            if (url == null || url.isEmpty()) {
                url = "https://" + region + "-aiplatform.googleapis.com";
            }
            this.url = url;

            this.project = project;
            this.region = region;
            this.token = token;
            this.httpClient = HttpClient.newHttpClient();
        }


        @Override
        public CompletionsService getCompletionsService(Map<String, Object> map) throws Exception {
            throw new UnsupportedOperationException("CompletionsService not implemented for VertexAI");
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> map) throws Exception {
            String model = (String) map.getOrDefault("model", "textembedding-gecko");
            return new VertexAIEmbeddingsService(model);
        }

        @Override
        public void close() {
        }

        private class VertexAIEmbeddingsService implements EmbeddingsService {
            private final String model;

            public VertexAIEmbeddingsService(String model) {
                this.model = model;
            }

            @Override
            @SneakyThrows
            public List<List<Double>> computeEmbeddings(List<String> list) {
                // https://cloud.google.com/vertex-ai/docs/generative-ai/embeddings/get-text-embeddings#generative-ai-get-text-embedding-drest

                RequestEmbeddings requestEmbeddings = new RequestEmbeddings(list);

                String finalUrl = URL_TEMPLATE
                        .formatted(url, project, region, model);
                String request = MAPPER.writeValueAsString(requestEmbeddings);
                log.info("URL: {}", finalUrl);
                log.info("Request: {}", request);
                HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                        .uri(URI.create(finalUrl))
                        .header("Authorization", "Bearer " + token)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(request))
                        .build(), HttpResponse.BodyHandlers.ofString());

                String body = response.body();
                log.info("Response: {}", body);

                Predictions predictions = MAPPER.readValue(body, Predictions.class);
                return predictions
                        .predictions.stream().map(p -> p.embeddings.values).collect(Collectors.toList());
            }
        }
    }

    @Data
    static class RequestEmbeddings {
        public RequestEmbeddings(List<String> instances) {
            this.instances = instances.stream().map(Instance::new).collect(Collectors.toList());
        }

        @Data
        @AllArgsConstructor
        static class Instance {
            String content;
        }
        final List<Instance> instances;
    }

    @Data
    static class Predictions {

        List<Prediction> predictions;
        @Data
        static class Prediction {

            Embeddings embeddings;

            @Data
            static class Embeddings {
                List<Double> values;
            }

        }
    }
}
