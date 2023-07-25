package com.datastax.oss.sga.ai.agents.services.impl;

import com.datastax.oss.sga.ai.agents.services.ServiceProviderProvider;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.datastax.oss.streaming.ai.util.TransformFunctionUtil;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class VertexAIProvider implements ServiceProviderProvider {

    private static final String VERTEX_URL_TEMPLATE = "%s/v1/projects/%s/locations/%s/publishers/google/models/%s:predict";

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
            String model = (String) map.getOrDefault("model", "chat-bison");
            return new VertexAICompletionsService(model);
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> map) throws Exception {
            String model = (String) map.getOrDefault("model", "textembedding-gecko");
            return new VertexAIEmbeddingsService(model);
        }



        private <R, T> T executeVertexCall(R requestEmbeddings, Class<T> responseType, String model) throws IOException, InterruptedException {
            String finalUrl = VERTEX_URL_TEMPLATE
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
            return MAPPER.readValue(body, responseType);
        }

        @Override
        public void close() {
        }

        private class VertexAICompletionsService implements CompletionsService {
            private final String model;

            public VertexAICompletionsService(String model) {
                this.model = model;
            }

            @Override
            @SneakyThrows
            public ChatCompletions getChatCompletions(List<ChatMessage> list, Map<String, Object> additionalConfiguration) {
                // https://cloud.google.com/vertex-ai/docs/generative-ai/chat/chat-prompts
                CompletionRequest request = new CompletionRequest();
                CompletionRequest.Instance instance = new CompletionRequest.Instance();
                request.instances.add(instance);
                request.parameters = new HashMap<>();

                if (additionalConfiguration.containsKey("temperature")) {
                    request.parameters.put("temperature", additionalConfiguration.get("temperature"));
                }
                if (additionalConfiguration.containsKey("max-tokens")) {
                    request.parameters.put("maxOutputTokens", additionalConfiguration.get("max-tokens"));
                }
                if (additionalConfiguration.containsKey("topP")) {
                    request.parameters.put("topP", additionalConfiguration.get("topP"));
                }
                if (additionalConfiguration.containsKey("topK")) {
                    request.parameters.put("topK", additionalConfiguration.get("topK"));
                }

                instance.context = "";
                instance.examples = new ArrayList<>();
                instance.messages = list
                        .stream()
                        .map(m -> {
                            CompletionRequest.Message message = new CompletionRequest.Message();
                            message.content = m.getContent();
                            message.author = m.getRole();
                            return message;
                        }).collect(Collectors.toList());

                Predictions predictions = executeVertexCall(request, Predictions.class, model);
                ChatCompletions completions = new ChatCompletions();
                completions.setChoices(predictions.predictions.stream().map(p -> {
                    if (!p.candidates.isEmpty()) {
                        ChatChoice completion = new ChatChoice();
                        completion.setMessage(new ChatMessage(p.candidates.get(0).author)
                                .setContent(p.candidates.get(0).content));
                        return completion;
                    } else {
                        ChatChoice completion = new ChatChoice();
                        completion.setMessage(new ChatMessage("")
                                .setContent(""));
                        return completion;
                    }
                }).collect(Collectors.toList()));
                return completions;
            }


            @Data
            static class CompletionRequest {
                Map<String, Object> parameters;

                List<Instance> instances = new ArrayList<>();

                @Data
                static class Instance {
                    String context;
                    List<Example> examples = new ArrayList<>();
                    List<Message> messages = new ArrayList<>();
                }

                @Data
                static class Example {
                    Map<String, Object> input;
                    Map<String, Object> output;
                }

                @Data
                static class Message {
                    String author;
                    String content;
                }

            }

            @Data
            static class Predictions {

                List<Prediction> predictions;
                @Data
                static class Prediction {

                    List<Candidate> candidates = new ArrayList<>();

                    @Data
                    static class Candidate {
                        String author;
                        String content;
                    }

                }
            }

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
                Predictions predictions = executeVertexCall(requestEmbeddings, Predictions.class, model);
                return predictions
                        .predictions.stream().map(p -> p.embeddings.values).collect(Collectors.toList());
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

}
