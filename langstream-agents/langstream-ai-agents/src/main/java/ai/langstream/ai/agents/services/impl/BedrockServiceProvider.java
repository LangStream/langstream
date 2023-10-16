package ai.langstream.ai.agents.services.impl;

import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.ai.agents.services.ServiceProviderProvider;
import ai.langstream.ai.agents.services.impl.bedrock.BaseInvokeModelRequest;
import ai.langstream.ai.agents.services.impl.bedrock.BedrockClient;
import ai.langstream.ai.agents.services.impl.bedrock.TitanEmbeddingsModel;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.completions.TextCompletionResult;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.processing.Completions;
import lombok.AllArgsConstructor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

public class BedrockServiceProvider implements ServiceProviderProvider {
    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("bedrock");
    }

    @Override
    public ServiceProvider createImplementation(Map<String, Object> agentConfiguration) {
        Map<String, Object> config = (Map<String, Object>) agentConfiguration.get("bedrock");
        String accessKey = (String) config.get("access-key");
        String secretKey = (String) config.get("secret-key");
        String region = (String) config.get("region");
        return new BedrockService(new BedrockClient(AwsBasicCredentials.create(accessKey, secretKey), region));
    }


    private static class BedrockService implements ServiceProvider {


        private final BedrockClient client;

        public BedrockService(BedrockClient client) {
            this.client = client;
        }

        @Override
        public CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration) throws Exception {
            return new BedrockCompletionsService(client);
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration) throws Exception {

            final String model = (String) additionalConfiguration.getOrDefault("model", "amazon.titan-embed-text-v1");

            return new EmbeddingsService() {
                @Override
                public CompletableFuture<List<List<Double>>> computeEmbeddings(List<String> texts) {

                    List<CompletableFuture<List<Double>>> all = new ArrayList<>();
                    for (String text : texts) {
                        all.add(client.invokeModel(TitanEmbeddingsModel.builder()
                                .modelId(model)
                                .inputText(text).build(), TitanEmbeddingsModel.ResponseBody.class)
                                .thenApply(r -> r.embedding())
                        );
                    }
                    CompletableFuture<Void> joinedPromise = CompletableFuture.allOf(all.toArray(CompletableFuture[]::new));
                    return joinedPromise.thenApply(voit -> all.stream().map(CompletableFuture::join).collect(
                            Collectors.toList()));
                }
            };
        }

        @Override
        public void close() {

        }
    }

    @AllArgsConstructor
    private static class BedrockCompletionsService implements CompletionsService {
        private final BedrockClient client;


        @Override
        public CompletableFuture<ChatCompletions> getChatCompletions(List<ChatMessage> message,
                                                                     StreamingChunksConsumer streamingChunksConsumer,
                                                                     Map<String, Object> options) {

            throw new UnsupportedOperationException("Chat completions not supported for Bedrock, please use text completions instead");
        }

        @Override
        public CompletableFuture<TextCompletionResult> getTextCompletions(List<String> prompt,
                                                                          StreamingChunksConsumer streamingChunksConsumer,
                                                                          Map<String, Object> agentConfiguration) {
            if (prompt.size() != 1) {
                throw new IllegalArgumentException(
                        "Bedrock models only support a single prompt for text completions.");
            }
            final String model = (String) agentConfiguration.get("model");
            final Map<String, Object> options = (Map<String, Object>) agentConfiguration.getOrDefault("options", Map.of());
            final Map<String, Object> parameters = (Map<String, Object>) options.getOrDefault("parameters", Map.of());
            final String promptField = (String) options.getOrDefault("prompt-parameter", "prompt");
            final String textCompletionExpression = (String) options.getOrDefault("text-completion-expression", null);
            Map<String, Object> body = new HashMap<>();
            body.put(promptField, prompt.get(0));
            body.putAll(parameters);

            final BaseInvokeModelRequest<Map> request = new BaseInvokeModelRequest<>() {
                @Override
                public Map getBodyObject() {
                    return body;
                }

                @Override
                public String getModelId() {
                    return model;
                }
            };

            return client.invokeModel(request, Map.class)
                    .thenApply(r -> {
                        System.out.println("got " + r);
                        final Map<String, Object> asMap = (Map<String, Object>) r;
                        final Map<String, Object> context =
                                asMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), v -> v.getValue()));
                        final Object result =
                                new JstlEvaluator(textCompletionExpression, String.class).evaluateRawContext(context);
                        if (result == null) {
                            throw new IllegalStateException("No result found in response (tried with expression " + textCompletionExpression + ", response was: " + r + ")");
                        }
                        return new TextCompletionResult(result.toString(), null);
                    });
        }
    }
}
