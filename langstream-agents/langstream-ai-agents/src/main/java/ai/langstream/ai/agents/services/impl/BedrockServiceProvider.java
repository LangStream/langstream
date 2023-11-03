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

import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.ai.agents.services.ServiceProviderProvider;
import ai.langstream.ai.agents.services.impl.bedrock.BaseInvokeModelRequest;
import ai.langstream.ai.agents.services.impl.bedrock.BedrockClient;
import ai.langstream.ai.agents.services.impl.bedrock.TitanEmbeddingsModel;
import ai.langstream.api.runner.code.MetricsReporter;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.completions.TextCompletionResult;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.services.ServiceProvider;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;

@Slf4j
public class BedrockServiceProvider implements ServiceProviderProvider {
    @Override
    public boolean supports(Map<String, Object> agentConfiguration) {
        return agentConfiguration.containsKey("bedrock");
    }

    @Override
    public ServiceProvider createImplementation(
            Map<String, Object> agentConfiguration, MetricsReporter metricsReporter) {
        Map<String, Object> config = (Map<String, Object>) agentConfiguration.get("bedrock");
        String accessKey = (String) config.get("access-key");
        String secretKey = (String) config.get("secret-key");
        String endpointOverride = (String) config.get("endpoint-override");
        String region = (String) config.getOrDefault("region", "us-east-1");
        return new BedrockService(
                () ->
                        new BedrockClient(
                                AwsBasicCredentials.create(accessKey, secretKey),
                                region,
                                endpointOverride));
    }

    private static class BedrockService implements ServiceProvider {

        private final BedrockClient client;

        public BedrockService(Supplier<BedrockClient> client) {
            this.client = client.get();
        }

        @Override
        public CompletionsService getCompletionsService(Map<String, Object> additionalConfiguration)
                throws Exception {
            return new BedrockCompletionsService(client);
        }

        @Override
        public EmbeddingsService getEmbeddingsService(Map<String, Object> additionalConfiguration)
                throws Exception {

            final String model =
                    (String)
                            additionalConfiguration.getOrDefault(
                                    "model", "amazon.titan-embed-text-v1");

            return texts -> {
                List<CompletableFuture<List<Double>>> all = new ArrayList<>();
                for (String text : texts) {
                    all.add(
                            client.invokeModel(
                                            TitanEmbeddingsModel.builder()
                                                    .modelId(model)
                                                    .inputText(text)
                                                    .build(),
                                            TitanEmbeddingsModel.ResponseBody.class)
                                    .thenApply(r -> r.embedding()));
                }
                CompletableFuture<Void> joinedPromise =
                        CompletableFuture.allOf(all.toArray(CompletableFuture[]::new));
                return joinedPromise.thenApply(
                        voit ->
                                all.stream()
                                        .map(CompletableFuture::join)
                                        .collect(Collectors.toList()));
            };
        }

        @Override
        public void close() {
            if (client != null) {
                client.close();
            }
        }
    }

    @AllArgsConstructor
    private static class BedrockCompletionsService implements CompletionsService {
        static final ObjectMapper MAPPER =
                new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        private final BedrockClient client;

        @Override
        public CompletableFuture<ChatCompletions> getChatCompletions(
                List<ChatMessage> message,
                StreamingChunksConsumer streamingChunksConsumer,
                Map<String, Object> options) {
            final List<String> prompt =
                    message.stream().map(ChatMessage::getContent).collect(Collectors.toList());
            return getTextCompletions(prompt, streamingChunksConsumer, options)
                    .thenApply(
                            r -> {
                                final ChatCompletions result = new ChatCompletions();
                                result.setChoices(
                                        List.of(new ChatChoice(new ChatMessage(null, r.text()))));
                                return result;
                            });
        }

        @Override
        public CompletableFuture<TextCompletionResult> getTextCompletions(
                List<String> prompt,
                StreamingChunksConsumer streamingChunksConsumer,
                Map<String, Object> agentConfiguration) {
            if (prompt.size() != 1) {
                throw new IllegalArgumentException(
                        "Bedrock models only support a single prompt for completions.");
            }
            final String model = (String) agentConfiguration.get("model");

            final BedrockOptions bedrockOptions =
                    MAPPER.convertValue(
                            agentConfiguration.getOrDefault("options", Map.of()),
                            BedrockOptions.class);

            final Map<String, Object> parameters = bedrockOptions.getRequestParameters();
            final String textCompletionExpression =
                    "${%s}".formatted(bedrockOptions.getResponseCompletionsExpression());
            Map<String, Object> body = new LinkedHashMap<>();
            body.put(bedrockOptions.getRequestPromptProperty(), prompt.get(0));
            if (parameters != null) {
                body.putAll(parameters);
            }

            final BaseInvokeModelRequest<Map> request =
                    new BaseInvokeModelRequest<>() {
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
                    .thenApply(
                            r -> {
                                final Map<String, Object> asMap = (Map<String, Object>) r;
                                final Map<String, Object> context =
                                        asMap.entrySet().stream()
                                                .collect(
                                                        Collectors.toMap(
                                                                e -> e.getKey(),
                                                                v -> v.getValue()));
                                final Object result =
                                        new JstlEvaluator(textCompletionExpression, Object.class)
                                                .evaluateRawContext(context);
                                if (result == null) {
                                    throw new IllegalStateException(
                                            "No result found in response (tried with expression "
                                                    + textCompletionExpression
                                                    + ", response was: "
                                                    + r
                                                    + ")");
                                }
                                String resultValue = result.toString();
                                if (resultValue.startsWith("\n")) {
                                    resultValue = resultValue.substring(1);
                                }
                                return new TextCompletionResult(resultValue, null);
                            });
        }

        @Data
        public static class BedrockOptions {
            @JsonProperty(value = "request-parameters")
            private Map<String, Object> requestParameters;

            @JsonProperty(value = "request-prompt-property")
            private String requestPromptProperty = "prompt";

            @JsonProperty(value = "response-completions-expression")
            private String responseCompletionsExpression;
        }
    }
}
