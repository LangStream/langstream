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

import static ai.langstream.api.util.ConfigurationUtils.getBoolean;
import static ai.langstream.api.util.ConfigurationUtils.getDouble;
import static ai.langstream.api.util.ConfigurationUtils.getInteger;

import com.azure.ai.openai.OpenAIAsyncClient;
import com.azure.ai.openai.models.ChatCompletionsOptions;
import com.azure.ai.openai.models.ChatRole;
import com.azure.ai.openai.models.CompletionsFinishReason;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
public class OpenAICompletionService implements CompletionsService {

    private OpenAIAsyncClient client;

    public OpenAICompletionService(OpenAIAsyncClient client) {
        this.client = client;
    }

    @Override
    @SneakyThrows
    public CompletableFuture<ChatCompletions> getChatCompletions(
            List<ChatMessage> messages,
            StreamingChunksConsumer streamingChunksConsumer,
            Map<String, Object> options) {
        int minChunksPerMessage = getInteger("min-chunks-per-message", 20, options);
        ChatCompletionsOptions chatCompletionsOptions =
                new ChatCompletionsOptions(
                                messages.stream()
                                        .map(
                                                message ->
                                                        new com.azure.ai.openai.models.ChatMessage(
                                                                ChatRole.fromString(
                                                                        message.getRole()),
                                                                message.getContent()))
                                        .collect(Collectors.toList()))
                        .setMaxTokens(getInteger("max-tokens", null, options))
                        .setTemperature(getDouble("temperature", null, options))
                        .setTopP(getDouble("top-p", null, options))
                        .setLogitBias((Map<String, Integer>) options.get("logit-bias"))
                        .setStream(getBoolean("stream", true, options))
                        .setUser((String) options.get("user"))
                        .setStop((List<String>) options.get("stop"))
                        .setPresencePenalty(getDouble("presence-penalty", null, options))
                        .setFrequencyPenalty(getDouble("frequency-penalty", null, options));

        ChatCompletions result = new ChatCompletions();
        // this is the default behavior, as it is async
        // it works even if the streamingChunksConsumer is null
        if (chatCompletionsOptions.isStream()) {
            CompletableFuture<?> finished = new CompletableFuture<>();
            Flux<com.azure.ai.openai.models.ChatCompletions> flux =
                    client.getChatCompletionsStream(
                            (String) options.get("model"), chatCompletionsOptions);

            ChatCompletionsConsumer chatCompletionsConsumer =
                    new ChatCompletionsConsumer(
                            streamingChunksConsumer, minChunksPerMessage, finished);

            flux.subscribe(chatCompletionsConsumer);

            flux.doOnError(
                    error -> {
                        log.error("Internal error while processing the streaming response", error);
                        finished.completeExceptionally(error);
                    });

            return finished.thenApply(
                    ___ -> {
                        result.setChoices(
                                List.of(
                                        new ChatChoice(
                                                chatCompletionsConsumer
                                                        .buildTotalAnswerMessage())));
                        return result;
                    });
        } else {
            com.azure.ai.openai.models.ChatCompletions chatCompletions =
                    client.getChatCompletions((String) options.get("model"), chatCompletionsOptions)
                            .toFuture()
                            .get();
            result.setChoices(
                    chatCompletions.getChoices().stream()
                            .map(c -> new ChatChoice(convertMessage(c)))
                            .collect(Collectors.toList()));
            return CompletableFuture.completedFuture(result);
        }
    }

    private static ChatMessage convertMessage(com.azure.ai.openai.models.ChatChoice c) {
        com.azure.ai.openai.models.ChatMessage message = c.getMessage();
        if (message == null) {
            message = c.getDelta();
        }
        return new ChatMessage(
                message.getRole() != null ? message.getRole().toString() : null,
                message.getContent());
    }

    private static class ChatCompletionsConsumer
            implements Consumer<com.azure.ai.openai.models.ChatCompletions> {
        private final StreamingChunksConsumer streamingChunksConsumer;
        private final CompletableFuture<?> finished;

        private final AtomicReference<String> role = new AtomicReference<>();
        private final StringWriter totalAnswer = new StringWriter();

        private final StringWriter writer = new StringWriter();
        private final AtomicInteger numberOfChunks = new AtomicInteger();
        private final int minChunksPerMessage;

        private AtomicInteger currentChunkSize = new AtomicInteger(1);
        private AtomicInteger index = new AtomicInteger();

        public ChatCompletionsConsumer(
                StreamingChunksConsumer streamingChunksConsumer,
                int minChunksPerMessage,
                CompletableFuture<?> finished) {
            this.minChunksPerMessage = minChunksPerMessage;
            this.streamingChunksConsumer =
                    streamingChunksConsumer != null
                            ? streamingChunksConsumer
                            : (answerId, index, chunk, last) -> {};
            this.finished = finished;
        }

        @Override
        public synchronized void accept(
                com.azure.ai.openai.models.ChatCompletions chatCompletions) {
            List<com.azure.ai.openai.models.ChatChoice> choices = chatCompletions.getChoices();
            String answerId = chatCompletions.getId();
            if (!choices.isEmpty()) {
                com.azure.ai.openai.models.ChatChoice first = choices.get(0);
                CompletionsFinishReason finishReason = first.getFinishReason();
                boolean last = finishReason != null;
                ChatChoice converted = new ChatChoice(convertMessage(first));
                ChatMessage message = converted.getMessage();
                if (message == null) {
                    return;
                }

                // the "role" is set only on the first message
                if (message.getRole() != null) {
                    role.set(message.getRole());
                } else {
                    message.setRole(role.get());
                }
                if (message.getContent() != null && !message.getContent().isEmpty()) {
                    writer.write(message.getContent());
                    totalAnswer.write(message.getContent());
                    numberOfChunks.incrementAndGet();
                }

                // start from 1 chunk, then double the size until we reach the minChunksPerMessage
                // this gives better latencies for the first message
                int currentMinChunksPerMessage = currentChunkSize.get();

                if (numberOfChunks.get() >= currentMinChunksPerMessage || last) {
                    currentChunkSize.set(
                            Math.min(currentMinChunksPerMessage * 2, minChunksPerMessage));
                    ChatChoice chunk =
                            new ChatChoice(new ChatMessage(role.get(), writer.toString()));
                    streamingChunksConsumer.consumeChunk(
                            answerId, index.incrementAndGet(), chunk, last);
                    writer.getBuffer().setLength(0);
                    numberOfChunks.set(0);
                }
                if (last) {
                    finished.complete(null);
                }
            }
        }

        public ChatMessage buildTotalAnswerMessage() {
            return new ChatMessage(role.get(), totalAnswer.toString());
        }
    }
}
