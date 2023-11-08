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

import ai.langstream.api.runner.code.MetricsReporter;
import com.azure.ai.openai.OpenAIAsyncClient;
import com.azure.ai.openai.models.ChatCompletionsOptions;
import com.azure.ai.openai.models.ChatRole;
import com.azure.ai.openai.models.CompletionsFinishReason;
import com.azure.ai.openai.models.CompletionsLogProbabilityModel;
import com.azure.ai.openai.models.CompletionsOptions;
import com.azure.ai.openai.models.CompletionsUsage;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.Chunk;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.completions.TextCompletionResult;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
public class OpenAICompletionService implements CompletionsService {

    private final OpenAIAsyncClient client;

    private final MetricsReporter.Counter textTotalTokens;
    private final MetricsReporter.Counter textPromptTokens;
    private final MetricsReporter.Counter textCompletionTokens;
    private final MetricsReporter.Counter textNumCalls;
    private final MetricsReporter.Counter textNumErrors;

    private final MetricsReporter.Counter chatTotalTokens;
    private final MetricsReporter.Counter chatPromptTokens;
    private final MetricsReporter.Counter chatCompletionTokens;
    private final MetricsReporter.Counter chatNumCalls;
    private final MetricsReporter.Counter chatNumErrors;

    public OpenAICompletionService(OpenAIAsyncClient client, MetricsReporter metricsReporter) {
        this.client = client;

        this.chatTotalTokens =
                metricsReporter.counter(
                        "openai_chat_completions_total_tokens",
                        "Total number of tokens exchanged with OpenAI Chat Completions");
        this.chatPromptTokens =
                metricsReporter.counter(
                        "openai_chat_completions_prompt_tokens",
                        "Total number of prompt tokens sent to OpenAI Chat Completions");
        this.chatCompletionTokens =
                metricsReporter.counter(
                        "openai_chat_completions_completions_tokens",
                        "Total number of completions tokens received from OpenAI Chat Completions");

        this.chatNumCalls =
                metricsReporter.counter(
                        "openai_chat_completions_num_calls",
                        "Total number of calls to OpenAI Chat Completions");

        this.chatNumErrors =
                metricsReporter.counter(
                        "openai_chat_completions_num_errors",
                        "Total number of errors while calling OpenAI Chat Completions");

        this.textTotalTokens =
                metricsReporter.counter(
                        "openai_text_completions_total_tokens",
                        "Total number of tokens exchanged with OpenAI Text Completions");

        this.textCompletionTokens =
                metricsReporter.counter(
                        "openai_chat_completions_completions_tokens",
                        "Total number of completions tokens received from OpenAI Text Completions");

        this.textPromptTokens =
                metricsReporter.counter(
                        "openai_text_completions_prompt_tokens",
                        "Total number of prompt tokens sent to OpenAI Text Completions");

        this.textNumCalls =
                metricsReporter.counter(
                        "openai_text_completions_num_calls",
                        "Total number of calls to OpenAI Text Completions");

        this.textNumErrors =
                metricsReporter.counter(
                        "openai_text_completions_num_errors",
                        "Total number of errors while calling OpenAI Text Completions");
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
        chatNumCalls.count(1);
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

            flux.doOnError(
                            error -> {
                                log.error(
                                        "Internal error while processing the streaming response",
                                        error);
                                chatNumErrors.count(1);
                                finished.completeExceptionally(error);
                            })
                    .doOnNext(chatCompletionsConsumer)
                    .subscribe();

            return finished.thenApply(
                    ___ -> {
                        result.setChoices(
                                List.of(
                                        new ChatChoice(
                                                chatCompletionsConsumer
                                                        .buildTotalAnswerMessage())));
                        chatTotalTokens.count(chatCompletionsConsumer.getTotalTokens().intValue());
                        chatPromptTokens.count(
                                chatCompletionsConsumer.getPromptTokens().intValue());
                        chatCompletionTokens.count(
                                chatCompletionsConsumer.getCompletionTokens().intValue());
                        return result;
                    });
        } else {
            CompletableFuture<ChatCompletions> resultHandle =
                    client.getChatCompletions((String) options.get("model"), chatCompletionsOptions)
                            .toFuture()
                            .thenApply(
                                    chatCompletions -> {
                                        result.setChoices(
                                                chatCompletions.getChoices().stream()
                                                        .map(c -> new ChatChoice(convertMessage(c)))
                                                        .collect(Collectors.toList()));
                                        CompletionsUsage usage = chatCompletions.getUsage();
                                        if (usage != null) {
                                            chatTotalTokens.count(usage.getTotalTokens());
                                            chatPromptTokens.count(usage.getPromptTokens());
                                            chatCompletionTokens.count(usage.getCompletionTokens());
                                        }
                                        return result;
                                    });

            resultHandle.exceptionally(
                    error -> {
                        chatNumErrors.count(1);
                        return null;
                    });
            return resultHandle;
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

        @Getter private final AtomicInteger totalTokens = new AtomicInteger();
        @Getter private final AtomicInteger promptTokens = new AtomicInteger();

        @Getter private final AtomicInteger completionTokens = new AtomicInteger();

        private final StringWriter writer = new StringWriter();
        private final AtomicInteger numberOfChunks = new AtomicInteger();
        private final int minChunksPerMessage;

        private final AtomicInteger currentChunkSize = new AtomicInteger(1);
        private final AtomicInteger index = new AtomicInteger();

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
            if (chatCompletions.getUsage() != null) {
                totalTokens.addAndGet(chatCompletions.getUsage().getTotalTokens());
                completionTokens.addAndGet(chatCompletions.getUsage().getCompletionTokens());
                promptTokens.addAndGet(chatCompletions.getUsage().getPromptTokens());
            }

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

    @Override
    public CompletableFuture<TextCompletionResult> getTextCompletions(
            List<String> prompt,
            StreamingChunksConsumer streamingChunksConsumer,
            Map<String, Object> options) {
        int minChunksPerMessage = getInteger("min-chunks-per-message", 20, options);
        CompletionsOptions completionsOptions =
                new CompletionsOptions(prompt)
                        .setMaxTokens(getInteger("max-tokens", null, options))
                        .setTemperature(getDouble("temperature", null, options))
                        .setTopP(getDouble("top-p", null, options))
                        .setLogitBias((Map<String, Integer>) options.get("logit-bias"))
                        .setStream(getBoolean("stream", true, options))
                        .setUser((String) options.get("user"))
                        .setStop((List<String>) options.get("stop"))
                        .setLogprobs(getInteger("logprobs", null, options))
                        .setPresencePenalty(getDouble("presence-penalty", null, options))
                        .setFrequencyPenalty(getDouble("frequency-penalty", null, options));

        // this is the default behavior, as it is async
        // it works even if the streamingChunksConsumer is null
        final String model = (String) options.get("model");
        textNumCalls.count(1);
        if (completionsOptions.isStream()) {
            CompletableFuture<?> finished = new CompletableFuture<>();
            Flux<com.azure.ai.openai.models.Completions> flux =
                    client.getCompletionsStream(model, completionsOptions);

            TextCompletionsConsumer textCompletionsConsumer =
                    new TextCompletionsConsumer(
                            streamingChunksConsumer, minChunksPerMessage, finished);

            flux.doOnError(
                            error -> {
                                log.error(
                                        "Internal error while processing the streaming response",
                                        error);
                                textNumErrors.count(1);
                                finished.completeExceptionally(error);
                            })
                    .doOnNext(textCompletionsConsumer)
                    .subscribe();

            return finished.thenApply(
                    ___ -> {
                        TextCompletionResult.LogProbInformation logProbs =
                                new TextCompletionResult.LogProbInformation(
                                        textCompletionsConsumer.logProbsTokens,
                                        textCompletionsConsumer.logProbsTokenLogProbabilities);
                        textTotalTokens.count(textCompletionsConsumer.getTotalTokens().intValue());
                        textPromptTokens.count(
                                textCompletionsConsumer.getPromptTokens().intValue());
                        textCompletionTokens.count(
                                textCompletionsConsumer.getCompletionTokens().intValue());
                        return new TextCompletionResult(
                                textCompletionsConsumer.totalAnswer.toString(), logProbs);
                    });
        } else {
            CompletableFuture<TextCompletionResult> resultHandle =
                    client.getCompletions(model, completionsOptions)
                            .toFuture()
                            .thenApply(
                                    completions -> {
                                        CompletionsUsage usage = completions.getUsage();
                                        if (usage != null) {
                                            textTotalTokens.count(usage.getTotalTokens());
                                            textPromptTokens.count(usage.getPromptTokens());
                                            textCompletionTokens.count(usage.getCompletionTokens());
                                        }
                                        final String text =
                                                completions.getChoices().get(0).getText();
                                        CompletionsLogProbabilityModel logprobs =
                                                completions.getChoices().get(0).getLogprobs();
                                        TextCompletionResult.LogProbInformation logProbs =
                                                completions.getChoices().get(0).getLogprobs()
                                                                != null
                                                        ? new TextCompletionResult
                                                                .LogProbInformation(
                                                                logprobs.getTokens(),
                                                                logprobs.getTokenLogProbabilities())
                                                        : new TextCompletionResult
                                                                .LogProbInformation(null, null);
                                        return new TextCompletionResult(text, logProbs);
                                    });
            resultHandle.exceptionally(
                    error -> {
                        textNumErrors.count(1);
                        return null;
                    });
            return resultHandle;
        }
    }

    private static class TextCompletionsConsumer
            implements Consumer<com.azure.ai.openai.models.Completions> {
        private final StreamingChunksConsumer streamingChunksConsumer;
        private final CompletableFuture<?> finished;

        private final StringWriter totalAnswer = new StringWriter();

        @Getter private final AtomicInteger totalTokens = new AtomicInteger();
        @Getter private final AtomicInteger promptTokens = new AtomicInteger();

        @Getter private final AtomicInteger completionTokens = new AtomicInteger();

        private final StringWriter writer = new StringWriter();
        private final AtomicInteger numberOfChunks = new AtomicInteger();
        private final int minChunksPerMessage;
        public List<String> logProbsTokens = new ArrayList<>();
        public List<Double> logProbsTokenLogProbabilities = new ArrayList<>();

        private final AtomicInteger currentChunkSize = new AtomicInteger(1);
        private final AtomicInteger index = new AtomicInteger();

        private final AtomicBoolean firstChunk = new AtomicBoolean(true);

        public TextCompletionsConsumer(
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
        @SneakyThrows
        public synchronized void accept(com.azure.ai.openai.models.Completions completions) {
            List<com.azure.ai.openai.models.Choice> choices = completions.getChoices();
            String answerId = completions.getId();
            if (completions.getUsage() != null) {
                totalTokens.addAndGet(completions.getUsage().getTotalTokens());
                completionTokens.addAndGet(completions.getUsage().getCompletionTokens());
                promptTokens.addAndGet(completions.getUsage().getPromptTokens());
            }
            if (!choices.isEmpty()) {
                com.azure.ai.openai.models.Choice first = choices.get(0);

                CompletionsFinishReason finishReason = first.getFinishReason();
                boolean last = finishReason != null;
                final String content = first.getText();
                if (content == null) {
                    return;
                }
                if (firstChunk.compareAndSet(true, false)) {
                    // Some models return two line break at the beginning of the first response,
                    // even though this is not documented
                    // https://community.openai.com/t/output-starts-often-with-linebreaks/36333/4
                    if (content.isBlank()) {
                        return;
                    }
                }
                writer.write(content);
                totalAnswer.write(content);

                CompletionsLogProbabilityModel logprobs = first.getLogprobs();
                if (logprobs != null) {
                    logProbsTokens.addAll(logprobs.getTokens());
                    logProbsTokenLogProbabilities.addAll(logprobs.getTokenLogProbabilities());
                }

                numberOfChunks.incrementAndGet();

                // start from 1 chunk, then double the size until we reach the minChunksPerMessage
                // this gives better latencies for the first message
                int currentMinChunksPerMessage = currentChunkSize.get();

                if (numberOfChunks.get() >= currentMinChunksPerMessage || last) {
                    currentChunkSize.set(
                            Math.min(currentMinChunksPerMessage * 2, minChunksPerMessage));
                    final String chunkContent = writer.toString();
                    final Chunk chunk = () -> chunkContent;
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
    }
}
