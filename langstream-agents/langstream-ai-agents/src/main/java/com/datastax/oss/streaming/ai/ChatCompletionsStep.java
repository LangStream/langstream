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
package com.datastax.oss.streaming.ai;

import static com.datastax.oss.streaming.ai.util.TransformFunctionUtil.convertToMap;

import ai.langstream.api.runner.code.Record;
import com.azure.ai.openai.models.ChatCompletionsOptions;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.model.JsonRecord;
import com.datastax.oss.streaming.ai.model.config.ChatCompletionsConfig;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

@Slf4j
public class ChatCompletionsStep implements TransformStep {

    private final CompletionsService completionsService;

    private final ChatCompletionsConfig config;

    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();

    private final Map<ChatMessage, Template> messageTemplates = new ConcurrentHashMap<>();
    private final StreamingAnswersConsumerFactory streamingAnswersConsumerFactory;

    private StreamingAnswersConsumer streamingAnswersConsumer;

    public interface StreamingAnswersConsumerFactory {
        StreamingAnswersConsumer create(String topicName);
    }

    public interface StreamingAnswersConsumer {
        void streamAnswerChunk(int index, String message, boolean last, TransformContext outputMessage);
    }

    // for tests
    public ChatCompletionsStep(
            CompletionsService completionsService,
            ChatCompletionsConfig config) {
        this(completionsService, (topicName) -> {
            return new StreamingAnswersConsumer() {
                @Override
                public void streamAnswerChunk(int index, String message, boolean last, TransformContext outputMessage) {
                    log.info("index: {}, message: {}, last: {}", index, message, last);
                }
            };
        }, config);
    }

    public ChatCompletionsStep(
            CompletionsService completionsService,
            StreamingAnswersConsumerFactory streamingAnswersConsumerFactory,
            ChatCompletionsConfig config) {
        this.streamingAnswersConsumerFactory = streamingAnswersConsumerFactory;
        this.completionsService = completionsService;
        this.config = config;
        this.streamingAnswersConsumer = (index, message, last, record) -> {};
        config.getMessages()
                .forEach(
                        chatMessage ->
                                messageTemplates.put(
                                        chatMessage,
                                        Mustache.compiler().compile(chatMessage.getContent())));
    }

    @Override
    public void init() throws Exception {
        if (config.getStreamToTopic() != null && !config.getStreamToTopic().isEmpty()) {
            log.info("Streaming answers to topic {}", config.getStreamToTopic());
            this.streamingAnswersConsumer = streamingAnswersConsumerFactory
                    .create(config.getStreamToTopic());
        }
    }

    @Override
    public CompletableFuture<?> processAsync(TransformContext transformContext) {
        JsonRecord jsonRecord = transformContext.toJsonRecord();

        List<ChatMessage> messages =
                config.getMessages().stream()
                        .map(
                                message ->
                                        new ChatMessage(message.getRole())
                                                .setContent(
                                                        messageTemplates
                                                                .get(message)
                                                                .execute(jsonRecord)))
                        .collect(Collectors.toList());

        ChatCompletionsOptions chatCompletionsOptions =
                new ChatCompletionsOptions(List.of())
                        .setMaxTokens(config.getMaxTokens())
                        .setTemperature(config.getTemperature())
                        .setTopP(config.getTopP())
                        .setLogitBias(config.getLogitBias())
                        .setStream(config.isStream())
                        .setUser(config.getUser())
                        .setStop(config.getStop())
                        .setPresencePenalty(config.getPresencePenalty())
                        .setFrequencyPenalty(config.getFrequencyPenalty());
        Map<String, Object> options = convertToMap(chatCompletionsOptions);
        options.put("model", config.getModel());
        options.remove("messages");

        CompletableFuture<ChatCompletions> chatCompletionsHandle =
                completionsService.getChatCompletions(messages, new CompletionsService.StreamingChunksConsumer() {
                    @Override
                    public void consumeChunk(int index, ChatChoice chunk, boolean last) {
                        streamingAnswersConsumer.streamAnswerChunk(index,
                                chunk.getMessage().getContent(),
                                last,
                                transformContext);
                    }
                }, options);


        return chatCompletionsHandle.thenApply(chatCompletions -> {

            String content = chatCompletions.getChoices().get(0).getMessage().getContent();
            String fieldName = config.getFieldName();
            transformContext.setResultField(
                    content,
                    fieldName,
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                    avroKeySchemaCache,
                    avroValueSchemaCache);

            String logField = config.getLogField();
            if (logField != null && !logField.isEmpty()) {
                Map<String, Object> logMap = new HashMap<>();
                logMap.put("model", config.getModel());
                logMap.put("options", options);
                logMap.put("messages", messages);
                transformContext.setResultField(
                        TransformContext.toJson(logMap),
                        logField,
                        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                        avroKeySchemaCache,
                        avroValueSchemaCache);
            }
            return null;
        });
    }
}
