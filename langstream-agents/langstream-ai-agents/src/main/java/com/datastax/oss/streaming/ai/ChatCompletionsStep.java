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

import ai.langstream.ai.agents.commons.JsonRecord;
import ai.langstream.ai.agents.commons.MutableRecord;
import com.datastax.oss.streaming.ai.completions.ChatChoice;
import com.datastax.oss.streaming.ai.completions.ChatCompletions;
import com.datastax.oss.streaming.ai.completions.ChatMessage;
import com.datastax.oss.streaming.ai.completions.Chunk;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.model.config.ChatCompletionsConfig;
import com.datastax.oss.streaming.ai.streaming.StreamingAnswersConsumer;
import com.datastax.oss.streaming.ai.streaming.StreamingAnswersConsumerFactory;
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

    // for tests
    public ChatCompletionsStep(
            CompletionsService completionsService, ChatCompletionsConfig config) {
        this(
                completionsService,
                (topicName) -> {
                    return new StreamingAnswersConsumer() {
                        @Override
                        public void streamAnswerChunk(
                                int index,
                                String message,
                                boolean last,
                                MutableRecord outputMessage) {
                            log.info("index: {}, message: {}, last: {}", index, message, last);
                        }
                    };
                },
                config);
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
                                        Mustache.compiler()
                                                .compile(
                                                        MustacheCompatibilityUtils
                                                                .handleLegacyTemplate(
                                                                        chatMessage
                                                                                .getContent()))));
    }

    @Override
    public void start() throws Exception {
        if (config.getStreamToTopic() != null && !config.getStreamToTopic().isEmpty()) {
            log.info("Streaming answers to topic {}", config.getStreamToTopic());
            this.streamingAnswersConsumer =
                    streamingAnswersConsumerFactory.create(config.getStreamToTopic());
        }
    }

    @Override
    public void close() throws Exception {
        if (this.streamingAnswersConsumer != null) {
            this.streamingAnswersConsumer.close();
        }
    }

    @Override
    public CompletableFuture<?> processAsync(MutableRecord mutableRecord) {
        JsonRecord jsonRecord = mutableRecord.toJsonRecord();

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

        Map<String, Object> options = convertToMap(config);
        options.put("min-chunks-per-message", config.getMinChunksPerMessage());

        CompletableFuture<ChatCompletions> chatCompletionsHandle =
                completionsService.getChatCompletions(
                        messages,
                        new CompletionsService.StreamingChunksConsumer() {
                            @Override
                            public void consumeChunk(
                                    String answerId, int index, Chunk chunk, boolean last) {

                                // we must copy the context because the same context is used for all
                                // chunks
                                // and also for the final answer
                                MutableRecord copy = mutableRecord.copy();

                                copy.getProperties().put("stream-id", answerId);
                                copy.getProperties().put("stream-index", index + "");
                                copy.getProperties().put("stream-last-message", last + "");

                                final String content = chunk.content();
                                applyResultFieldToContext(copy, content, true);
                                streamingAnswersConsumer.streamAnswerChunk(
                                        index, content, last, copy);
                            }
                        },
                        options);

        return chatCompletionsHandle.thenApply(
                chatCompletions -> {
                    ChatChoice chatChoice = chatCompletions.getChoices().get(0);
                    applyResultFieldToContext(mutableRecord, chatChoice.content(), false);

                    String logField = config.getLogField();
                    if (logField != null && !logField.isEmpty()) {
                        Map<String, Object> logMap = new HashMap<>();
                        logMap.put("model", config.getModel());
                        logMap.put("options", options);
                        logMap.put("messages", messages);
                        mutableRecord.setResultField(
                                MutableRecord.toJson(logMap),
                                logField,
                                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                                avroKeySchemaCache,
                                avroValueSchemaCache);
                    }
                    return null;
                });
    }

    private void applyResultFieldToContext(
            MutableRecord mutableRecord, String content, boolean streamingAnswer) {
        String fieldName = config.getFieldName();

        // maybe we want a different field in the streaming answer
        // typically you want to directly stream the answer as the whole "value"
        if (streamingAnswer
                && config.getStreamResponseCompletionField() != null
                && !config.getStreamResponseCompletionField().isEmpty()) {
            fieldName = config.getStreamResponseCompletionField();
        }
        mutableRecord.setResultField(
                content,
                fieldName,
                Schema.create(Schema.Type.STRING),
                avroKeySchemaCache,
                avroValueSchemaCache);
    }
}
