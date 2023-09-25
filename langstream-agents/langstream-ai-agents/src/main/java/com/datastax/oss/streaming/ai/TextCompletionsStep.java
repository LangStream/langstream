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

import com.azure.ai.openai.models.CompletionsOptions;
import com.datastax.oss.streaming.ai.completions.Chunk;
import com.datastax.oss.streaming.ai.completions.CompletionsService;
import com.datastax.oss.streaming.ai.model.JsonRecord;
import com.datastax.oss.streaming.ai.model.config.TextCompletionsConfig;
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
public class TextCompletionsStep implements TransformStep {

    private final CompletionsService completionsService;

    private final TextCompletionsConfig config;

    private final Map<Schema, Schema> avroValueSchemaCache = new ConcurrentHashMap<>();

    private final Map<Schema, Schema> avroKeySchemaCache = new ConcurrentHashMap<>();

    private final Map<String, Template> messageTemplates = new ConcurrentHashMap<>();
    private final StreamingAnswersConsumerFactory streamingAnswersConsumerFactory;

    private StreamingAnswersConsumer streamingAnswersConsumer;

    public TextCompletionsStep(
            CompletionsService completionsService,
            StreamingAnswersConsumerFactory streamingAnswersConsumerFactory,
            TextCompletionsConfig config) {
        this.streamingAnswersConsumerFactory = streamingAnswersConsumerFactory;
        this.completionsService = completionsService;
        this.config = config;
        this.streamingAnswersConsumer = (index, message, last, record) -> {};
        config.getPrompt().forEach(p -> messageTemplates.put(p, Mustache.compiler().compile(p)));
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
    public CompletableFuture<?> processAsync(TransformContext transformContext) {
        JsonRecord jsonRecord = transformContext.toJsonRecord();

        List<String> prompt =
                config.getPrompt().stream()
                        .map(p -> messageTemplates.get(p).execute(jsonRecord))
                        .collect(Collectors.toList());

        CompletionsOptions completionsOptions =
                new CompletionsOptions(List.of())
                        .setMaxTokens(config.getMaxTokens())
                        .setTemperature(config.getTemperature())
                        .setTopP(config.getTopP())
                        .setLogitBias(config.getLogitBias())
                        .setStream(config.isStream())
                        .setUser(config.getUser())
                        .setStop(config.getStop())
                        .setPresencePenalty(config.getPresencePenalty())
                        .setFrequencyPenalty(config.getFrequencyPenalty());
        Map<String, Object> options = convertToMap(completionsOptions);
        options.put("model", config.getModel());
        options.put("min-chunks-per-message", config.getMinChunksPerMessage());
        options.remove("messages");

        CompletableFuture<String> chatCompletionsHandle =
                completionsService.getTextCompletions(
                        prompt,
                        new CompletionsService.StreamingChunksConsumer() {
                            @Override
                            public void consumeChunk(
                                    String answerId, int index, Chunk chunk, boolean last) {

                                // we must copy the context because the same context is used for all
                                // chunks
                                // and also for the final answer
                                TransformContext copy = transformContext.copy();

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
                content -> {
                    applyResultFieldToContext(transformContext, content, false);

                    String logField = config.getLogField();
                    if (logField != null && !logField.isEmpty()) {
                        Map<String, Object> logMap = new HashMap<>();
                        logMap.put("model", config.getModel());
                        logMap.put("options", options);
                        logMap.put("messages", prompt);
                        transformContext.setResultField(
                                TransformContext.toJson(logMap),
                                logField,
                                Schema.create(Schema.Type.STRING),
                                avroKeySchemaCache,
                                avroValueSchemaCache);
                    }
                    return null;
                });
    }

    private void applyResultFieldToContext(
            TransformContext transformContext, String content, boolean streamingAnswer) {
        String fieldName = config.getFieldName();

        // maybe we want a different field in the streaming answer
        // typically you want to directly stream the answer as the whole "value"
        if (streamingAnswer
                && config.getStreamResponseCompletionField() != null
                && !config.getStreamResponseCompletionField().isEmpty()) {
            fieldName = config.getStreamResponseCompletionField();
        }
        transformContext.setResultField(
                content,
                fieldName,
                Schema.create(Schema.Type.STRING),
                avroKeySchemaCache,
                avroValueSchemaCache);
    }
}
