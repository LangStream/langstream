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

import ai.langstream.api.util.BatchExecutor;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.model.JsonRecord;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.avro.Schema;

/**
 * Compute AI Embeddings from a template filled with the received message fields and metadata and
 * put the value into a new or existing field.
 */
public class ComputeAIEmbeddingsStep implements TransformStep {

    private final Template template;
    private final String embeddingsFieldName;
    private final EmbeddingsService embeddingsService;

    private final BatchExecutor<RecordHolder> batchExecutor;

    private final ScheduledExecutorService executorService;
    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroValueSchemaCache =
            new ConcurrentHashMap<>();

    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroKeySchemaCache =
            new ConcurrentHashMap<>();

    public ComputeAIEmbeddingsStep(
            String text,
            String embeddingsFieldName,
            int batchSize,
            long flushInterval,
            EmbeddingsService embeddingsService) {
        this.template = Mustache.compiler().compile(text);
        this.embeddingsFieldName = embeddingsFieldName;
        this.embeddingsService = embeddingsService;
        this.executorService =
                flushInterval > 0 ? Executors.newSingleThreadScheduledExecutor() : null;
        this.batchExecutor =
                new BatchExecutor<>(batchSize, this::processBatch, flushInterval, executorService);
    }

    @Override
    public void start() throws Exception {
        batchExecutor.start();
    }

    private void processBatch(List<RecordHolder> records) {

        // prepare batch API call
        List<String> texts = new ArrayList<>();
        for (RecordHolder holder : records) {
            TransformContext transformContext = holder.transformContext();
            JsonRecord jsonRecord = transformContext.toJsonRecord();
            String text = template.execute(jsonRecord);
            texts.add(text);
        }

        CompletableFuture<List<List<Double>>> embeddings =
                embeddingsService.computeEmbeddings(texts);

        embeddings.whenComplete(
                (result, error) -> {
                    if (error != null) {
                        for (int i = 0; i < records.size(); i++) {
                            RecordHolder holder = records.get(i);
                            holder.handle.completeExceptionally(error);
                        }
                        return;
                    }

                    for (int i = 0; i < records.size(); i++) {
                        RecordHolder holder = records.get(i);
                        TransformContext transformContext = holder.transformContext();
                        List<Double> embeddingsForText = result.get(i);
                        transformContext.setResultField(
                                embeddingsForText,
                                embeddingsFieldName,
                                Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
                                avroKeySchemaCache,
                                avroValueSchemaCache);
                        holder.handle().complete(null);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        if (executorService != null) {
            executorService.shutdown();
        }
        batchExecutor.stop();
        if (embeddingsService != null) {
            embeddingsService.close();
        }
    }

    record RecordHolder(TransformContext transformContext, CompletableFuture<?> handle) {}

    @Override
    public CompletableFuture<?> processAsync(TransformContext transformContext) {
        CompletableFuture<?> handle = new CompletableFuture<>();
        batchExecutor.add(new RecordHolder(transformContext, handle));
        return handle;
    }
}
