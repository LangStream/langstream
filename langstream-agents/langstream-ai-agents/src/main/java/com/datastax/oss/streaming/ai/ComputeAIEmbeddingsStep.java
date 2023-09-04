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

import static com.datastax.oss.streaming.ai.util.TransformFunctionUtil.executeInBatches;

import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.datastax.oss.streaming.ai.model.JsonRecord;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/**
 * Compute AI Embeddings from a template filled with the received message fields and metadata and
 * put the value into a new or existing field.
 */
@Slf4j
public class ComputeAIEmbeddingsStep implements TransformStep {

    private final Template template;
    private final String embeddingsFieldName;
    private final int batchSize;
    private final EmbeddingsService embeddingsService;
    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroValueSchemaCache =
            new ConcurrentHashMap<>();

    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroKeySchemaCache =
            new ConcurrentHashMap<>();

    public ComputeAIEmbeddingsStep(
            String text,
            String embeddingsFieldName,
            int batchSize,
            EmbeddingsService embeddingsService) {
        this.template = Mustache.compiler().compile(text);
        this.batchSize = batchSize;
        this.embeddingsFieldName = embeddingsFieldName;
        this.embeddingsService = embeddingsService;
    }

    @Override
    public void close() throws Exception {
        if (embeddingsService != null) {
            embeddingsService.close();
        }
    }

    @Override
    public boolean supportsBatch() {
        return true;
    }

    @Override
    public void processAsync(
            List<ContextWithOriginalRecord> records,
            BiConsumer<ContextWithOriginalRecord, Throwable> whenComplete) {

        executeInBatches(
                records,
                (batch) -> {
                    processSingleBatch(batch, whenComplete);
                },
                batchSize);
    }

    private void processSingleBatch(
            List<ContextWithOriginalRecord> records,
            BiConsumer<ContextWithOriginalRecord, Throwable> whenComplete) {
        List<String> texts = new ArrayList<>();
        for (ContextWithOriginalRecord record : records) {
            if (!record.context().isDropCurrentRecord()) {
                JsonRecord jsonRecord = record.context().toJsonRecord();
                String text = template.execute(jsonRecord);
                texts.add(text);
            }
        }
        final CompletableFuture<List<List<Double>>> embeddingsHandle =
                embeddingsService.computeEmbeddings(texts);
        embeddingsHandle.whenComplete(
                (embeddings, throwable) -> {
                    if (throwable != null) {
                        for (ContextWithOriginalRecord record : records) {
                            whenComplete.accept(record, throwable);
                        }
                    } else {
                        if (texts.size() != embeddings.size()) {
                            throw new IllegalStateException(
                                    "Expected embeddings for "
                                            + texts.size()
                                            + " texts but got "
                                            + embeddings.size());
                        }
                        int indexEmbeddings = 0;
                        for (int i = 0; i < records.size(); i++) {
                            ContextWithOriginalRecord recordWithContext = records.get(i);
                            TransformContext context = recordWithContext.context();
                            if (context.isDropCurrentRecord()) {
                                // skipped
                            } else {
                                List<Double> resultForRecord = embeddings.get(indexEmbeddings++);
                                context.setResultField(
                                        resultForRecord,
                                        embeddingsFieldName,
                                        Schema.createArray(Schema.create(Schema.Type.DOUBLE)),
                                        avroKeySchemaCache,
                                        avroValueSchemaCache);
                            }
                            whenComplete.accept(recordWithContext, null);
                        }
                    }
                });
    }

    @Override
    public void process(TransformContext transformContext) {}
}
