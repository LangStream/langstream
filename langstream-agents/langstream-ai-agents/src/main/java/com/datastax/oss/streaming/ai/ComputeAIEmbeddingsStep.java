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

import ai.langstream.ai.agents.commons.JsonRecord;
import ai.langstream.ai.agents.commons.MutableRecord;
import ai.langstream.ai.agents.commons.jstl.JstlEvaluator;
import ai.langstream.api.util.OrderedAsyncBatchExecutor;
import com.datastax.oss.streaming.ai.embeddings.EmbeddingsService;
import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/**
 * Compute AI Embeddings from a template filled with the received message fields and metadata and
 * put the value into a new or existing field.
 */
@Slf4j
public class ComputeAIEmbeddingsStep implements TransformStep {

    static final Random RANDOM = new Random();

    private final Template template;
    private final JstlEvaluator<List> loopOverAccessor;
    private final String embeddingsFieldName;
    private final String loopOverFieldName;
    private final String fieldInRecord;
    private final EmbeddingsService embeddingsService;

    private final OrderedAsyncBatchExecutor<RecordHolder> batchExecutor;

    private final ScheduledExecutorService executorService;
    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroValueSchemaCache =
            new ConcurrentHashMap<>();

    private final Map<org.apache.avro.Schema, org.apache.avro.Schema> avroKeySchemaCache =
            new ConcurrentHashMap<>();

    public ComputeAIEmbeddingsStep(
            String text,
            String embeddingsFieldName,
            String loopOver,
            int batchSize,
            long flushInterval,
            int concurrency,
            EmbeddingsService embeddingsService) {
        this.template =
                Mustache.compiler().compile(MustacheCompatibilityUtils.handleLegacyTemplate(text));
        this.loopOverFieldName = loopOver;
        this.embeddingsFieldName = embeddingsFieldName;
        if (loopOver != null && !loopOver.isEmpty()) {
            this.loopOverAccessor = new JstlEvaluator<List>("${" + loopOver + "}", List.class);
            if (!embeddingsFieldName.startsWith("record.")) {
                throw new IllegalArgumentException(
                        "With loop-over the embeddings field but be something like record.xxx");
            }
            this.fieldInRecord = embeddingsFieldName.substring(7);
            if (fieldInRecord.contains(".")) {
                throw new IllegalArgumentException(
                        "With loop-over the embeddings field but be something like record.xxx,"
                                + "it cannot be record.xxx.yyy");
            }
        } else {
            this.loopOverAccessor = null;
            this.fieldInRecord = null;
        }
        this.embeddingsService = embeddingsService;
        this.executorService =
                flushInterval > 0 ? Executors.newSingleThreadScheduledExecutor() : null;
        int numBuckets = concurrency > 0 ? concurrency : 1;
        this.batchExecutor =
                new OrderedAsyncBatchExecutor<>(
                        batchSize,
                        this::processBatch,
                        flushInterval,
                        numBuckets,
                        ComputeAIEmbeddingsStep::computeHashForRecord,
                        executorService);
    }

    private static int computeHashForRecord(RecordHolder record) {
        Object key = record.mutableRecord.getKeyObject();
        if (key != null) {
            return Objects.hashCode(key);
        } else {
            return RANDOM.nextInt();
        }
    }

    @Override
    public void start() throws Exception {
        batchExecutor.start();
    }

    private record TextAndReference(String text, Consumer<List<Double>> completion) {}

    private void processBatch(List<RecordHolder> records, CompletableFuture<?> completionHandle) {

        // prepare batch API call
        List<TextAndReference> textsAndCompletions = new ArrayList<>();
        List<String> texts = new ArrayList<>();

        try {
            for (RecordHolder holder : records) {
                MutableRecord mutableRecord = holder.mutableRecord();
                JsonRecord jsonRecord = mutableRecord.toJsonRecord();
                if (loopOverAccessor == null) {
                    String text = template.execute(jsonRecord);
                    texts.add(text);
                    textsAndCompletions.add(
                            new TextAndReference(
                                    text,
                                    (List<Double> embeddingsForText) -> {
                                        mutableRecord.setResultField(
                                                embeddingsForText,
                                                embeddingsFieldName,
                                                Schema.createArray(
                                                        Schema.create(Schema.Type.DOUBLE)),
                                                avroKeySchemaCache,
                                                avroValueSchemaCache);
                                        holder.handle().complete(null);
                                    }));
                } else {
                    List<Object> nestedRecords = loopOverAccessor.evaluate(mutableRecord);
                    List<Map<String, Object>> newList = new CopyOnWriteArrayList<>();
                    AtomicInteger remaining = new AtomicInteger(nestedRecords.size());

                    for (Object document : nestedRecords) {
                        log.info("Processing nested record {}", document);
                        // this is a mutable map
                        Map<String, Object> newMap =
                                new ConcurrentHashMap<>((Map<String, Object>) document);
                        newList.add(newMap);
                        Map<String, Object> mustacheContext = new HashMap<>();
                        mustacheContext.put("record", document);
                        String text = template.execute(mustacheContext);
                        texts.add(text);
                        log.info("text {}", text);
                        textsAndCompletions.add(
                                new TextAndReference(
                                        text,
                                        (List<Double> embeddingsForText) -> {
                                            newMap.put(fieldInRecord, embeddingsForText);
                                            int r = remaining.decrementAndGet();
                                            log.info("Remaining {}", r);
                                            if (r == 0) {
                                                log.info("final list {}", newList);
                                                // all the nested records are done, we can override
                                                // the field, in the original record
                                                mutableRecord.setResultField(
                                                        newList,
                                                        loopOverFieldName,
                                                        Schema.createArray(
                                                                Schema.createMap(
                                                                        Schema.createMap(
                                                                                Schema.create(
                                                                                        Schema.Type
                                                                                                .STRING)))),
                                                        avroKeySchemaCache,
                                                        avroValueSchemaCache);
                                                holder.handle().complete(null);
                                            }
                                        }));
                    }
                }
            }
        } catch (Throwable error) {
            // we cannot fail only some records, because we must keep the order
            log.error(
                    "At least one error failed the conversion to JSON, failing the whole batch",
                    error);
            errorForAll(records, error);
            completionHandle.complete(null);
            return;
        }

        CompletableFuture<List<List<Double>>> embeddings =
                embeddingsService.computeEmbeddings(texts);

        embeddings
                .thenAccept(
                        (result) -> {
                            for (int i = 0; i < textsAndCompletions.size(); i++) {
                                List<Double> embeddingsForText = result.get(i);
                                textsAndCompletions.get(i).completion().accept(embeddingsForText);
                            }
                        })
                .whenComplete(
                        (a, b) -> {
                            if (b != null) {
                                log.error("Error while processing batch", b);
                                errorForAll(records, b);
                            }
                            completionHandle.complete(null);
                        });
    }

    private static void errorForAll(List<RecordHolder> records, Throwable error) {
        for (RecordHolder holder : records) {
            holder.handle.completeExceptionally(error);
        }
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

    record RecordHolder(MutableRecord mutableRecord, CompletableFuture<?> handle) {}

    @Override
    public CompletableFuture<?> processAsync(MutableRecord mutableRecord) {
        CompletableFuture<?> handle = new CompletableFuture<>();
        batchExecutor.add(new RecordHolder(mutableRecord, handle));
        return handle;
    }
}
