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
package ai.langstream.api.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Aggregate records in batches, depending on a batch size and a maximum idle time.
 *
 * @param <T>
 */
@Slf4j
public class OrderedAsyncBatchExecutor<T> {
    private final int batchSize;
    private final Bucket[] buckets;
    private final int numBuckets;
    private final long flushInterval;
    private final ScheduledExecutorService scheduledExecutorService;

    private ScheduledFuture<?> scheduledFuture;

    private final BiConsumer<List<T>, CompletableFuture<?>> processor;

    private final Function<T, Integer> hashFunction;

    public OrderedAsyncBatchExecutor(
            int batchSize,
            BiConsumer<List<T>, CompletableFuture<?>> processor,
            long maxIdleTime,
            int numBuckets,
            Function<T, Integer> hashFunction,
            ScheduledExecutorService scheduledExecutorService) {
        this.numBuckets = numBuckets;
        this.hashFunction = hashFunction;
        Object[] buckets = new Object[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            buckets[i] = new Bucket();
        }
        // try to avoid "generic array creation compile error"
        this.buckets = Arrays.copyOf(buckets, numBuckets, Bucket[].class);
        this.batchSize = batchSize;
        this.processor = processor;
        this.flushInterval = maxIdleTime;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void start() {
        if (flushInterval > 0) {
            scheduledFuture =
                    scheduledExecutorService.scheduleWithFixedDelay(
                            this::flush, flushInterval, flushInterval, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        flush();
    }

    private void flush() {
        for (Bucket bucket : buckets) {
            bucket.flush();
        }
    }

    private Bucket bucket(int hash) {
        if (numBuckets == 1) {
            return buckets[0];
        }
        return buckets[Math.abs(hash % numBuckets)];
    }

    public void add(T t) {
        int hash = hashFunction.apply(t);
        Bucket bucket = bucket(hash);
        bucket.add(t);
    }

    private class Bucket {
        private final Queue<List<T>> pendingBatches = new ArrayDeque<>();
        private final List<T> currentBatch = new ArrayList<>();

        private final AtomicReference<UUID> processing = new AtomicReference<>();

        synchronized void add(T t) {
            currentBatch.add(t);
            if (currentBatch.size() >= batchSize || flushInterval <= 0) {
                scheduleCurrentBatchExecution();
            }
        }

        private synchronized void scheduleCurrentBatchExecution() {
            if (currentBatch.isEmpty()) {
                return;
            }
            List<T> batchToProcess = new ArrayList<>(currentBatch);
            currentBatch.clear();
            addToPendingBatches(batchToProcess);
        }

        private synchronized void processNextBatch() {
            if (pendingBatches.isEmpty()) {
                return;
            }
            List<T> nextBatch = pendingBatches.poll();
            executeBatch(nextBatch);
        }

        private synchronized void addToPendingBatches(List<T> batchToProcess) {
            if (processing.get() == null) {
                executeBatch(batchToProcess);
            } else {
                pendingBatches.add(batchToProcess);
            }
        }

        private void executeBatch(List<T> batchToProcess) {
            UUID batchId = UUID.randomUUID();
            CompletableFuture<?> currentBatchHandle = new CompletableFuture<>();
            currentBatchHandle.whenComplete(
                    (result, error) -> {
                        boolean check = processing.compareAndSet(batchId, null);
                        if (!check) {
                            log.error(
                                    "Something went wrong, batch {} was not processed",
                                    processing.get());
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("Batch {} completed", batchId);
                            }
                            processNextBatch();
                        }
                    });
            boolean check = processing.compareAndSet(null, batchId);
            if (!check) {
                throw new IllegalStateException(
                        "Something went wrong, the processor is still processing");
            }
            if (log.isDebugEnabled()) {
                log.debug("Batch {} in bucket {} started for {}", batchId, this, batchToProcess);
            }
            processor.accept(batchToProcess, currentBatchHandle);
        }

        private synchronized void flush() {
            scheduleCurrentBatchExecution();
        }
    }
}
