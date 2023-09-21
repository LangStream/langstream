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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Aggregate records in batches, depending on a batch size and a maximum idle time.
 *
 * @param <T>
 */
public class BatchExecutor<T> {
    private final int batchSize;
    private List<T> batch;
    private long flushInterval;
    private ScheduledExecutorService scheduledExecutorService;

    private ScheduledFuture<?> scheduledFuture;

    private final Consumer<List<T>> processor;

    public BatchExecutor(
            int batchSize,
            Consumer<List<T>> processor,
            long maxIdleTime,
            ScheduledExecutorService scheduledExecutorService) {
        this.batchSize = batchSize;
        this.batch = new ArrayList<>(batchSize);
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
        List<T> batchToProcess = null;
        synchronized (this) {
            if (batch.isEmpty()) {
                return;
            }
            batchToProcess = batch;
            batch = new ArrayList<>(batchSize);
        }
        // execute the processor out of the synchronized block
        processor.accept(batchToProcess);
    }

    public void add(T t) {
        List<T> batchToProcess = null;
        synchronized (this) {
            batch.add(t);
            if (batch.size() >= batchSize || flushInterval <= 0) {
                batchToProcess = batch;
                batch = new ArrayList<>(batchSize);
            }
        }

        // execute the processor out of the synchronized block
        if (batchToProcess != null) {
            processor.accept(batchToProcess);
        }
    }
}
