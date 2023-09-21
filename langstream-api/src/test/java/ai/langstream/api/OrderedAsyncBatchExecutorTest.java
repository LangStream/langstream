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
package ai.langstream.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.api.util.OrderedAsyncBatchExecutor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
class OrderedAsyncBatchExecutorTest {

    static Object[][] batchSizes() {
        return new Object[][] {
            new Object[] {0, 1},
            new Object[] {1, 1},
            new Object[] {1, 2},
            new Object[] {2, 1},
            new Object[] {2, 2},
            new Object[] {3, 5},
            new Object[] {5, 3}
        };
    }

    @ParameterizedTest
    @MethodSource("batchSizes")
    void executeInBatchesTestWithFlushInterval(int numRecords, int batchSize) {
        long flushInterval = 1000;
        int numBuckets = 4;
        Function<String, Integer> hashFunction = String::hashCode;
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        List<String> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add("text " + i);
        }

        List<String> result = new CopyOnWriteArrayList<>();
        OrderedAsyncBatchExecutor<String> executor =
                new OrderedAsyncBatchExecutor<>(
                        batchSize,
                        (batch, future) -> {
                            result.addAll(batch);
                            future.complete(null);
                        },
                        flushInterval,
                        numBuckets,
                        hashFunction,
                        executorService);
        executor.start();
        records.forEach(executor::add);
        // this may happen after some time
        Awaitility.await().untilAsserted(() -> assertEqualsInAnyOrder(records, result));
        executor.stop();
        executorService.shutdown();
    }

    private static <T> void assertEqualsInAnyOrder(List<T> a, List<T> b) {
        if (a.size() != b.size()) {
            throw new AssertionError("Lists have different sizes");
        }
        assertEquals(new HashSet<>(a), new HashSet<>(b));
    }

    @ParameterizedTest
    @MethodSource("batchSizes")
    void executeInBatchesTestWithNoFlushInterval(int numRecords, int batchSize) {
        long flushInterval = 0;
        int numBuckets = 4;
        Function<String, Integer> hashFunction = String::hashCode;
        // this is not needed
        ScheduledExecutorService executorService = null;
        List<String> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add("text " + i);
        }
        List<String> result = new CopyOnWriteArrayList<>();
        OrderedAsyncBatchExecutor<String> executor =
                new OrderedAsyncBatchExecutor<>(
                        batchSize,
                        (batch, future) -> {
                            // as flush interval is zero, this is called immediately
                            log.info("processed batch {}", batch);
                            assertEquals(1, batch.size());
                            result.addAll(batch);
                            future.complete(null);
                        },
                        flushInterval,
                        numBuckets,
                        hashFunction,
                        executorService);
        executor.start();
        records.forEach(executor::add);
        // this may happen after some time
        Awaitility.await().untilAsserted(() -> assertEquals(records, result));
        executor.stop();
    }

    static Object[][] batchSizesAndDelays() {
        return new Object[][] {
            new Object[] {0, 1, 0},
            new Object[] {1, 1, 0},
            new Object[] {1, 2, 0},
            new Object[] {2, 1, 0},
            new Object[] {2, 2, 0},
            new Object[] {3, 5, 0},
            new Object[] {5, 3, 0},
            new Object[] {37, 5, 0},
            new Object[] {50, 3, 0},
            new Object[] {0, 1, 200},
            new Object[] {1, 1, 200},
            new Object[] {1, 2, 200},
            new Object[] {2, 1, 200},
            new Object[] {2, 2, 200},
            new Object[] {3, 5, 200},
            new Object[] {5, 3, 200},
            new Object[] {37, 5, 200},
            new Object[] {50, 3, 200}
        };
    }

    @ParameterizedTest
    @MethodSource("batchSizesAndDelays")
    void executeInBatchesTestWithKeyOrdering(int numRecords, int batchSize, long delay) {

        record KeyValue(int key, String value) {}

        long flushInterval = 1000;
        int numBuckets = 4;
        Function<KeyValue, Integer> hashFunction = KeyValue::key;
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService completionsExecutorService = Executors.newScheduledThreadPool(4);

        List<KeyValue> records = new ArrayList<>();
        Map<Integer, List<KeyValue>> recordsByKey = new HashMap<>();

        for (int i = 0; i < numRecords; i++) {
            int key = i % 7;
            KeyValue record = new KeyValue(key, "text " + i);
            records.add(record);
            List<KeyValue> valuesForKey =
                    recordsByKey.computeIfAbsent(record.key, l -> new ArrayList<>());
            valuesForKey.add(record);
        }

        Random random = new Random();

        Map<Integer, List<KeyValue>> results = new ConcurrentHashMap<>();
        OrderedAsyncBatchExecutor<KeyValue> executor =
                new OrderedAsyncBatchExecutor<>(
                        batchSize,
                        (batch, future) -> {
                            for (KeyValue record : batch) {
                                List<KeyValue> resultsForKey =
                                        results.computeIfAbsent(
                                                record.key, l -> new CopyOnWriteArrayList<>());
                                resultsForKey.add(record);
                            }
                            if (delay == 0) {
                                // execute the completions in the same thread
                                future.complete(null);
                            } else {
                                // delay the completion of the future, and do it in a separate
                                // thread
                                long delayMillis = random.nextInt((int) delay);
                                completionsExecutorService.schedule(
                                        () -> future.complete(null),
                                        delayMillis,
                                        TimeUnit.MILLISECONDS);
                            }
                        },
                        flushInterval,
                        numBuckets,
                        hashFunction,
                        executorService);
        executor.start();
        records.forEach(executor::add);
        // this may happen after some time
        Awaitility.await()
                .untilAsserted(
                        () -> {
                            recordsByKey.forEach(
                                    (key, values) -> {
                                        List<KeyValue> resultsForKey = results.get(key);
                                        log.info(
                                                "key: {}, values: {}, results: {}",
                                                key,
                                                values,
                                                resultsForKey);
                                        // the order must be preserved
                                        assertEquals(values, resultsForKey);
                                    });
                        });
        executor.stop();
        executorService.shutdown();
        completionsExecutorService.shutdown();
    }

    @ParameterizedTest
    @MethodSource("batchSizesAndDelays")
    void executeInBatchesTestWithKeyOrderingWithoutFlush(
            int numRecords, int batchSize, long delay) {

        record KeyValue(int key, String value) {}

        long flushInterval = 0;
        int numBuckets = 4;
        Function<KeyValue, Integer> hashFunction = KeyValue::key;
        ScheduledExecutorService executorService = null;
        ScheduledExecutorService completionsExecutorService = Executors.newScheduledThreadPool(4);

        List<KeyValue> records = new ArrayList<>();
        Map<Integer, List<KeyValue>> recordsByKey = new HashMap<>();

        for (int i = 0; i < numRecords; i++) {
            int key = i % 7;
            KeyValue record = new KeyValue(key, "text " + i);
            records.add(record);
            List<KeyValue> valuesForKey =
                    recordsByKey.computeIfAbsent(record.key, l -> new ArrayList<>());
            valuesForKey.add(record);
        }

        Random random = new Random();

        Map<Integer, List<KeyValue>> results = new ConcurrentHashMap<>();
        OrderedAsyncBatchExecutor<KeyValue> executor =
                new OrderedAsyncBatchExecutor<>(
                        batchSize,
                        (batch, future) -> {
                            for (KeyValue record : batch) {
                                List<KeyValue> resultsForKey =
                                        results.computeIfAbsent(
                                                record.key, l -> new CopyOnWriteArrayList<>());
                                resultsForKey.add(record);
                            }
                            if (delay == 0) {
                                // execute the completions in the same thread
                                future.complete(null);
                            } else {
                                // delay the completion of the future, and do it in a separate
                                // thread
                                long delayMillis = random.nextInt((int) delay);
                                completionsExecutorService.schedule(
                                        () -> future.complete(null),
                                        delayMillis,
                                        TimeUnit.MILLISECONDS);
                            }
                        },
                        flushInterval,
                        numBuckets,
                        hashFunction,
                        executorService);
        executor.start();
        records.forEach(executor::add);
        // this flushes the pending records
        executor.stop();
        // this may happen after some time
        Awaitility.await()
                .untilAsserted(
                        () -> {
                            recordsByKey.forEach(
                                    (key, values) -> {
                                        List<KeyValue> resultsForKey = results.get(key);
                                        log.info(
                                                "key: {}, values: {}, results: {}",
                                                key,
                                                values,
                                                resultsForKey);
                                        // the order must be preserved
                                        assertEquals(values, resultsForKey);
                                    });
                        });
        completionsExecutorService.shutdown();
    }
}
