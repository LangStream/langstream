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

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.util.BatchExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.awaitility.Awaitility;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class BatchExecutorTest {

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
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        List<String> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add("text " + i);
        }
        List<String> result = new CopyOnWriteArrayList<>();
        BatchExecutor<String> executor =
                new BatchExecutor<>(
                        batchSize,
                        (batch) -> {
                            result.addAll(batch);
                        },
                        flushInterval,
                        executorService);
        executor.start();
        records.forEach(executor::add);
        // this may happen after some time
        Awaitility.await().untilAsserted(() -> assertEquals(records, result));
        executor.stop();
        executorService.shutdown();
    }

    @ParameterizedTest
    @MethodSource("batchSizes")
    void executeInBatchesTestWithNoFlushInterval(int numRecords, int batchSize) {
        long flushInterval = 0;
        // this is not needed
        ScheduledExecutorService executorService = null;
        List<String> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add("text " + i);
        }
        List<String> result = new CopyOnWriteArrayList<>();
        BatchExecutor<String> executor =
                new BatchExecutor<>(
                        batchSize,
                        (batch) -> {
                            result.addAll(batch);
                        },
                        flushInterval,
                        executorService);
        executor.start();
        records.forEach(executor::add);
        // this may happen after some time
        Awaitility.await().untilAsserted(() -> assertEquals(records, result));
        executor.stop();
    }
}
