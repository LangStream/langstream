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
package com.datastax.oss.streaming.ai.util;

import static com.datastax.oss.streaming.ai.util.TransformFunctionUtil.executeInBatches;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TransformFunctionUtilTest {

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
    void executeInBatchesTest(int numRecords, int batchSize) {
        List<String> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            records.add("text " + i);
        }
        List<String> result = new ArrayList<>();

        executeInBatches(
                records,
                (batch) -> {
                    result.addAll(batch);
                },
                batchSize);
        assertEquals(result, records);
    }
}
