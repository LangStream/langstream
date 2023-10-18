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

import ai.langstream.ai.agents.commons.MutableRecord;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface TransformStep extends AutoCloseable {
    default void close() throws Exception {}

    default void start() throws Exception {}

    default void process(MutableRecord mutableRecord) throws Exception {
        try {
            processAsync(mutableRecord).get();
        } catch (ExecutionException err) {
            if (err.getCause() instanceof Exception) {
                throw (Exception) err.getCause();
            } else {
                throw err;
            }
        }
    }

    default CompletableFuture<?> processAsync(MutableRecord mutableRecord) {
        try {
            process(mutableRecord);
            return CompletableFuture.completedFuture(null);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }
}
