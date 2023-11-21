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
package ai.langstream.api.database;

import ai.langstream.api.runner.code.Record;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This is the interface for writing to a vector database. this interface is really simple by
 * intention. For advanced usages users should use Kafka Connect connectors.
 */
public interface VectorDatabaseWriter extends AutoCloseable {

    default void initialise(Map<String, Object> agentConfiguration) throws Exception {}

    /**
     * Update a record, insert if it does not exist. If value is NULL then the record is deleted.
     *
     * @param record the record
     * @param context additional context
     */
    CompletableFuture<?> upsert(Record record, Map<String, Object> context);

    default void close() throws Exception {}
}
