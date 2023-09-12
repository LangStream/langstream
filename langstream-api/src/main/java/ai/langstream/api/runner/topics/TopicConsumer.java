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
package ai.langstream.api.runner.topics;

import ai.langstream.api.runner.code.Record;
import java.util.List;
import java.util.Map;

public interface TopicConsumer extends AutoCloseable {

    /**
     * This method works well only if the TopicConsumer has been loaded from the same classloader of
     * the caller. It is currently used only by the Kafka Connect adapter because it needs to access
     * the native consumer to handle the offsets.
     *
     * @return the native consumer.
     */
    default Object getNativeConsumer() {
        return null;
    }

    default void start() throws Exception {}

    default void close() throws Exception {}

    default List<Record> read() throws Exception {
        return List.of();
    }

    default void commit(List<Record> records) throws Exception {}

    default Map<String, Object> getInfo() {
        return Map.of();
    }

    long getTotalOut();
}
