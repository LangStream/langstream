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
package ai.langstream.api.runner.code;

import ai.langstream.api.runtime.ComponentType;
import java.util.concurrent.CompletableFuture;

/** Body of the agent */
public interface AgentSink extends AgentCode {

    /**
     * The agent processes records and typically writes to an external service.
     *
     * @param record the record to write
     * @throws Exception if the agent fails to process the records
     * @return an handle to the asynchronous write
     */
    CompletableFuture<?> write(Record record);

    @Override
    default ComponentType componentType() {
        return ComponentType.SINK;
    }

    /**
     * @return true if the agent handles commit of consumed record (e.g. in case of batching)
     */
    default boolean handlesCommit() {
        return false;
    }

    /** Let's sink handle offset commit if handlesCommit() == true. */
    default void commit() throws Exception {}
}
