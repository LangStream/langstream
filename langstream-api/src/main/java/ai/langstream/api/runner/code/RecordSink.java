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

import java.util.List;

public interface RecordSink {
    /**
     * Emit a record to the downstream agent. This method can be called by any thread and it is
     * guaranteed to not throw any exception.
     *
     * @param recordAndResult the result of the agent processing
     */
    void emit(AgentProcessor.SourceRecordAndResult recordAndResult);

    default void emitEmptyList(Record sourceRecord) {
        emit(new AgentProcessor.SourceRecordAndResult(sourceRecord, List.of(), null));
    }

    default void emitSingleResult(Record sourceRecord, Record result) {
        emit(new AgentProcessor.SourceRecordAndResult(sourceRecord, List.of(result), null));
    }

    default void emitError(Record sourceRecord, Throwable error) {
        emit(new AgentProcessor.SourceRecordAndResult(sourceRecord, null, error));
    }
}
