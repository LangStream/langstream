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
import java.util.List;
import java.util.Objects;

/** Body of the agent */
public interface AgentProcessor extends AgentCode {

    /**
     * The agent processes records and returns a list of records. The transactionality of the
     * function is guaranteed by the runtime. This method should not throw any exceptions, but
     * report errors to the RecordSink. It is expected that the agent will emit the same number of
     * records as it received.
     *
     * @param records the list of input records, this is never empty
     * @param recordSink the sink to emit records and errors to
     */
    void process(List<Record> records, RecordSink recordSink);

    @Override
    default ComponentType componentType() {
        return ComponentType.PROCESSOR;
    }

    record SourceRecordAndResult(Record sourceRecord, List<Record> resultRecords, Throwable error) {
        public SourceRecordAndResult {
            resultRecords = Objects.requireNonNullElseGet(resultRecords, List::of);
        }
    }
}
