/**
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
package com.datastax.oss.sga.api.runner.code;

import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * Body of the agent
 */
public interface AgentProcessor extends AgentCode {

    /**
     * The agent processes records and returns a list of records.
     * The transactionality of the function is guaranteed by the runtime.
     * @param records the list of input records
     * @return the list of output records
     * @throws Exception if the agent fails to process the records
     */
    List<SourceRecordAndResult> process(List<Record> records) throws Exception;

    @Getter
    @ToString
    static class SourceRecordAndResult {
        final Record sourceRecord;
        final List<Record> resultRecords;
        final Throwable error;

        public SourceRecordAndResult(Record sourceRecord, List<Record> resultRecords, Throwable error) {
            this.sourceRecord = sourceRecord;
            this.resultRecords = resultRecords == null ? List.of() : resultRecords;
            this.error = error;
        }
    }
}
