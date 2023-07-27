package com.datastax.oss.sga.api.runner.code;

import lombok.Getter;

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
    static class SourceRecordAndResult {
        public final Record sourceRecord;
        public final List<Record> resultRecords;

        public SourceRecordAndResult(Record sourceRecord, List<Record> resultRecords) {
            this.sourceRecord = sourceRecord;
            this.resultRecords = resultRecords;
        }
    }
}
