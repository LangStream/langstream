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
            this.resultRecords = resultRecords;
            this.error = error;
        }
    }
}
