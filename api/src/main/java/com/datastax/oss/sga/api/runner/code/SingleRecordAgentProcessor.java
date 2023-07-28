package com.datastax.oss.sga.api.runner.code;

import java.util.ArrayList;
import java.util.List;

public abstract class SingleRecordAgentProcessor implements AgentProcessor {

    public abstract List<Record> processRecord(Record record) throws Exception;

    @Override
    public final List<SourceRecordAndResult> process(List<Record> records) throws Exception {
        List<SourceRecordAndResult> result = new ArrayList<>();
        for (Record record : records) {
            List<Record> process = processRecord(record);
            if (!process.isEmpty()) {
                result.add(new SourceRecordAndResult(record, process));
            }
        }
        return result;
    }
}
