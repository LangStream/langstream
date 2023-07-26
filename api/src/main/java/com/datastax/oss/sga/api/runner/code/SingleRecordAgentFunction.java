package com.datastax.oss.sga.api.runner.code;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SingleRecordAgentFunction implements AgentFunction {

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
