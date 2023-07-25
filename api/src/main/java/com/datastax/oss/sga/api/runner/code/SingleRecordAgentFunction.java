package com.datastax.oss.sga.api.runner.code;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SingleRecordAgentFunction implements AgentFunction {

    public abstract List<Record> processRecord(Record record) throws Exception;

    @Override
    public final Map<Record, List<Record>> process(List<Record> records) throws Exception {
        Map<Record, List<Record>> result = new HashMap<>();
        for (Record record : records) {
            List<Record> process = processRecord(record);

            result.put(record, process);
        }
        return result;
    }
}
