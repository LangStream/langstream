package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

class SourceRecordTracker implements AgentSink.CommitCallback {
    private final Map<Record, Record> sinkToSourceMapping = new ConcurrentHashMap<>();
    private final Map<Record, AtomicInteger> remainingSinkRecordsForSourceRecord = new ConcurrentHashMap<>();
    private final AgentSource source;

    public SourceRecordTracker(AgentSource source) {
        this.source = source;
    }

    @Override
    @SneakyThrows
    public void commit(List<Record> sinkRecords) {
        List<Record> sourceRecordsToCommit = new ArrayList<>();
        for (Record record : sinkRecords) {
            Record sourceRecord = sinkToSourceMapping.get(record);
            if (sourceRecord != null) {
                AtomicInteger remaining = remainingSinkRecordsForSourceRecord.get(sourceRecord);
                if (remaining.decrementAndGet() == 0) {
                    sourceRecordsToCommit.add(sourceRecord);
                }
            }
        }
        source.commit(sourceRecordsToCommit);
        // forget about this batch SinkRecords
        sinkRecords.forEach(sinkToSourceMapping::remove);
    }

    public void track(Map<Record, List<Record>> sinkRecords) {

        // map each sink record to the original source record
        sinkRecords.forEach((sourceRecord, recordList) -> {

            remainingSinkRecordsForSourceRecord.put(sourceRecord, new AtomicInteger(recordList.size()));

            recordList.forEach(sinkRecord -> {
                sinkToSourceMapping.put(sinkRecord, sourceRecord);
            });
        });
    }
}
