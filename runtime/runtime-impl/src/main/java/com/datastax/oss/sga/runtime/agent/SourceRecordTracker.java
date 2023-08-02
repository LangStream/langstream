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
package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentProcessor;
import com.datastax.oss.sga.api.runner.code.AgentSink;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Record;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class SourceRecordTracker implements AgentSink.CommitCallback {
    final Map<Record, Record> sinkToSourceMapping = new ConcurrentHashMap<>();
    final Map<Record, AtomicInteger> remainingSinkRecordsForSourceRecord = new ConcurrentHashMap<>();
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
                remaining.decrementAndGet();
            }
        }
        remainingSinkRecordsForSourceRecord.forEach((sourceRecord, remaining) -> {
            if (remaining.get() == 0) {
                sourceRecordsToCommit.add(sourceRecord);
            }
        });
        sourceRecordsToCommit.forEach(remainingSinkRecordsForSourceRecord::remove);

        source.commit(sourceRecordsToCommit);
        // forget about this batch SinkRecords
        sinkRecords.forEach(sinkToSourceMapping::remove);
    }

    public void track(List<AgentProcessor.SourceRecordAndResult> sinkRecords) {

        // map each sink record to the original source record
        sinkRecords.forEach((sourceRecordAndResult) -> {

            Record sourceRecord = sourceRecordAndResult.getSourceRecord();
            List<Record> resultRecords = sourceRecordAndResult.getResultRecords();
            remainingSinkRecordsForSourceRecord.put(sourceRecord, new AtomicInteger(resultRecords.size()));
            sourceRecordAndResult.getResultRecords().forEach(sinkRecord -> {
                sinkToSourceMapping.put(sinkRecord, sourceRecord);
            });
        });
    }

    public void errored(Record sourceRecord) {
        remainingSinkRecordsForSourceRecord.remove(sourceRecord);
        sinkToSourceMapping.forEach((sinkRecord, record) -> {
            if (record.equals(sourceRecord)) {
                sinkToSourceMapping.remove(sinkRecord);
            }
        });
    }
}
