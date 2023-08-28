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
package ai.langstream.runtime.agent;

import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.AgentSink;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Record;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class SourceRecordTracker implements AgentSink.CommitCallback {
    final Map<Record, Record> sinkToSourceMapping = new ConcurrentHashMap<>();
    final Map<Record, AtomicInteger> remainingSinkRecordsForSourceRecord = new ConcurrentHashMap<>();

    final Queue<Record> orderedSourceRecordsToCommit = new ConcurrentLinkedQueue<>();
    private final AgentSource source;

    public SourceRecordTracker(AgentSource source) {
        this.source = source;
    }

    @Override
    @SneakyThrows
    public synchronized void commit(List<Record> sinkRecords) {
        List<Record> sourceRecordsToCommit = new ArrayList<>();
        for (Record record : sinkRecords) {
            Record sourceRecord = sinkToSourceMapping.get(record);
            if (sourceRecord != null) {
                AtomicInteger remaining = remainingSinkRecordsForSourceRecord.get(sourceRecord);
                remaining.decrementAndGet();
            }
        }

        // we can commit only in order,
        // so here we find the longest sequence of records that can be committed
        for (Record record : orderedSourceRecordsToCommit) {
            AtomicInteger remaining = remainingSinkRecordsForSourceRecord.get(record);
            log.info("remaining {} for record {}", remaining, record);
            if (remaining.get() == 0) {
                sourceRecordsToCommit.add(record);
            } else {
                log.info("record {} still has {} sink records to commit", record, remaining.get());
                break;
            }
        }

        sourceRecordsToCommit.forEach(r-> {
            log.info("Record {} is done", r);
            remainingSinkRecordsForSourceRecord.remove(r);
        });

        source.commit(sourceRecordsToCommit);

        // forget about the committed records
        for (Record committed : sourceRecordsToCommit) {
            orderedSourceRecordsToCommit.remove(committed);
            remainingSinkRecordsForSourceRecord.remove(committed);
        }

        // forget about this batch SinkRecords
        sinkRecords.forEach(sinkToSourceMapping::remove);
    }

    public synchronized void track(List<AgentProcessor.SourceRecordAndResult> sinkRecords) {

        // map each sink record to the original source record
        sinkRecords.forEach((sourceRecordAndResult) -> {

            Record sourceRecord = sourceRecordAndResult.sourceRecord();
            orderedSourceRecordsToCommit.add(sourceRecord);

            List<Record> resultRecords = sourceRecordAndResult.resultRecords();
            remainingSinkRecordsForSourceRecord.put(sourceRecord, new AtomicInteger(resultRecords.size()));
            sourceRecordAndResult.resultRecords()
                .forEach(sinkRecord -> sinkToSourceMapping.put(sinkRecord, sourceRecord));
        });
    }

}
