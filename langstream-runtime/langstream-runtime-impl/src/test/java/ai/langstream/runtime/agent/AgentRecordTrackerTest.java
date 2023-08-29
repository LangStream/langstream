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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.langstream.api.runner.code.AbstractAgentCode;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.Record;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;

public class AgentRecordTrackerTest {

    private record MyRecord(
            Object key, Object value, String origin, Long timestamp, Collection<Header> headers)
            implements Record {}

    private static class MySource extends AbstractAgentCode implements AgentSource {

        List<Record> committed = new ArrayList<>();

        @Override
        public void commit(List<Record> records) {
            committed.addAll(records);
        }

        @Override
        public List<Record> read() {
            return List.of();
        }
    }

    @Test
    public void testTracker() {

        MySource agentSource = new MySource();
        SourceRecordTracker tracker = new SourceRecordTracker(agentSource);

        Record sourceRecord = new MyRecord("key", "sourceValue", "origin", 0L, null);
        Record sinkRecord = new MyRecord("key", "sinkValue", "origin", 0L, null);

        tracker.track(
                List.of(
                        new AgentProcessor.SourceRecordAndResult(
                                sourceRecord, List.of(sinkRecord), null)));

        tracker.commit(List.of(sinkRecord));

        assertEquals(1, agentSource.committed.size());
        assertEquals(sourceRecord, agentSource.committed.get(0));
        agentSource.committed.clear();

        // ensure no leaks
        assertTrue(tracker.remainingSinkRecordsForSourceRecord.isEmpty());
        assertTrue(tracker.sinkToSourceMapping.isEmpty());
    }

    @Test
    public void testChunking() {
        MySource agentSource = new MySource();
        SourceRecordTracker tracker = new SourceRecordTracker(agentSource);

        Record sourceRecord = new MyRecord("key", "sourceValue", "origin", 0L, null);
        Record sinkRecord = new MyRecord("key", "sinkValue", "origin", 0L, null);
        Record sinkRecord2 = new MyRecord("key", "sinkValue2", "origin", 0L, null);

        tracker.track(
                List.of(
                        new AgentProcessor.SourceRecordAndResult(
                                sourceRecord, List.of(sinkRecord, sinkRecord2), null)));

        // the sink commits only 1 of the 2 records
        tracker.commit(List.of(sinkRecord));

        assertEquals(0, agentSource.committed.size());
        tracker.commit(List.of(sinkRecord2));

        assertEquals(1, agentSource.committed.size());
        assertEquals(sourceRecord, agentSource.committed.get(0));
        agentSource.committed.clear();
        // ensure no leaks
        assertTrue(tracker.remainingSinkRecordsForSourceRecord.isEmpty());
        assertTrue(tracker.sinkToSourceMapping.isEmpty());
    }

    @Test
    public void testSkippedRecord() {

        MySource agentSource = new MySource();
        SourceRecordTracker tracker = new SourceRecordTracker(agentSource);

        Record sourceRecord = new MyRecord("key", "sourceValue", "origin", 0L, null);
        Record sinkRecord = new MyRecord("key", "sinkValue", "origin", 0L, null);

        tracker.track(
                List.of(new AgentProcessor.SourceRecordAndResult(sourceRecord, List.of(), null)));

        tracker.commit(List.of(sinkRecord));

        assertEquals(1, agentSource.committed.size());
        assertEquals(sourceRecord, agentSource.committed.get(0));
        agentSource.committed.clear();
        // ensure no leaks
        assertTrue(tracker.remainingSinkRecordsForSourceRecord.isEmpty());
        assertTrue(tracker.sinkToSourceMapping.isEmpty());
    }
}
