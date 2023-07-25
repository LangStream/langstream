package com.datastax.oss.sga.runtime.agent;

import com.datastax.oss.sga.api.runner.code.AgentContext;
import com.datastax.oss.sga.api.runner.code.AgentSource;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class AgentRecordTrackerTest {

    private static record MyRecord (Object key, Object value, String origin, Long timestamp, Collection<Header> headers) implements Record {
    }

    private static class MySource implements AgentSource {
        List<Record> committed = new ArrayList<>();

        @Override
        public void commit(List<Record> records) throws Exception {
            committed.addAll(records);
        }

        @Override
        public void init(Map<String, Object> configuration) throws Exception {

        }

        @Override
        public void setContext(AgentContext context) throws Exception {

        }

        @Override
        public void start() throws Exception {

        }

        @Override
        public void close() throws Exception {

        }

        @Override
        public List<Record> read() throws Exception {
            return List.of();
        }
    }

    @Test
    public void testTracker() throws Exception {

        MySource agentSource = new MySource();
        SourceRecordTracker tracker = new SourceRecordTracker(agentSource);

        Record sourceRecord = new MyRecord("key", "sourceValue", "origin", 0L, null);
        Record sinkRecord = new MyRecord("key", "sinkValue", "origin", 0L, null);

        tracker.track(Map.of(sourceRecord, List.of(sinkRecord)));

        tracker.commit(List.of(sinkRecord));

        assertEquals(1, agentSource.committed.size());
        assertEquals(sourceRecord, agentSource.committed.get(0));
        agentSource.committed.clear();

    }

    @Test
    public void testChunking() throws Exception {
        MySource agentSource = new MySource();
        SourceRecordTracker tracker = new SourceRecordTracker(agentSource);

        Record sourceRecord = new MyRecord("key", "sourceValue", "origin", 0L, null);
        Record sinkRecord = new MyRecord("key", "sinkValue", "origin", 0L, null);
        Record sinkRecord2 = new MyRecord("key", "sinkValue2", "origin", 0L, null);

        tracker.track(Map.of(sourceRecord, List.of(sinkRecord, sinkRecord2)));

        // the sink commits only 1 of the 2 records
        tracker.commit(List.of(sinkRecord));

        assertEquals(0, agentSource.committed.size());
        tracker.commit(List.of(sinkRecord2));

        assertEquals(1, agentSource.committed.size());
        assertEquals(sourceRecord, agentSource.committed.get(0));
        agentSource.committed.clear();
    }

}
