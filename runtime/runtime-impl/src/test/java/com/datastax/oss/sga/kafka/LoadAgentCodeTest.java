package com.datastax.oss.sga.kafka;


import com.datastax.oss.sga.api.runner.code.AgentCodeRegistry;
import com.datastax.oss.sga.api.runner.code.AgentProcessor;
import com.datastax.oss.sga.api.runner.code.Header;
import com.datastax.oss.sga.api.runner.code.Record;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class LoadAgentCodeTest {

    @Test
    public void testLoadNoop() throws Exception {
        AgentCodeRegistry registry = new AgentCodeRegistry();
        AgentProcessor noop = (AgentProcessor) registry.getAgentCode("noop");
        MyRecord myRecord = new MyRecord();
        assertTrue(noop.process(List.of(myRecord)).isEmpty());
    }

    @Test
    public void testLoadIdentity() throws Exception {
        AgentCodeRegistry registry = new AgentCodeRegistry();
        AgentProcessor noop = (AgentProcessor) registry.getAgentCode("identity");
        MyRecord myRecord = new MyRecord();
        assertEquals(1, noop.process(List.of(myRecord)).get(0).getResultRecords().size());
        assertSame(myRecord, noop.process(List.of(myRecord)).get(0).getResultRecords().get(0));
        assertSame(myRecord, noop.process(List.of(myRecord)).get(0).getSourceRecord());
    }

    private static class MyRecord implements Record {
        @Override
        public Object key() {
            return null;
        }

        @Override
        public Object value() {
            return null;
        }

        @Override
        public String origin() {
            return null;
        }

        @Override
        public Long timestamp() {
            return null;
        }

        @Override
        public List<Header> headers() {
            return null;
        }
    }
}