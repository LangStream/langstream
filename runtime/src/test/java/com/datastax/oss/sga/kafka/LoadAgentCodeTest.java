package com.datastax.oss.sga.kafka;


import com.datastax.oss.sga.api.runner.code.AgentCode;
import com.datastax.oss.sga.api.runner.code.AgentCodeRegistry;
import com.datastax.oss.sga.api.runner.code.Record;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class LoadAgentCodeTest {

    @Test
    public void testLoadNoop() throws Exception {
        AgentCodeRegistry registry = new AgentCodeRegistry();
        AgentCode noop = registry.getAgentCode("noop");
        assertTrue(noop.process(List.of(new Record() {
            @Override
            public Object key() {
                return null;
            }

            @Override
            public Object value() {
                return null;
            }
        })).isEmpty());
    }

    @Test
    public void testLoadIdentity() throws Exception {
        AgentCodeRegistry registry = new AgentCodeRegistry();
        AgentCode noop = registry.getAgentCode("identity");
        assertEquals(1, noop.process(List.of(new Record() {
            @Override
            public Object key() {
                return null;
            }

            @Override
            public Object value() {
                return null;
            }
        })).size());
    }

}