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
package ai.langstream.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentProcessor;
import ai.langstream.api.runner.code.Header;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
class LoadAgentCodeTest {

    @Test
    public void testLoadNoop() throws Exception {
        AgentCodeRegistry registry = new AgentCodeRegistry();
        AgentProcessor noop = (AgentProcessor) registry.getAgentCode("noop").agentCode();
        setContext(noop);
        MyRecord myRecord = new MyRecord();
        List<AgentProcessor.SourceRecordAndResult> res = new ArrayList<>();
        noop.process(List.of(myRecord), res::add);
        assertEquals(1, res.size());
        assertTrue(res.get(0).resultRecords().isEmpty());
    }

    @Test
    public void testLoadIdentity() throws Exception {
        AgentCodeRegistry registry = new AgentCodeRegistry();
        AgentProcessor noop = (AgentProcessor) registry.getAgentCode("identity").agentCode();
        setContext(noop);
        MyRecord myRecord = new MyRecord();
        List<AgentProcessor.SourceRecordAndResult> res = new ArrayList<>();
        noop.process(List.of(myRecord), res::add);
        assertEquals(1, res.size());
        res.clear();
        noop.process(List.of(myRecord), res::add);
        assertSame(myRecord, res.get(0).resultRecords().get(0));
        res.clear();
        noop.process(List.of(myRecord), res::add);
        assertSame(myRecord, res.get(0).sourceRecord());
    }

    private static void setContext(AgentProcessor noop) throws Exception {
        AgentContext agentContext = mock(AgentContext.class);
        noop.setContext(agentContext);
        when(agentContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
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
