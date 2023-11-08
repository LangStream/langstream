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
package ai.langstream.agents.camel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.AgentSource;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class CamelSourceTest {

    private static final AgentCodeRegistry AGENT_CODE_REGISTRY = new AgentCodeRegistry();

    @BeforeAll
    static void setup() {}

    @Test
    void testRead() throws Exception {
        AgentSource agentSource =
                buildAgentSource("timer://test", Map.of("period", 1, "repeatCount", 1));
        Awaitility.await()
                .untilAsserted(
                        () -> {
                            List<Record> read = agentSource.read();
                            assertEquals(1, read.size());
                        });
    }

    private AgentSource buildAgentSource(String uri, Map<String, Object> componentOptions)
            throws Exception {
        AgentSource agentSource =
                (AgentSource) AGENT_CODE_REGISTRY.getAgentCode("camel-source").agentCode();
        Map<String, Object> configs = new HashMap<>();
        configs.put("component-uri", uri);
        configs.put("component-options", componentOptions);
        configs.put("max-buffered-records", 10);
        AgentContext agentContext = mock(AgentContext.class);
        when(agentContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agentSource.init(configs);
        agentSource.setContext(agentContext);
        agentSource.start();
        return agentSource;
    }
}
