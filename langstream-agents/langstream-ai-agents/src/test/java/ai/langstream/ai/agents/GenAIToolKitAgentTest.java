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
package ai.langstream.ai.agents;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class GenAIToolKitAgentTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testCompute() throws Exception {
        String value =
                MAPPER.writeValueAsString(
                        Map.of(
                                "fieldInt",
                                1,
                                "fieldText",
                                "text",
                                "fieldCsv",
                                "a,b,c",
                                "fieldJson",
                                "{\"this\":\"that\"}"));

        assertEquals(1, compute("value.fieldInt", value));
        assertEquals("text", compute("value.fieldText", value));
        assertEquals(List.of("a", "b", "c"), compute("fn:split(value.fieldCsv,',')", value));
        // compute cannot return a Map, so we return a String
        assertEquals("{this=that}", compute("fn:str(fn:fromJson(value.fieldJson))", value));

        // compute cannot return a Map, so we return a JSON String
        assertEquals(
                Map.of("f1", "a", "f2", "b", "f3", "c"),
                MAPPER.readValue(
                        compute("fn:toJson(fn:unpack(value.fieldCsv, 'f1,f2,f3'))", value)
                                .toString(),
                        Map.class));
    }

    Object compute(String expression, Object value) throws Exception {
        GenAIToolKitAgent agent = new GenAIToolKitAgent();
        AgentContext mockContext = mock(AgentContext.class);
        when(mockContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agent.setContext(mockContext);
        agent.init(
                Map.of(
                        "steps",
                        List.of(
                                Map.of(
                                        "type",
                                        "compute",
                                        "fields",
                                        List.of(
                                                Map.of(
                                                        "name",
                                                        "value.computedField",
                                                        "expression",
                                                        expression))))));
        agent.start();
        SimpleRecord record = SimpleRecord.builder().value(value).build();
        Record result = agent.processRecord(record).get().get(0);
        Map<String, Object> resultValueParsed =
                MAPPER.readValue(result.value().toString(), Map.class);
        agent.close();
        return resultValueParsed.get("computedField");
    }
}
