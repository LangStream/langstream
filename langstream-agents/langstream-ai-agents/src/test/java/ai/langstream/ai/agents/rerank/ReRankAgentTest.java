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
package ai.langstream.ai.agents.rerank;

import static org.junit.jupiter.api.Assertions.*;

import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReRankAgentTest {

    @Test
    void testNone() throws Exception {
        try (ReRankAgent agent = new ReRankAgent(); ) {
            agent.init(
                    Map.of(
                            "field",
                            "value.field",
                            "output-field",
                            "value.output-field",
                            "algorithm",
                            "none"));
            SimpleRecord record =
                    SimpleRecord.of(
                            "key",
                            """
                    {
                        "field": [
                        ]
                    }
                    """);
            Record result = agent.processRecord(record).get(0);
            assertEquals(
                    """
                    {"field":[],"output-field":[]}""",
                    result.value().toString());
        }
    }

    @Test
    void testMMR() throws Exception {
        try (ReRankAgent agent = new ReRankAgent(); ) {
            agent.init(
                    Map.of(
                            "field",
                            "value.field",
                            "output-field",
                            "value.output-field",
                            "field-in-record",
                            "text",
                            "query",
                            "some text",
                            "lambda",
                            0.7,
                            "algorithm",
                            "MMR"));
            SimpleRecord record =
                    SimpleRecord.of(
                            "key",
                            Map.of("field", List.of(Map.of("text", "one"), Map.of("text", "two"))));
            Record result = agent.processRecord(record).get(0);
            assertEquals(
                    """
                                  {field=[{text=one}, {text=two}], output-field=[{text=two}, {text=one}]}""",
                    result.value().toString());
        }
    }
}
