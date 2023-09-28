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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
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
                            // field to re-rank
                            "field", "value.query_results",
                            // new field to store re-ranked results
                            "output-field", "value.output_field",
                            // field in the message that contains the query
                            "query-text", "value.query",
                            // field in the message that contains the embeddings computed for the
                            // query
                            "query-embeddings", "value.query_embeddings",
                            // field in the record that contains the text
                            "text-field", "record.text",
                            // field in the record that contains the embeddings
                            "embeddings-field", "record.embeddings",
                            // lambda parameter for MMR
                            "lambda", 0.7,
                            // algorithm to use
                            "algorithm", "MMR"));
            SimpleRecord record =
                    SimpleRecord.of(
                            "key",
                            Map.of(
                                    "query",
                                    "tell my a number",
                                    "query_embeddings",
                                    List.of(3d, 4d),
                                    "query_results",
                                    List.of(
                                            Map.of("text", "one", "embeddings", List.of(1d, 2d)),
                                            Map.of("text", "two", "embeddings", List.of(3d, 4d)))));
            Record result = agent.processRecord(record).get(0);
            log.info("{}", result.value());
            assertEquals(
                    """
                                {query_embeddings=[3.0, 4.0], query_results=[{embeddings=[1.0, 2.0], text=one}, {embeddings=[3.0, 4.0], text=two}], query=tell my a number, output_field=[{embeddings=[1.0, 2.0], text=one}, {embeddings=[3.0, 4.0], text=two}]}""",
                    result.value().toString());
        }
    }
}
