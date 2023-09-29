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
package ai.langstream.agents.vector.datasource.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.langstream.agents.vector.VectorDBSinkAgent;
import ai.langstream.agents.vector.pinecone.PineconeDataSource;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
class PineconeWriterTest {

    @Test
    @Disabled
    void testPineconeWrite() throws Exception {

        Map<String, Object> datasourceConfig =
                Map.of(
                        "service",
                        "pinecone",
                        "api-key",
                        "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                        "environment",
                        "asia-southeast1-gcp-free",
                        "project-name",
                        "032e3d0",
                        "index-name",
                        "example-index");
        VectorDBSinkAgent agent =
                (VectorDBSinkAgent)
                        new AgentCodeRegistry().getAgentCode("vector-db-sink").agentCode();
        Map<String, Object> configuration = new HashMap<>();
        configuration.put("datasource", datasourceConfig);

        configuration.put("vector.id", "value.id");
        configuration.put("vector.vector", "value.vector");
        configuration.put("vector.namespace", "");
        configuration.put("vector.metadata.genre", "value.genre");

        agent.init(configuration);
        agent.start();
        List<Record> committed = new CopyOnWriteArrayList<>();
        String genre = "random" + UUID.randomUUID();
        List<Float> vector = new ArrayList<>();
        for (int i = 0; i < 1536; i++) {
            vector.add(1f / i);
        }
        Map<String, Object> value = Map.of("id", "1", "vector", vector, "genre", genre);
        SimpleRecord record = SimpleRecord.of(null, new ObjectMapper().writeValueAsString(value));
        agent.write(record).thenRun(() -> committed.add(record)).get();

        assertEquals(committed.get(0), record);
        agent.close();

        PineconeDataSource dataSource = new PineconeDataSource();
        QueryStepDataSource implementation =
                dataSource.createDataSourceImplementation(datasourceConfig);
        implementation.initialize(null);

        String query =
                """
                {
                      "vector": ?,
                      "topK": 5,
                      "filter":
                        {"genre": ?}
                    }
                """;
        List<Object> params = List.of(vector, genre);
        List<Map<String, Object>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);

        assertEquals(results.size(), 1);
    }
}
