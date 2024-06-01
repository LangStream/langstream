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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.langstream.agents.vector.VectorDBSinkAgent;
import ai.langstream.agents.vector.couchbase.CouchbaseDataSource;
import ai.langstream.api.runner.code.AgentCodeRegistry;
import ai.langstream.api.runner.code.AgentContext;
import ai.langstream.api.runner.code.MetricsReporter;
import ai.langstream.api.runner.code.Record;
import ai.langstream.api.runner.code.SimpleRecord;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class CouchbaseWriterTest {

    BucketDefinition bucketDefinition = new BucketDefinition("bucket-name");

    // Explicitly declare the image as a compatible substitute
    private DockerImageName couchbaseImage =
            DockerImageName.parse("couchbase/server:latest")
                    .asCompatibleSubstituteFor("couchbase/server");

    @Test
    @Disabled
    void testCouchbaseWrite() throws Exception {

        Map<String, Object> datasourceConfig =
                Map.of(
                        "service", "couchbase",
                        "connection-string", "couchbases://",
                        "bucket-name", "",
                        "username", "",
                        "password", "");

        VectorDBSinkAgent agent =
                (VectorDBSinkAgent)
                        new AgentCodeRegistry().getAgentCode("vector-db-sink").agentCode();

        Map<String, Object> configuration = new HashMap<>();
        configuration.put("datasource", datasourceConfig);
        configuration.put("vector.id", "value.id");
        configuration.put("vector.vector", "value.vector");

        AgentContext agentContext = mock(AgentContext.class);
        when(agentContext.getMetricsReporter()).thenReturn(MetricsReporter.DISABLED);
        agent.init(configuration);
        agent.start();
        agent.setContext(agentContext);

        List<Record> committed = new CopyOnWriteArrayList<>();
        List<Double> vector = new ArrayList<>();
        for (int i = 1; i <= 1536; i++) {
            vector.add(1.0 / i);
        }
        Map<String, Object> value =
                Map.of("id", "test-doc1", "document", "Hello", "embeddings", vector);
        SimpleRecord record = SimpleRecord.of(null, new ObjectMapper().writeValueAsString(value));
        agent.write(record).thenRun(() -> committed.add(record)).get();

        assertEquals(committed.get(0), record);
        agent.close();

        // Sleep for a while to allow the data to be indexed
        log.info("Sleeping for 5 seconds to allow the data to be indexed");
        Thread.sleep(5000);

        CouchbaseDataSource dataSource = new CouchbaseDataSource();
        QueryStepDataSource implementation =
                dataSource.createDataSourceImplementation(datasourceConfig);
        implementation.initialize(null);

        String query =
                """
                {
                      "vector": ?,
                      "topK": 5
                    }
                """;
        List<Object> params = List.of(vector);
        List<Map<String, Object>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);

        assertEquals(5, results.size());
    }
}
