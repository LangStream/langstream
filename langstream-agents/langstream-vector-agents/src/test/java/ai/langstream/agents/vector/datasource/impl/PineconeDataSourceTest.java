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

import ai.langstream.agents.vector.pinecone.PineconeDataSource;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
class PineconeDataSourceTest {

    @Test
    @Disabled
    void testPineconeQuery() throws Exception {
        PineconeDataSource dataSource = new PineconeDataSource();
        Map<String, Object> config =
                Map.of(
                        "api-key", "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                        "environment", "asia-southeast1-gcp-free",
                        "project-name", "032e3d0",
                        "index-name", "example-index");
        QueryStepDataSource implementation = dataSource.createDataSourceImplementation(config);
        implementation.initialize(null);

        String query =
                """
                {
                      "vector": ?,
                      "topK": 5
                    }
                """;
        List<Object> params = List.of(List.of(0.1f, 0.1f, 0.1f, 0.1f, 0.1f));
        List<Map<String, Object>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);
    }

    @Test
    @Disabled
    void testPineconeQueryWithFilter() throws Exception {
        PineconeDataSource dataSource = new PineconeDataSource();
        Map<String, Object> config =
                Map.of(
                        "api-key", "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                        "environment", "asia-southeast1-gcp-free",
                        "project-name", "032e3d0",
                        "index-name", "example-index");
        QueryStepDataSource implementation = dataSource.createDataSourceImplementation(config);
        implementation.initialize(null);

        String query =
                """
                {
                      "vector": ?,
                      "topK": 5,
                      "filter":
                        {"$and": [{"genre": ?}, {"year":?}]}
                    }
                """;
        List<Object> params = List.of(List.of(0.1f, 0.1f, 0.1f, 0.1f, 0.1f), "comedy", 2019);
        List<Map<String, Object>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);
    }
}
