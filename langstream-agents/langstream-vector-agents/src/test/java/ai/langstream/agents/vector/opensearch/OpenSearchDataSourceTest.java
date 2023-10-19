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
package ai.langstream.agents.vector.opensearch;

import static ai.langstream.agents.vector.opensearch.OpenSearchDataSource.OpenSearchQueryStepDataSource.convertSearchRequest;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.core.SearchRequest;

class OpenSearchDataSourceTest {
    @Test
    void test() throws Exception {
        SearchRequest searchRequest =
                convertSearchRequest(
                        """
                {
                    "query": {
                        "match_all": {}
                    }
                }""",
                        List.of(),
                        "index1");
        assertEquals("index1", searchRequest.index().get(0));
        assertNotNull(searchRequest.query().matchAll());

        searchRequest =
                convertSearchRequest(
                        """
                {
                    "size": 12,
                    "query": {
                      "knn": {
                        "embeddings": {
                          "vector": ?,
                          "k": 12
                        }
                      }
                    }
                }""",
                        List.of(List.of(0.1f, 0.2f)),
                        "index1");
        assertEquals("index1", searchRequest.index().get(0));
        assertEquals("embeddings", searchRequest.query().knn().field());
        assertEquals(0.1f, searchRequest.query().knn().vector()[0]);
        assertEquals(0.2f, searchRequest.query().knn().vector()[1]);
        assertEquals(12, searchRequest.query().knn().k());
        assertEquals(12, searchRequest.size());
    }
}
