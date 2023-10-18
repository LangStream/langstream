package ai.langstream.agents.vector.opensearch;

import static ai.langstream.agents.vector.opensearch.OpenSearchDataSource.OpenSearchQueryStepDataSource.convertSearchRequest;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch.core.SearchRequest;

class OpenSearchDataSourceTest {
    @Test
    void test() throws Exception {
        SearchRequest searchRequest = convertSearchRequest("""
                {
                    "query": {
                        "match_all": {}
                    }
                }""", List.of(), "index1"
        );
        assertEquals("index1", searchRequest.index().get(0));
        assertNotNull(searchRequest.query().matchAll());

        searchRequest = convertSearchRequest("""
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
                }""", List.of(List.of(0.1f, 0.2f)), "index1"
        );
        assertEquals("index1", searchRequest.index().get(0));
        assertEquals("embeddings", searchRequest.query().knn().field());
        assertEquals(0.1f, searchRequest.query().knn().vector()[0]);
        assertEquals(0.2f, searchRequest.query().knn().vector()[1]);
        assertEquals(12, searchRequest.query().knn().k());
        assertEquals(12, searchRequest.size());

    }
}