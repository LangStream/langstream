package ai.langstream.agents.vector.datasource.impl;

import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class PineconeDataSourceTest {

    @Test
    @Disabled
    void testPineconeQuery() {
        PineconeDataSource dataSource = new PineconeDataSource();
        Map<String, Object> config = Map.of(
                "api-key", "1ba1052e-e4a7-48a0-8568-42dba961207e",
                "environment","asia-southeast1-gcp-free",
                "project-name", "032e3d0",
                "index-name", "example-index");
        QueryStepDataSource implementation = dataSource.createImplementation(config);
        implementation.initialize(null);

        String query = """
                {
                      "vector": ?,
                      "topK": 5
                    }
                """;
        List<Object> params = List.of(
                List.of(0.1f, 0.1f, 0.1f, 0.1f, 0.1f)
        );
        List<Map<String, String>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);
    }

    @Test
    @Disabled
    void testPineconeQueryWithFilter() {
        PineconeDataSource dataSource = new PineconeDataSource();
        Map<String, Object> config = Map.of(
                "api-key", "1ba1052e-e4a7-48a0-8568-42dba961207e",
                "environment","asia-southeast1-gcp-free",
                "project-name", "032e3d0",
                "index-name", "example-index");
        QueryStepDataSource implementation = dataSource.createImplementation(config);
        implementation.initialize(null);

        String query = """
                {
                      "vector": ?,
                      "topK": 5,
                      "filter":
                        {"$and": [{"genre": ?}, {"year":?}]}
                    }
                """;
        List<Object> params = List.of(
                List.of(0.1f, 0.1f, 0.1f, 0.1f, 0.1f),
                "comedy",
                2019
        );
        List<Map<String, String>> results = implementation.fetchData(query, params);
        log.info("Results: {}", results);
    }
}