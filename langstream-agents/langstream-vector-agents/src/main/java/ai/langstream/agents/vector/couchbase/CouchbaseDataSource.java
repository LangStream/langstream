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
package ai.langstream.agents.vector.couchbase;

import ai.langstream.agents.vector.InterpolationUtils;
import ai.langstream.ai.agents.commons.jstl.JstlFunctions;
import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.vector.VectorQuery;
import com.couchbase.client.java.search.vector.VectorSearch;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseDataSource implements DataSourceProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "couchbase".equals(dataSourceConfig.get("service"));
    }

    @Data
    public static final class CouchbaseConfig {
        @JsonProperty(value = "connection-string", required = true)
        private String connectionString;

        @JsonProperty(value = "bucket-name", required = true)
        private String bucketName;

        @JsonProperty(value = "username", required = true)
        private String username;

        @JsonProperty(value = "password", required = true)
        private String password;

        @JsonProperty(value = "scope-name", required = true)
        private String scopeName;

        @JsonProperty(value = "collection-name", required = true)
        private String collectionName;
    }

    @Override
    public QueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {
        CouchbaseConfig config = MAPPER.convertValue(dataSourceConfig, CouchbaseConfig.class);
        return new CouchbaseQueryStepDataSource(config);
    }

    public static class CouchbaseQueryStepDataSource implements QueryStepDataSource {

        @Getter private final CouchbaseConfig clientConfig;
        private Cluster cluster;

        // private Collection collection;

        public CouchbaseQueryStepDataSource(CouchbaseConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) {
            cluster =
                    Cluster.connect(
                            clientConfig.connectionString,
                            clientConfig.username,
                            clientConfig.password);
            log.info("Connected to Couchbase Bucket: {}", clientConfig.bucketName);
        }

        @Override
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                Map<String, Object> queryMap =
                        InterpolationUtils.buildObjectFromJson(query, Map.class, params);
                if (queryMap.isEmpty()) {
                    throw new UnsupportedOperationException("Query is empty");
                }

                float[] vector = JstlFunctions.toArrayOfFloat(queryMap.remove("vector"));
                Integer topK = (Integer) queryMap.remove("topK");
                // scope namen comes from querymap

                SearchRequest request =
                        SearchRequest.create(
                                VectorSearch.create(
                                        VectorQuery.create("embeddings", vector)
                                                .numCandidates(topK)));
                log.debug("SearchRequest created: {}", request);

                SearchResult result =
                        cluster.search(
                                ""
                                        + clientConfig.bucketName
                                        + "."
                                        + clientConfig.scopeName
                                        + ".vector-search",
                                request);

                return result.rows().stream()
                        .map(
                                hit -> {
                                    Map<String, Object> r = new HashMap<>();
                                    r.put("id", hit.id());
                                    r.put("similarity", hit.score()); // Adds the similarity score

                                    return r;
                                })
                        .collect(Collectors.toList());

            } catch (Exception e) {
                log.error("Error executing query: {}", e.getMessage(), e);
                throw new RuntimeException("Error during search", e);
            }
        }

        @Override
        public void close() {
            if (cluster != null) {
                cluster.disconnect();
                log.info("Disconnected from Couchbase Bucket: {}", clientConfig.bucketName);
            }
        }
    }
}
