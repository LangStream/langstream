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
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.SearchRequest;
import com.couchbase.client.java.search.result.SearchResult;
import com.couchbase.client.java.search.result.SearchRow;
import com.couchbase.client.java.search.vector.VectorQuery;
import com.couchbase.client.java.search.vector.VectorSearch;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

        @JsonProperty(value = "username", required = true)
        private String username;

        @JsonProperty(value = "password", required = true)
        private String password;
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
            log.info("Connected to Couchbase: {}", clientConfig.connectionString);
        }

        @Override
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                Map<String, Object> queryMap =
                        InterpolationUtils.buildObjectFromJson(query, Map.class, params);
                if (queryMap.isEmpty()) {
                    throw new UnsupportedOperationException("Query is empty");
                }
                log.info("QueryMap: {}", queryMap);
                log.info("Params: {}", params);

                float[] vector = JstlFunctions.toArrayOfFloat(queryMap.remove("vector"));
                Integer topK = (Integer) queryMap.remove("topK");
                String vecPlanId = (String) queryMap.remove("vecPlanId");
                String bucketName = (String) queryMap.remove("bucket-name");
                String scopeName = (String) queryMap.remove("scope-name");
                String collectionName = (String) queryMap.remove("collection-name");
                String vectorIndexName = (String) queryMap.remove("vector-name");
                String semanticIndexName = (String) queryMap.remove("semantic-name");

                // Perform the term search for vecPlanId first
                SearchRequest termSearchRequest =
                        SearchRequest.create(SearchQuery.match(vecPlanId).field("vecPlanId"));

                SearchResult termSearchResult =
                        cluster.search(
                                bucketName + "." + scopeName + "." + semanticIndexName,
                                termSearchRequest);

                List<SearchRow> termSearchRows = termSearchResult.rows();
                Set<String> validIds =
                        termSearchRows.stream().map(SearchRow::id).collect(Collectors.toSet());
                log.info("Term Search Result IDs: {}", validIds);

                if (validIds.isEmpty()) {
                    return Collections.emptyList();
                }

                Map<String, Double> termSearchScores =
                        termSearchRows.stream()
                                .collect(Collectors.toMap(SearchRow::id, SearchRow::score));

                log.info("Term Search Scores: {}", termSearchScores);

                // Perform the vector search on the filtered documents
                SearchRequest vectorSearchRequest =
                        SearchRequest.create(SearchQuery.match(vecPlanId).field("vecPlanId"))
                                .vectorSearch(
                                        VectorSearch.create(
                                                VectorQuery.create("embeddings", vector)
                                                        .numCandidates(topK)));

                SearchResult vectorSearchResult =
                        cluster.search(
                                bucketName + "." + scopeName + "." + vectorIndexName,
                                vectorSearchRequest);

                // Process and collect results
                List<Map<String, Object>> results =
                        vectorSearchResult.rows().stream()
                                .filter(hit -> validIds.contains(hit.id()))
                                .limit(topK)
                                .map(
                                        hit -> {
                                            Map<String, Object> result = new HashMap<>();
                                            double adjustedScore =
                                                    hit.score() - termSearchScores.get(hit.id());
                                            result.put("similarity", adjustedScore);
                                            result.put("id", hit.id());

                                            // Fetch and add the document content using collection
                                            // API

                                            try {
                                                String documentId = hit.id();
                                                GetResult getResult =
                                                        cluster.bucket(bucketName)
                                                                .scope(scopeName)
                                                                .collection(collectionName)
                                                                .get(documentId);
                                                if (getResult != null) {
                                                    JsonObject content =
                                                            getResult.contentAsObject();
                                                    content.removeKey("embeddings");
                                                    result.putAll(content.toMap());
                                                }
                                            } catch (Exception e) {
                                                log.error(
                                                        "Error retrieving document content for ID: {}",
                                                        hit.id(),
                                                        e);
                                            }

                                            return result;
                                        })
                                .collect(Collectors.toList());

                return results;

            } catch (Exception e) {
                log.error("Error executing query: {}", e.getMessage(), e);
                throw new RuntimeException("Error during search", e);
            }
        }

        @Override
        public void close() {
            if (cluster != null) {
                cluster.disconnect();
                log.info("Disconnected from Couchbase Bucket: {}", clientConfig.connectionString);
            }
        }
    }
}
