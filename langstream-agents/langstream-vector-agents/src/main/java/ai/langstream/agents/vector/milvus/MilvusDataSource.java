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
package ai.langstream.agents.vector.milvus;

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.SearchResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.SearchResultsWrapper;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MilvusDataSource implements DataSourceProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "milvus".equals(dataSourceConfig.get("service"));
    }

    @Data
    public static final class MilvusConfig {

        @JsonProperty(value = "user", required = true)
        private String user = "default";

        @JsonProperty(value = "password", required = true)
        private String password;

        @JsonProperty(value = "host", required = true)
        private String host;

        @JsonProperty(value = "port")
        private int port = 19530;
    }

    @Override
    public QueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {

        MilvusConfig clientConfig = MAPPER.convertValue(dataSourceConfig, MilvusConfig.class);

        return new MilvusQueryStepDataSource(clientConfig);
    }

    private static class MilvusQueryStepDataSource implements QueryStepDataSource {

        private final MilvusConfig clientConfig;
        private MilvusServiceClient milvusClient;

        public MilvusQueryStepDataSource(MilvusConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) {
            this.milvusClient =
                    new MilvusServiceClient(
                            ConnectParam.newBuilder()
                                    .withHost(clientConfig.host)
                                    .withPort(clientConfig.port)
                                    .withAuthorization(clientConfig.user, clientConfig.password)
                                    .build());
        }

        @Override
        public List<Map<String, String>> fetchData(String query, List<Object> params) {
            try {
                Query parsedQuery;
                try {
                    log.info("Query {}", query);
                    params.forEach(
                            param ->
                                    log.info(
                                            "Param {} {}",
                                            param,
                                            param != null ? param.getClass() : null));
                    // interpolate the query
                    query = interpolate(query, params);
                    log.info("Interpolated query {}", query);

                    parsedQuery = MAPPER.readValue(query, Query.class);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                log.info("Parsed query: {}", parsedQuery);

                SearchParam searchParam =
                        SearchParam.newBuilder()
                                .withCollectionName(parsedQuery.collectionName)
                                .withConsistencyLevel(ConsistencyLevelEnum.STRONG)
                                .withMetricType(MetricType.L2)
                                .withOutFields(parsedQuery.outputFields)
                                .withTopK(parsedQuery.topK)
                                .withVectors(parsedQuery.vector)
                                .withVectorFieldName(parsedQuery.vectorFieldName)
                                .withParams(parsedQuery.params)
                                .build();
                R<SearchResults> respSearch = milvusClient.search(searchParam);

                if (respSearch.getException() != null) {
                    throw new RuntimeException(respSearch.getException());
                }

                SearchResultsWrapper wrapperSearch =
                        new SearchResultsWrapper(respSearch.getData().getResults());

                wrapperSearch
                        .getRowRecords()
                        .forEach(
                                r -> {
                                    log.info("Record ");
                                });

                return List.of();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        static String interpolate(String query, List<Object> array) {
            if (query == null || !query.contains("?")) {
                return query;
            }
            for (Object value : array) {
                int questionMark = query.indexOf("?");
                if (questionMark < 0) {
                    return query;
                }
                Object valueAsString = convertValueToJson(value);
                query =
                        query.substring(0, questionMark)
                                + valueAsString
                                + query.substring(questionMark + 1);
            }

            return query;
        }

        @SneakyThrows
        private static String convertValueToJson(Object value) {
            return MAPPER.writeValueAsString(value);
        }

        @Override
        public void close() {
            if (milvusClient != null) {
                milvusClient.close();
            }
        }
    }

    /** JSON model for a Milvus query. */
    @Data
    public static final class Query {
        @JsonProperty("vector")
        private List<Float> vector;

        @JsonProperty("outputFields")
        private List<String> outputFields;

        @JsonProperty("collectionName")
        private String collectionName;

        @JsonProperty("topK")
        private int topK = 1;

        @JsonProperty("vectorFieldName")
        private String vectorFieldName;

        @JsonProperty("params")
        private String params;
    }
}
