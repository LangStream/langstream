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
package ai.langstream.agents.vector.pinecone;

import static ai.langstream.agents.vector.InterpolationUtils.buildObjectFromJson;

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.StatusRuntimeException;
import io.pinecone.PineconeClient;
import io.pinecone.PineconeClientConfig;
import io.pinecone.PineconeConnection;
import io.pinecone.PineconeConnectionConfig;
import io.pinecone.proto.QueryRequest;
import io.pinecone.proto.QueryResponse;
import io.pinecone.proto.QueryVector;
import io.pinecone.proto.SparseValues;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class PineconeDataSource implements DataSourceProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "pinecone".equals(dataSourceConfig.get("service"));
    }

    @Data
    public static final class PineconeConfig {
        @JsonProperty(value = "api-key", required = true)
        private String apiKey;

        @JsonProperty(value = "environment", required = true)
        private String environment = "default";

        @JsonProperty(value = "project-name", required = true)
        private String projectName;

        @JsonProperty(value = "index-name", required = true)
        private String indexName;

        @JsonProperty(value = "endpoint")
        private String endpoint;

        @JsonProperty("server-side-timeout-sec")
        private int serverSideTimeoutSec = 10;
    }

    @Override
    public QueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {

        PineconeConfig clientConfig = MAPPER.convertValue(dataSourceConfig, PineconeConfig.class);

        return new PineconeQueryStepDataSource(clientConfig);
    }

    private static class PineconeQueryStepDataSource implements QueryStepDataSource {

        private final PineconeConfig clientConfig;
        private PineconeConnection connection;

        public PineconeQueryStepDataSource(PineconeConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) {
            PineconeClientConfig pineconeClientConfig =
                    new PineconeClientConfig()
                            .withApiKey(clientConfig.getApiKey())
                            .withEnvironment(clientConfig.getEnvironment())
                            .withProjectName(clientConfig.getProjectName())
                            .withServerSideTimeoutSec(clientConfig.getServerSideTimeoutSec());
            PineconeClient pineconeClient = new PineconeClient(pineconeClientConfig);
            PineconeConnectionConfig connectionConfig =
                    new PineconeConnectionConfig().withIndexName(clientConfig.getIndexName());
            if (clientConfig.getEndpoint() == null) {
                connection = pineconeClient.connect(connectionConfig);
            }
        }

        @Override
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                Query parsedQuery = buildObjectFromJson(query, Query.class, params);

                QueryRequest batchQueryRequest = mapQueryToQueryRequest(parsedQuery);

                List<Map<String, Object>> results;

                if (clientConfig.getEndpoint() == null) {
                    results = executeQueryUsingClien(batchQueryRequest, parsedQuery);
                } else {
                    results = executeQueryWithMockHttpService(batchQueryRequest);
                }
                return results;
            } catch (IOException | StatusRuntimeException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private List<Map<String, Object>> executeQueryWithMockHttpService(
                QueryRequest batchQueryRequest) throws IOException, InterruptedException {
            List<Map<String, Object>> results;
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request =
                    HttpRequest.newBuilder(URI.create(clientConfig.getEndpoint()))
                            .POST(HttpRequest.BodyPublishers.ofString(batchQueryRequest.toString()))
                            .build();
            String body = client.send(request, HttpResponse.BodyHandlers.ofString()).body();
            log.info("Mock result {}", body);
            results = MAPPER.readValue(body, new TypeReference<>() {});
            return results;
        }

        @NotNull
        private List<Map<String, Object>> executeQueryUsingClien(
                QueryRequest batchQueryRequest, Query parsedQuery) {
            List<Map<String, Object>> results;
            QueryResponse queryResponse = connection.getBlockingStub().query(batchQueryRequest);

            if (log.isDebugEnabled()) {
                log.debug("Query response: {}", queryResponse);
            }
            log.info("Query response: {}", queryResponse);

            results = new ArrayList<>();
            queryResponse
                    .getResultsList()
                    .forEach(
                            res ->
                                    res.getMatchesList()
                                            .forEach(
                                                    match -> {
                                                        String id = match.getId();
                                                        Map<String, Object> row = new HashMap<>();

                                                        if (parsedQuery.includeMetadata) {
                                                            // put all the metadata
                                                            if (match.getMetadata() != null) {
                                                                match.getMetadata()
                                                                        .getFieldsMap()
                                                                        .forEach(
                                                                                (key, value) -> {
                                                                                    if (log
                                                                                            .isDebugEnabled()) {
                                                                                        log.debug(
                                                                                                "Key: {}, value: {} {}",
                                                                                                key,
                                                                                                value,
                                                                                                value
                                                                                                                != null
                                                                                                        ? value
                                                                                                                .getClass()
                                                                                                        : null);
                                                                                    }
                                                                                    Object
                                                                                            converted =
                                                                                                    valueToObject(
                                                                                                            value);
                                                                                    row.put(
                                                                                            key,
                                                                                            converted
                                                                                                            != null
                                                                                                    ? converted
                                                                                                            .toString()
                                                                                                    : null);
                                                                                });
                                                            }
                                                        }
                                                        row.put("id", id);
                                                        results.add(row);
                                                    }));
            return results;
        }

        @NotNull
        private QueryRequest mapQueryToQueryRequest(Query parsedQuery) {
            QueryVector.Builder builder = QueryVector.newBuilder();

            if (parsedQuery.vector != null) {
                builder.addAllValues(parsedQuery.vector);
            }

            if (parsedQuery.sparseVector != null) {
                builder.setSparseValues(
                        SparseValues.newBuilder()
                                .addAllValues(parsedQuery.sparseVector.getValues())
                                .addAllIndices(parsedQuery.sparseVector.getIndices())
                                .build());
            }

            if (parsedQuery.filter != null && !parsedQuery.filter.isEmpty()) {
                builder.setFilter(buildFilter(parsedQuery.filter));
            }

            if (parsedQuery.namespace != null) {
                builder.setNamespace(parsedQuery.namespace);
            }

            QueryVector queryVector = builder.build();
            QueryRequest.Builder requestBuilder = QueryRequest.newBuilder();

            if (parsedQuery.namespace != null) {
                requestBuilder.setNamespace(parsedQuery.namespace);
            }

            QueryRequest batchQueryRequest =
                    requestBuilder
                            .addQueries(queryVector)
                            .setTopK(parsedQuery.topK)
                            .setIncludeMetadata(parsedQuery.includeMetadata)
                            .setIncludeValues(parsedQuery.includeValues)
                            .build();
            return batchQueryRequest;
        }

        public static Object valueToObject(Value value) {
            return switch (value.getKindCase()) {
                case NULL_VALUE -> null;
                case NUMBER_VALUE -> value.getNumberValue();
                case STRING_VALUE -> value.getStringValue();
                case BOOL_VALUE -> value.getBoolValue();
                case LIST_VALUE -> value.getListValue().getValuesList().stream()
                        .map(PineconeQueryStepDataSource::valueToObject)
                        .toList();
                case STRUCT_VALUE -> value.getStructValue().getFieldsMap().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> valueToObject(e.getValue())));
                default -> null;
            };
        }

        private Struct buildFilter(Map<String, Object> filter) {
            Struct.Builder builder = Struct.newBuilder();
            filter.forEach((key, value) -> builder.putFields(key, convertToValue(value)));
            return builder.build();
        }

        @Override
        public void close() {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * JSON model for Pinecone querys.
     *
     * <p>"vector": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1], "filter": {"genre": {"$in": ["comedy",
     * "documentary", "drama"]}}, "topK": 1, "includeMetadata": true
     */
    @Data
    public static final class Query {
        @JsonProperty("vector")
        private List<Float> vector;

        @JsonProperty("filter")
        private Map<String, Object> filter;

        @JsonProperty("topK")
        private int topK = 1;

        @JsonProperty("includeMetadata")
        private boolean includeMetadata = true;

        @JsonProperty("includeValues")
        private boolean includeValues = false;

        @JsonProperty("namespace")
        private String namespace;

        @JsonProperty("sparseVector")
        private SparseVector sparseVector;
    }

    @Data
    public static final class SparseVector {
        @JsonProperty("indices")
        private List<Integer> indices;

        @JsonProperty("values")
        private List<Float> values;
    }

    static Value convertToValue(Object value) {
        if (value instanceof Map) {
            Struct.Builder builder = Struct.newBuilder();
            ((Map<String, Object>) value)
                    .forEach((key, val) -> builder.putFields(key, convertToValue(val)));
            return Value.newBuilder().setStructValue(builder.build()).build();
        } else if (value instanceof String) {
            return Value.newBuilder().setStringValue(value.toString()).build();
        } else if (value instanceof Number n) {
            return Value.newBuilder().setNumberValue(n.doubleValue()).build();
        } else if (value instanceof Boolean b) {
            return Value.newBuilder().setBoolValue(b).build();
        } else if (value instanceof List list) {
            ListValue.Builder listValue = ListValue.newBuilder();
            for (Object item : list) {
                listValue.addValues(convertToValue(item));
            }
            return Value.newBuilder().setListValue(listValue).build();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported value of type: "
                            + value.getClass().getName()
                            + " in Pinecone filter");
        }
    }
}
