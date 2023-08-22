/**
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

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.pinecone.PineconeClient;
import io.pinecone.PineconeClientConfig;
import io.pinecone.PineconeConnection;
import io.pinecone.PineconeConnectionConfig;
import io.pinecone.PineconeException;
import io.pinecone.proto.QueryRequest;
import io.pinecone.proto.QueryResponse;
import io.pinecone.proto.QueryVector;
import io.pinecone.proto.SparseValues;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
public class PineconeDataSource implements DataSourceProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
    public QueryStepDataSource createImplementation(Map<String, Object> dataSourceConfig) {

        PineconeConfig clientConfig = MAPPER.convertValue(dataSourceConfig, PineconeConfig.class);

        return new PinecodeQueryStepDataSource(clientConfig);
    }

    private static class PinecodeQueryStepDataSource implements QueryStepDataSource {


        private final PineconeConfig clientConfig;
        private PineconeConnection connection;

        public PinecodeQueryStepDataSource(PineconeConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(DataSourceConfig config) {
            PineconeClientConfig pineconeClientConfig = new PineconeClientConfig()
                    .withApiKey(clientConfig.getApiKey())
                    .withEnvironment(clientConfig.getEnvironment())
                    .withProjectName(clientConfig.getProjectName())
                    .withServerSideTimeoutSec(clientConfig.getServerSideTimeoutSec());
            PineconeClient pineconeClient = new PineconeClient(pineconeClientConfig);
            PineconeConnectionConfig connectionConfig = new PineconeConnectionConfig()
                    .withIndexName(clientConfig.getIndexName());
            if (clientConfig.getEndpoint() == null) {
                connection = pineconeClient.connect(connectionConfig);
            }
        }

        @Override
        public List<Map<String, String>> fetchData(String query, List<Object> params) {
            try {
                Query parsedQuery;
                try {
                    // interpolate the query
                    query = interpolate(query, params);

                    parsedQuery = MAPPER.readValue(query, Query.class);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                QueryVector.Builder builder = QueryVector.newBuilder();

                if (parsedQuery.vector != null) {
                    builder.addAllValues(parsedQuery.vector);
                }

                if (parsedQuery.sparseVector != null) {
                    builder.setSparseValues(SparseValues.newBuilder()
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

                QueryRequest batchQueryRequest = requestBuilder
                        .addQueries(queryVector)
                        .setTopK(parsedQuery.topK)
                        .setIncludeMetadata(parsedQuery.includeMetadata)
                        .setIncludeValues(parsedQuery.includeValues)
                        .build();

                List<Map<String, String>> results;

                if (clientConfig.getEndpoint() == null) {

                    QueryResponse queryResponse = connection
                            .getBlockingStub()
                            .query(batchQueryRequest);

                    if (log.isDebugEnabled()) {
                        log.debug("Query response: {}", queryResponse);
                    }

                    results = new ArrayList<>();
                    queryResponse.getResultsList()
                            .forEach(res -> {
                                res.getMatchesList().forEach(match -> {
                                    String id = match.getId();
                                    Map<String, String> row = new HashMap<>();

                                    if (parsedQuery.includeMetadata) {
                                        // put all the metadata
                                        if (match.getMetadata() != null) {
                                            match.getMetadata().getFieldsMap().forEach((key, value) -> {
                                                if (log.isDebugEnabled()) {
                                                    log.debug("Key: {}, value: {} {}", key, value, value != null ? value.getClass() : null);
                                                }
                                                Object converted = valueToObject(value);
                                                row.put(key, converted != null ? converted.toString() : null);
                                            });
                                        }
                                    }
                                    row.put("id", id);
                                    results.add(row);
                                });
                            });
                } else {
                    HttpClient client = HttpClient.newHttpClient();
                    HttpRequest request = HttpRequest
                            .newBuilder(URI.create(clientConfig.getEndpoint()))
                            .POST(HttpRequest.BodyPublishers
                                    .ofString(batchQueryRequest.toString()))
                            .build();
                    String body = client.send(request, HttpResponse.BodyHandlers.ofString())
                            .body();
                    log.info("Mock result {}", body);
                    results = MAPPER.readValue(body, new TypeReference<List<Map<String, String>>>() {
                    });
                }
                return results;
            } catch (IOException | StatusRuntimeException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        static String interpolate(String query, List<Object> array) {
            if (query == null || !query.contains("?")) {
                return query;
            }
            for (Object value :array) {
                int questionMark = query.indexOf("?");
                if (questionMark < 0) {
                    return query;
                }
                Object valueAsString = convertValueToJson(value);
                query = query.substring(0, questionMark)
                        + valueAsString
                        + query.substring(questionMark + 1);
            }

            return query;
        }

        @SneakyThrows
        private static String convertValueToJson(Object value) {
            return MAPPER.writeValueAsString(value);
        }

        public static Object valueToObject(Value value) {
            switch (value.getKindCase()) {
                case NULL_VALUE:
                    return null;
                case NUMBER_VALUE:
                    return value.getNumberValue();
                case STRING_VALUE:
                    return value.getStringValue();
                case BOOL_VALUE:
                    return value.getBoolValue();
                case LIST_VALUE:
                    return value.getListValue().getValuesList().stream()
                            .map(v-> valueToObject(v))
                            .toList();
                case STRUCT_VALUE:
                    return value.getStructValue().getFieldsMap().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> valueToObject(e.getValue())));
                default:
                    return null;
            }
        }

        private static Value convertToValue(Object value) {
            if (value instanceof Map) {
                Struct.Builder builder = Struct.newBuilder();
                ((Map<String, Object>) value).forEach((key, val) -> {
                    builder.putFields(key, convertToValue(val));
                });
                return Value.newBuilder().setStructValue(builder.build()).build();
            } else if (value instanceof String){
                return Value.newBuilder()
                        .setStringValue(value.toString())
                        .build();
            } else if (value instanceof Number n){
                return Value.newBuilder()
                        .setNumberValue(n.doubleValue())
                        .build();
            } else if (value instanceof Boolean b) {
                return Value.newBuilder()
                        .setBoolValue(b.booleanValue())
                        .build();
            } else if (value instanceof List list) {
                ListValue.Builder listValue = ListValue.newBuilder();
                for (Object item : list) {
                    listValue.addValues(convertToValue(item));
                }
                return Value.newBuilder()
                        .setListValue(listValue).build();
            } else {
                throw new IllegalArgumentException("Unsupported value of type: " + value.getClass().getName() + " in Pinecone filter");
            }
        }

        private Struct buildFilter(Map<String, Object> filter) {
            Struct.Builder builder = Struct.newBuilder();
            filter.forEach((key, value) -> {
                builder.putFields(key, convertToValue(value));
            });
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
     * "vector": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
     *     "filter": {"genre": {"$in": ["comedy", "documentary", "drama"]}},
     *     "topK": 1,
     *     "includeMetadata": true
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

}
