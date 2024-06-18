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
package ai.langstream.agents.vector.elasticsearch;

import static ai.langstream.agents.vector.InterpolationUtils.buildObjectFromJson;
import static ai.langstream.agents.vector.elasticsearch.ElasticSearchDataSource.ElasticSearchQueryStepDataSource.convertSearchRequest;

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpDeserializer;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class ElasticSearchDataSource implements DataSourceProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "elasticsearch".equals(dataSourceConfig.get("service"));
    }

    @Data
    public static final class ElasticSearchConfig {

        private boolean https = true;
        private String host;
        private int port = 9200;

        @JsonProperty("api-key")
        private String apiKey;
    }

    @Override
    public ElasticSearchQueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {

        ElasticSearchConfig clientConfig =
                MAPPER.convertValue(dataSourceConfig, ElasticSearchConfig.class);

        return new ElasticSearchQueryStepDataSource(clientConfig);
    }

    public static class ElasticSearchQueryStepDataSource implements QueryStepDataSource {

        @Getter private final ElasticSearchConfig clientConfig;
        @Getter private ElasticsearchClient client;

        public ElasticSearchQueryStepDataSource(ElasticSearchConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        @SneakyThrows
        public void initialize(Map<String, Object> config) {
            final String host = clientConfig.getHost();
            final int port = clientConfig.getPort();
            final String apiKey = clientConfig.getApiKey();
            final boolean https = clientConfig.isHttps();
            HttpHost httpHost = new HttpHost(host, port, https ? "https" : "http");
            RestClientBuilder builder = RestClient.builder(httpHost);
            if (apiKey != null) {
                builder.setDefaultHeaders(
                        new org.apache.http.Header[] {
                            new org.apache.http.message.BasicHeader(
                                    "Authorization", "ApiKey " + apiKey)
                        });
            }
            ElasticsearchTransport transport =
                    new RestClientTransport(builder.build(), new JacksonJsonpMapper());
            this.client = new ElasticsearchClient(transport);
            log.info("Connecting to ElasticSearch at {}", httpHost);
        }

        @Override
        @SneakyThrows
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                final SearchRequest searchRequest = convertSearchRequest(query, params);
                final SearchResponse<Map> result = client.search(searchRequest, Map.class);
                return result.hits().hits().stream()
                        .map(
                                h -> {
                                    Map<String, Object> object = new HashMap<>();
                                    object.put("id", h.id());
                                    object.put("similarity", h.score());
                                    object.put("index", h.index());
                                    Map<String, Object> source = h.source();
                                    if (source != null) {
                                        source.forEach(
                                                (key, value) -> {
                                                    if (!key.equals("vector")) {
                                                        object.put(key, value);
                                                    }
                                                });
                                    }
                                    return object;
                                })
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ElasticsearchException e) {
                final String causes =
                        e.error().rootCause().stream()
                                .map(
                                        cause ->
                                                "type: "
                                                        + cause.type()
                                                        + " reason: "
                                                        + cause.reason())
                                .collect(Collectors.joining("\n"));
                String errMessage =
                        "Error executing ElasticSearch query: "
                                + e.getMessage()
                                + "\nRoot causes:\n"
                                + causes
                                + "\nQuery: "
                                + query;
                log.error(errMessage, e);
                throw new RuntimeException(errMessage, e);
            }
        }

        @NotNull
        static SearchRequest convertSearchRequest(String query, List<Object> params) {
            final Map asMap = buildObjectFromJson(query, Map.class, params, OBJECT_MAPPER);
            return parseElasticSearchRequestBodyJson(asMap, SearchRequest._DESERIALIZER);
        }

        @Override
        public void close() {
            if (client != null) {
                try {
                    client._transport().close();
                } catch (Exception e) {
                    log.warn("Error closing ElasticSearch client", e);
                }
            }
        }
    }

    protected static final ObjectMapper OBJECT_MAPPER =
            new ObjectMapper()
                    .configure(SerializationFeature.INDENT_OUTPUT, false)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    protected static final JacksonJsonpMapper JACKSON_JSONP_MAPPER =
            new JacksonJsonpMapper(OBJECT_MAPPER);

    public static <T> T parseElasticSearchRequestBodyJson(
            String json, JsonpDeserializer<T> deserializer) throws IOException {
        return parseElasticSearchRequestBodyJson(
                OBJECT_MAPPER.readValue(json, Map.class), deserializer);
    }

    public static <T> T parseElasticSearchRequestBodyJson(
            Map asMap, JsonpDeserializer<T> deserializer) {
        return JsonData.of(asMap, JACKSON_JSONP_MAPPER).deserialize(deserializer);
    }
}
