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

import static ai.langstream.agents.vector.InterpolationUtils.buildObjectFromJson;

import ai.langstream.ai.agents.datasource.DataSourceProvider;
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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;

@Slf4j
public class OpenSearchDataSource implements DataSourceProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "opensearch".equals(dataSourceConfig.get("service"));
    }

    @Data
    public static final class OpenSearchConfig {

        private boolean https = true;
        private String host;
        private int port = 9200;
        private String region;
        // BASIC AUTH
        private String username;
        private String password;

        @JsonProperty("index-name")
        private String indexName;
    }

    @Override
    public OpenSearchQueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {

        OpenSearchConfig clientConfig =
                MAPPER.convertValue(dataSourceConfig, OpenSearchConfig.class);

        return new OpenSearchQueryStepDataSource(clientConfig);
    }

    public static class OpenSearchQueryStepDataSource implements QueryStepDataSource {

        @Getter private final OpenSearchConfig clientConfig;
        @Getter private OpenSearchClient client;

        public OpenSearchQueryStepDataSource(OpenSearchConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        @SneakyThrows
        public void initialize(Map<String, Object> config) {
            SdkHttpClient httpClient = new DefaultSdkHttpClientBuilder().build();

            final String host = clientConfig.getHost();
            if (host == null) {
                throw new IllegalArgumentException("Missing host");
            }
            boolean useAwsSdk = host.endsWith("amazonaws.com");
            final OpenSearchTransport transport;

            if (useAwsSdk) {
                final AwsBasicCredentials credentials =
                        AwsBasicCredentials.create(
                                clientConfig.getUsername(), clientConfig.getPassword());
                transport =
                        new AwsSdk2Transport(
                                httpClient,
                                host.replace("https://", ""),
                                "aoss",
                                Region.of(clientConfig.getRegion()),
                                AwsSdk2TransportOptions.builder()
                                        .setCredentials(
                                                StaticCredentialsProvider.create(credentials))
                                        .build());
            } else {
                final HttpHost httpHost =
                        new HttpHost(
                                clientConfig.isHttps() ? "https" : "http",
                                host,
                                clientConfig.getPort());
                final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(
                        new AuthScope(httpHost),
                        new UsernamePasswordCredentials(
                                clientConfig.getUsername(),
                                clientConfig.getPassword().toCharArray()));
                transport =
                        ApacheHttpClient5TransportBuilder.builder(httpHost)
                                .setHttpClientConfigCallback(
                                        httpClientBuilder ->
                                                httpClientBuilder.setDefaultCredentialsProvider(
                                                        credentialsProvider))
                                .build();
            }

            this.client = new OpenSearchClient(transport);
            log.info("Connecting to OpenSearch at {}", host);
        }

        @Override
        @SneakyThrows
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                final SearchRequest searchRequest =
                        convertSearchRequest(query, params, clientConfig.getIndexName());

                final SearchResponse<Map> result = client.search(searchRequest, Map.class);
                return result.hits().hits().stream()
                        .map(
                                h -> {
                                    Map<String, Object> object = new HashMap<>();
                                    object.put("id", h.id());
                                    object.put("document", h.source());
                                    object.put("score", h.score());
                                    object.put("index", h.index());
                                    return object;
                                })
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (OpenSearchException e) {
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
                        "Error executing OpenSearch query: "
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
        static SearchRequest convertSearchRequest(
                String query, List<Object> params, String indexName) throws IllegalAccessException {
            final Map asMap = buildObjectFromJson(query, Map.class, params, OBJECT_MAPPER);
            final SearchRequest searchRequest =
                    OpenSearchDataSource.parseOpenSearchRequestBodyJson(
                            asMap, SearchRequest._DESERIALIZER);
            FieldUtils.writeField(searchRequest, "index", List.of(indexName), true);
            return searchRequest;
        }

        @Override
        public void close() {
            if (client != null) {
                try {
                    client._transport().close();
                } catch (Exception e) {
                    log.warn("Error closing OpenSearch client", e);
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

    public static <T> T parseOpenSearchRequestBodyJson(
            String json, JsonpDeserializer<T> deserializer) throws IOException {
        return parseOpenSearchRequestBodyJson(
                OBJECT_MAPPER.readValue(json, Map.class), deserializer);
    }

    public static <T> T parseOpenSearchRequestBodyJson(
            Map asMap, JsonpDeserializer<T> deserializer) {
        return JsonData.of(asMap, JACKSON_JSONP_MAPPER).deserialize(deserializer);
    }
}
