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
package ai.langstream.agents.vector.solr;

import ai.langstream.agents.vector.InterpolationUtils;
import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;

@Slf4j
public class SolrDataSource implements DataSourceProvider {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "solr".equals(dataSourceConfig.get("service"));
    }

    @Data
    public static final class SolrConfig {

        @JsonProperty(value = "user")
        private String user;

        @JsonProperty(value = "password")
        private String password;

        @JsonProperty(value = "host")
        private String host = "localhost";

        @JsonProperty(value = "port")
        private int port = 8983;

        @JsonProperty(value = "protocol")
        private String protocol = "http";

        @JsonProperty(value = "collection-name")
        private String collectionName = "documents";
    }

    @Override
    public SolrQueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {

        SolrConfig clientConfig = MAPPER.convertValue(dataSourceConfig, SolrConfig.class);

        return new SolrQueryStepDataSource(clientConfig);
    }

    public static class SolrQueryStepDataSource implements QueryStepDataSource {

        @Getter private final SolrConfig clientConfig;
        @Getter private Http2SolrClient client;
        @Getter private String collectionUrl;
        @Getter private String baseUrl;

        public SolrQueryStepDataSource(SolrConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) {
            baseUrl = clientConfig.protocol + "://" + clientConfig.host + ":" + clientConfig.port;
            collectionUrl = baseUrl + "/solr/" + clientConfig.collectionName;
            Http2SolrClient.Builder builder = new Http2SolrClient.Builder(collectionUrl);
            if (clientConfig.user != null && !clientConfig.user.isEmpty()) {
                builder.withBasicAuthCredentials(clientConfig.user, clientConfig.password);
            }
            client = builder.build();
            log.info("Connecting to Solr at {}", collectionUrl);
        }

        @Override
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                Map<String, Object> queryMap =
                        InterpolationUtils.buildObjectFromJson(query, Map.class, params);
                if (log.isDebugEnabled()) {
                    log.debug("Executing query {}", queryMap);
                }
                SolrQuery solrQuery = new SolrQuery();
                queryMap.forEach((k, v) -> solrQuery.set(k, v.toString()));
                // this is a workaround to handle the fact that the embeddings are
                // an huge array of floats and putting them in the GET query string
                // makes the requests fail on the server side (request header too large)
                QueryResponse response =
                        new QueryRequest(solrQuery, SolrRequest.METHOD.POST).process(client, null);
                if (log.isDebugEnabled()) {
                    log.debug("response: numFound {}", response.getResults().getNumFound());
                    log.debug(
                            "response: numFoundExact {}", response.getResults().getNumFoundExact());
                    log.debug("response: maxScore {}", response.getResults().getMaxScore());
                    log.debug("response: explainMap {}", response.getExplainMap());
                    log.debug("response: size {}", response.getResults().size());
                }

                return response.getResults().stream()
                        .map(
                                doc -> {
                                    Map<String, Object> result = new HashMap<>();
                                    doc.getFieldNames()
                                            .forEach(
                                                    name ->
                                                            result.put(
                                                                    name, doc.getFieldValue(name)));
                                    if (log.isDebugEnabled()) {
                                        log.debug("Result row: {}", result);
                                    }
                                    return result;
                                })
                        .toList();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            if (client != null) {
                client.close();
            }
        }

        public String getRESTCollectionUrl() {
            return getBaseUrl()
                    + "/api/collections/"
                    + URLEncoder.encode(getCollectionName(), StandardCharsets.UTF_8);
        }

        public String getCollectionName() {
            return getClientConfig().getCollectionName();
        }
    }
}
