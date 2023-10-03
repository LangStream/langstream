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

import static ai.langstream.agents.vector.InterpolationUtils.buildObjectFromJson;

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.highlevel.dml.SearchSimpleParam;
import io.milvus.param.highlevel.dml.response.SearchResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.Getter;
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

        // Milvus OSS

        @JsonProperty(value = "user")
        private String user = "default";

        @JsonProperty(value = "password")
        private String password;

        @JsonProperty(value = "host")
        private String host;

        @JsonProperty(value = "port")
        private int port = 19530;

        // Zillis service

        @JsonProperty(value = "url")
        private String url;

        @JsonProperty(value = "token")
        private String token;
    }

    @Override
    public MilvusQueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {

        MilvusConfig clientConfig = MAPPER.convertValue(dataSourceConfig, MilvusConfig.class);

        return new MilvusQueryStepDataSource(clientConfig);
    }

    public static class MilvusQueryStepDataSource implements QueryStepDataSource {

        private final MilvusConfig clientConfig;
        @Getter private MilvusServiceClient milvusClient;

        public MilvusQueryStepDataSource(MilvusConfig clientConfig) {
            this.clientConfig = clientConfig;
        }

        @Override
        public void initialize(Map<String, Object> config) {
            if (clientConfig.url != null && !clientConfig.url.isEmpty()) {
                log.info("Connecting to Milvus service at {}", clientConfig.url);
                this.milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withUri(clientConfig.url)
                                        .withToken(clientConfig.token)
                                        .build());
            } else {
                log.info("Connecting to Milvus at {}:{}", clientConfig.host, clientConfig.port);
                this.milvusClient =
                        new MilvusServiceClient(
                                ConnectParam.newBuilder()
                                        .withHost(clientConfig.host)
                                        .withPort(clientConfig.port)
                                        .withAuthorization(clientConfig.user, clientConfig.password)
                                        .build());
            }
        }

        @Override
        public List<Map<String, Object>> fetchData(String query, List<Object> params) {
            try {
                SearchSimpleParam searchParam =
                        buildObjectFromJson(
                                        query,
                                        SearchSimpleParam.Builder.class,
                                        params,
                                        MilvusModel.getMapper())
                                .build();
                if (log.isDebugEnabled()) {
                    log.debug("Command {}", searchParam);
                }
                R<SearchResponse> respSearch = milvusClient.search(searchParam);
                if (log.isDebugEnabled()) {
                    log.debug("Response {}", respSearch);
                }
                MilvusModel.handleException(respSearch);

                SearchResponse data = respSearch.getData();

                data.getRowRecords()
                        .forEach(
                                r -> {
                                    log.info("Record {}", r);
                                });

                return data.getRowRecords().stream()
                        .map(
                                r -> {
                                    Map<String, Object> result = new HashMap<>();
                                    r.getFieldValues()
                                            .forEach(
                                                    (k, v) -> {
                                                        result.put(k, v);
                                                    });
                                    return result;
                                })
                        .toList();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            if (milvusClient != null) {
                milvusClient.close();
            }
        }
    }
}
