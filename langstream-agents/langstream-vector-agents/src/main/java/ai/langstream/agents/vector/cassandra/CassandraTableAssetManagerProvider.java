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
package ai.langstream.agents.vector.cassandra;

import ai.langstream.api.model.AssetDefinition;
import ai.langstream.api.runner.assets.AssetManager;
import ai.langstream.api.runner.assets.AssetManagerProvider;
import com.datastax.oss.streaming.ai.datasource.AstraDBDataSource;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CassandraTableAssetManagerProvider implements AssetManagerProvider {

    @Override
    public boolean supports(String assetType) {
        return "cassandra-table".equals(assetType);
    }

    @Override
    public AssetManager createInstance(String assetType) {
        return new CassandraTableAssetManager();
    }

    private static class CassandraTableAssetManager implements AssetManager {

        private static final ObjectMapper MAPPER =
                new ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        @Override
        public boolean assetExists(AssetDefinition assetDefinition) throws Exception {
            String tableName = (String) assetDefinition.getConfig().get("table-name");
            try (AstraDBDataSource datasource = buildDataSource(assetDefinition); ) {
                String cql = "DESCRIBE TABLE ?";
                log.info("Executing: {} with params: {}", cql, tableName);
                List<Map<String, String>> results = datasource.fetchData(cql, List.of(tableName));
                log.info("Results: {}", results);
                return false;
            }
        }

        private static AstraDBDataSource buildDataSource(AssetDefinition assetDefinition) {
            AstraDBDataSource dataSource = new AstraDBDataSource();
            Map<String, Object> datasourceConfiguration =
                    (Map<String, Object>) assetDefinition.getConfig().get("datasource");
            dataSource.initialize(datasourceConfiguration);
            return dataSource;
        }

        @Override
        public void deployAsset(AssetDefinition assetDefinition) throws Exception {
            try (AstraDBDataSource datasource = buildDataSource(assetDefinition); ) {
                List<String> statements =
                        (List<String>) assetDefinition.getConfig().get("statements");
                for (String statement : statements) {
                    log.info("Executing: {}", statement);
                    datasource.executeStatement(statement, List.of());
                }
            }
        }
    }
}
