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
package ai.langstream.agents.vector.astra;

import ai.langstream.api.util.ConfigurationUtils;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.dtsx.astra.sdk.AstraDB;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraCollectionsDataSource implements QueryStepDataSource {

    AstraDB astraDB;

    @Override
    public void initialize(Map<String, Object> dataSourceConfig) {
        log.info(
                "Initializing CassandraDataSource with config {}",
                ConfigurationUtils.redactSecrets(dataSourceConfig));
        String astraToken = ConfigurationUtils.getString("token", "", dataSourceConfig);
        String astraEndpoint = ConfigurationUtils.getString("endpoint", "", dataSourceConfig);
        this.astraDB = new AstraDB(astraToken, astraEndpoint);
    }

    @Override
    public void close() {}

    @Override
    public List<Map<String, Object>> fetchData(String query, List<Object> params) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Executing query {} with params {} ({})",
                    query,
                    params,
                    params.stream()
                            .map(v -> v == null ? "null" : v.getClass().toString())
                            .collect(Collectors.joining(",")));
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> executeStatement(
            String query, List<String> generatedKeys, List<Object> params) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "Executing statement {} with params {} ({})",
                    query,
                    params,
                    params.stream()
                            .map(v -> v == null ? "null" : v.getClass().toString())
                            .collect(Collectors.joining(",")));
        }
        throw new UnsupportedOperationException();
    }

    public AstraDB getAstraDB() {
        return astraDB;
    }
}
