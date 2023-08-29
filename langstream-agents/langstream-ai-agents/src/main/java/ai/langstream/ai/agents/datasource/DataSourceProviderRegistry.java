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
package ai.langstream.ai.agents.datasource;

import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.util.Map;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

/** This is the API to load a CodeStorage implementation. */
@Slf4j
public class DataSourceProviderRegistry {

    public static QueryStepDataSource getQueryStepDataSource(Map<String, Object> dataSourceConfig) {
        if (dataSourceConfig == null || dataSourceConfig.isEmpty()) {
            return null;
        }
        log.info("Loading DataSource implementation for {}", dataSourceConfig);

        ServiceLoader<DataSourceProvider> loader = ServiceLoader.load(DataSourceProvider.class);
        ServiceLoader.Provider<DataSourceProvider> provider =
                loader.stream()
                        .filter(p -> p.get().supports(dataSourceConfig))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "No DataSource found for resource "
                                                        + dataSourceConfig));

        return provider.get().createDataSourceImplementation(dataSourceConfig);
    }
}
