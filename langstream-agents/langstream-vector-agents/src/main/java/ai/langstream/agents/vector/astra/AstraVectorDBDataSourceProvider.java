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

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AstraVectorDBDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        String service = (String) dataSourceConfig.get("service");
        return "astra-vector-db".equals(service);
    }

    @Override
    public QueryStepDataSource createDataSourceImplementation(
            Map<String, Object> dataSourceConfig) {
        return new AstraVectorDBDataSource();
    }
}
