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
package ai.langstream.ai.agents.datasource.impl;

import ai.langstream.ai.agents.datasource.DataSourceProvider;
import com.datastax.oss.streaming.ai.datasource.AstraDBDataSource;
import com.datastax.oss.streaming.ai.datasource.QueryStepDataSource;
import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;

import java.util.Map;

public class AstraDataSource implements DataSourceProvider {

    @Override
    public boolean supports(Map<String, Object> dataSourceConfig) {
        return "astra".equals(dataSourceConfig.get("service"));
    }

    @Override
    public QueryStepDataSource createImplementation(Map<String, Object> dataSourceConfig) {
        AstraDBDataSource result =  new AstraDBDataSource();
        DataSourceConfig implConfig = new DataSourceConfig();
        implConfig.setPassword((String) dataSourceConfig.get("password"));
        implConfig.setService("astra");
        implConfig.setUsername((String) dataSourceConfig.get("username"));
        implConfig.setSecureBundle((String) dataSourceConfig.get("secureBundle"));
        result.initialize(implConfig);
        return result;
    }
}
