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
package com.datastax.oss.streaming.ai.datasource;

import com.datastax.oss.streaming.ai.model.config.DataSourceConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Slf4j
public class AstraDBDataSourceTest {
    @Test
    @Disabled
    void testQueryWithParameters() throws Exception {
        AstraDBDataSource source = new AstraDBDataSource();
        DataSourceConfig dataSourceConfig = buildDataSourceConfig();
        source.initialize(dataSourceConfig);

        String query = "select * from vsearch.products where id=?";
        List<Object> params = new ArrayList<>();
        params.add(1);
        List<Map<String, String>> maps = source.fetchData(query, params);
        log.info("maps {}", maps);
    }

    @Disabled
    void testQueryWithVectorSearch() throws Exception {
        AstraDBDataSource source = new AstraDBDataSource();
        DataSourceConfig dataSourceConfig = buildDataSourceConfig();
        source.initialize(dataSourceConfig);

        String query = "SELECT * FROM vsearch.products ORDER BY item_vector ANN OF ? LIMIT 1;";
        List<Object> params = new ArrayList<>();
        params.add(Arrays.asList(0.1, 0.2, 0.3, 0.4, 0.5));
        List<Map<String, String>> maps = source.fetchData(query, params);
        log.info("maps {}", maps);
    }

    private static DataSourceConfig buildDataSourceConfig() {
        DataSourceConfig dataSourceConfig = new DataSourceConfig();
        dataSourceConfig.setService("astra");
        dataSourceConfig.setUsername("set-your-client-id");
        dataSourceConfig.setPassword("set-your-secret");
        dataSourceConfig.setSecureBundle("xxx-set-base64-encoded-bundle-xxx");
        return dataSourceConfig;
    }
}
