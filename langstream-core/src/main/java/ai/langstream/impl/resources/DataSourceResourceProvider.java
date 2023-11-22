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
package ai.langstream.impl.resources;

import ai.langstream.impl.resources.datasource.AstraDatasourceConfig;
import ai.langstream.impl.resources.datasource.AstraVectorDBDatasourceConfig;
import ai.langstream.impl.resources.datasource.CassandraDatasourceConfig;
import ai.langstream.impl.resources.datasource.JDBCDatasourceConfig;
import ai.langstream.impl.resources.datasource.OpenSearchDatasourceConfig;
import java.util.Map;

public class DataSourceResourceProvider extends BaseDataSourceResourceProvider {

    protected static final String SERVICE_ASTRA = "astra";

    protected static final String SERVICE_ASTRA_VECTOR_DB = "astra-vector-db";
    protected static final String SERVICE_CASSANDRA = "cassandra";
    protected static final String SERVICE_JDBC = "jdbc";
    protected static final String SERVICE_OPENSEARCH = "opensearch";

    public DataSourceResourceProvider() {
        super(
                "datasource",
                Map.of(
                        SERVICE_ASTRA, AstraDatasourceConfig.CONFIG,
                        SERVICE_CASSANDRA, CassandraDatasourceConfig.CONFIG,
                        SERVICE_JDBC, JDBCDatasourceConfig.CONFIG,
                        SERVICE_OPENSEARCH, OpenSearchDatasourceConfig.CONFIG,
                        SERVICE_ASTRA_VECTOR_DB, AstraVectorDBDatasourceConfig.CONFIG));
    }
}
