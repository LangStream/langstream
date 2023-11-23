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
import ai.langstream.impl.resources.datasource.MilvusDatasourceConfig;
import ai.langstream.impl.resources.datasource.OpenSearchDatasourceConfig;
import ai.langstream.impl.resources.datasource.PineconeDatasourceConfig;
import ai.langstream.impl.resources.datasource.SolrDatasourceConfig;
import java.util.Map;

public class VectorDatabaseResourceProvider extends BaseDataSourceResourceProvider {

    public VectorDatabaseResourceProvider() {
        super(
                "vector-database",
                Map.of(
                        "astra", AstraDatasourceConfig.CONFIG,
                        "cassandra", CassandraDatasourceConfig.CONFIG,
                        "pinecone", PineconeDatasourceConfig.CONFIG,
                        "milvus", MilvusDatasourceConfig.CONFIG,
                        "solr", SolrDatasourceConfig.CONFIG,
                        "astra-vector-db", AstraVectorDBDatasourceConfig.CONFIG,
                        "opensearch", OpenSearchDatasourceConfig.CONFIG));
    }
}
