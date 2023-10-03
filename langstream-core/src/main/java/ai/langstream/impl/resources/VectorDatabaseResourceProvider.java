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

import static ai.langstream.api.util.ConfigurationUtils.requiredField;
import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;
import static ai.langstream.api.util.ConfigurationUtils.validateEnumField;

import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.resources.datasource.AstraDatasourceConfig;
import ai.langstream.impl.resources.datasource.CassandraDatasourceConfig;
import ai.langstream.impl.resources.datasource.JDBCDatasourceConfig;
import ai.langstream.impl.resources.datasource.MilvusDatasourceConfig;
import ai.langstream.impl.resources.datasource.PineconeDatasourceConfig;
import java.util.Map;
import java.util.Set;

public class VectorDatabaseResourceProvider extends BaseDataSourceResourceProvider {

    public VectorDatabaseResourceProvider() {
        super("vector-database", Map.of(
                "astra", AstraDatasourceConfig.CONFIG,
                "cassandra", CassandraDatasourceConfig.CONFIG,
                "pinecone", PineconeDatasourceConfig.CONFIG,
                "milvus", MilvusDatasourceConfig.CONFIG
        ));
    }
}
