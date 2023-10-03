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

import static ai.langstream.api.util.ConfigurationUtils.getString;
import static ai.langstream.api.util.ConfigurationUtils.requiredField;
import static ai.langstream.api.util.ConfigurationUtils.requiredNonEmptyField;
import static ai.langstream.api.util.ConfigurationUtils.validateEnumField;
import static ai.langstream.api.util.ConfigurationUtils.validateInteger;

import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ConfigPropertyModel;
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.doc.ResourceConfigurationModel;
import ai.langstream.api.model.Resource;
import ai.langstream.api.runtime.ComputeClusterRuntime;
import ai.langstream.api.runtime.PluginsRegistry;
import ai.langstream.api.runtime.ResourceNodeProvider;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.resources.datasource.AstraDatasourceConfig;
import ai.langstream.impl.resources.datasource.CassandraDatasourceConfig;
import ai.langstream.impl.resources.datasource.JDBCDatasourceConfig;
import ai.langstream.impl.uti.ClassConfigValidator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

public class DataSourceResourceProvider extends BaseDataSourceResourceProvider {

    protected static final String SERVICE_ASTRA = "astra";
    protected static final String SERVICE_CASSANDRA = "cassandra";
    protected static final String SERVICE_JDBC = "jdbc";

    public DataSourceResourceProvider() {
        super("datasource", Map.of(
                SERVICE_ASTRA, AstraDatasourceConfig.CONFIG,
                SERVICE_CASSANDRA, CassandraDatasourceConfig.CONFIG,
                SERVICE_JDBC, JDBCDatasourceConfig.CONFIG
        ));
    }
}
