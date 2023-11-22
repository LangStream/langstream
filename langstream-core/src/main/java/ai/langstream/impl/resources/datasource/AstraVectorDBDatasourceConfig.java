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
package ai.langstream.impl.resources.datasource;

import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.model.Resource;
import ai.langstream.impl.resources.BaseDataSourceResourceProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import lombok.Data;

@Data
@ResourceConfig(
        name = "Astra Vector DB",
        description = "Connect to DataStax Astra Vector DB service.")
public class AstraVectorDBDatasourceConfig extends BaseDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {

                @Override
                public Class getResourceConfigModelClass() {
                    return AstraVectorDBDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(
                            resource,
                            AstraVectorDBDatasourceConfig.class,
                            resource.configuration(),
                            false);
                }
            };

    @ConfigProperty(
            description =
                    """
                            API Endpoint.
                            """)
    private String endpoint;

    @ConfigProperty(
            description =
                    """
                            Astra Token (AstraCS:xxx) for connecting to the database.
                            """)
    private String token;
}
