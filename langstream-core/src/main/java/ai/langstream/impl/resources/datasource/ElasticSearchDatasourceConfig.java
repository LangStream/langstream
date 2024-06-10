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
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@ResourceConfig(
        name = "ElasticSearch",
        description = "Connect to Elastic service or ElasticSearch service.")
public class ElasticSearchDatasourceConfig extends BaseDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {

                @Override
                public Class getResourceConfigModelClass() {
                    return ElasticSearchDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(
                            resource,
                            ElasticSearchDatasourceConfig.class,
                            resource.configuration(),
                            false);
                }
            };

    @ConfigProperty(
            description =
                    """
                    Whether to use https or not.
                    """,
            defaultValue = "true")
    private boolean https = true;

    @ConfigProperty(
            description =
                    """
                    Host parameter for connecting to OpenSearch.
                    Valid both for OpenSearch and AWS OpenSearch Service/Serverless.
                    """,
            required = true)
    private String host;

    @ConfigProperty(
            description =
                    """
                    Port parameter for connecting to OpenSearch service.
                    """,
            defaultValue = "9200")
    private int port;

    @ConfigProperty(
            description = """
                    Api Key.
                    """,
            required = false)
    @JsonProperty("api-key")
    private String apiKey;
}
