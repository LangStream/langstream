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
        name = "OpenSearch",
        description = "Connect to OpenSearch service or AWS OpenSearch Service/Serverless.")
public class OpenSearchDatasourceConfig extends BaseDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {

                @Override
                public Class getResourceConfigModelClass() {
                    return OpenSearchDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(
                            resource,
                            OpenSearchDatasourceConfig.class,
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
            description =
                    """
                    Region parameter for connecting to AWS OpenSearch Service/Serveless.
                    """)
    private String region;

    @ConfigProperty(
            description =
                    """
                    Basic authentication for connecting to OpenSearch.
                    In case of AWS OpenSearch Service/Serverless, this is the access key.
                    """)
    private String username;

    @ConfigProperty(
            description =
                    """
                    Basic authentication for connecting to OpenSearch.
                    In case of AWS OpenSearch Service/Serverless, this is the secret key.
                    """)
    private String password;

    @ConfigProperty(
            description =
                    """
                    Name of the index to to connect to.
                    """,
            required = true)
    @JsonProperty("index-name")
    private String indexName;
}
