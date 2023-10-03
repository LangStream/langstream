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
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.resources.BaseDataSourceResourceProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@ResourceConfig(name = "Milvus", description = "Connect to Milvus/Zillis service.")
public class MilvusDatasourceConfig extends BaseDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {

                @Override
                public Class getResourceConfigModelClass() {
                    return MilvusDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(
                            resource,
                            MilvusDatasourceConfig.class,
                            resource.configuration(),
                            false);
                    ConfigurationUtils.validateInteger(
                            resource.configuration(),
                            "port",
                            1,
                            300000,
                            () -> new ClassConfigValidator.ResourceEntityRef(resource).ref());
                }
            };

    @ConfigProperty(
            description =
                    """
                            User parameter for connecting to Milvus.
                                    """,
            defaultValue = "default")
    private String user;

    @ConfigProperty(
            description =
                    """
                            Host parameter for connecting to Milvus.
                                    """)
    private String host;

    @ConfigProperty(
            description =
                    """
                            Password parameter for connecting to Milvus.
                                    """)
    private String password;

    @ConfigProperty(
            description =
                    """
                            Port parameter for connecting to Milvus.
                                    """,
            defaultValue = "19530")
    private int port;

    @ConfigProperty(
            description =
                    """
                            Url parameter for connecting to Zillis service.
                                    """)
    @JsonProperty("index-name")
    private String url;

    @ConfigProperty(
            description =
                    """
                            Token parameter for connecting to Zillis service.
                                    """)
    private String token;
}
