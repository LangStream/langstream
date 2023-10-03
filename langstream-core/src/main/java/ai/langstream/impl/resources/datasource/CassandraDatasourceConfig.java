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

import static ai.langstream.api.util.ConfigurationUtils.validateInteger;

import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.model.Resource;
import ai.langstream.impl.resources.BaseDataSourceResourceProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Data;

@Data
@ResourceConfig(name = "Cassandra", description = "Connect to Apache cassandra.")
public class CassandraDatasourceConfig extends BaseDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {
                @Override
                public Class getResourceConfigModelClass() {
                    return CassandraDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(
                            resource,
                            CassandraDatasourceConfig.class,
                            resource.configuration(),
                            false);
                    Map<String, Object> configuration = resource.configuration();
                    validateInteger(
                            configuration,
                            "port",
                            1,
                            65535,
                            () -> new ClassConfigValidator.ResourceEntityRef(resource).ref());
                }
            };

    @ConfigProperty(
            description =
                    """
                            Contact points of the cassandra cluster.
                            """,
            required = true)
    @JsonProperty("contact-points")
    private String contactPoints;

    @ConfigProperty(
            description =
                    """
                            Load balancing local datacenter.
                            """,
            required = true)
    @JsonProperty("loadBalancing-localDc")
    private String loadBalancingLocalDc;

    @ConfigProperty(
            description =
                    """
                            Cassandra port.
                            """,
            defaultValue = "9042")
    private int port;

    @ConfigProperty(
            description =
                    """
                            User username.
                            """)
    private String username;

    @ConfigProperty(
            description =
                    """
                            User password.
                            """)
    private String password;
}
