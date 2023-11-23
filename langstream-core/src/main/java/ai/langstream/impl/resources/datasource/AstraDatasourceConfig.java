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
import java.util.Base64;
import java.util.Map;
import lombok.Data;

@Data
@ResourceConfig(name = "Astra", description = "Connect to DataStax Astra Database service.")
public class AstraDatasourceConfig extends BaseDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {

                @Override
                public Class getResourceConfigModelClass() {
                    return AstraDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(
                            resource, AstraDatasourceConfig.class, resource.configuration(), false);
                    Map<String, Object> configuration = resource.configuration();

                    String secureBundle =
                            ConfigurationUtils.getString("secureBundle", "", configuration);
                    if (secureBundle.isEmpty()) {
                        if (configuration.get("token") == null
                                || (configuration.get("database") == null
                                        && configuration.get("database-id") == null)) {
                            throw new IllegalArgumentException(
                                    ClassConfigValidator.formatErrString(
                                            new ClassConfigValidator.ResourceEntityRef(resource),
                                            "token",
                                            "token and database are required for Astra service if secureBundle is not"
                                                    + " configured."));
                        }
                    } else {
                        if (secureBundle.startsWith("base64:")) {
                            secureBundle = secureBundle.substring("base64:".length());
                        }
                        try {
                            Base64.getDecoder().decode(secureBundle);
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                    ClassConfigValidator.formatErrString(
                                            new ClassConfigValidator.ResourceEntityRef(resource),
                                            "secureBundle",
                                            "secureBundle must be a valid base64 string."));
                        }
                    }
                }
            };

    @ConfigProperty(
            description =
                    """
                            Secure bundle of the database. Must be encoded in base64.
                            """)
    private String secureBundle;

    @ConfigProperty(
            description =
                    """
                            Astra Token (AstraCS:xxx) for connecting to the database. If secureBundle is provided, this field is ignored.
                            """)
    private String token;

    @ConfigProperty(
            description =
                    """
                            Astra Database name to connect to. If secureBundle is provided, this field is ignored.
                            """)
    private String database;

    @ConfigProperty(
            description =
                    """
                            Astra Database ID name to connect to. If secureBundle is provided, this field is ignored.
                            """)
    @JsonProperty("database-id")
    private String databaseId;

    @ConfigProperty(
            description =
                    """
                            Astra Token clientId to use.
                            """,
            required = true)
    private String clientId;

    @ConfigProperty(
            description =
                    """
                            Astra Token secret to use.
                            """,
            required = true)
    private String secret;

    @ConfigProperty(
            description =
                    """
                            DEPRECATED: use clientId instead.
                            """)
    private String username;

    @ConfigProperty(
            description =
                    """
                            DEPRECATED: use secret instead.
                            """)
    private String password;

    public enum Environments {
        DEV,
        PROD,
        TEST;
    }

    @ConfigProperty(
            description =
                    """
                            Astra environment.
                            """,
            defaultValue = "PROD")
    private Environments environment;
}
