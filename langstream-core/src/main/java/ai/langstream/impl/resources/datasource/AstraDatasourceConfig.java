package ai.langstream.impl.resources.datasource;

import ai.langstream.api.doc.ConfigProperty;
import ai.langstream.api.doc.ResourceConfig;
import ai.langstream.api.model.Resource;
import ai.langstream.api.util.ConfigurationUtils;
import ai.langstream.impl.resources.BaseDataSourceResourceProvider;
import ai.langstream.impl.resources.DataSourceResourceProvider;
import ai.langstream.impl.uti.ClassConfigValidator;
import java.util.Base64;
import java.util.Map;
import lombok.Data;

@Data
@ResourceConfig(name = "Astra", description = "Connect to DataStax Astra Database service.")
public class AstraDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {

                @Override
                public Class getResourceConfigModelClass() {
                    return AstraDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(resource,
                            AstraDatasourceConfig.class, resource.configuration(), false);
                    Map<String, Object> configuration = resource.configuration();

                    String secureBundle = ConfigurationUtils.getString("secureBundle", "", configuration);
                    if (secureBundle.isEmpty()) {
                        if (configuration.get("token") == null || configuration.get("database") == null) {
                            throw new IllegalArgumentException(
                                    ClassConfigValidator.formatErrString(
                                            new ClassConfigValidator.ResourceEntityRef(resource), "token",
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
                            throw new IllegalArgumentException(ClassConfigValidator.formatErrString(
                                    new ClassConfigValidator.ResourceEntityRef(resource), "secureBundle",
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

    public enum Environments {
        DEV, PROD, TEST;
    }

    @ConfigProperty(
            description =
                    """
                            Astra environment.
                            """,
            defaultValue = "PROD")
    private Environments environment;

}
