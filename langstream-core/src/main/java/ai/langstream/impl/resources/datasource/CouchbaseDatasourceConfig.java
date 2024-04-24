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
@ResourceConfig(name = "Couchbase", description = "Connect to Couchbase service.")
public class CouchbaseDatasourceConfig extends BaseDatasourceConfig {

    public static final BaseDataSourceResourceProvider.DatasourceConfig CONFIG =
            new BaseDataSourceResourceProvider.DatasourceConfig() {

                @Override
                public Class getResourceConfigModelClass() {
                    return CouchbaseDatasourceConfig.class;
                }

                @Override
                public void validate(Resource resource) {
                    ClassConfigValidator.validateResourceModelFromClass(
                            resource,
                            CouchbaseDatasourceConfig.class,
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
                            Username parameter for connecting to Couchbase.
                                    """,
            defaultValue = "")
    private String user;

    @ConfigProperty(
            description =
                    """
                            Password parameter for connecting to Couchbase.
                                    """)
    private String password;

    @ConfigProperty(
            description =
                    """
                            Host parameter for connecting to Couchbase.
                                    """)
    private String host;

    @ConfigProperty(
            description =
                    """
                            Port parameter for connecting to Couchbase.
                                    """,
            defaultValue = "8091")
    private int port;

    @ConfigProperty(
            description =
                    """
                            Bucket to use in Couchbase.
                                    """)
    @JsonProperty("bucket-name")
    private String bucketName;
}
